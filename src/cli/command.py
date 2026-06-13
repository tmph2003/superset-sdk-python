"""
Một lệnh để đồng bộ dbt models/metrics sang Superset.
"""


import json
import logging
import os
import os.path
from pathlib import Path
from typing import Dict, List, Optional, Tuple


import click
import yaml
from yarl import URL

from datetime import datetime
from api.clients.dbt import (
    MFSQLEngine,
    ModelSchema,
)
from api.clients.superset import SupersetClient
from auth.superset import SupersetJWTAuth, UsernamePasswordAuth
from cli.databases import sync_database
from cli.datasets import sync_datasets
from cli.lib import (
    ModelKey,
    apply_select,
    list_failed_models,
    load_profiles,
)
from cli.metricflow import (
    get_sl_metric,
    classify_metrics,
    process_sl_metrics_concurrently,
    merge_sl_metrics_into_models,
)
from cli.metrics import (
    get_superset_metrics_per_model,
)
from exceptions import CLIError, DatabaseNotFoundError
from lib import raise_cli_errors, setup_logging
os.environ["PYTHONIOENCODING"] = "utf-8"
_logger = logging.getLogger(__name__)


@click.command()
@click.argument("instance")
@click.argument("file", type=click.Path(exists=True, resolve_path=True))
@click.option("--jwt-token", default=None, help="JWT token")
@click.option("-u", "--username", default="admin", help="Username")
@click.option(
    "-p",
    "--password",
    prompt=True,
    prompt_required=False,
    default="admin",
    hide_input=True,
    help="Password (leave empty for prompt)",
)
@click.option("--loglevel", default="INFO")
@click.option("--project", help="Name of the dbt project", default=None)
@click.option("--profile", help="Name of the dbt profile", default=None)
@click.option("--target", help="Target name", default=None)
@click.option(
    "--profiles",
    help="Location of profiles.yml file",
    type=click.Path(exists=True, resolve_path=True),
)
@click.option(
    "--disallow-edits",
    is_flag=True,
    default=False,
    help="Mark resources as managed externally to prevent edits",
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.option(
    "--select",
    "-s",
    help="Model selection",
    multiple=True,
)
@click.option(
    "--exclude",
    "-x",
    help="Models to exclude",
    multiple=True,
)
@click.option(
    "--preserve-metadata",
    is_flag=True,
    default=False,
    help="Preserve column and metric configurations defined in Superset",
)
@click.option(
    "--merge-metadata",
    is_flag=True,
    default=False,
    help="Update Superset configurations based on dbt metadata. Superset-only metrics are preserved",
)
@click.option(
    "--metrics",
    "-m",
    help="Comma-separated or multiple metric names to sync (e.g. -m revenue -m profit). If not specified, all metrics are synced.",
    multiple=True,
)
@click.option(
    "--raise-failures",
    is_flag=True,
    default=False,
    help="End the execution with an error if a model fails to sync or a deprecated feature is used",
)
@click.option(
    "--max-workers",
    type=int,
    default=3,
    help="Maximum number of parallel workers for processing semantic layer metrics (default: 5)",
)
@raise_cli_errors
@click.pass_context
def main(  # pylint: disable=too-many-arguments, too-many-branches, too-many-locals ,too-many-statements # noqa: C901
    ctx: click.core.Context,
    instance: str,
    file: str,
    jwt_token: Optional[str],
    username: str,
    password: str,
    loglevel: str,
    project: Optional[str],
    profile: Optional[str],
    target: Optional[str],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    metrics: Tuple[str, ...] = (),
    profiles: Optional[str] = None,

    disallow_edits: bool = False,
    external_url_prefix: str = "",
    preserve_metadata: bool = False,
    merge_metadata: bool = False,
    raise_failures: bool = False,
    max_workers: int = 5,
) -> None:
    """
    Đồng bộ models/metrics từ dbt Core sang Superset.
    """
    setup_logging(loglevel)
    
    start_time = datetime.now()
    url = URL(instance)
    
    if jwt_token:
        auth = SupersetJWTAuth(jwt_token, url)
    else:
        auth = UsernamePasswordAuth(url, username, password)
        
    client = SupersetClient(url, auth)
    deprecation_notice: bool = False

    if metrics:
        metrics_list = []
        for m in metrics:
            metrics_list.extend([x.strip() for x in m.split(",")])
        metrics = tuple(metrics_list)

    if preserve_metadata and merge_metadata:
        error_message = (
            "``--preserve-metadata`` and ``--merge-metadata``\n"
            "can't be combined. Please include only one to the command."
        )
        raise CLIError(error_message, 1)

    reload_columns = not (preserve_metadata or merge_metadata)

    if profiles is None:
        profiles = os.path.expanduser("~/.dbt/profiles.yml")

    file_path = Path(file)

    if ctx.obj is not None and "MANAGER_URL" not in ctx.obj and disallow_edits:
        warn_message = (
            "The managed externally feature was only introduced in Superset v1.5."
            "Make sure you are running a compatible version."
        )
        _logger.debug(warn_message)
    if file_path.name == "manifest.json":
        manifest = file_path
        profile = profile or "default"
    elif file_path.name == "dbt_project.yml":
        deprecation_notice = True
        warn_message = (
            "Passing the dbt_project.yml file is deprecated and "
            "will be removed in a future version. "
            "Please pass the manifest.json file instead."
        )
        _logger.warning(warn_message)
        with open(file_path, encoding="utf-8") as input_:
            dbt_project = yaml.load(input_, Loader=yaml.SafeLoader)

        manifest = file_path.parent / \
            dbt_project["target-path"] / "manifest.json"
        profile = dbt_project["profile"]
        project = project or dbt_project["name"]
    else:
        raise CLIError(
            "FILE should be either ``manifest.json`` or ``dbt_project.yml``",
            1,
        )

    with open(manifest, encoding="utf-8") as input_:
        configs = json.load(input_)

    profiles_config = load_profiles(Path(profiles), project, profile, target)
    dialect = profiles_config[profile]["outputs"][target]["type"]
    database_profile = profiles_config[profile]["outputs"][target]["database"]
    try:
        mf_dialect = MFSQLEngine(dialect.upper())
    except ValueError:
        mf_dialect = None

    model_schema = ModelSchema()
    models = []
    for node_config in configs["nodes"].values():
        if node_config["resource_type"] == "model":
            unique_id = node_config["uniqueId"] = node_config["unique_id"]
            node_config["children"] = configs["child_map"][unique_id]
            node_config["columns"] = list(node_config["columns"].values())
            models.append(model_schema.load(node_config))
    models = apply_select(models, select, exclude)

    # ── Schema rewriting (publish_mart logic) ──────────────────────────
    # Khi Superset target là ClickHouse nhưng dbt dùng Trino,
    # schema trong manifest (vd: 'gold') không khớp với schema trên
    # ClickHouse (vd: 'kinhdoanh'). Giống logic publish_mart macro:
    #   target_schema = model.fqn[2]  khi fqn có >= 4 phần tử
    superset_meta = profiles_config[profile]["outputs"][target].get("meta", {}).get("superset", {})
    db_name = superset_meta.get("database_name", "")
    is_clickhouse = "clickhouse" in db_name.lower()
    if is_clickhouse:
        _logger.info("ClickHouse mode detected (database_name=%s). Rewriting model schemas from fqn.", db_name)
        for model in models:
            old_schema = model["schema"]
            model["_original_schema"] = old_schema
            ch_schema = model.get("config", {}).get("clickhouse_schema")
            fqn = model.get("fqn", [])
            if ch_schema:
                model["schema"] = ch_schema
            elif len(fqn) > 3:
                model["schema"] = fqn[2]
            # ClickHouse không dùng catalog kiểu Trino → xóa để Superset không gửi catalog sai
            model["database"] = None
            if old_schema != model["schema"]:
                _logger.debug("Schema rewrite: %s → %s for model %s", old_schema, model["schema"], model["name"])

    failures: List[str] = []
    superset_metrics: Dict[str, list] = {}
    model_map = {ModelKey(model["schema"], model["name"]): model for model in models}
    # Khi ClickHouse mode, MetricFlow SQL vẫn dùng schema gốc (vd: 'gold'),
    # cần thêm entry với schema cũ để get_models_from_sql có thể match
    if is_clickhouse:
        for model in models:
            original_schema = model.get("_original_schema")
            if original_schema and original_schema != model["schema"]:
                model_map[ModelKey(original_schema, model["name"])] = model

    # Chuẩn hóa các đường dẫn lựa chọn để lọc metrics
    select_paths = []
    if select:
        for selection in select:
            for condition in selection.split(","):
                p = Path(condition).as_posix()
                # Nếu condition là thư mục thì thêm '/', nếu là file thì giữ nguyên
                if p.endswith(".yml") or p.endswith(".sql"):
                    select_paths.append(p)
                else:
                    select_paths.append(p.rstrip("/") + "/")

    og_metrics, sl_metric_configs = classify_metrics(
        configs["metrics"],
        metrics,
        select_paths,
        dialect,
        mf_dialect,
    )

    sl_metrics = process_sl_metrics_concurrently(
        sl_metric_configs,
        model_map,
        mf_dialect,
        configs,
        profiles_config[profile]["outputs"][target],
        max_workers,
    )

    merge_sl_metrics_into_models(
        sl_metrics, models, database_profile,
    )

    superset_metrics = get_superset_metrics_per_model(
        og_metrics, sl_metrics)

    try:
        database = sync_database(
            client,
            Path(profiles),
            project,
            profile,
            target,
            disallow_edits,
            external_url_prefix,
        )
    except DatabaseNotFoundError:
        raise CLIError(
            "No database connection was found in Superset. "
            "Please create the database connection in Superset first.",
            1,
        )

    datasets, failures = sync_datasets(
        client,
        models,
        superset_metrics,
        database,
        disallow_edits,
        external_url_prefix,
        reload_columns=reload_columns,
        merge_metadata=merge_metadata,
        selective_metrics=bool(metrics),
    )

    if failures and raise_failures:
        failed_models = list_failed_models(failures)
        raise CLIError(failed_models, 1)

    if deprecation_notice and raise_failures:
        raise CLIError("Review deprecation warnings", 1)

    duration = datetime.now() - start_time
    metrics_count = sum(len(v) for v in superset_metrics.values())

    _logger.info(
        "\nDbt sync completed in %s\n"
        "Datasets: %d completed, %d failures\n"
        "Metrics: %d",
        duration,
        len(datasets),
        len(failures),
        metrics_count,
    )


if __name__ == "__main__":
    main()
