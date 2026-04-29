"""
A command to sync dbt models/metrics to Superset and charts/dashboards back as exposures.
"""

import json
import logging
import os
import os.path
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import yaml
from yarl import URL

from datetime import datetime
from superset_cli.api.clients.dbt import (
    MFSQLEngine,
    ModelSchema,
)
from superset_cli.api.clients.superset import SupersetClient

from superset_cli.cli.superset.sync.dbt.databases import sync_database
from superset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey, sync_exposures
from superset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    get_og_metric_from_config,
    list_failed_models,
    load_profiles,
)
from superset_cli.cli.superset.sync.dbt.metricflow import get_sl_metric
from superset_cli.cli.superset.sync.dbt.metrics import (
    get_superset_metrics_per_model,
)
from superset_cli.exceptions import CLIError, DatabaseNotFoundError
from superset_cli.lib import raise_cli_errors
os.environ["PYTHONIOENCODING"] = "utf-8"
_logger = logging.getLogger(__name__)


@click.command()
@click.argument("file", type=click.Path(exists=True, resolve_path=True))
@click.option("--project", help="Name of the dbt project", default=None)
@click.option("--profile", help="Name of the dbt profile", default=None)
@click.option("--target", help="Target name", default=None)
@click.option(
    "--profiles",
    help="Location of profiles.yml file",
    type=click.Path(exists=True, resolve_path=True),
)
@click.option(
    "--exposures",
    help="Path to file where exposures will be written",
    type=click.Path(exists=False),
)
@click.option(
    "--import-db",
    is_flag=True,
    default=False,
    help="Import (or update) the database connection to Superset",
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
    "--exposures-only",
    is_flag=True,
    default=False,
    help="Do not sync models to datasets and only fetch exposures instead",
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
@raise_cli_errors
@click.pass_context
def dbt_core(  # pylint: disable=too-many-arguments, too-many-branches, too-many-locals ,too-many-statements # noqa: C901
    ctx: click.core.Context,
    file: str,
    project: Optional[str],
    profile: Optional[str],
    target: Optional[str],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    metrics: Tuple[str, ...] = (),
    profiles: Optional[str] = None,
    exposures: Optional[str] = None,
    import_db: bool = False,
    disallow_edits: bool = False,
    external_url_prefix: str = "",
    exposures_only: bool = False,
    preserve_metadata: bool = False,
    merge_metadata: bool = False,
    raise_failures: bool = False,
) -> None:
    """
    Sync models/metrics from dbt Core to Superset and charts/dashboards to dbt exposures.
    """
    start_time = datetime.now()
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
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

    if "MANAGER_URL" not in ctx.obj and disallow_edits:
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

        manifest = file_path.parent / dbt_project["target-path"] / "manifest.json"
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
    model_map = {ModelKey(model["schema"], model["name"]): model for model in models}

    failures: List[str] = []
    superset_metrics: Dict[str, list] = {}

    if exposures_only:
        datasets = [
            dataset
            for dataset in client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        # Build normalized select paths for filtering metrics
        select_paths = []
        if select:
            for selection in select:
                for condition in selection.split(","):
                    select_paths.append(
                        Path(condition).as_posix().rstrip("/") + "/"
                    )

        og_metrics = [] # original metrics of superset
        sl_metrics = [] # metrics defined in the semantic layer (only for MF dialects)
        for metric_config in configs["metrics"].values():
            if metrics and metric_config["name"] not in metrics:
                continue
            # Filter metrics by --select path
            if select_paths:
                metric_path = Path(
                    metric_config.get("original_file_path", ""),
                ).as_posix()
                if not any(metric_path.startswith(sp) for sp in select_paths):
                    continue
            # dbt is shifting from `metric.meta` to `metric.config.meta`
            metric_config["meta"] = metric_config.get("meta") or metric_config.get("config", {}).get(
                "meta",
                {},
            )
            # Lấy công thức metrics từ superset
            # First validate if metadata is already available
            if metric_config["meta"].get("superset", {}).get("model") and (
                sql := metric_config["meta"].get("superset", {}).pop("expression")
            ):
                metric = get_og_metric_from_config(
                    metric_config,
                    dialect,
                    depends_on=[],
                    sql=sql,
                )
                og_metrics.append(metric)

            # dbt legacy schema
            elif "calculation_method" in metric_config or "sql" in metric_config:
                metric = get_og_metric_from_config(metric_config, dialect)
                og_metrics.append(metric)

            # dbt semantic layer
            # Only validate semantic layer metrics if MF dialect is specified
            elif mf_dialect is not None and (
                sl_metric := get_sl_metric(metric_config, model_map, mf_dialect, configs, profiles_config[profile]["outputs"][target])
            ):
                sl_metrics.append(sl_metric)

        #TODO: Tìm dataset sẽ chứa derived metric & update 
        seen_virtual_models = {}
        virtual_metrics_to_add = []
        for sl_metric in sl_metrics:
            if sl_metric["meta"].get("ref_dataset"):
                _logger.debug(
                    "Metric detected: %s (type=%s)",
                    sl_metric["name"],
                    sl_metric.get("type"),
                )
                model_name = sl_metric["meta"].get("ref_dataset")
                if not model_name:
                    raise CLIError(
                        "Chưa đặt tên virtual dataset cho derived metric cần tạo", 1,
                    )
                virtual_unique_id = f"model.semantic_layer.{model_name}"

                # Chỉ tạo model 1 lần cho mỗi ref_dataset
                if model_name not in seen_virtual_models:
                    # Build column metadata with verbose_name from column_labels
                    column_labels = sl_metric.get("column_labels", {})
                    column_descriptions = sl_metric.get("column_descriptions", {})
                    columns = [
                        {
                            "name": col_name,
                            "description": column_descriptions.get(col_name, ""),
                            "meta": {"superset": {
                                "verbose_name": label,
                            }},
                        }
                        for col_name, label in column_labels.items()
                    ]
                    model = {
                        "name": model_name,
                        "unique_id": virtual_unique_id,
                        "schema": None,       # → virtual dataset (không có physical table)
                        "database": database_profile,
                        "description": "",
                        "meta": {},
                        "columns": columns,
                        "sql": sl_metric.get("model_sql"),
                    }
                    seen_virtual_models[model_name] = model
                    models.append(model)
                    _logger.info(
                        "Created virtual model '%s'",
                        model_name
                    )
                else:
                    existing_model = seen_virtual_models[model_name]
                    if not existing_model.get("sql") and sl_metric.get("model_sql"):
                        existing_model["sql"] = sl_metric.get("model_sql")
                    
                    existing_cols = {c["name"]: c for c in existing_model["columns"]}
                    for col_name, label in sl_metric.get("column_labels", {}).items():
                        if col_name not in existing_cols:
                            new_col = {
                                "name": col_name,
                                "description": sl_metric.get("column_descriptions", {}).get(col_name, ""),
                                "meta": {"superset": {"verbose_name": label}},
                            }
                            existing_model["columns"].append(new_col)
                            existing_cols[col_name] = new_col

                import copy
                if sl_metric.get("model"):
                    virtual_metric = copy.deepcopy(sl_metric)
                    virtual_metric["model"] = virtual_unique_id
                    virtual_metrics_to_add.append(virtual_metric)
                    
                    # Original metric will stay with the physical model, clear rename map
                    sl_metric["column_rename_map"] = {}
                else:
                    sl_metric["model"] = virtual_unique_id

        sl_metrics.extend(virtual_metrics_to_add)

        superset_metrics = get_superset_metrics_per_model(og_metrics, sl_metrics)

        try:
            database = sync_database(
                client,
                Path(profiles),
                project,
                profile,
                target,
                import_db,
                disallow_edits,
                external_url_prefix,
            )
        except DatabaseNotFoundError:
            click.echo("No database was found, pass ``--import-db`` to create")
            return
        
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

    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(client, Path(exposures), datasets, model_map)

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

