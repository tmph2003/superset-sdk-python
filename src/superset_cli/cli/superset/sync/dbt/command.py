"""
A command to sync dbt models/metrics to Superset and charts/dashboards back as exposures.
"""

import logging
import os.path
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import os
import click
import yaml
from yarl import URL

from collections import defaultdict
from sql_metadata import Parser
from datetime import datetime
from superset_cli.api.clients.dbt import (
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from superset_cli.api.clients.superset import SupersetClient
from superset_cli.auth.token import TokenAuth
from superset_cli.cli.superset.sync.dbt.databases import sync_database
from superset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey, sync_exposures
from superset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    get_og_metric_from_config,
    list_failed_models,
    load_profiles,
)
from superset_cli.cli.superset.sync.dbt.metrics import (
    get_models_from_sql,
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
    help="Preserve column and metric configurations defined in Preset",
)
@click.option(
    "--merge-metadata",
    is_flag=True,
    default=False,
    help="Update Preset configurations based on dbt metadata. Preset-only metrics are preserved",
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
        configs = yaml.load(input_, Loader=yaml.SafeLoader)

    config = load_profiles(Path(profiles), project, profile, target)
    dialect = config[profile]["outputs"][target]["type"]
    try:
        mf_dialect = MFSQLEngine(dialect.upper())
    except ValueError:
        mf_dialect = None

    model_schema = ModelSchema()
    models = []
    for config in configs["nodes"].values():
        if config["resource_type"] == "model":
            unique_id = config["uniqueId"] = config["unique_id"]
            config["children"] = configs["child_map"][unique_id]
            config["columns"] = list(config["columns"].values())
            models.append(model_schema.load(config))
    models = apply_select(models, select, exclude)
    model_map = {ModelKey(model["schema"], model["name"]): model for model in models}

    failures: List[str] = []

    if exposures_only:
        datasets = [
            dataset
            for dataset in client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        og_metrics = [] # original metrics of superset
        sl_metrics = [] # metrics defined in the semantic layer (only for MF dialects)
        for config in configs["metrics"].values():
            # dbt is shifting from `metric.meta` to `metric.config.meta`
            config["meta"] = config.get("meta") or config.get("config", {}).get(
                "meta",
                {},
            )
            # Lấy công thức metrics từ superset
            # First validate if metadata is already available
            if config["meta"].get("superset", {}).get("model") and (
                sql := config["meta"].get("superset", {}).pop("expression")
            ):
                metric = get_og_metric_from_config(
                    config,
                    dialect,
                    depends_on=[],
                    sql=sql,
                )
                og_metrics.append(metric)

            # dbt legacy schema
            elif "calculation_method" in config or "sql" in config:
                metric = get_og_metric_from_config(config, dialect)
                og_metrics.append(metric)

            # dbt semantic layer
            # Only validate semantic layer metrics if MF dialect is specified
            elif mf_dialect is not None and (
                sl_metric := get_sl_metric(config, model_map, mf_dialect)
            ):
                sl_metrics.append(sl_metric)

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


def get_sl_metric(
    metric: Dict[str, Any],
    model_map: Dict[ModelKey, ModelSchema],
    dialect: MFSQLEngine,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Compute a SL metric using the ``mf`` CLI.
    """
    mf_metric_schema = MFMetricWithSQLSchema()

    command = ["mf", "query", "--explain", "--metrics", metric["name"], '--quiet']
    try:
        _logger.info(
            "Using `mf` command to retrieve SQL syntax for metric %s",
            metric["name"],
        )
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding="utf-8", env=os.environ, errors="ignore")
    except FileNotFoundError:
        _logger.warning(
            "`mf` command not found, if you're using Metricflow make sure you have it "
            "installed in order to sync metrics",
        )
        return None
    except subprocess.CalledProcessError:
        _logger.warning(
            "Could not generate SQL for metric %s (this happens for some metrics)",
            metric["name"],
        )
        return None
    sql = result.stdout.strip()

    models = get_models_from_sql(sql, dialect, model_map)
    if not models or len(models) > 1:
        return None
    model = models[0]

    return mf_metric_schema.load(
        {
            "name": metric["name"],
            "label": metric["label"],
            "type": metric["type"],
            "description": metric["description"],
            "sql": sql,
            "dialect": dialect.value,
            "model": model["unique_id"],
            "meta": metric["meta"],
        },
    )