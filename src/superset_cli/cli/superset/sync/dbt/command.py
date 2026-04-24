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
from datetime import datetime
from superset_cli.api.clients.dbt import (
    MFMetricWithSQLSchema,
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
    help="Preserve column and metric configurations defined in Superset",
)
@click.option(
    "--merge-metadata",
    is_flag=True,
    default=False,
    help="Update Superset configurations based on dbt metadata. Superset-only metrics are preserved",
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
        og_metrics = [] # original metrics of superset
        sl_metrics = [] # metrics defined in the semantic layer (only for MF dialects)
        for metric_config in configs["metrics"].values():
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
        for sl_metric in sl_metrics:
            if sl_metric.get("model") is None:
                _logger.info(
                    "Derived metric detected: %s (type=%s)",
                    sl_metric["name"],
                    sl_metric.get("type"),
                )
                model_name = sl_metric["meta"].get("ref_dataset", None)
                if not model_name:
                    raise CLIError(
                        "Chưa đặt tên virtual dataset cho derived metric cần tạo", 1,
                    )
                virtual_unique_id = f"model.semantic_layer.{model_name}"

                # Chỉ tạo model 1 lần cho mỗi ref_dataset
                if model_name not in seen_virtual_models:
                    model = {
                        "name": model_name,
                        "unique_id": virtual_unique_id,
                        "schema": None,       # → virtual dataset (không có physical table)
                        "database": database_profile,
                        "description": "",
                        "meta": {},
                        "columns": [],
                        "sql": sl_metric["model_sql"],
                    }
                    seen_virtual_models[model_name] = model
                    models.append(model)
                    _logger.info(
                        "Created virtual model '%s'",
                        model_name
                    )

                # Trỏ metric về virtual dataset thay vì model gốc
                sl_metric["model"] = virtual_unique_id

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
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Compute a SL metric using the ``mf`` CLI.
    """
    mf_metric_schema = MFMetricWithSQLSchema()

    command = ["mf", "query", "--explain", "--metrics", metric["name"], '--quiet']
    try:
        _logger.info(
            "Parsing metric %s %s\nFROM: %s",
            metric["name"],
            "(derived)" if metric.get("type", "").lower() == "derived" else "",
            metric["path"],
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
    if not models:
        return None

    model_sql = None
    model_id = None

    if len(models) == 1:
        model_id = models[0]["unique_id"]
    else:
        table_fqns = [
            f"{m['database']}.{m['schema']}.{m['name']}" for m in models
        ]
        try:
            model_sql = handle_relation_derived_metric(table_fqns, configs, target_config)
        except Exception as exc:
            _logger.warning(
                "Cannot generate derived metric SQL for %s: %s",
                metric["name"],
                exc,
            )
            return None

    return mf_metric_schema.load(
        {
            "name": metric["name"],
            "label": metric["label"],
            "type": metric["type"],
            "description": metric["description"],
            "sql": sql,
            "dialect": dialect.value,
            "model": model_id,
            "meta": metric["meta"],
            "model_sql": model_sql,
        },
    )

def _get_fqn_parts(fqn: str) -> Tuple[str, str, str]:
    """Tách catalog.schema.table thành tuple (catalog, schema, table)."""
    parts = fqn.split(".")
    return parts[0], parts[1], parts[2]


def _fetch_relation_groups(
    table_names: List[str],
    rd_config: Dict[str, Any] = None,
) -> List[Tuple[int, str, str]]:
    """
    Query relation_members DB để lấy các nhóm quan hệ chứa đủ các bảng.

    Connection info is read from the dbt profile target's
    ``meta.relation_designer`` section::

        meta:
          relation_designer:
            host: localhost
            port: 5432
            dbname: relationship_designer
            user: postgres
            password: postgres

    Args:
        table_names: List of fully-qualified table names.
        rd_config: ``meta.relation_designer`` dict from the dbt profile target.

    Returns:
        List of (group_id, fqn, column_name) tuples.
    """
    import psycopg2

    if not rd_config:
        _logger.error("Missing relation_designer configuration in dbt profile target meta.")
        raise ValueError(
            "Thiếu cấu hình relation_designer trong meta của dbt profile target. "
            "Vui lòng thêm section 'meta.relation_designer' với host, port, dbname, user, password."
        )

    host = rd_config.get("host", "localhost")
    port = rd_config.get("port", 5432)
    dbname = rd_config["dbname"]
    user = rd_config["user"]

    _logger.info(
        "Connecting to relation DB at %s:%s (db=%s, user=%s)",
        host,
        port,
        dbname,
        user,
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=rd_config.get("password", ""),
    )
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(table_names))
        query = f"""
            SELECT
                rm.group_id,
                rm.catalog || '.' || rm.schema || '.' || rm.table_name AS fqn,
                rm.column_name
            FROM public.relation_members rm
            WHERE rm.group_id IN (
                SELECT rm2.group_id
                FROM public.relation_members rm2
                WHERE rm2.catalog || '.' || rm2.schema || '.' || rm2.table_name
                      IN ({placeholders})
                GROUP BY rm2.group_id
                HAVING COUNT(DISTINCT rm2.catalog || '.' || rm2.schema || '.' || rm2.table_name)
                       = %s
            )
            AND rm.catalog || '.' || rm.schema || '.' || rm.table_name
                IN ({placeholders})
        """
        params = list(table_names) + [len(table_names)] + list(table_names)
        
        _logger.debug("Executing relation_members query for %s tables.", len(table_names))
        cur.execute(query, params)
        rows = cur.fetchall()
        
        _logger.info("Found %s relation group entries for the specified tables.", len(rows))
        return rows
    except psycopg2.Error as e:
        _logger.error("Database error while fetching relation groups: %s", e)
        raise
    finally:
        conn.close()


def _get_measurement_columns(
    table_names: List[str],
    configs: Dict[str, Any],
) -> Dict[str, List[str]]:
    """
    Lấy danh sách measurement columns từ dbt manifest.

    Column được coi là measurement nếu có ``is_measurement: True``
    trong ``column.meta`` hoặc ``column.config.meta``.

    Returns:
        Dict mapping table FQN → list of measurement column names.
    """
    model_lookup: Dict[str, Dict[str, Any]] = {}
    for node in configs["nodes"].values():
        if node.get("resource_type") == "model":
            fqn = f"{node['database']}.{node['schema']}.{node['name']}"
            model_lookup[fqn] = node

    result: Dict[str, List[str]] = {}
    for table_fqn in table_names:
        model = model_lookup.get(table_fqn)
        if not model:
            _logger.warning("Không tìm thấy model trong manifest cho bảng %s", table_fqn)
            continue

        columns = model.get("columns", [])
        if isinstance(columns, dict):
            columns = list(columns.values())

        meas_cols = [
            col["name"]
            for col in columns
            if col.get("meta", {}).get("is_measurement")
            or col.get("config", {}).get("meta", {}).get("is_measurement")
        ]
        if meas_cols:
            result[table_fqn] = meas_cols

    return result


def _build_cte(
    fqn: str,
    join_cols: List[str],
    meas_cols: List[str],
) -> Tuple[str, str]:
    """
    Build 1 CTE cho 1 bảng.

    Returns:
        Tuple (cte_name, cte_sql).
    """
    _, schema, table = _get_fqn_parts(fqn)
    cte_name = f"cte_{table}"
    source = f"{schema}.{table}"
    col_sep = ",\n        "

    if meas_cols:
        select_exprs = join_cols + [f"SUM({c}) AS {c}" for c in meas_cols]
        cte_sql = (
            f"{cte_name} AS (\n"
            f"    SELECT\n"
            f"        {col_sep.join(select_exprs)}\n"
            f"    FROM {source}\n"
            f"    GROUP BY {', '.join(join_cols)}\n"
            f")"
        )
    else:
        cte_sql = (
            f"{cte_name} AS (\n"
            f"    SELECT DISTINCT\n"
            f"        {col_sep.join(join_cols)}\n"
            f"    FROM {source}\n"
            f")"
        )

    return cte_name, cte_sql


def _build_join_conditions(
    current_table: str,
    current_alias: str,
    prev_tables: List[str],
    alias_map: Dict[str, str],
    groups: Dict[int, Dict[str, List[str]]],
) -> List[str]:
    """
    Build ON conditions cho FULL OUTER JOIN giữa current_table và các bảng trước đó.

    Returns:
        List of condition strings, mỗi phần tử ứng với 1 relation group.
    """
    conditions = []
    for group_tables in groups.values():
        if current_table not in group_tables:
            continue

        relevant_prev = [t for t in prev_tables if t in group_tables]
        if not relevant_prev:
            continue

        current_cols = group_tables[current_table]
        equalities = [
            f"{alias_map[prev_t]}.{pc} = {current_alias}.{cc}"
            for prev_t in relevant_prev
            for pc in group_tables[prev_t]
            for cc in current_cols
        ]

        if len(equalities) == 1:
            conditions.append(equalities[0])
        elif len(equalities) > 1:
            conditions.append(f"({' OR '.join(equalities)})")

    return conditions


def handle_relation_derived_metric(
    table_names: List[str],
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> str:
    """
    Sinh câu query CTE + FULL OUTER JOIN cho derived metric.

    Flow:
      1. Query relation_members → lấy join columns giữa các bảng
      2. Đọc manifest → lấy measurement columns (is_measurement: True)
      3. Mỗi bảng → 1 CTE: GROUP BY join columns, SUM measurement columns
      4. FULL OUTER JOIN các CTE

    Args:
        table_names: Danh sách bảng ở dạng catalog.schema.table.
        configs: dbt manifest dict đã load từ manifest.json.
        target_config: Cấu hình connection của database từ profile.

    Returns:
        SQL string dạng WITH ... SELECT ... FROM ... FULL OUTER JOIN ...
    """
    _logger.info("Generating derived metric SQL via CTE + FULL OUTER JOIN")
    _logger.info("Involved tables: %s", ", ".join(table_names))

    if len(table_names) < 2:
        _logger.error("Insufficient tables for JOIN: %s", table_names)
        raise ValueError("Cần ít nhất 2 bảng để tạo JOIN")

    # ── 1. Lấy relation groups (join metadata) ─────────────────────────
    rd_config = (target_config or {}).get("meta", {}).get("relation_designer", {})
    rows = _fetch_relation_groups(table_names, rd_config)
    
    if not rows:
        _logger.error("No relation groups found for tables: %s", table_names)
        raise ValueError(
            f"Không tìm thấy nhóm quan hệ nào chứa đủ các bảng: {table_names}"
        )

    groups: Dict[int, Dict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list),
    )
    all_tables_seen: set = set()
    for group_id, fqn, column_name in rows:
        groups[group_id][fqn].append(column_name)
        all_tables_seen.add(fqn)

    # Join columns per table (union across groups, sorted for stability)
    table_join_cols: Dict[str, List[str]] = {}
    join_cols_sets: Dict[str, set] = defaultdict(set)
    for group_tables in groups.values():
        for fqn, cols in group_tables.items():
            join_cols_sets[fqn].update(cols)
    table_join_cols = {fqn: sorted(cs) for fqn, cs in join_cols_sets.items()}
    
    _logger.debug("Resolved join columns: %s", table_join_cols)

    # ── 2. Lấy measurement columns từ manifest ────────────────────────
    measurements = _get_measurement_columns(table_names, configs)
    _logger.debug("Resolved measurement columns: %s", measurements)

    # ── 3. Build CTEs ──────────────────────────────────────────────────
    ordered_tables = [fqn for fqn in table_names if fqn in all_tables_seen]
    aliases = [chr(ord("a") + i) for i in range(len(ordered_tables))]
    alias_map = dict(zip(ordered_tables, aliases))

    cte_names = []
    cte_sqls = []
    for fqn in ordered_tables:
        cte_name, cte_sql = _build_cte(
            fqn,
            table_join_cols.get(fqn, []),
            measurements.get(fqn, []),
        )
        cte_names.append(cte_name)
        cte_sqls.append(cte_sql)
        _logger.info("Built CTE '%s' for table '%s'", cte_name, fqn)

    # ── 4. Build final SELECT ──────────────────────────────────────────
    # Collect unique join columns (preserving order)
    all_join_cols: List[str] = []
    seen: set = set()
    for fqn in ordered_tables:
        for col in table_join_cols.get(fqn, []):
            if col not in seen:
                all_join_cols.append(col)
                seen.add(col)

    select_exprs: List[str] = []

    # Join columns → COALESCE
    for col in all_join_cols:
        sources = [
            f"{alias}.{col}"
            for fqn, alias in zip(ordered_tables, aliases)
            if col in table_join_cols.get(fqn, [])
        ]
        if len(sources) == 1:
            select_exprs.append(f"{sources[0]} AS {col}")
        else:
            select_exprs.append(f"COALESCE({', '.join(sources)}) AS {col}")

    # Measurement columns
    for fqn, alias in zip(ordered_tables, aliases):
        for col in measurements.get(fqn, []):
            select_exprs.append(f"{alias}.{col}")

    if not select_exprs:
        select_exprs = ["*"]

    # ── 5. Build FULL OUTER JOIN ───────────────────────────────────────
    join_parts = [f"FROM {cte_names[0]} {aliases[0]}"]

    for i in range(1, len(ordered_tables)):
        conditions = _build_join_conditions(
            ordered_tables[i],
            aliases[i],
            ordered_tables[:i],
            alias_map,
            groups,
        )
        if not conditions:
            _logger.error(
                "Missing join conditions for table '%s' against previously joined tables", 
                ordered_tables[i]
            )
            raise ValueError(
                f"Không tìm thấy điều kiện join cho bảng {ordered_tables[i]}"
            )

        on_str = "\n       AND ".join(conditions)
        join_parts.append(
            f"FULL OUTER JOIN {cte_names[i]} {aliases[i]}\n"
            f"    ON {on_str}"
        )

    # ── Assemble ───────────────────────────────────────────────────────
    col_sep = ",\n    "
    final_sql = (
        f"WITH {(','+chr(10)).join(cte_sqls)}\n"
        f"SELECT\n"
        f"    {col_sep.join(select_exprs)}\n"
        f"{chr(10).join(join_parts)}"
    )
    
    _logger.info("Successfully generated CTE FULL OUTER JOIN query")
    return final_sql