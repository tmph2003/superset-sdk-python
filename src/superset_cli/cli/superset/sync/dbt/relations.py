"""
Relation-based SQL generation for derived virtual datasets.

This module handles:
  - Querying the ``relation_members`` database for join metadata.
  - Extracting measurement columns from the dbt manifest.
  - Building CTE + FULL OUTER JOIN SQL for multi-table derived metrics.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_fqn_parts(fqn: str) -> Tuple[str, str, str]:
    """Split ``catalog.schema.table`` into a tuple ``(catalog, schema, table)``."""
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

    _logger.debug(
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
        
        _logger.debug("Found %s relation group entries for the specified tables.", len(rows))
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def handle_relation_derived_metric(
    table_names: List[str],
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> Tuple[str, Dict[str, str], Dict[str, str]]:
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
        Tuple of (sql, column_rename_map, column_labels).
    """
    _logger.debug("Generating derived metric SQL via CTE + FULL OUTER JOIN")
    _logger.debug("Involved tables: %s", ", ".join(table_names))

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

    # Build dbt label lookup from manifest columns
    dbt_column_labels: Dict[str, Dict[str, str]] = {}  # table_fqn → {col_name: label}
    model_lookup: Dict[str, Dict[str, Any]] = {}
    for node in configs["nodes"].values():
        if node.get("resource_type") == "model":
            fqn = f"{node['database']}.{node['schema']}.{node['name']}"
            model_lookup[fqn] = node
    for fqn in table_names:
        model_node = model_lookup.get(fqn)
        if not model_node:
            continue
        cols = model_node.get("columns", {})
        if isinstance(cols, dict):
            cols = list(cols.values())
        dbt_column_labels[fqn] = {
            col["name"]: col["label"]
            for col in cols
            if col.get("label")
        }

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
        _logger.debug("Built CTE '%s' for table '%s'", cte_name, fqn)

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
    # Column labels for verbose_name in Superset
    column_labels: Dict[str, str] = {}

    # Join columns → COALESCE
    for col in all_join_cols:
        sources = [
            f"{alias_map[fqn]}.{col}"
            for fqn in ordered_tables
            if col in table_join_cols.get(fqn, [])
        ]
        if len(sources) == 1:
            select_exprs.append(f"{sources[0]} AS {col}")
        else:
            select_exprs.append(f"COALESCE({', '.join(sources)}) AS {col}")
        # Use dbt label if available for join columns
        for fqn in ordered_tables:
            if col in (dbt_column_labels.get(fqn) or {}):
                column_labels[col] = dbt_column_labels[fqn][col]
                break
        # If no dbt label found, don't set → shows raw column name

    # Measurement columns + build column rename map
    # Detect duplicate column names across tables
    all_meas_cols: Dict[str, int] = {}
    for fqn in ordered_tables:
        for col in measurements.get(fqn, []):
            all_meas_cols[col] = all_meas_cols.get(col, 0) + 1

    column_rename_map: Dict[str, str] = {}
    for fqn, alias in zip(ordered_tables, aliases):
        _, _, table = _get_fqn_parts(fqn)
        for col in measurements.get(fqn, []):
            col_alias = f"{table}__{col}"
            select_exprs.append(f"COALESCE({alias}.{col}, 0) AS {col_alias}")
            dbt_label = (dbt_column_labels.get(fqn) or {}).get(col)
            if dbt_label:
                if all_meas_cols.get(col, 0) > 1:
                    # Duplicate column name — add table description to disambiguate
                    node = model_lookup.get(fqn, {})
                    table_desc = node.get('description') or node.get('name', '')
                    column_labels[col_alias] = f"{dbt_label} ({table_desc})"
                else:
                    column_labels[col_alias] = dbt_label
            if col in column_rename_map:
                _logger.warning(
                    "Column '%s' exists in multiple tables — "
                    "metric expressions referencing this column may be ambiguous. "
                    "Keeping first mapping: %s",
                    col,
                    column_rename_map[col],
                )
            else:
                column_rename_map[col] = col_alias

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
    
    _logger.debug("Measurements dict: %s", measurements)
    _logger.debug("Column rename map: %s", column_rename_map)
    _logger.debug("Generated virtual dataset SQL:\n%s", final_sql)
    _logger.debug("Column labels: %s", column_labels)
    return final_sql, column_rename_map, column_labels
