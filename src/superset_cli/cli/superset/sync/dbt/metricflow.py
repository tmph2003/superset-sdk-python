"""
MetricFlow CLI interaction and output processing.

This module handles:
  - Running the ``mf query --explain`` command.
  - Sanitising MetricFlow stdout (stripping ANSI codes, version warnings, etc.).
  - Building the semantic-layer metric schema from MetricFlow output.
"""

import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional

from superset_cli.api.clients.dbt import (
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey
from superset_cli.cli.superset.sync.dbt.metrics import get_models_from_sql
from superset_cli.cli.superset.sync.dbt.relations import handle_relation_derived_metric

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# SQL keywords that mark the beginning of a valid SQL statement
_SQL_START_KEYWORDS = frozenset({
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
    "DROP", "MERGE", "EXPLAIN",
})

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")

# dbt MetricFlow dialect → sqlglot dialect string
_DIALECT_MAP = {
    "BIGQUERY": "bigquery",
    "POSTGRES": "postgres",
    "SNOWFLAKE": "snowflake",
    "REDSHIFT": "redshift",
    "DUCKDB": "duckdb",
    "DATABRICKS": "databricks",
    "TRINO": "trino",
}


# ---------------------------------------------------------------------------
# Output sanitisation
# ---------------------------------------------------------------------------


def clean_mf_sql_output(raw: str) -> str:
    """
    Strip non-SQL content from MetricFlow ``mf query --explain`` output.

    MetricFlow may write version-upgrade warnings, emoji lines, and ANSI
    colour codes to *stdout* even when ``--quiet`` is used.  This function
    removes those lines so that only the SQL statement remains.

    Strategy:
      1. Remove ANSI escape sequences.
      2. Walk lines from the top; drop everything until we hit a line whose
         first token is a known SQL keyword (SELECT, WITH, …).
      3. Walk lines from the bottom; drop trailing non-SQL lines (warnings,
         blank lines, emoji notices, etc.).
    """
    # 1. Strip ANSI
    cleaned = _ANSI_ESCAPE_RE.sub("", raw)

    lines = cleaned.splitlines()

    # 2. Find first SQL line
    start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        first_token = stripped.split()[0].upper().rstrip("(")
        if first_token in _SQL_START_KEYWORDS:
            start = i
            break
    else:
        # No SQL keyword found at all
        return ""

    # 3. Trim trailing non-SQL noise
    end = len(lines)
    for j in range(len(lines) - 1, start - 1, -1):
        stripped = lines[j].strip()
        if not stripped:
            end = j
            continue
        # Lines starting with emoji / special chars are not SQL
        if stripped[0] in "⚠💡‼🔔🚀✅❌ℹ️":
            end = j
            continue
        # If the line looks like normal text (no SQL punctuation), skip it
        # but be conservative — only trim if clearly not SQL
        break
    else:
        return ""

    return "\n".join(lines[start:end]).strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_sl_metric(
    metric: Dict[str, Any],
    model_map: Dict[ModelKey, ModelSchema],
    dialect: MFSQLEngine,
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Compute a SL metric using the ``mf`` CLI.

    Runs ``mf query --explain --metrics <name> --quiet``, cleans the output,
    resolves the referenced models, and returns a fully-loaded
    :class:`MFMetricWithSQLSchema`.
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
    except subprocess.CalledProcessError as exc:
        _logger.warning(
            "Could not generate SQL for metric %s (this happens for some metrics)\n"
            "stderr: %s",
            metric["name"],
            exc.stderr,
        )
        return None
    raw_output = result.stdout.strip()

    # MetricFlow may emit non-SQL lines to stdout (version warnings, emoji
    # notices, etc.) even with --quiet.  Strip them so sqlglot can parse.
    sql = clean_mf_sql_output(raw_output)

    if not sql:
        _logger.warning(
            "mf returned no usable SQL for metric %s. Raw output:\n%s",
            metric["name"],
            raw_output,
        )
        return None

    _logger.debug("mf raw output:\n%s", raw_output)
    _logger.debug("Cleaned SQL:\n%s", sql)

    # Debug: show what tables sqlglot finds vs what's in model_map
    from sqlglot import parse_one as _parse_one
    from sqlglot.expressions import Table as _Table
    _dialect_str = _DIALECT_MAP.get(dialect.value)
    _parsed = _parse_one(sql, dialect=_dialect_str)
    _tables = list(_parsed.find_all(_Table))
    for _t in _tables:
        _key = ModelKey(_t.db, _t.name)
        _logger.debug(
            "  Table in SQL: db=%s name=%s catalog=%s → ModelKey=%s → in model_map: %s",
            _t.db, _t.name, getattr(_t, 'catalog', None), _key, _key in model_map,
        )
    _logger.debug("  model_map keys (first 10): %s", list(model_map.keys())[:10])

    models = get_models_from_sql(sql, dialect, model_map)
    if not models:
        _logger.warning("get_models_from_sql returned None/empty for metric %s", metric["name"])
        return None

    model_sql = None
    model_id = None
    column_rename_map = {}
    column_descriptions = {}
    column_labels = {}

    if len(models) == 1:
        model_id = models[0]["unique_id"]
        if metric.get("meta", {}).get("ref_dataset"):
            table_name = models[0]["name"]
            model_cols = models[0].get("columns", {})
            if isinstance(model_cols, dict):
                model_cols_list = list(model_cols.values())
            else:
                model_cols_list = model_cols
            for col in model_cols_list:
                col_name = col["name"]
                is_measurement = col.get("meta", {}).get("is_measurement") or col.get("config", {}).get("meta", {}).get("is_measurement")
                if is_measurement:
                    aliased = f"{table_name}__{col_name}"
                    column_rename_map[col_name] = aliased
                    column_descriptions[aliased] = col.get("description", "")
                    if col.get("label"):
                        column_labels[aliased] = col.get("label")
    else:
        table_fqns = [
            f"{m['database']}.{m['schema']}.{m['name']}" for m in models
        ]
        try:
            model_sql, column_rename_map, column_labels = handle_relation_derived_metric(table_fqns, configs, target_config)
        except Exception as exc:
            _logger.warning(
                "Cannot generate derived metric SQL for %s: %s",
                metric["name"],
                exc,
            )
            return None

        # Build column descriptions from original model columns in manifest
        for m in models:
            table_name = m["name"]
            model_cols = m.get("columns", {})
            # model_cols is a dict: {col_name: {description: ...}}
            if isinstance(model_cols, dict):
                for col_name, col_meta in model_cols.items():
                    desc = col_meta.get("description", "") if isinstance(col_meta, dict) else ""
                    # Measurement columns: aliased as table__col
                    aliased = f"{table_name}__{col_name}"
                    column_descriptions[aliased] = desc
                    # Join columns: use first model's description
                    if col_name not in column_descriptions:
                        column_descriptions[col_name] = desc

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
            "column_rename_map": column_rename_map,
            "column_labels": column_labels if model_sql or column_rename_map else {},
            "column_descriptions": column_descriptions,
        },
    )
