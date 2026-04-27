"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

import json
import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import Expression, ParseError, exp, parse_one
from sqlglot.expressions import (
    Alias,
    Case,
    Distinct,
    Identifier,
    If,
    Join,
    Select,
    Table,
    Where,
)
from sqlglot.optimizer import traverse_scope

from superset_cli.api.clients.dbt import (
    FilterSchema,
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
    OGMetricSchema,
)
from superset_cli.api.clients.superset import SupersetMetricDefinition
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey
from superset_cli.cli.superset.sync.dbt.lib import parse_metric_meta

_logger = logging.getLogger(__name__)

# dbt => sqlglot
DIALECT_MAP = {
    MFSQLEngine.BIGQUERY: "bigquery",
    MFSQLEngine.DUCKDB: "duckdb",
    MFSQLEngine.REDSHIFT: "redshift",
    MFSQLEngine.POSTGRES: "postgres",
    MFSQLEngine.SNOWFLAKE: "snowflake",
    MFSQLEngine.DATABRICKS: "databricks",
    MFSQLEngine.TRINO: "trino",
}


# pylint: disable=too-many-locals
def get_metric_expression(metric_name: str, metrics: Dict[str, OGMetricSchema]) -> str:
    """
    Return a SQL expression for a given dbt metric using sqlglot.
    """
    if metric_name not in metrics:
        raise Exception(f"Invalid metric {metric_name}")

    metric = metrics[metric_name]
    if "calculation_method" in metric:
        # dbt >= 1.3
        type_ = metric["calculation_method"]
        sql = metric["expression"]
    elif "sql" in metric:
        # dbt < 1.3
        type_ = metric["type"]
        sql = metric["sql"]
    else:
        raise Exception(f"Unable to generate metric expression from: {metric}")

    if metric.get("filters"):
        sql = apply_filters(sql, metric["filters"])

    simple_mappings = {
        "count": "COUNT",
        "sum": "SUM",
        "average": "AVG",
        "min": "MIN",
        "max": "MAX",
    }

    if type_ in simple_mappings:
        function = simple_mappings[type_]
        return f"{function}({sql})"

    if type_ == "count_distinct":
        return f"COUNT(DISTINCT {sql})"

    if type_ in {"expression", "derived"}:
        if metric.get("skip_parsing"):
            return sql.strip()

        try:
            expression = sqlglot.parse_one(sql, dialect=metric["dialect"])
            tokens = expression.find_all(exp.Column)

            for token in tokens:
                if token.sql() in metrics:
                    parent_sql = get_metric_expression(token.sql(), metrics)
                    parent_expression = sqlglot.parse_one(
                        parent_sql,
                        dialect=metric["dialect"],
                    )
                    token.replace(parent_expression)

            return expression.sql(dialect=metric["dialect"])
        except ParseError:
            sql = replace_metric_syntax(sql, metric["depends_on"], metrics)
            return sql

    sorted_metric = dict(sorted(metric.items()))
    raise Exception(f"Unable to generate metric expression from: {sorted_metric}")


def apply_filters(sql: str, filters: List[FilterSchema]) -> str:
    """
    Apply filters to SQL expression.
    """
    condition = " AND ".join(
        "{field} {operator} {value}".format(**filter_) for filter_ in filters
    )
    return f"CASE WHEN {condition} THEN {sql} END"


def get_metrics_for_model(
    model: ModelSchema,
    metrics: List[OGMetricSchema],
) -> List[OGMetricSchema]:
    """
    Given a list of metrics, return those that are based on a given model.
    """
    metric_map = {metric["unique_id"]: metric for metric in metrics}
    related_metrics = []

    for metric in metrics:
        parents = set()
        queue = [metric]
        while queue:
            node = queue.pop()
            depends_on = node["depends_on"]
            if is_derived(node):
                queue.extend(metric_map[parent] for parent in depends_on)
            else:
                parents.update(depends_on)

        if len(parents) > 1:
            _logger.warning(
                "Metric %s cannot be calculated because it depends on multiple models: %s",
                metric["name"],
                ", ".join(sorted(parents)),
            )
            continue

        if parents == {model["unique_id"]}:
            related_metrics.append(metric)

    return related_metrics


def get_metric_models(unique_id: str, metrics: List[OGMetricSchema]) -> Set[str]:
    """
    Given a metric, return the models it depends on.
    """
    metric_map = {metric["unique_id"]: metric for metric in metrics}
    metric = metric_map[unique_id]
    depends_on = metric["depends_on"]

    if is_derived(metric):
        return {
            model
            for parent in depends_on
            for model in get_metric_models(parent, metrics)
        }

    return set(depends_on)


def get_metric_definition(
    metric_name: str,
    metrics: List[OGMetricSchema],
) -> SupersetMetricDefinition:
    """
    Build a Superset metric definition from an OG (< 1.6) dbt metric.
    """
    metric_map = {metric["name"]: metric for metric in metrics}
    metric = metric_map[metric_name]
    metric_meta = parse_metric_meta(metric)
    final_metric_name = metric_meta["metric_name_override"] or metric_name

    return {
        "expression": get_metric_expression(metric_name, metric_map),
        "metric_name": final_metric_name,
        "metric_type": (metric.get("type") or metric.get("calculation_method")),
        "verbose_name": metric.get("label", final_metric_name),
        "description": metric.get("description", ""),
        "extra": json.dumps(metric_meta["meta"]),
        **metric_meta["kwargs"],  # type: ignore
    }


def get_superset_metrics_per_model(
    og_metrics: List[OGMetricSchema],
    sl_metrics: Optional[List[MFMetricWithSQLSchema]] = None,
) -> Dict[str, List[SupersetMetricDefinition]]:
    """
    Build a dictionary of Superset metrics for each dbt model.
    """
    superset_metrics = defaultdict(list)
    for metric in og_metrics:
        # dbt supports creating derived metrics with raw syntax. In case the metric doesn't
        # rely on other metrics (or rely on other metrics that aren't associated with any
        # model), it's required to specify the dataset the metric should be associated with
        # under the ``meta.superset.model`` key. If the derived metric is just an expression
        # with no dependency, it's not required to parse the metric SQL.
        if model := metric.get("meta", {}).get("superset", {}).pop("model", None):
            if len(metric["depends_on"]) == 0:
                metric["skip_parsing"] = True
        else:
            metric_models = get_metric_models(metric["unique_id"], og_metrics)
            if len(metric_models) == 0:
                _logger.warning(
                    "Metric %s cannot be calculated because it's not associated with any model."
                    " Please specify the model under metric.meta.superset.model.",
                    metric["name"],
                )
                continue

            if len(metric_models) != 1:
                _logger.warning(
                    "Metric %s cannot be calculated because it depends on multiple models: %s",
                    metric["name"],
                    ", ".join(sorted(metric_models)),
                )
                continue
            model = metric_models.pop()

        metric_definition = get_metric_definition(
            metric["name"],
            og_metrics,
        )
        superset_metrics[model].append(metric_definition)

    for sl_metric in sl_metrics or []:
        metric_definition = convert_metric_flow_to_superset(sl_metric)
        model = sl_metric["model"]
        superset_metrics[model].append(metric_definition)

    return superset_metrics


def wrap_with_where(ast: exp.Expression, where_condition: exp.Expression) -> exp.Expression:
    """
    Wrap the top-level aggregations in `ast` with a CASE WHEN `where_condition` THEN ... END.
    If there are no aggregations, wrap the entire expression.
    """
    aggs = (exp.Sum, exp.Max, exp.Min, exp.Avg, exp.Count)

    has_agg = False
    for node in list(ast.find_all(aggs)):
        is_top_level = True
        parent = node.parent
        while parent:
            if isinstance(parent, aggs):
                is_top_level = False
                break
            parent = parent.parent
            
        if is_top_level:
            has_agg = True
            arg = node.this
            if isinstance(arg, exp.Distinct):
                inner_exprs = arg.expressions
                case_expr = exp.Case(
                    ifs=[exp.If(this=where_condition.copy(), true=inner_exprs[0])]
                )
                arg.set("expressions", [case_expr] + inner_exprs[1:])
            else:
                if isinstance(node, exp.Count) and isinstance(arg, exp.Star):
                    arg = exp.Literal.number(1)
                case_expr = exp.Case(ifs=[exp.If(this=where_condition.copy(), true=arg)])
                node.set("this", case_expr)

    if has_agg:
        return ast
    
    return exp.Case(ifs=[exp.If(this=where_condition.copy(), true=ast)])


def convert_query_to_projection(sql: str, dialect: MFSQLEngine) -> str:
    """
    Convert a MetricFlow compiled SQL to a projection.

    Handles both single-table and multi-table (JOIN in subqueries) queries.
    Evaluates scopes bottom-up to resolve aliases and push WHERE clauses into 
    CASE statements.
    """
    dialect_str = DIALECT_MAP.get(dialect)
    parsed_query = parse_one(sql, dialect=dialect_str)

    scopes = traverse_scope(parsed_query)
    
    for scope in scopes:
        scope.exports = {}
        
        # 1. Build available columns from source scopes
        available_columns = {}
        for alias, source in scope.sources.items():
            if hasattr(source, "exports"):
                for col, ast in source.exports.items():
                    available_columns[(alias, col)] = ast
                    available_columns[(None, col)] = ast
                    
        # 3. Handle WHERE condition
        where_expr = scope.expression.args.get("where")
        where_condition = None
        if where_expr:
            where_condition = where_expr.this.copy()
            for column_node in list(where_condition.find_all(exp.Column)):
                col_name = column_node.name
                table_name = column_node.table if column_node.table else None
                key = (table_name, col_name)
                if key in available_columns:
                    column_node.replace(available_columns[key].copy())
                elif (None, col_name) in available_columns:
                    column_node.replace(available_columns[(None, col_name)].copy())
                else:
                    # Fallback: MetricFlow sometimes appends metric types (e.g., _sum) to outer aliases
                    for avail_table, avail_col in available_columns.keys():
                        if avail_col and col_name != avail_col and (col_name.startswith(avail_col + "_") or avail_col.startswith(col_name + "_")):
                            column_node.replace(available_columns[(avail_table, avail_col)].copy())
                            break
            
        # 4. Process SELECT expressions and apply substitutions / WHERE wrapping
        if isinstance(scope.expression, exp.Select):
            for proj in scope.expression.expressions:
                if isinstance(proj, exp.Alias):
                    exported_name = proj.alias
                    val_ast = proj.this.copy()
                else:
                    exported_name = proj.name
                    val_ast = proj.copy()
                    
                for column_node in list(val_ast.find_all(exp.Column)):
                    col_name = column_node.name
                    table_name = column_node.table if column_node.table else None
                    key = (table_name, col_name)
                    if key in available_columns:
                        column_node.replace(available_columns[key].copy())
                    elif (None, col_name) in available_columns:
                        column_node.replace(available_columns[(None, col_name)].copy())
                    else:
                        for avail_table, avail_col in available_columns.keys():
                            if avail_col and col_name != avail_col and (col_name.startswith(avail_col + "_") or avail_col.startswith(col_name + "_")):
                                column_node.replace(available_columns[(avail_table, avail_col)].copy())
                                break
                
                if where_condition:
                    val_ast = wrap_with_where(val_ast, where_condition)
                    
                scope.exports[exported_name] = val_ast

    # Final expression is the exported column of the outermost scope
    last_scope = scopes[-1]
    final_exports = list(last_scope.exports.values())
    if not final_exports:
        raise ValueError("No projection found in the query")
        
    metric_expression = final_exports[0]

    # Strip COALESCE wrappers
    changed = True
    while changed:
        changed = False
        for node in list(metric_expression.find_all(exp.Coalesce)):
            if node.this:
                if node is metric_expression:
                    metric_expression = node.this.copy()
                else:
                    node.replace(node.this)
                changed = True
                break

    # Flatten nested aggregations (e.g., MAX(SUM(x)) -> SUM(x))
    aggs = (exp.Sum, exp.Max, exp.Min, exp.Avg, exp.Count)
    changed = True
    while changed:
        changed = False
        for node in list(metric_expression.find_all(aggs)):
            inner_aggs = list(node.find_all(aggs))
            if len(inner_aggs) > 1:
                if node.this:
                    if node is metric_expression:
                        metric_expression = node.this.copy()
                    else:
                        node.replace(node.this)
                    changed = True
                    break

    return metric_expression.sql(dialect=dialect_str)


def apply_column_rename_map(
    expression_sql: str,
    column_rename_map: Dict[str, str],
    dialect: MFSQLEngine,
) -> str:
    """
    Replace column references in a metric expression using the rename map.

    This is needed because virtual dataset columns are aliased (e.g. ``table__col``)
    while the MetricFlow-generated expression uses the original column names.
    """
    if not column_rename_map:
        return expression_sql

    dialect_str = DIALECT_MAP.get(dialect)
    try:
        parsed = parse_one(expression_sql, dialect=dialect_str)
        for col_node in parsed.find_all(exp.Column):
            original_name = col_node.name
            if original_name in column_rename_map:
                col_node.set(
                    "this",
                    exp.to_identifier(column_rename_map[original_name]),
                )
        return parsed.sql(dialect=dialect_str)
    except ParseError:
        _logger.warning(
            "Could not parse expression for column renaming, falling back to string replacement",
        )
        # Fallback: simple string replacement
        for original, renamed in column_rename_map.items():
            expression_sql = re.sub(
                r"\b" + re.escape(original) + r"\b",
                renamed,
                expression_sql,
            )
        return expression_sql


def convert_metric_flow_to_superset(
    sl_metric: MFMetricWithSQLSchema,
) -> SupersetMetricDefinition:
    """
    Convert a MetricFlow metric to a Superset metric.

    Before MetricFlow we could build the metrics based on the metadata returned by the
    GraphQL API. With MetricFlow we only have access to the compiled SQL used to
    compute the metric, so we need to parse it and build a single projection for
    Superset.

    For example, this:

        SELECT
            SUM(order_count) AS large_order
        FROM (
            SELECT
                order_total AS order_id__order_total_dim
                , 1 AS order_count
            FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_106
        ) subq_796
        WHERE order_id__order_total_dim >= 20

    Becomes:

        SUM(CASE WHEN order_total > 20 THEN 1 END)

    """
    metric_meta = parse_metric_meta(sl_metric)
    expression = convert_query_to_projection(
        sl_metric["sql"],
        sl_metric["dialect"],
    )

    # Remap column names for virtual datasets (e.g. sale_amount → fact_table__sale_amount)
    column_rename_map = sl_metric.get("column_rename_map", {})
    if column_rename_map:
        expression = apply_column_rename_map(
            expression,
            column_rename_map,
            sl_metric["dialect"],
        )

    return {
        "expression": expression,
        "metric_name": metric_meta["metric_name_override"] or sl_metric["name"],
        "metric_type": sl_metric["type"],
        "verbose_name": sl_metric["label"],
        "description": sl_metric["description"],
        "extra": json.dumps(metric_meta["meta"]),
        **metric_meta["kwargs"],  # type: ignore
    }


def get_models_from_sql(
    sql: str,
    dialect: MFSQLEngine,
    model_map: Dict[ModelKey, ModelSchema],
) -> Optional[List[ModelSchema]]:
    """
    Return the model associated with a SQL query.
    """
    parsed_query = parse_one(sql, dialect=DIALECT_MAP.get(dialect))
    sources = list(parsed_query.find_all(Table))

    for table in sources:
        if ModelKey(table.db, table.name) not in model_map:
            return None

    return [model_map[ModelKey(table.db, table.name)] for table in sources]


def replace_metric_syntax(
    sql: str,
    dependencies: List[str],
    metrics: Dict[str, OGMetricSchema],
) -> str:
    """
    Replace metric keys with their SQL syntax.
    This method is a fallback in case ``sqlglot`` raises a ``ParseError``.
    """
    for parent_metric in dependencies:
        parent_metric_name = parent_metric.split(".")[-1]
        pattern = r"\b" + re.escape(parent_metric_name) + r"\b"
        parent_metric_syntax = get_metric_expression(
            parent_metric_name,
            metrics,
        )
        sql = re.sub(pattern, parent_metric_syntax, sql)

    return sql.strip()


def extract_aliases(parsed_query: Expression) -> Dict[str, str]:
    """
    Extract column aliases from a SQL query.
    """
    aliases = {}
    for expression in parsed_query.find_all(Alias):
        alias_name = expression.alias
        expression_text = expression.this.sql()
        aliases[alias_name] = expression_text

    return aliases


def is_derived(metric: OGMetricSchema) -> bool:
    """
    Return if the metric is derived.
    """
    return (
        metric.get("calculation_method") == "derived"  # dbt >= 1.3
        or metric.get("type") == "expression"  # dbt < 1.3
        or metric.get("type") == "derived"  # WTF dbt Cloud
    )

