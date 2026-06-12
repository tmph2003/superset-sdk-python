"""
Chuyển đổi metric.

Module này được dùng để chuyển đổi các dbt metric thành Superset metric.
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
    Table,
)
from sqlglot.optimizer import traverse_scope

from api.clients.dbt import (
    FilterSchema,
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
    OGMetricSchema,
)
from api.clients.superset import SupersetMetricDefinition
from cli.lib import ModelKey
from cli.lib import parse_metric_meta

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
    Trả về câu lệnh SQL (SQL expression) cho một dbt metric nhất định bằng cách sử dụng sqlglot.
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
    Áp dụng filter (bộ lọc) vào câu lệnh SQL.
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
    Với một danh sách các metric được truyền vào, trả về những metric được xây dựng trên một model nhất định.
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
    Với một metric được truyền vào, trả về danh sách các model mà nó phụ thuộc (depends on).
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
    Xây dựng một định nghĩa Superset metric từ một OG (< 1.6) dbt metric.
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
    Xây dựng một từ điển chứa các Superset metric cho từng dbt model.
    """
    superset_metrics = defaultdict(list)
    for metric in og_metrics:
        # dbt hỗ trợ tạo derived metric bằng cú pháp thô. Trong trường hợp metric không
        # phụ thuộc vào các metric khác (hoặc phụ thuộc vào các metric không được gắn với 
        # bất kỳ model nào), bắt buộc phải chỉ định dataset mà metric cần được gắn vào
        # dưới key ``meta.superset.model``. Nếu derived metric chỉ là một biểu thức
        # không có dependencies, không cần thiết phải phân tích (parse) SQL của metric đó.
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
    Bao bọc (wrap) các phép tổng hợp (aggregation) ngoài cùng (top-level) trong `ast` 
    bằng cú pháp CASE WHEN `where_condition` THEN ... END.
    Nếu không có phép tổng hợp nào, bọc lại toàn bộ biểu thức.
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


def _resolve_column_nodes(
    ast_node: exp.Expression,
    available_columns: dict,
) -> None:
    """
    Phân giải các tham chiếu cột trong *ast_node* sử dụng *available_columns*.

    Thay thế mỗi ``exp.Column`` bằng AST cụ thể từ một source scope
    khi tìm thấy kết nối bởi ``(table, col)`` hoặc ``(None, col)``.
    Có cơ chế dự phòng (fallback) để khớp mờ (fuzzy match) tiền tố/hậu tố cho các alias của MetricFlow
    (ví dụ: ``col_sum`` với ``col``).
    """
    for column_node in list(ast_node.find_all(exp.Column)):
        col_name = column_node.name
        table_name = column_node.table or None
        key = (table_name, col_name)
        if key in available_columns:
            column_node.replace(available_columns[key].copy())
        elif (None, col_name) in available_columns:
            column_node.replace(available_columns[(None, col_name)].copy())
        else:
            # Cơ chế dự phòng (Fallback): Đôi khi MetricFlow gắn thêm loại metric 
            # (ví dụ: _sum) vào alias ở vòng ngoài
            for avail_table, avail_col in available_columns:
                if (
                    avail_col
                    and col_name != avail_col
                    and (
                        col_name.startswith(avail_col + "_")
                        or avail_col.startswith(col_name + "_")
                    )
                ):
                    column_node.replace(
                        available_columns[(avail_table, avail_col)].copy(),
                    )
                    break


def _strip_coalesce(node: exp.Expression) -> exp.Expression:
    """
    Bỏ đi các lớp bao bọc COALESCE.
    """
    changed = True
    while changed:
        changed = False
        for coalesce in list(node.find_all(exp.Coalesce)):
            if coalesce.this:
                if coalesce is node:
                    node = coalesce.this.copy()
                else:
                    coalesce.replace(coalesce.this)
                changed = True
                break
    return node


def _flatten_nested_aggregations(node: exp.Expression) -> exp.Expression:
    """
    Làm phẳng các aggregation lồng nhau (ví dụ: MAX(SUM(x)) -> SUM(x)).
    """
    aggs = (exp.Sum, exp.Max, exp.Min, exp.Avg, exp.Count)
    changed = True
    while changed:
        changed = False
        for agg_node in list(node.find_all(aggs)):
            if len(list(agg_node.find_all(aggs))) > 1 and agg_node.this:
                if agg_node is node:
                    node = agg_node.this.copy()
                else:
                    agg_node.replace(agg_node.this)
                changed = True
                break
    return node


def convert_query_to_projection(sql: str, dialect: MFSQLEngine, metric_name: str = None) -> str:
    """
    Chuyển đổi một đoạn SQL được biên dịch bởi MetricFlow thành một phép chiếu (projection).

    Xử lý cả câu truy vấn đơn bảng lẫn đa bảng (dùng JOIN trong các truy vấn con - subquery).
    Đánh giá các scope từ dưới lên (bottom-up) để phân giải các alias và đẩy các mệnh đề WHERE
    vào bên trong cấu trúc CASE.
    """
    dialect_str = DIALECT_MAP.get(dialect)
    parsed_query = parse_one(sql, dialect=dialect_str)

    scopes = traverse_scope(parsed_query)
    
    for scope in scopes:
        scope.exports = {}
        
        # 1. Xây dựng các cột khả dụng (available columns) từ các source scope
        available_columns = {}
        for alias, source in scope.sources.items():
            if hasattr(source, "exports"):
                for col, ast in source.exports.items():
                    available_columns[(alias, col)] = ast
                    available_columns[(None, col)] = ast
                    
        # 2. Xử lý điều kiện WHERE
        where_expr = scope.expression.args.get("where")
        where_condition = None
        if where_expr:
            where_condition = where_expr.this.copy()
            _resolve_column_nodes(where_condition, available_columns)
            
        # 3. Xử lý các biểu thức SELECT và áp dụng các phép thay thế / bao bọc WHERE
        if isinstance(scope.expression, exp.Select):
            for proj in scope.expression.expressions:
                if isinstance(proj, exp.Alias):
                    exported_name = proj.alias
                    val_ast = proj.this.copy()
                else:
                    exported_name = proj.name
                    val_ast = proj.copy()
                    
                _resolve_column_nodes(val_ast, available_columns)
                
                if where_condition:
                    val_ast = wrap_with_where(val_ast, where_condition)
                    
                scope.exports[exported_name] = val_ast

    # Biểu thức cuối cùng chính là cột được export từ outermost scope
    last_scope = scopes[-1]
    
    if metric_name and metric_name in last_scope.exports:
        metric_expression = last_scope.exports[metric_name]
    else:
        final_exports = list(last_scope.exports.values())
        if not final_exports:
            raise ValueError("No projection found in the query")
        
        # Thử tìm cột có chứa phép toán aggregate (Sum, Max, Min, v.v) thay vì luôn lấy cột đầu tiên
        aggs = (exp.Sum, exp.Max, exp.Min, exp.Avg, exp.Count)
        found_agg = False
        for exp_ast in final_exports:
            if list(exp_ast.find_all(aggs)):
                metric_expression = exp_ast
                found_agg = True
                break
        
        if not found_agg:
            metric_expression = final_exports[-1] if len(final_exports) > 1 else final_exports[0]

    metric_expression = _strip_coalesce(metric_expression)
    metric_expression = _flatten_nested_aggregations(metric_expression)

    return metric_expression.sql(dialect=dialect_str)


def apply_column_rename_map(
    expression_sql: str,
    column_rename_map: Dict[str, str],
    dialect: MFSQLEngine,
) -> str:
    """
    Thay thế các tham chiếu cột trong câu lệnh SQL của metric bằng cách dùng rename map.

    Việc này là cần thiết vì các cột trong virtual dataset đã được đặt alias (ví dụ: ``table__col``)
    trong khi đó đoạn SQL do MetricFlow sinh ra lại dùng các tên cột gốc.
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
        # Cơ chế dự phòng (Fallback): thay thế chuỗi đơn giản
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
    Chuyển đổi một MetricFlow metric thành một Superset metric.

    Trước khi có MetricFlow, chúng ta có thể xây dựng các metric dựa trên metadata 
    trả về bởi GraphQL API. Với MetricFlow, chúng ta chỉ có quyền truy cập vào câu lệnh 
    SQL đã được biên dịch dùng để tính toán metric, vì vậy chúng ta cần phân tích nó 
    và xây dựng một phép chiếu (projection) duy nhất cho Superset.

    Ví dụ đoạn truy vấn này:

        SELECT
            SUM(order_count) AS large_order
        FROM (
            SELECT
                order_total AS order_id__order_total_dim
                , 1 AS order_count
            FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_106
        ) subq_796
        WHERE order_id__order_total_dim >= 20

    Sẽ trở thành:

        SUM(CASE WHEN order_total > 20 THEN 1 END)

    """
    metric_meta = parse_metric_meta(sl_metric)
    expression = convert_query_to_projection(
        sl_metric["sql"],
        sl_metric["dialect"],
        sl_metric["name"],
    )

    # Ánh xạ lại các tên cột cho virtual dataset (ví dụ: sale_amount → fact_table__sale_amount)
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
    Trả về model được liên kết với một câu lệnh truy vấn SQL.
    """
    parsed_query = parse_one(sql, dialect=DIALECT_MAP.get(dialect))
    
    # Thu thập các tên CTE để không coi chúng là các physical model (bảng vật lý)
    ctes = {cte.alias for cte in parsed_query.find_all(sqlglot.expressions.CTE)}
    
    sources = [
        table for table in parsed_query.find_all(Table)
        if table.name not in ctes
    ]

    result = []
    for table in sources:
        key = ModelKey(table.db, table.name)
        if key in model_map:
            result.append(model_map[key])
        else:
            # MetricFlow sinh SQL sử dụng alias của bảng nếu có
            matched = False
            for m in model_map.values():
                if m.get("schema") == table.db and m.get("alias") == table.name:
                    result.append(m)
                    matched = True
                    break
            if not matched:
                return None

    return result


def replace_metric_syntax(
    sql: str,
    dependencies: List[str],
    metrics: Dict[str, OGMetricSchema],
) -> str:
    """
    Thay thế các khóa (keys) của metric bằng cú pháp SQL của chúng.
    Phương thức này đóng vai trò dự phòng (fallback) trong trường hợp ``sqlglot`` ném ra một lỗi ``ParseError``.
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
    Trích xuất các alias cột từ một câu lệnh truy vấn SQL.
    """
    aliases = {}
    for expression in parsed_query.find_all(Alias):
        alias_name = expression.alias
        expression_text = expression.this.sql()
        aliases[alias_name] = expression_text

    return aliases


def is_derived(metric: OGMetricSchema) -> bool:
    """
    Trả về True nếu metric là một derived metric (được dẫn xuất từ các metric khác).
    """
    return (
        metric.get("calculation_method") == "derived"  # dbt >= 1.3
        or metric.get("type") == "expression"  # dbt < 1.3
        or metric.get("type") == "derived"  # do cú pháp kỳ cục của dbt Cloud
    )

