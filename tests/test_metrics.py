"""
Tests for superset_cli.cli.superset.sync.dbt.metrics — metric conversion logic.

Covers:
  - get_metric_expression (sum, count, count_distinct, average, min, max, expression, derived)
  - apply_filters
  - get_metrics_for_model
  - get_metric_models
  - is_derived
  - extract_aliases
  - apply_column_rename_map
  - replace_metric_syntax
  - wrap_with_where
  - get_models_from_sql
  - convert_metric_flow_to_superset
  - get_metric_definition
  - get_superset_metrics_per_model
"""

import json

import pytest
from sqlglot import exp, parse_one

from superset_cli.api.clients.dbt import MFSQLEngine
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey
from superset_cli.cli.superset.sync.dbt.metrics import (
    DIALECT_MAP,
    apply_column_rename_map,
    apply_filters,
    convert_metric_flow_to_superset,
    extract_aliases,
    get_metric_definition,
    get_metric_expression,
    get_metric_models,
    get_metrics_for_model,
    get_models_from_sql,
    get_superset_metrics_per_model,
    is_derived,
    replace_metric_syntax,
    wrap_with_where,
)


# ─── apply_filters ────────────────────────────────────────────────────────
class TestApplyFilters:
    def test_single_filter(self):
        filters = [{"field": "status", "operator": "=", "value": "'active'"}]
        result = apply_filters("amount", filters)
        assert result == "CASE WHEN status = 'active' THEN amount END"

    def test_multiple_filters(self):
        filters = [
            {"field": "status", "operator": "=", "value": "'active'"},
            {"field": "type", "operator": "!=", "value": "'draft'"},
        ]
        result = apply_filters("amount", filters)
        assert "CASE WHEN" in result
        assert "AND" in result
        assert "status = 'active'" in result
        assert "type != 'draft'" in result


# ─── is_derived ───────────────────────────────────────────────────────────
class TestIsDerived:
    def test_calculation_method_derived(self):
        metric = {"calculation_method": "derived", "name": "x"}
        assert is_derived(metric) is True

    def test_type_expression(self):
        metric = {"type": "expression", "name": "x"}
        assert is_derived(metric) is True

    def test_type_derived(self):
        metric = {"type": "derived", "name": "x"}
        assert is_derived(metric) is True

    def test_not_derived_sum(self):
        metric = {"type": "sum", "name": "x"}
        assert is_derived(metric) is False

    def test_not_derived_count(self):
        metric = {"calculation_method": "count", "name": "x"}
        assert is_derived(metric) is False


# ─── get_metric_expression ────────────────────────────────────────────────
class TestGetMetricExpression:
    def _metric(self, name, **kwargs):
        """Utility to make a minimal metric dict."""
        base = {
            "name": name,
            "unique_id": f"metric.project.{name}",
            "depends_on": [f"model.project.fact_{name}"],
            "meta": {},
        }
        base.update(kwargs)
        return base

    def test_sum(self):
        metrics = {"revenue": self._metric("revenue", type="sum", sql="amount")}
        result = get_metric_expression("revenue", metrics)
        assert result == "SUM(amount)"

    def test_count(self):
        metrics = {"orders": self._metric("orders", type="count", sql="order_id")}
        result = get_metric_expression("orders", metrics)
        assert result == "COUNT(order_id)"

    def test_count_distinct(self):
        metrics = {
            "unique_customers": self._metric(
                "unique_customers", type="count_distinct", sql="customer_id"
            ),
        }
        result = get_metric_expression("unique_customers", metrics)
        assert result == "COUNT(DISTINCT customer_id)"

    def test_average(self):
        metrics = {"avg_price": self._metric("avg_price", type="average", sql="price")}
        result = get_metric_expression("avg_price", metrics)
        assert result == "AVG(price)"

    def test_min(self):
        metrics = {"min_price": self._metric("min_price", type="min", sql="price")}
        result = get_metric_expression("min_price", metrics)
        assert result == "MIN(price)"

    def test_max(self):
        metrics = {"max_price": self._metric("max_price", type="max", sql="price")}
        result = get_metric_expression("max_price", metrics)
        assert result == "MAX(price)"

    def test_with_filters(self):
        metrics = {
            "active_revenue": self._metric(
                "active_revenue",
                type="sum",
                sql="amount",
                filters=[{"field": "status", "operator": "=", "value": "'active'"}],
            ),
        }
        result = get_metric_expression("active_revenue", metrics)
        assert "CASE WHEN" in result
        assert "SUM" in result

    def test_dbt_13_calculation_method(self):
        """dbt >= 1.3 uses calculation_method + expression instead of type + sql."""
        metrics = {
            "revenue": self._metric(
                "revenue",
                calculation_method="sum",
                expression="sale_amount",
            ),
        }
        result = get_metric_expression("revenue", metrics)
        assert result == "SUM(sale_amount)"

    def test_invalid_metric_raises(self):
        with pytest.raises(Exception, match="Invalid metric"):
            get_metric_expression("nonexistent", {})

    def test_derived_expression(self):
        """Derived metrics replace child metric references recursively."""
        revenue = self._metric(
            "revenue",
            type="sum",
            sql="amount",
            depends_on=["model.project.fact"],
            dialect="trino",
        )
        cost = self._metric(
            "cost",
            type="sum",
            sql="cost_amount",
            depends_on=["model.project.fact"],
            dialect="trino",
        )
        profit = self._metric(
            "profit",
            type="expression",
            sql="revenue - cost",
            depends_on=["metric.project.revenue", "metric.project.cost"],
            dialect="trino",
        )
        metrics = {"revenue": revenue, "cost": cost, "profit": profit}
        result = get_metric_expression("profit", metrics)
        assert "SUM(amount)" in result
        assert "SUM(cost_amount)" in result

    def test_skip_parsing(self):
        metrics = {
            "custom": self._metric(
                "custom",
                type="expression",
                sql="  custom_expr(a, b)  ",
                skip_parsing=True,
                depends_on=[],
                dialect="trino",
            ),
        }
        result = get_metric_expression("custom", metrics)
        assert result == "custom_expr(a, b)"


# ─── get_metrics_for_model ────────────────────────────────────────────────
class TestGetMetricsForModel:
    def test_matching_model(self):
        model = {"unique_id": "model.project.fact_sale", "name": "fact_sale"}
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
            },
            {
                "name": "orders",
                "unique_id": "metric.project.orders",
                "depends_on": ["model.project.fact_order"],
                "type": "count",
            },
        ]
        result = get_metrics_for_model(model, metrics)
        assert len(result) == 1
        assert result[0]["name"] == "revenue"

    def test_no_matching_metrics(self):
        model = {"unique_id": "model.project.dim_x", "name": "dim_x"}
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
            },
        ]
        result = get_metrics_for_model(model, metrics)
        assert result == []


# ─── get_metric_models ────────────────────────────────────────────────────
class TestGetMetricModels:
    def test_simple_metric(self):
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
            },
        ]
        result = get_metric_models("metric.project.revenue", metrics)
        assert result == {"model.project.fact_sale"}

    def test_derived_metric(self):
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
            },
            {
                "name": "double_revenue",
                "unique_id": "metric.project.double_revenue",
                "depends_on": ["metric.project.revenue"],
                "calculation_method": "derived",
            },
        ]
        result = get_metric_models("metric.project.double_revenue", metrics)
        assert result == {"model.project.fact_sale"}


# ─── extract_aliases ──────────────────────────────────────────────────────
class TestExtractAliases:
    def test_basic(self):
        parsed = parse_one("SELECT amount AS total, price AS unit_price FROM t")
        aliases = extract_aliases(parsed)
        assert "total" in aliases
        assert "unit_price" in aliases

    def test_no_aliases(self):
        parsed = parse_one("SELECT amount, price FROM t")
        aliases = extract_aliases(parsed)
        # No aliases — result might be empty or have column-level aliases
        assert isinstance(aliases, dict)


# ─── apply_column_rename_map ─────────────────────────────────────────────
class TestApplyColumnRenameMap:
    def test_basic_rename(self):
        result = apply_column_rename_map(
            "SUM(amount)",
            {"amount": "fact_sale__amount"},
            MFSQLEngine.TRINO,
        )
        assert "fact_sale__amount" in result
        assert "SUM" in result

    def test_empty_map_noop(self):
        result = apply_column_rename_map(
            "SUM(amount)",
            {},
            MFSQLEngine.TRINO,
        )
        assert result == "SUM(amount)"

    def test_multiple_renames(self):
        result = apply_column_rename_map(
            "SUM(amount) + SUM(cost)",
            {"amount": "t__amount", "cost": "t__cost"},
            MFSQLEngine.TRINO,
        )
        assert "t__amount" in result
        assert "t__cost" in result


# ─── replace_metric_syntax ───────────────────────────────────────────────
class TestReplaceMetricSyntax:
    def test_basic_replacement(self):
        metrics = {
            "revenue": {
                "name": "revenue",
                "type": "sum",
                "sql": "amount",
                "depends_on": [],
                "meta": {},
            },
        }
        result = replace_metric_syntax(
            "revenue * 2",
            ["metric.project.revenue"],
            metrics,
        )
        assert "SUM(amount)" in result
        assert "* 2" in result


# ─── wrap_with_where ──────────────────────────────────────────────────────
class TestWrapWithWhere:
    def test_sum_with_where(self):
        ast = parse_one("SUM(amount)")
        where = parse_one("status = 'active'")
        result = wrap_with_where(ast, where)
        sql = result.sql()
        assert "CASE WHEN" in sql
        assert "SUM" in sql

    def test_count_star_with_where(self):
        ast = parse_one("COUNT(*)")
        where = parse_one("is_active = TRUE")
        result = wrap_with_where(ast, where)
        sql = result.sql()
        assert "CASE WHEN" in sql
        assert "COUNT" in sql

    def test_count_distinct_with_where(self):
        ast = parse_one("COUNT(DISTINCT customer_id)")
        where = parse_one("status = 'active'")
        result = wrap_with_where(ast, where)
        sql = result.sql()
        assert "CASE WHEN" in sql
        assert "COUNT" in sql

    def test_no_agg_wraps_entire(self):
        ast = parse_one("amount + cost")
        where = parse_one("status = 'active'")
        result = wrap_with_where(ast, where)
        sql = result.sql()
        assert "CASE WHEN" in sql


# ─── get_models_from_sql ─────────────────────────────────────────────────
class TestGetModelsFromSql:
    def test_known_model(self):
        model_map = {
            ModelKey("gold", "fact_sale"): {
                "name": "fact_sale",
                "unique_id": "model.project.fact_sale",
                "schema": "gold",
            },
        }
        sql = 'SELECT * FROM "gold"."fact_sale"'
        result = get_models_from_sql(sql, MFSQLEngine.TRINO, model_map)
        assert result is not None
        assert len(result) == 1
        assert result[0]["name"] == "fact_sale"

    def test_unknown_model_returns_none(self):
        model_map = {}
        sql = 'SELECT * FROM "gold"."unknown_table"'
        result = get_models_from_sql(sql, MFSQLEngine.TRINO, model_map)
        assert result is None


# ─── convert_metric_flow_to_superset ─────────────────────────────────────
class TestConvertMetricFlowToSuperset:
    def test_simple_metric(self):
        sl_metric = {
            "name": "total_revenue",
            "label": "Total Revenue",
            "type": "SIMPLE",
            "description": "Sum of revenue",
            "sql": """
                SELECT SUM(amount) AS total_amount
                FROM (
                    SELECT amount AS amount
                    FROM "db"."gold"."fact_sale" fact_sale_src
                ) subq_1
            """,
            "dialect": MFSQLEngine.TRINO,
            "model": "model.project.fact_sale",
            "meta": {},
        }
        result = convert_metric_flow_to_superset(sl_metric)
        assert result["metric_name"] == "total_revenue"
        assert result["verbose_name"] == "Total Revenue"
        assert "SUM" in result["expression"]

    def test_metric_with_rename_map(self):
        sl_metric = {
            "name": "total_revenue",
            "label": "Total Revenue",
            "type": "SIMPLE",
            "description": "Sum of revenue",
            "sql": """
                SELECT SUM(amount) AS total_amount
                FROM (
                    SELECT amount AS amount
                    FROM "db"."gold"."fact_sale" fact_sale_src
                ) subq_1
            """,
            "dialect": MFSQLEngine.TRINO,
            "model": "model.project.fact_sale",
            "meta": {},
            "column_rename_map": {"amount": "fact_sale__amount"},
        }
        result = convert_metric_flow_to_superset(sl_metric)
        assert "fact_sale__amount" in result["expression"]


# ─── get_metric_definition ───────────────────────────────────────────────
class TestGetMetricDefinition:
    def test_basic(self):
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
                "sql": "amount",
                "label": "Revenue",
                "description": "Total revenue",
                "meta": {},
            },
        ]
        result = get_metric_definition("revenue", metrics)
        assert result["metric_name"] == "revenue"
        assert result["expression"] == "SUM(amount)"
        assert result["verbose_name"] == "Revenue"
        assert result["description"] == "Total revenue"

    def test_with_metric_name_override(self):
        metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
                "sql": "amount",
                "label": "Revenue",
                "description": "",
                "meta": {"superset": {"metric_name": "custom_revenue_name"}},
            },
        ]
        result = get_metric_definition("revenue", metrics)
        assert result["metric_name"] == "custom_revenue_name"


# ─── get_superset_metrics_per_model ──────────────────────────────────────
class TestGetSupersetMetricsPerModel:
    def test_og_metrics_only(self):
        og_metrics = [
            {
                "name": "revenue",
                "unique_id": "metric.project.revenue",
                "depends_on": ["model.project.fact_sale"],
                "type": "sum",
                "sql": "amount",
                "label": "Revenue",
                "description": "",
                "meta": {},
            },
        ]
        result = get_superset_metrics_per_model(og_metrics)
        assert "model.project.fact_sale" in result
        assert len(result["model.project.fact_sale"]) == 1
        assert result["model.project.fact_sale"][0]["metric_name"] == "revenue"

    def test_sl_metrics(self):
        sl_metrics = [
            {
                "name": "total_amount",
                "label": "Total Amount",
                "type": "SIMPLE",
                "description": "",
                "sql": """
                    SELECT SUM(amount) AS total
                    FROM (
                        SELECT amount AS amount
                        FROM "db"."gold"."fact_sale" src
                    ) subq
                """,
                "dialect": MFSQLEngine.TRINO,
                "model": "model.project.fact_sale",
                "meta": {},
            },
        ]
        result = get_superset_metrics_per_model([], sl_metrics)
        assert "model.project.fact_sale" in result

    def test_metric_with_model_in_meta(self):
        """Metric with meta.superset.model should be assigned to that model."""
        og_metrics = [
            {
                "name": "custom_metric",
                "unique_id": "metric.project.custom_metric",
                "depends_on": [],
                "type": "expression",
                "sql": "1 + 1",
                "label": "Custom",
                "description": "",
                "meta": {"superset": {"model": "model.project.custom_target"}},
                "skip_parsing": True,
                "dialect": "trino",
            },
        ]
        result = get_superset_metrics_per_model(og_metrics)
        assert "model.project.custom_target" in result


# ─── DIALECT_MAP ──────────────────────────────────────────────────────────
class TestDialectMap:
    def test_all_engines_mapped(self):
        for engine in MFSQLEngine:
            assert engine in DIALECT_MAP, f"Missing DIALECT_MAP entry for {engine}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
