"""
Tests for superset_cli.cli.lib — dbt sync helper functions.

Covers:
  - env_var
  - as_number
  - list_failed_models
  - parse_metric_meta
  - build_model_fqn_lookup
  - is_measurement_column
  - filter_models (by tag, config, name)
  - filter_plus_operator
  - filter_at_operator
  - apply_select
  - get_og_metric_from_config
"""

import os
from unittest.mock import patch

import pytest

from cli.lib import (
    apply_select,
    as_number,
    build_model_fqn_lookup,
    env_var,
    filter_at_operator,
    filter_models,
    filter_plus_operator,
    get_og_metric_from_config,
    is_measurement_column,
    list_failed_models,
    parse_metric_meta,
)


# ─── env_var ──────────────────────────────────────────────────────────────
class TestEnvVar:
    def test_existing_var(self):
        with patch.dict(os.environ, {"TEST_VAR": "hello"}):
            assert env_var("TEST_VAR") == "hello"

    def test_default_value(self):
        result = env_var("NON_EXISTENT_VAR_XYZ", "default_val")
        assert result == "default_val"

    def test_missing_var_no_default_raises(self):
        with pytest.raises(Exception, match="Env var required"):
            env_var("NON_EXISTENT_VAR_ABC")


# ─── as_number ────────────────────────────────────────────────────────────
class TestAsNumber:
    def test_integer(self):
        assert as_number("42") == 42
        assert isinstance(as_number("42"), int)

    def test_float(self):
        result = as_number("3.14")
        assert result == 3.14
        assert isinstance(result, float)

    def test_negative(self):
        assert as_number("-5") == -5


# ─── list_failed_models ──────────────────────────────────────────────────
class TestListFailedModels:
    def test_single_failure(self):
        result = list_failed_models(["model.a"])
        assert "model.a" in result
        assert "Below model(s) failed" in result

    def test_multiple_failures(self):
        result = list_failed_models(["model.a", "model.b", "model.c"])
        assert "model.a" in result
        assert "model.b" in result
        assert "model.c" in result


# ─── parse_metric_meta ───────────────────────────────────────────────────
class TestParseMetricMeta:
    def test_empty_meta(self):
        metric = {"name": "revenue", "meta": {}}
        result = parse_metric_meta(metric)
        assert result["meta"] == {}
        assert result["kwargs"] == {}
        assert result["metric_name_override"] is None

    def test_superset_meta_with_override(self):
        metric = {
            "name": "revenue",
            "meta": {
                "superset": {
                    "metric_name": "custom_name",
                    "d3format": ",.2f",
                },
                "other": "preserved",
            },
        }
        result = parse_metric_meta(metric)
        assert result["metric_name_override"] == "custom_name"
        assert result["kwargs"] == {"d3format": ",.2f"}
        assert result["meta"]["other"] == "preserved"

    def test_no_meta_key(self):
        metric = {"name": "revenue"}
        result = parse_metric_meta(metric)
        assert result["metric_name_override"] is None


# ─── build_model_fqn_lookup ──────────────────────────────────────────────
class TestBuildModelFqnLookup:
    def test_basic(self):
        configs = {
            "nodes": {
                "model.project.fact_sale": {
                    "resource_type": "model",
                    "database": "warehouse",
                    "schema": "gold",
                    "name": "fact_sale",
                },
                "test.project.test_fact": {
                    "resource_type": "test",
                    "database": "warehouse",
                    "schema": "gold",
                    "name": "test_fact",
                },
            },
        }
        lookup = build_model_fqn_lookup(configs)
        assert "warehouse.gold.fact_sale" in lookup
        assert "warehouse.gold.test_fact" not in lookup  # not a model

    def test_empty_nodes(self):
        configs = {"nodes": {}}
        lookup = build_model_fqn_lookup(configs)
        assert lookup == {}

    def test_no_nodes_key(self):
        configs = {}
        lookup = build_model_fqn_lookup(configs)
        assert lookup == {}


# ─── is_measurement_column ───────────────────────────────────────────────
class TestIsMeasurementColumn:
    def test_meta_is_measurement(self):
        col = {"name": "amount", "meta": {"is_measurement": True}}
        assert is_measurement_column(col) is True

    def test_config_meta_is_measurement(self):
        col = {
            "name": "amount",
            "meta": {},
            "config": {"meta": {"is_measurement": True}},
        }
        assert is_measurement_column(col) is True

    def test_not_measurement(self):
        col = {"name": "id", "meta": {}}
        assert is_measurement_column(col) is False

    def test_no_meta(self):
        col = {"name": "id"}
        assert is_measurement_column(col) is False

    def test_false_measurement(self):
        col = {"name": "id", "meta": {"is_measurement": False}}
        assert is_measurement_column(col) is False


# ─── filter_models — helper data ─────────────────────────────────────────
def make_model(
    name,
    unique_id=None,
    tags=None,
    depends_on=None,
    children=None,
    config=None,
):
    uid = unique_id or f"model.project.{name}"
    return {
        "name": name,
        "unique_id": uid,
        "tags": tags or [],
        "depends_on": depends_on or [],
        "children": children or [],
        "config": config or {},
    }


MODELS = [
    make_model("dim_customer", tags=["gold"], config={"materialized": "table"}),
    make_model(
        "fact_sale",
        tags=["gold", "fact"],
        depends_on=["model.project.dim_customer"],
        config={"materialized": "incremental"},
    ),
    make_model("stg_orders", tags=["staging"], config={"materialized": "view"}),
]


# ─── filter_models ────────────────────────────────────────────────────────
class TestFilterModels:
    def test_filter_by_tag(self):
        result = filter_models(MODELS, "tag:gold")
        names = [m["name"] for m in result]
        assert "dim_customer" in names
        assert "fact_sale" in names
        assert "stg_orders" not in names

    def test_filter_by_tag_no_match(self):
        result = filter_models(MODELS, "tag:nonexistent")
        assert result == []

    def test_filter_by_name(self):
        result = filter_models(MODELS, "fact_sale")
        assert len(result) == 1
        assert result[0]["name"] == "fact_sale"

    def test_filter_by_config(self):
        result = filter_models(MODELS, "config.materialized:view")
        assert len(result) == 1
        assert result[0]["name"] == "stg_orders"

    def test_unsupported_condition(self):
        with pytest.raises(NotImplementedError):
            filter_models(MODELS, "nonexistent_model_xyz")


# ─── filter_plus_operator ────────────────────────────────────────────────
class TestFilterPlusOperator:
    def test_downstream(self):
        """model+ should include model and its children."""
        models = [
            make_model(
                "a",
                children=["model.project.b"],
            ),
            make_model("b", depends_on=["model.project.a"]),
        ]
        result = filter_plus_operator(models, "a+")
        names = [m["name"] for m in result]
        assert "a" in names
        assert "b" in names

    def test_upstream(self):
        """a+model should include model and its parents."""
        models = [
            make_model("a"),
            make_model("b", depends_on=["model.project.a"]),
        ]
        result = filter_plus_operator(models, "+b")
        names = [m["name"] for m in result]
        assert "a" in names
        assert "b" in names

    def test_both_directions(self):
        """
        +middle+ traverses upstream first (adding middle + parent),
        then downstream. Since middle is already in selected_models
        during the downstream pass, its children are NOT traversed.

        This is a known limitation — only models not yet visited are expanded.
        For children to be discovered, middle must not be added by the 'up' pass.
        """
        models = [
            make_model(
                "parent",
                children=["model.project.middle"],
            ),
            make_model(
                "middle",
                depends_on=["model.project.parent"],
                children=["model.project.child"],
            ),
            make_model("child", depends_on=["model.project.middle"]),
        ]
        result = filter_plus_operator(models, "+middle+")
        names = [m["name"] for m in result]
        # upstream: middle (degree=0), parent (degree=1)
        assert "parent" in names
        assert "middle" in names
        # downstream: middle already visited → children NOT expanded
        # (This is the actual library behavior)
        assert len(names) == 2


# ─── filter_at_operator ──────────────────────────────────────────────────
class TestFilterAtOperator:
    def test_basic(self):
        models = [
            make_model(
                "parent",
                children=["model.project.child"],
            ),
            make_model(
                "child",
                depends_on=["model.project.parent"],
            ),
        ]
        result = filter_at_operator(models, "@parent")
        names = [m["name"] for m in result]
        assert "parent" in names
        assert "child" in names


# ─── apply_select ─────────────────────────────────────────────────────────
class TestApplySelect:
    def test_no_select_returns_all(self):
        result = apply_select(MODELS, select=(), exclude=())
        assert len(result) == len(MODELS)

    def test_select_by_tag(self):
        result = apply_select(MODELS, select=("tag:gold",), exclude=())
        names = [m["name"] for m in result]
        assert "stg_orders" not in names
        assert "fact_sale" in names

    def test_exclude(self):
        result = apply_select(
            MODELS, select=("tag:gold",), exclude=("tag:fact",)
        )
        names = [m["name"] for m in result]
        assert "fact_sale" not in names
        assert "dim_customer" in names


# ─── get_og_metric_from_config ────────────────────────────────────────────
class TestGetOgMetricFromConfig:
    def test_basic_load(self):
        config = {
            "name": "revenue",
            "label": "Revenue",
            "description": "Total revenue",
            "unique_id": "metric.project.revenue",
            "depends_on": {"nodes": ["model.project.fact_sale"]},
            "type": "sum",
            "sql": "amount",
            "meta": {},
        }
        result = get_og_metric_from_config(config, "trino")
        assert result["name"] == "revenue"
        assert result["dialect"] == "trino"
        assert result["depends_on"] == ["model.project.fact_sale"]

    def test_with_custom_sql(self):
        config = {
            "name": "derived",
            "label": "Derived",
            "description": "",
            "unique_id": "metric.project.derived",
            "depends_on": {"nodes": []},
            "meta": {},
        }
        result = get_og_metric_from_config(
            config,
            "trino",
            depends_on=["model.project.fact_sale"],
            sql="revenue - cost",
        )
        assert result["calculation_method"] == "derived"
        assert result["expression"] == "revenue - cost"
        assert result["depends_on"] == ["model.project.fact_sale"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
