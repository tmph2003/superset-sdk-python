"""
Tests for superset_cli.api.clients.dbt — dbt schema definitions.

Covers:
  - PostelSchema (unknown fields handling)
  - PostelEnumField
  - ModelSchema
  - FilterSchema
  - MetricSchema / OGMetricSchema
  - MFMetricType / MFSQLEngine enums
  - MFMetricSchema / MFMetricWithSQLSchema
"""

import pytest

from superset_cli.api.clients.dbt import (
    FilterSchema,
    MFMetricSchema,
    MFMetricType,
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
    OGMetricSchema,
    PostelSchema,
)


# ─── PostelSchema ─────────────────────────────────────────────────────────
class TestPostelSchema:
    def test_allows_unknown_fields(self):
        """PostelSchema should silently accept unknown fields."""
        schema = PostelSchema()
        data = {"known_field": "value", "unknown_field": 123}
        result = schema.load(data)
        assert result["unknown_field"] == 123

    def test_empty_data(self):
        schema = PostelSchema()
        result = schema.load({})
        assert result == {}


# ─── ModelSchema ──────────────────────────────────────────────────────────
class TestModelSchema:
    def test_basic_load(self):
        schema = ModelSchema()
        data = {
            "name": "fact_sale",
            "uniqueId": "model.project.fact_sale",
            "database": "warehouse",
            "schema": "gold",
            "description": "Sales fact table",
            "meta": {},
            "tags": ["gold"],
            "columns": None,
            "config": {},
        }
        result = schema.load(data)
        assert result["name"] == "fact_sale"
        assert result["unique_id"] == "model.project.fact_sale"
        assert result["database"] == "warehouse"
        assert result["tags"] == ["gold"]

    def test_depends_on_deserialization(self):
        schema = ModelSchema()
        data = {
            "name": "test",
            "uniqueId": "model.test",
            "dependsOn": ["model.project.dim_a"],
            "childrenL1": ["model.project.fact_b"],
        }
        result = schema.load(data)
        assert result["depends_on"] == ["model.project.dim_a"]
        assert result["children"] == ["model.project.fact_b"]

    def test_unknown_fields_preserved(self):
        """ModelSchema inherits from PostelSchema — unknown fields are kept."""
        schema = ModelSchema()
        data = {"name": "test", "uniqueId": "uid", "custom_field": "hello"}
        result = schema.load(data)
        assert result["custom_field"] == "hello"


# ─── FilterSchema ─────────────────────────────────────────────────────────
class TestFilterSchema:
    def test_basic_load(self):
        schema = FilterSchema()
        data = {"field": "status", "operator": "is", "value": "active"}
        result = schema.load(data)
        assert result["field"] == "status"
        assert result["operator"] == "is"
        assert result["value"] == "active"


# ─── OGMetricSchema ──────────────────────────────────────────────────────
class TestOGMetricSchema:
    def test_basic_load(self):
        schema = OGMetricSchema()
        data = {
            "name": "revenue",
            "label": "Total Revenue",
            "description": "Sum of revenue",
            "uniqueId": "metric.project.revenue",
            "dependsOn": ["model.project.fact_sale"],
            "type": "sum",
            "sql": "revenue_amount",
            "meta": {},
        }
        result = schema.load(data)
        assert result["name"] == "revenue"
        assert result["unique_id"] == "metric.project.revenue"
        assert result["depends_on"] == ["model.project.fact_sale"]
        assert result["type"] == "sum"

    def test_dbt_13_fields(self):
        """dbt >= 1.3 uses calculation_method + expression."""
        schema = OGMetricSchema()
        data = {
            "name": "revenue",
            "uniqueId": "metric.revenue",
            "dependsOn": [],
            "calculation_method": "sum",
            "expression": "amount",
            "dialect": "trino",
        }
        result = schema.load(data)
        assert result["calculation_method"] == "sum"
        assert result["expression"] == "amount"

    def test_skip_parsing_flag(self):
        schema = OGMetricSchema()
        data = {
            "name": "custom",
            "uniqueId": "metric.custom",
            "dependsOn": [],
            "skip_parsing": True,
        }
        result = schema.load(data)
        assert result["skip_parsing"] is True


# ─── MFMetricType enum ───────────────────────────────────────────────────
class TestMFMetricType:
    def test_all_types(self):
        assert MFMetricType.SIMPLE == "SIMPLE"
        assert MFMetricType.RATIO == "RATIO"
        assert MFMetricType.CUMULATIVE == "CUMULATIVE"
        assert MFMetricType.DERIVED == "DERIVED"

    def test_is_str_subclass(self):
        assert isinstance(MFMetricType.SIMPLE, str)


# ─── MFSQLEngine enum ────────────────────────────────────────────────────
class TestMFSQLEngine:
    def test_all_engines(self):
        assert MFSQLEngine.BIGQUERY == "BIGQUERY"
        assert MFSQLEngine.DUCKDB == "DUCKDB"
        assert MFSQLEngine.REDSHIFT == "REDSHIFT"
        assert MFSQLEngine.POSTGRES == "POSTGRES"
        assert MFSQLEngine.SNOWFLAKE == "SNOWFLAKE"
        assert MFSQLEngine.DATABRICKS == "DATABRICKS"
        assert MFSQLEngine.TRINO == "TRINO"

    def test_is_str_subclass(self):
        assert isinstance(MFSQLEngine.TRINO, str)


# ─── MFMetricWithSQLSchema ───────────────────────────────────────────────
class TestMFMetricWithSQLSchema:
    def test_basic_load(self):
        schema = MFMetricWithSQLSchema()
        data = {
            "name": "total_revenue",
            "label": "Total Revenue",
            "type": "SIMPLE",
            "description": "Sum of sale amounts",
            "sql": "SELECT SUM(amount) FROM fact_sale",
            "dialect": "TRINO",
            "model": "model.project.fact_sale",
            "meta": {},
        }
        result = schema.load(data)
        assert result["name"] == "total_revenue"
        assert result["sql"] == "SELECT SUM(amount) FROM fact_sale"
        assert result["dialect"] == "TRINO"
        assert result["model"] == "model.project.fact_sale"

    def test_null_model(self):
        """model can be None for derived metrics."""
        schema = MFMetricWithSQLSchema()
        data = {
            "name": "derived",
            "label": "Derived",
            "type": "DERIVED",
            "description": "",
            "sql": "SELECT 1",
            "dialect": "TRINO",
            "model": None,
        }
        result = schema.load(data)
        assert result["model"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
