"""
Tests for superset_cli.cli.metricflow — MetricFlow output processing.

Covers:
  - clean_mf_sql_output (ANSI stripping, non-SQL removal)
  - _build_single_model_column_metadata (column aliasing logic)
"""

import pytest

from cli.metricflow import (
    clean_mf_sql_output,
    _build_single_model_column_metadata,
)


# ─── clean_mf_sql_output ─────────────────────────────────────────────────
class TestCleanMfSqlOutput:
    def test_pure_sql(self):
        """Clean SQL should pass through unchanged."""
        sql = "SELECT SUM(amount) FROM fact_sale"
        result = clean_mf_sql_output(sql)
        assert result == sql

    def test_strips_ansi_codes(self):
        raw = "\x1b[32mSELECT\x1b[0m SUM(amount) FROM fact_sale"
        result = clean_mf_sql_output(raw)
        assert "\x1b[" not in result
        assert "SELECT" in result

    def test_strips_leading_warnings(self):
        raw = (
            "⚠ A new version of MetricFlow is available\n"
            "💡 Run `pip install metricflow` to upgrade\n"
            "\n"
            "SELECT\n"
            "    SUM(amount) AS total\n"
            "FROM fact_sale"
        )
        result = clean_mf_sql_output(raw)
        assert result.startswith("SELECT")
        assert "SUM(amount)" in result

    def test_strips_trailing_noise(self):
        raw = (
            "SELECT\n"
            "    SUM(amount) AS total\n"
            "FROM fact_sale\n"
            "\n"
            "⚠ Some trailing warning\n"
        )
        result = clean_mf_sql_output(raw)
        assert "trailing warning" not in result
        assert "SELECT" in result

    def test_no_sql_returns_empty(self):
        raw = "⚠ Some warning message\n💡 Upgrade notice"
        result = clean_mf_sql_output(raw)
        assert result == ""

    def test_empty_input(self):
        result = clean_mf_sql_output("")
        assert result == ""

    def test_with_statement(self):
        raw = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        result = clean_mf_sql_output(raw)
        assert "WITH" in result

    def test_mixed_ansi_and_warnings(self):
        raw = (
            "\x1b[33m⚠ Upgrade available\x1b[0m\n"
            "\n"
            "\x1b[32mSELECT\x1b[0m\n"
            "    SUM(amount)\n"
            "FROM fact_sale\n"
            "\n"
            "\x1b[33m⚠ Deprecation\x1b[0m"
        )
        result = clean_mf_sql_output(raw)
        assert "SELECT" in result
        assert "SUM(amount)" in result
        assert "Upgrade" not in result
        assert "\x1b[" not in result

    def test_multiline_sql_preserved(self):
        raw = (
            "SELECT\n"
            "    SUM(sale_amount) AS sale_amount\n"
            "FROM (\n"
            "    SELECT\n"
            "        sale_amount AS sale_amount\n"
            '    FROM "lakehouse"."gold"."fact_sale" fact_sale_src\n'
            ") subq_1"
        )
        result = clean_mf_sql_output(raw)
        assert "subq_1" in result
        assert result.startswith("SELECT")


# ─── _build_single_model_column_metadata ─────────────────────────────────
class TestBuildSingleModelColumnMetadata:
    def test_no_measurement_columns(self):
        model = {
            "name": "fact_sale",
            "columns": [
                {"name": "customer_id", "meta": {}, "description": "Customer ID"},
                {"name": "product_id", "meta": {}, "description": "Product ID"},
            ],
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert rename_map == {}
        assert set(select_cols) == {"customer_id", "product_id"}
        assert descriptions["customer_id"] == "Customer ID"

    def test_with_measurement_columns(self):
        model = {
            "name": "fact_sale",
            "columns": [
                {"name": "customer_id", "meta": {}, "description": "Customer ID"},
                {
                    "name": "amount",
                    "meta": {"is_measurement": True},
                    "description": "Sale amount",
                },
            ],
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert rename_map == {"amount": "fact_sale__amount"}
        assert "fact_sale__amount" in select_cols
        assert "customer_id" in select_cols
        assert descriptions["fact_sale__amount"] == "Sale amount"

    def test_with_labels(self):
        model = {
            "name": "fact_sale",
            "columns": [
                {
                    "name": "amount",
                    "meta": {"is_measurement": True},
                    "description": "Amount",
                    "label": "Doanh số",
                },
                {
                    "name": "customer_id",
                    "meta": {},
                    "description": "ID",
                    "label": "Mã KH",
                },
            ],
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert labels["fact_sale__amount"] == "Doanh số"
        assert labels["customer_id"] == "Mã KH"

    def test_dict_columns(self):
        """Columns can be a dict (dbt manifest format)."""
        model = {
            "name": "fact_sale",
            "columns": {
                "customer_id": {
                    "name": "customer_id",
                    "meta": {},
                    "description": "Customer ID",
                },
                "amount": {
                    "name": "amount",
                    "meta": {"is_measurement": True},
                    "description": "Sale amount",
                },
            },
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert "amount" in rename_map
        assert rename_map["amount"] == "fact_sale__amount"

    def test_config_meta_measurement(self):
        """is_measurement can also be in column.config.meta."""
        model = {
            "name": "fact_sale",
            "columns": [
                {
                    "name": "cost",
                    "meta": {},
                    "config": {"meta": {"is_measurement": True}},
                    "description": "Cost",
                },
            ],
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert "cost" in rename_map
        assert rename_map["cost"] == "fact_sale__cost"

    def test_empty_columns(self):
        model = {
            "name": "fact_sale",
            "columns": [],
        }
        rename_map, descriptions, labels, select_cols = (
            _build_single_model_column_metadata(model)
        )
        assert rename_map == {}
        assert select_cols == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
