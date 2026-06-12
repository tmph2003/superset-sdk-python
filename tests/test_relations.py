"""
Tests for superset_cli.cli.relations — relation-based SQL generation.

Covers:
  - _get_fqn_parts
  - _build_cte (with and without measurement columns)
  - _build_join_conditions
  - ModelKey (from exposures)
"""

import pytest

from cli.relations import (
    _get_fqn_parts,
    _build_cte,
    _build_join_conditions,
)
from cli.lib import ModelKey


# ─── _get_fqn_parts ──────────────────────────────────────────────────────
class TestGetFqnParts:
    def test_basic(self):
        catalog, schema, table = _get_fqn_parts("warehouse.gold.fact_sale")
        assert catalog == "warehouse"
        assert schema == "gold"
        assert table == "fact_sale"

    def test_with_special_chars(self):
        catalog, schema, table = _get_fqn_parts("my-db.my_schema.my_table")
        assert catalog == "my-db"
        assert schema == "my_schema"
        assert table == "my_table"


# ─── _build_cte ──────────────────────────────────────────────────────────
class TestBuildCte:
    def test_with_measurements(self):
        cte_name, cte_sql = _build_cte(
            "warehouse.gold.fact_sale",
            join_cols=["customer_id", "product_id"],
            meas_cols=["amount", "quantity"],
        )
        assert cte_name == "cte_fact_sale"
        assert "SUM(amount)" in cte_sql
        assert "SUM(quantity)" in cte_sql
        assert "GROUP BY customer_id, product_id" in cte_sql
        assert "gold.fact_sale" in cte_sql

    def test_without_measurements(self):
        """No measurement columns → SELECT DISTINCT join cols only."""
        cte_name, cte_sql = _build_cte(
            "warehouse.gold.dim_customer",
            join_cols=["customer_id"],
            meas_cols=[],
        )
        assert cte_name == "cte_dim_customer"
        assert "SELECT DISTINCT" in cte_sql
        assert "customer_id" in cte_sql
        assert "GROUP BY" not in cte_sql

    def test_single_join_col(self):
        cte_name, cte_sql = _build_cte(
            "db.schema.fact_a",
            join_cols=["id"],
            meas_cols=["value"],
        )
        assert "GROUP BY id" in cte_sql
        assert "SUM(value)" in cte_sql

    def test_source_format(self):
        """Source should be schema.table (not catalog.schema.table)."""
        cte_name, cte_sql = _build_cte(
            "catalog.myschema.mytable",
            join_cols=["id"],
            meas_cols=[],
        )
        assert "myschema.mytable" in cte_sql
        # Should not include catalog in FROM
        assert "catalog.myschema.mytable" not in cte_sql


# ─── _build_join_conditions ──────────────────────────────────────────────
class TestBuildJoinConditions:
    def test_basic_join(self):
        """Two tables sharing a relation group on customer_id."""
        groups = {
            1: {
                "db.gold.fact_sale": ["customer_id"],
                "db.gold.fact_order": ["customer_id"],
            },
        }
        conditions = _build_join_conditions(
            current_table="db.gold.fact_order",
            current_alias="b",
            prev_tables=["db.gold.fact_sale"],
            alias_map={
                "db.gold.fact_sale": "a",
                "db.gold.fact_order": "b",
            },
            groups=groups,
        )
        assert len(conditions) == 1
        assert "a.customer_id = b.customer_id" in conditions[0]

    def test_no_matching_group(self):
        """If current table is not in any group, return empty."""
        groups = {
            1: {
                "db.gold.fact_sale": ["customer_id"],
                "db.gold.dim_customer": ["customer_id"],
            },
        }
        conditions = _build_join_conditions(
            current_table="db.gold.fact_unrelated",
            current_alias="c",
            prev_tables=["db.gold.fact_sale"],
            alias_map={
                "db.gold.fact_sale": "a",
                "db.gold.fact_unrelated": "c",
            },
            groups=groups,
        )
        assert conditions == []

    def test_multiple_groups(self):
        """Multiple relation groups generate multiple conditions."""
        groups = {
            1: {
                "db.gold.fact_a": ["id"],
                "db.gold.fact_b": ["id"],
            },
            2: {
                "db.gold.fact_a": ["date"],
                "db.gold.fact_b": ["date"],
            },
        }
        conditions = _build_join_conditions(
            current_table="db.gold.fact_b",
            current_alias="b",
            prev_tables=["db.gold.fact_a"],
            alias_map={
                "db.gold.fact_a": "a",
                "db.gold.fact_b": "b",
            },
            groups=groups,
        )
        assert len(conditions) == 2

    def test_no_relevant_prev_tables(self):
        """If prev_tables don't appear in the group, skip."""
        groups = {
            1: {
                "db.gold.fact_a": ["id"],
                "db.gold.fact_b": ["id"],
            },
        }
        conditions = _build_join_conditions(
            current_table="db.gold.fact_b",
            current_alias="b",
            prev_tables=["db.gold.fact_c"],  # not in group
            alias_map={
                "db.gold.fact_b": "b",
                "db.gold.fact_c": "c",
            },
            groups=groups,
        )
        assert conditions == []


# ─── ModelKey ─────────────────────────────────────────────────────────────
class TestModelKey:
    def test_creation(self):
        key = ModelKey(schema="gold", table="fact_sale")
        assert key.schema == "gold"
        assert key.table == "fact_sale"

    def test_as_dict_key(self):
        key1 = ModelKey("gold", "fact_sale")
        key2 = ModelKey("gold", "fact_sale")
        d = {key1: "value"}
        assert d[key2] == "value"

    def test_different_keys(self):
        key1 = ModelKey("gold", "fact_sale")
        key2 = ModelKey("silver", "fact_sale")
        assert key1 != key2

    def test_none_schema(self):
        key = ModelKey(schema=None, table="virtual_dataset")
        assert key.schema is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
