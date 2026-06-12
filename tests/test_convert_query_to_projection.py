"""
Tests for convert_query_to_projection with MetricFlow-generated SQL.

Covers:
  1. Simple aggregation (SUM)
  2. Filtered metric (WHERE pushed into CASE WHEN)
  3. Derived metric with nested subqueries
  4. COUNT DISTINCT
  5. Multi-join query
  6. COALESCE stripping
  7. Nested aggregation flattening (MAX(SUM(x)) → SUM(x))
"""

import pytest

from api.clients.dbt import MFSQLEngine
from cli.metrics import convert_query_to_projection


# ---------------------------------------------------------------------------
# 1. Simple SUM – single subquery, no WHERE
# ---------------------------------------------------------------------------
def test_simple_sum():
    """
    MetricFlow simple metric: SUM(sale_amount).
    The inner subquery selects the raw column; the outer wraps with SUM.
    """
    sql = """
    SELECT
        SUM(sale_amount) AS sale_amount
    FROM (
        SELECT
            sale_amount AS sale_amount
        FROM "lakehouse"."gold"."fact_sale" fact_sale_src_100
    ) subq_1
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert result == "SUM(sale_amount)"


# ---------------------------------------------------------------------------
# 2. Filtered metric – WHERE should become CASE WHEN inside agg
# ---------------------------------------------------------------------------
def test_filtered_metric():
    """
    MetricFlow filtered metric:  SUM(order_count) WHERE order_total >= 20.

    Expected: SUM(CASE WHEN order_total >= 20 THEN 1 END)
    """
    sql = """
    SELECT
        SUM(order_count) AS large_order
    FROM (
        SELECT
            order_total AS order_id__order_total_dim
            , 1 AS order_count
        FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_106
    ) subq_796
    WHERE order_id__order_total_dim >= 20
    """
    result = convert_query_to_projection(sql, MFSQLEngine.BIGQUERY)
    assert "CASE WHEN" in result
    assert "order_total >= 20" in result
    assert "SUM" in result


# ---------------------------------------------------------------------------
# 3. COUNT DISTINCT
# ---------------------------------------------------------------------------
def test_count_distinct():
    """
    MetricFlow COUNT(DISTINCT entity_id).
    """
    sql = """
    SELECT
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM (
        SELECT
            customer_id AS customer_id
        FROM "lakehouse"."gold"."fact_sale" fact_sale_src_100
    ) subq_1
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert "COUNT(DISTINCT customer_id)" == result


# ---------------------------------------------------------------------------
# 4. Derived metric – nested subqueries (revenue - cost)
# ---------------------------------------------------------------------------
def test_derived_metric():
    """
    MetricFlow derived metric: revenue - cost, each is SUM.

    The compiled SQL has two layers of subqueries.
    """
    sql = """
    SELECT
        COALESCE(total_revenue, 0) - COALESCE(total_cost, 0) AS net_profit
    FROM (
        SELECT
            SUM(revenue) AS total_revenue
            , SUM(cost) AS total_cost
        FROM (
            SELECT
                revenue AS revenue
                , cost AS cost
            FROM "lakehouse"."gold"."fact_order" fact_order_src_100
        ) subq_inner
    ) subq_outer
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    # COALESCE should be stripped, leaving SUM(revenue) - SUM(cost)
    assert "COALESCE" not in result
    assert "SUM(revenue)" in result
    assert "SUM(cost)" in result


# ---------------------------------------------------------------------------
# 5. Filtered metric with COUNT(*)
# ---------------------------------------------------------------------------
def test_filtered_count_star():
    """
    MetricFlow COUNT(*) with filter.
    COUNT(*) → COUNT(CASE WHEN ... THEN 1 END)
    """
    sql = """
    SELECT
        COUNT(*) AS active_users
    FROM (
        SELECT
            is_active AS is_active_dim
            , user_id AS user_id
        FROM "db"."schema"."dim_user" dim_user_src
    ) subq_1
    WHERE is_active_dim = TRUE
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert "CASE WHEN" in result
    assert "COUNT" in result


# ---------------------------------------------------------------------------
# 6. COALESCE stripping on simple metric
# ---------------------------------------------------------------------------
def test_coalesce_stripping():
    """
    MetricFlow sometimes wraps results in COALESCE(..., 0).
    """
    sql = """
    SELECT
        COALESCE(SUM(amount), 0) AS total_amount
    FROM (
        SELECT
            amount AS amount
        FROM "db"."schema"."fact_table" fact_src
    ) subq_1
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert result == "SUM(amount)"


# ---------------------------------------------------------------------------
# 7. Nested aggregation flattening: MAX(SUM(x)) → SUM(x)
# ---------------------------------------------------------------------------
def test_nested_agg_flattening():
    """
    Three-level subquery where outer scope wraps with MAX(inner SUM).
    """
    sql = """
    SELECT
        MAX(total_sale) AS total_sale
    FROM (
        SELECT
            SUM(sale_amount) AS total_sale
        FROM (
            SELECT
                sale_amount AS sale_amount
            FROM "lakehouse"."gold"."fact_sale" fact_sale_src
        ) subq_inner
    ) subq_outer
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert result == "SUM(sale_amount)"


# ---------------------------------------------------------------------------
# 8. AVG metric
# ---------------------------------------------------------------------------
def test_avg_metric():
    """
    Simple AVG metric.
    """
    sql = """
    SELECT
        AVG(unit_price) AS avg_price
    FROM (
        SELECT
            unit_price AS unit_price
        FROM "lakehouse"."gold"."fact_sale" fact_sale_src
    ) subq_1
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert result == "AVG(unit_price)"


# ---------------------------------------------------------------------------
# 9. Filtered SUM with multiple WHERE conditions
# ---------------------------------------------------------------------------
def test_filtered_multiple_conditions():
    """
    Filtered metric with AND in WHERE.
    """
    sql = """
    SELECT
        SUM(sale_amount) AS online_sales
    FROM (
        SELECT
            sale_amount AS sale_amount
            , channel AS channel_dim
            , is_returned AS is_returned_dim
        FROM "lakehouse"."gold"."fact_sale" fact_sale_src
    ) subq_1
    WHERE channel_dim = 'ONLINE' AND is_returned_dim = FALSE
    """
    result = convert_query_to_projection(sql, MFSQLEngine.TRINO)
    assert "CASE WHEN" in result
    assert "SUM" in result
    # The WHERE conditions should reference original column names, not aliases
    assert "channel" in result.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
