"""
Test that MFMetricWithSQLSchema preserves model_sql and other custom fields
through marshmallow .load().
"""
from superset_cli.api.clients.dbt import MFMetricWithSQLSchema

mf_metric_schema = MFMetricWithSQLSchema()

# Simulate what get_sl_metric returns for a derived metric (e.g. sale_vs_forecast_gap)
fake_derived_metric_data = {
    "name": "sale_vs_forecast_gap",
    "label": "Chênh lệch thực tế vs dự báo (HFL)",
    "type": "DERIVED",
    "description": "Test derived metric",
    "sql": "SUM(sale_amount) - SUM(allocated_amount)",
    "dialect": "SNOWFLAKE",
    "model": None,
    "meta": {"ref_dataset": "loi_nhuan_dataset"},
    "model_sql": "WITH cte_0 AS (\n  SELECT * FROM dp_warehouse.gold.fact_agg_sale_by_month\n), cte_1 AS (\n  SELECT * FROM dp_warehouse.gold.fact_sf_month_allocated\n)\nSELECT * FROM cte_0\nFULL OUTER JOIN cte_1 ON cte_0.cost_center_code = cte_1.cost_center_code",
    "column_rename_map": {"sale_amount": "fact_agg_sale_by_month__sale_amount"},
    "column_labels": {"fact_agg_sale_by_month__sale_amount": "Doanh số"},
    "column_descriptions": {"fact_agg_sale_by_month__sale_amount": "Doanh số bán hàng"},
    "select_columns": ["cost_center_code", "fact_agg_sale_by_month__sale_amount"],
}

result = mf_metric_schema.load(fake_derived_metric_data)

print("=== After mf_metric_schema.load() ===")
print(f"model_sql present: {'model_sql' in result}")
print(f"model_sql value: {result.get('model_sql', '<<MISSING>>')}")
print()
print(f"column_rename_map present: {'column_rename_map' in result}")
print(f"column_rename_map value: {result.get('column_rename_map', '<<MISSING>>')}")
print()
print(f"select_columns present: {'select_columns' in result}")
print(f"select_columns value: {result.get('select_columns', '<<MISSING>>')}")
print()
print(f"column_labels present: {'column_labels' in result}")
print(f"column_descriptions present: {'column_descriptions' in result}")

# Verify the critical field
has_full_outer_join = "FULL OUTER JOIN" in (result.get("model_sql") or "")
print(f"\nmodel_sql contains FULL OUTER JOIN: {has_full_outer_join}")

if result.get("model_sql") and has_full_outer_join:
    print("\n✅ PASS: model_sql survived marshmallow schema with FULL OUTER JOIN intact")
else:
    print("\n❌ FAIL: model_sql was stripped or missing!")
