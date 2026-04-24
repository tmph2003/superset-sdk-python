"""
Test derived metric flow — simulates get_sl_metric's multi-model path.
Uses real manifest + real relation_members DB.
"""
import sys
import json

sys.path.insert(0, "src")

MANIFEST_PATH = r"d:\data_pipeline_transform\target\manifest.json"

print("=" * 70)
print("STEP 1: Load manifest")
print("=" * 70)
with open(MANIFEST_PATH, encoding="utf-8") as f:
    configs = json.load(f)
print("  OK")

# ── Simulate what get_sl_metric does when mf returns SQL with 2+ tables ──
# In reality, `mf query --explain --metrics net_profit` returns SQL like:
#   SELECT ... FROM gold.fact_agg_sale_by_month ... FULL JOIN gold.fact_salein_planned_information ...
# Then get_models_from_sql() parses it and finds 2 models.
# We simulate this by directly providing the table FQNs.

print()
print("=" * 70)
print("STEP 2: Simulate multi-model metric (get_sl_metric else branch)")
print("=" * 70)

# These are the 2 tables that relation_members DB has relationships for
table_fqns = [
    "dp_warehouse.gold.fact_agg_sale_by_month",
    "dp_warehouse.gold.fact_salein_planned_information",
]
print(f"  Simulated models: {table_fqns}")

# Simulate the metric config
metric = {
    "name": "net_profit",
    "label": "Net Profit",
    "type": "derived",
    "description": "Sale amount minus cost, joined with planned data",
    "meta": {"ref_dataset": "virtual_sale_vs_plan"},
}

print()
print("=" * 70)
print("STEP 3: handle_relation_derived_metric()")
print("=" * 70)

from superset_cli.cli.superset.sync.dbt.command import handle_relation_derived_metric

try:
    derived_sql = handle_relation_derived_metric(table_fqns, configs)
    print("  ✅ SQL generated:")
    print()
    print(derived_sql)
except Exception as e:
    print(f"  ❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("=" * 70)
print("STEP 4: Simulate mf_metric_schema.load() result")
print("=" * 70)

result = {
    "name": metric["name"],
    "label": metric["label"],
    "type": metric["type"],
    "description": metric["description"],
    "sql": derived_sql,
    "dialect": "TRINO",
    "model": None,  # Virtual Dataset
    "meta": metric["meta"],
}

print(f"  name: {result['name']}")
print(f"  type: {result['type']}")
print(f"  model: {result['model']}")
print(f"  meta.ref_dataset: {result['meta'].get('ref_dataset')}")
print(f"  sql length: {len(result['sql'])} chars")

print()
print("=" * 70)
print("STEP 5: Simulate virtual dataset creation (dbt_core logic)")
print("=" * 70)

model_name = result["meta"].get("ref_dataset")
if model_name:
    virtual_model = {
        "name": model_name,
        "unique_id": f"model.semantic_layer.{model_name}",
        "schema": None,
        "database": None,
        "description": "",
        "meta": {},
        "columns": [],
        "sql": result["sql"],
    }
    print(f"  Virtual dataset: {virtual_model['name']}")
    print(f"  unique_id: {virtual_model['unique_id']}")
    print(f"  schema: {virtual_model['schema']} (→ virtual dataset path)")
    print(f"  SQL preview:")
    print(f"    {result['sql'][:200]}...")
else:
    print("  ⚠ No ref_dataset in meta!")

print()
print("=" * 70)
print("DONE ✓ — Full derived metric flow simulated successfully")
print("=" * 70)
