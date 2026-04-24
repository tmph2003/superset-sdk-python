"""
Integration test: handle_relation_derived_metric (refactored — clean code)
Dùng real manifest.json và real DB (relationship_designer).
"""
import sys
import json

sys.path.insert(0, "src")

MANIFEST_PATH = r"d:\data_pipeline_transform\target\manifest.json"

# ============================================================
# 1. Load manifest
# ============================================================
print("=" * 70)
print("STEP 1: Load manifest.json")
print("=" * 70)
with open(MANIFEST_PATH, encoding="utf-8") as f:
    configs = json.load(f)

print(f"  Manifest loaded OK.")

# ============================================================
# 2. Lấy danh sách bảng từ relation_members
# ============================================================
print()
print("=" * 70)
print("STEP 2: Lấy bảng từ relation_members DB")
print("=" * 70)
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="relationship_designer", user="postgres", password="postgres",
)
cur = conn.cursor()
cur.execute("""
    SELECT DISTINCT rm.catalog || '.' || rm.schema || '.' || rm.table_name
    FROM public.relation_members rm
""")
table_names = sorted(row[0] for row in cur.fetchall())
conn.close()
print(f"  Bảng: {table_names}")

# ============================================================
# 3. Test handle_relation_derived_metric (new signature)
# ============================================================
print()
print("=" * 70)
print("STEP 3: handle_relation_derived_metric(table_names, configs)")
print("=" * 70)
from superset_cli.cli.superset.sync.dbt.command import handle_relation_derived_metric

try:
    sql = handle_relation_derived_metric(table_names, configs)
    print(sql)
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 70)
print("DONE ✓")
print("=" * 70)
