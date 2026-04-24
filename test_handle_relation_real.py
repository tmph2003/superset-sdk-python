"""Test handle_relation_derived_metric with real DB - show query and result."""
import sys
sys.path.insert(0, "src")

import psycopg2

# 1. Xem toàn bộ dữ liệu trong relation_members
conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="relationship_designer", user="postgres", password="postgres",
)
cur = conn.cursor()
cur.execute("""
    SELECT rm.group_id,
           rm.catalog || '.' || rm.schema || '.' || rm.table_name AS fqn,
           rm.column_name
    FROM public.relation_members rm
    ORDER BY rm.group_id, fqn
""")
rows = cur.fetchall()
conn.close()

print("=" * 70)
print("DỮ LIỆU TRONG relation_members")
print("=" * 70)
print(f"{'group_id':<12} {'fqn':<55} {'column_name'}")
print("-" * 70)
for gid, fqn, col in rows:
    print(f"{gid:<12} {fqn:<55} {col}")

# 2. Lấy danh sách tất cả bảng unique
all_tables = sorted(set(r[1] for r in rows))
print(f"\nTất cả bảng: {all_tables}")

# 3. Test hàm thực tế
from superset_cli.cli.superset.sync.dbt.command import handle_relation_derived_metric

print()
print("=" * 70)
print("KẾT QUẢ: 2 bảng gold")
print("=" * 70)
try:
    sql = handle_relation_derived_metric([
        "dp_warehouse.gold.fact_salein_planned_information",
        "dp_warehouse.gold.fact_agg_sale_by_month",
    ])
    print(sql)
except Exception as e:
    print(f"ERROR: {e}")

# Nếu có 3 bảng, test luôn
if len(all_tables) >= 3:
    print()
    print("=" * 70)
    print(f"KẾT QUẢ: 3 bảng")
    print("=" * 70)
    try:
        sql = handle_relation_derived_metric(all_tables)
        print(sql)
    except Exception as e:
        print(f"ERROR: {e}")
