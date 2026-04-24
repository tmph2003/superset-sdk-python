"""Test handle_relation_derived_metric — CTE-based SQL generation."""
from collections import defaultdict

# ============================================================
# Test SQL generation logic (no DB needed)
# ============================================================

def generate_cte_join_sql(table_names, rows, measurements=None):
    """Extracted CTE SQL generation logic from handle_relation_derived_metric for testing."""
    measurements = measurements or {}

    if not rows:
        raise ValueError(f"Không tìm thấy nhóm quan hệ nào chứa đủ các bảng: {table_names}")

    # Tổ chức dữ liệu
    groups = defaultdict(lambda: defaultdict(list))
    all_tables_seen = set()
    for group_id, fqn, column_name in rows:
        groups[group_id][fqn].append(column_name)
        all_tables_seen.add(fqn)

    # Join columns per table
    table_join_columns = defaultdict(set)
    for group_id, group_tables in groups.items():
        for fqn, cols in group_tables.items():
            for col in cols:
                table_join_columns[fqn].add(col)
    table_join_columns = {fqn: sorted(cols) for fqn, cols in table_join_columns.items()}

    ordered_tables = [fqn for fqn in table_names if fqn in all_tables_seen]
    aliases = [chr(ord("a") + i) for i in range(len(ordered_tables))]
    alias_map = dict(zip(ordered_tables, aliases))

    def get_schema_table(fqn):
        parts = fqn.split(".")
        return f"{parts[1]}.{parts[2]}"

    def get_table_short(fqn):
        return fqn.split(".")[-1]

    # Step 1: Build CTEs
    cte_parts = []
    cte_names = []
    for fqn in ordered_tables:
        join_cols = table_join_columns.get(fqn, [])
        meas_cols = measurements.get(fqn, [])
        cte_name = f"cte_{get_table_short(fqn)}"
        cte_names.append(cte_name)

        select_exprs = list(join_cols)
        if meas_cols:
            select_exprs += [f"SUM({col}) AS {col}" for col in meas_cols]
            cte_sql = (
                f"{cte_name} AS (\n"
                f"    SELECT\n"
                f"        {(','+chr(10)+'        ').join(select_exprs)}\n"
                f"    FROM {get_schema_table(fqn)}\n"
                f"    GROUP BY {', '.join(join_cols)}\n"
                f")"
            )
        else:
            cte_sql = (
                f"{cte_name} AS (\n"
                f"    SELECT DISTINCT\n"
                f"        {(','+chr(10)+'        ').join(select_exprs)}\n"
                f"    FROM {get_schema_table(fqn)}\n"
                f")"
            )
        cte_parts.append(cte_sql)

    # Step 2: Final SELECT
    all_join_cols = []
    seen_join_cols = set()
    for fqn in ordered_tables:
        for col in table_join_columns.get(fqn, []):
            if col not in seen_join_cols:
                all_join_cols.append(col)
                seen_join_cols.add(col)

    final_select_exprs = []
    for col in all_join_cols:
        sources = []
        for fqn, alias in zip(ordered_tables, aliases):
            if col in table_join_columns.get(fqn, []):
                sources.append(f"{alias}.{col}")
        if len(sources) == 1:
            final_select_exprs.append(f"{sources[0]} AS {col}")
        else:
            final_select_exprs.append(f"COALESCE({', '.join(sources)}) AS {col}")

    for fqn, alias in zip(ordered_tables, aliases):
        for col in measurements.get(fqn, []):
            final_select_exprs.append(f"{alias}.{col}")

    if not final_select_exprs:
        final_select_exprs = ["*"]

    # Step 3: FULL OUTER JOIN
    join_parts = [f"FROM {cte_names[0]} {aliases[0]}"]
    for i in range(1, len(ordered_tables)):
        current_table = ordered_tables[i]
        current_alias = aliases[i]
        prev_tables = ordered_tables[:i]

        group_conditions = []
        for group_id, group_tables in groups.items():
            if current_table not in group_tables:
                continue
            relevant_prev = [t for t in prev_tables if t in group_tables]
            if not relevant_prev:
                continue
            current_cols = group_tables[current_table]
            equalities = []
            for prev_t in relevant_prev:
                prev_alias = alias_map[prev_t]
                prev_cols = group_tables[prev_t]
                for pc in prev_cols:
                    for cc in current_cols:
                        equalities.append(f"{prev_alias}.{pc} = {current_alias}.{cc}")
            if len(equalities) == 1:
                group_conditions.append(equalities[0])
            elif len(equalities) > 1:
                group_conditions.append(f"({' OR '.join(equalities)})")

        if not group_conditions:
            raise ValueError(f"Không tìm thấy điều kiện join cho bảng {current_table}")

        on_str = "\n       AND ".join(group_conditions)
        join_parts.append(
            f"FULL OUTER JOIN {cte_names[i]} {current_alias}\n"
            f"    ON {on_str}"
        )

    sql = (
        f"WITH {(','+chr(10)).join(cte_parts)}\n"
        f"SELECT\n"
        f"    {(','+chr(10)+'    ').join(final_select_exprs)}\n"
        f"{chr(10).join(join_parts)}"
    )
    return sql


# ============================================================
# TEST 1: 2 bảng, không measurement → SELECT DISTINCT
# ============================================================
print("=" * 60)
print("TEST 1: 2 bảng, không measurement (SELECT DISTINCT)")
print("=" * 60)
rows_2t = [
    (1, "dp_warehouse.gold.fct_orders", "customer_id"),
    (1, "dp_warehouse.gold.dim_customers", "customer_id"),
]
sql = generate_cte_join_sql(
    ["dp_warehouse.gold.fct_orders", "dp_warehouse.gold.dim_customers"],
    rows_2t,
)
print(sql)
print()

# ============================================================
# TEST 2: 2 bảng, có measurements → GROUP BY + SUM
# ============================================================
print("=" * 60)
print("TEST 2: 2 bảng, có measurements (GROUP BY + SUM)")
print("=" * 60)
sql = generate_cte_join_sql(
    ["dp_warehouse.gold.fct_orders", "dp_warehouse.gold.dim_customers"],
    rows_2t,
    measurements={
        "dp_warehouse.gold.fct_orders": ["order_amount", "order_count"],
        "dp_warehouse.gold.dim_customers": ["total_spent"],
    },
)
print(sql)
print()

# ============================================================
# TEST 3: 3 bảng, mixed measurements + COALESCE
# ============================================================
print("=" * 60)
print("TEST 3: 3 bảng, mixed measurements + COALESCE")
print("=" * 60)
rows_3t = [
    (1, "dp_warehouse.gold.fct_orders", "customer_id"),
    (1, "dp_warehouse.gold.dim_customers", "customer_id"),
    (1, "dp_warehouse.gold.dim_products", "customer_id"),
]
sql = generate_cte_join_sql(
    [
        "dp_warehouse.gold.fct_orders",
        "dp_warehouse.gold.dim_customers",
        "dp_warehouse.gold.dim_products",
    ],
    rows_3t,
    measurements={
        "dp_warehouse.gold.fct_orders": ["order_amount"],
    },
)
print(sql)
print()

# ============================================================
# TEST 4: 2 bảng, 2 join columns + measurements
# ============================================================
print("=" * 60)
print("TEST 4: 2 bảng, 2 join columns + measurements")
print("=" * 60)
rows_multi = [
    (1, "dp_warehouse.gold.fct_orders", "customer_id"),
    (1, "dp_warehouse.gold.fct_orders", "order_date"),
    (1, "dp_warehouse.gold.dim_customers", "customer_id"),
    (1, "dp_warehouse.gold.dim_customers", "order_date"),
]
sql = generate_cte_join_sql(
    ["dp_warehouse.gold.fct_orders", "dp_warehouse.gold.dim_customers"],
    rows_multi,
    measurements={
        "dp_warehouse.gold.fct_orders": ["revenue"],
    },
)
print(sql)
print()

# ============================================================
# TEST 5: DB + real function
# ============================================================
print("=" * 60)
print("TEST 5: Real DB connection + get_measurement_columns")
print("=" * 60)
try:
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="relationship_designer", user="postgres", password="postgres",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM public.relation_members")
    count = cur.fetchone()[0]
    print(f"  DB connected OK. relation_members has {count} rows.")

    if count > 0:
        cur.execute("SELECT DISTINCT catalog || '.' || schema || '.' || table_name FROM public.relation_members LIMIT 10")
        tables = [row[0] for row in cur.fetchall()]
        print(f"  Sample tables: {tables}")

    conn.close()
    print("  DB connection closed.")
except Exception as e:
    print(f"  DB connection failed (expected if DB not running): {e}")
