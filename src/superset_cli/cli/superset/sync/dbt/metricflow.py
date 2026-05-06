"""
Tương tác với MetricFlow CLI và xử lý đầu ra.

Module này chịu trách nhiệm:
  - Chạy lệnh ``mf query --explain``.
  - Làm sạch (sanitise) stdout của MetricFlow (loại bỏ mã ANSI, cảnh báo phiên bản, v.v.).
  - Xây dựng schema cho semantic-layer metric từ đầu ra của MetricFlow.
"""

import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional

from superset_cli.api.clients.dbt import (
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from superset_cli.cli.superset.sync.dbt.exposures import ModelKey
from superset_cli.cli.superset.sync.dbt.lib import is_measurement_column
from superset_cli.cli.superset.sync.dbt.metrics import get_models_from_sql
from superset_cli.cli.superset.sync.dbt.relations import handle_relation_derived_metric

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Các từ khóa SQL đánh dấu sự bắt đầu của một câu lệnh SQL hợp lệ
_SQL_START_KEYWORDS = frozenset({
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
    "DROP", "MERGE", "EXPLAIN",
})

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")



# ---------------------------------------------------------------------------
# Output sanitisation
# ---------------------------------------------------------------------------


def clean_mf_sql_output(raw: str) -> str:
    """
    Loại bỏ các nội dung không phải là SQL khỏi đầu ra của lệnh MetricFlow ``mf query --explain``.

    MetricFlow có thể in ra các cảnh báo nâng cấp phiên bản, các dòng chứa emoji và mã
    màu ANSI ra *stdout* ngay cả khi sử dụng tùy chọn ``--quiet``. Hàm này sẽ
    loại bỏ những dòng đó để chỉ giữ lại câu lệnh SQL.

    Chiến lược (Strategy):
      1. Loại bỏ các chuỗi ANSI escape.
      2. Duyệt các dòng từ trên xuống; bỏ qua mọi thứ cho đến khi gặp một dòng mà
         token đầu tiên của nó là một từ khóa SQL hợp lệ (SELECT, WITH, …).
      3. Duyệt các dòng từ dưới lên; loại bỏ các dòng không phải SQL ở cuối (cảnh báo,
         dòng trống, thông báo có emoji, v.v.).
    """
    # 1. Loại bỏ mã ANSI
    cleaned = _ANSI_ESCAPE_RE.sub("", raw)

    lines = cleaned.splitlines()

    # 2. Tìm dòng SQL đầu tiên
    start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        first_token = stripped.split()[0].upper().rstrip("(")
        if first_token in _SQL_START_KEYWORDS:
            start = i
            break
    else:
        # Không tìm thấy từ khóa SQL nào cả
        return ""

    # 3. Loại bỏ rác (không phải SQL) ở cuối đoạn text
    end = len(lines)
    for j in range(len(lines) - 1, start - 1, -1):
        stripped = lines[j].strip()
        if not stripped:
            end = j
            continue
        # Các dòng bắt đầu bằng emoji / ký tự đặc biệt không phải là SQL
        if stripped[0] in "⚠💡‼🔔🚀✅❌ℹ️":
            end = j
            continue
        # Nếu dòng đó trông giống văn bản bình thường (không có dấu câu của SQL), hãy bỏ qua nó
        # nhưng phải bảo thủ — chỉ cắt nếu chắc chắn nó không phải SQL
        break
    else:
        return ""

    return "\n".join(lines[start:end]).strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _build_single_model_column_metadata(
    model: ModelSchema,
) -> tuple:
    """
    Xây dựng map để đổi tên cột (rename map), mô tả (descriptions), nhãn (labels),
    và các cột được select (select columns) cho một single-model metric hướng tới virtual dataset.

    Các measurement column (cột dùng để đo lường) sẽ được đổi alias thành ``<table>__<column>``; 
    các dimension column (cột chiều dữ liệu) sẽ giữ nguyên tên ban đầu.

    Returns:
        Tuple gồm (column_rename_map, column_descriptions, column_labels,
        select_columns).
    """
    table_name = model["name"]
    model_cols = model.get("columns", {})
    if isinstance(model_cols, dict):
        model_cols_list = list(model_cols.values())
    else:
        model_cols_list = model_cols

    column_rename_map: Dict[str, str] = {}
    column_descriptions: Dict[str, str] = {}
    column_labels: Dict[str, str] = {}
    select_columns = [col["name"] for col in model_cols_list]

    for col in model_cols_list:
        col_name = col["name"]
        if is_measurement_column(col):
            aliased = f"{table_name}__{col_name}"
            column_rename_map[col_name] = aliased
            column_descriptions[aliased] = col.get("description", "")
            if col.get("label"):
                column_labels[aliased] = col["label"]
            # Thay thế tên gốc bằng alias trong select_columns
            select_columns = [
                aliased if c == col_name else c for c in select_columns
            ]
        else:
            column_descriptions[col_name] = col.get("description", "")
            if col.get("label"):
                column_labels[col_name] = col["label"]

    return column_rename_map, column_descriptions, column_labels, select_columns

def get_sl_metric(
    metric: Dict[str, Any],
    model_map: Dict[ModelKey, ModelSchema],
    dialect: MFSQLEngine,
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Tính toán một SL (Semantic Layer) metric sử dụng ``mf`` CLI.

    Chạy lệnh ``mf query --explain --metrics <name> --quiet``, làm sạch đầu ra,
    phân giải các model được tham chiếu và trả về một
    :class:`MFMetricWithSQLSchema` chứa đầy đủ dữ liệu.
    """
    mf_metric_schema = MFMetricWithSQLSchema()

    command = ["mf", "query", "--explain", "--metrics", metric["name"], '--quiet']
    try:
        _logger.info(
            "Parsing metric %s %s\nFROM: %s",
            metric["name"],
            f"({metric.get('type', '').lower()})" if metric.get("type", "").lower() in ("derived", "ratio") else "",
            metric["path"],
        )
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding="utf-8", env=os.environ, errors="ignore")
    except FileNotFoundError:
        _logger.warning(
            "`mf` command not found, if you're using Metricflow make sure you have it "
            "installed in order to sync metrics",
        )
        return None
    except subprocess.CalledProcessError as exc:
        _logger.warning(
            "Could not generate SQL for metric %s (this happens for some metrics)\n"
            "stderr: %s",
            metric["name"],
            exc.stderr,
        )
        return None
    raw_output = result.stdout.strip()

    # MetricFlow có thể in ra các dòng không phải SQL ra stdout (cảnh báo phiên bản, thông báo
    # emoji, v.v.) ngay cả với tùy chọn --quiet. Loại bỏ chúng để sqlglot có thể parse (phân tích) được.
    sql = clean_mf_sql_output(raw_output)

    if not sql:
        _logger.warning(
            "mf returned no usable SQL for metric %s. Raw output:\n%s",
            metric["name"],
            raw_output,
        )
        return None

    _logger.debug("mf raw output:\n%s", raw_output)
    _logger.debug("Cleaned SQL:\n%s", sql)


    models = get_models_from_sql(sql, dialect, model_map)
    if not models:
        _logger.warning("get_models_from_sql returned None/empty for metric %s", metric["name"])
        return None

    # Khử trùng lặp (deduplicate) các model bằng unique_id.
    # Đối với derived metric mà các sub-metric cùng nằm trên *một* bảng, SQL được sinh ra 
    # từ MetricFlow sẽ tham chiếu bảng đó nhiều lần (mỗi sub-metric CTE một lần).
    # Nếu không khử trùng lặp, code sẽ chạy nhầm vào nhánh multi-table (FULL OUTER JOIN).
    seen_ids = set()
    unique_models = []
    for m in models:
        if m["unique_id"] not in seen_ids:
            seen_ids.add(m["unique_id"])
            unique_models.append(m)
    models = unique_models

    _logger.debug(
        "Metric %s: %d unique models after dedup: %s",
        metric["name"],
        len(models),
        [m["unique_id"] for m in models],
    )

    model_sql = None
    model_id = None
    column_rename_map = {}
    column_descriptions = {}
    column_labels = {}
    select_columns = []

    if len(models) == 1:
        model_id = models[0]["unique_id"]
        if metric.get("meta", {}).get("ref_dataset"):
            table_name = models[0]["name"]
            column_rename_map, column_descriptions, column_labels, select_columns = (
                _build_single_model_column_metadata(models[0])
            )
            # Tạo một câu SELECT ánh xạ các cột đo lường gốc sang tên bí danh (alias) của chúng
            db_name = models[0].get("database", "")
            schema_name = models[0].get("schema", "")
            
            model_cols = models[0].get("columns", {})
            model_cols_list = list(model_cols.values()) if isinstance(model_cols, dict) else model_cols
            
            select_items = []
            for col in model_cols_list:
                orig = col["name"]
                if orig in column_rename_map:
                    select_items.append(f"{orig} AS {column_rename_map[orig]}")
                else:
                    select_items.append(orig)
            
            select_clause = ",\n    ".join(select_items)
            model_sql = f"SELECT\n    {select_clause}\nFROM {db_name}.{schema_name}.{table_name}"
    else:
        table_fqns = [
            f"{m['database']}.{m['schema']}.{m['name']}" for m in models
        ]
        try:
            model_sql, column_rename_map, column_labels, select_columns = handle_relation_derived_metric(table_fqns, configs, target_config)
        except Exception as exc:
            _logger.warning(
                "Cannot generate derived metric SQL for %s: %s",
                metric["name"],
                exc,
            )
            return None

        _logger.debug(
            "Metric %s: multi-model SQL generated (has FULL OUTER JOIN: %s):\n%s",
            metric["name"],
            "FULL OUTER JOIN" in (model_sql or ""),
            model_sql,
        )

        # Xây dựng mô tả cho các cột từ các model column nguyên bản trong manifest
        for m in models:
            table_name = m["name"]
            model_cols = m.get("columns", {})
            # model_cols là một dict dạng: {col_name: {description: ...}}
            if isinstance(model_cols, dict):
                for col_name, col_meta in model_cols.items():
                    desc = col_meta.get("description", "") if isinstance(col_meta, dict) else ""
                    # Measurement columns: sẽ được tạo alias là table__col
                    aliased = f"{table_name}__{col_name}"
                    column_descriptions[aliased] = desc
                    # Join columns: sử dụng mô tả của model đầu tiên
                    if col_name not in column_descriptions:
                        column_descriptions[col_name] = desc

    return mf_metric_schema.load(
        {
            "name": metric["name"],
            "label": metric["label"],
            "type": metric["type"],
            "description": metric["description"],
            "sql": sql,
            "dialect": dialect.value,
            "model": model_id,
            "meta": metric["meta"],
            "model_sql": model_sql,
            "column_rename_map": column_rename_map,
            "column_labels": column_labels if model_sql or column_rename_map else {},
            "column_descriptions": column_descriptions,
            "select_columns": select_columns,
        },
    )
