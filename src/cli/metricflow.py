"""
Tương tác với MetricFlow CLI và xử lý đầu ra.

Module này chịu trách nhiệm:
  - Chạy lệnh ``mf query --explain``.
  - Làm sạch (sanitise) stdout của MetricFlow (loại bỏ mã ANSI, cảnh báo phiên bản, v.v.).
  - Xây dựng schema cho semantic-layer metric từ đầu ra của MetricFlow.
"""

import copy
import concurrent.futures
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from api.clients.dbt import (
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from cli.lib import ModelKey
from cli.lib import (
    get_og_metric_from_config,
    is_measurement_column,
)
from cli.metrics import get_models_from_sql
from cli.relations import handle_relation_derived_metric

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

def _execute_mf_query(metric: Dict[str, Any]) -> Optional[str]:
    """
    Chạy ``mf query --explain`` và trả về SQL đã được làm sạch, hoặc None nếu thất bại.
    """
    command = ["mf", "query", "--explain", "--metrics", metric["name"], "--quiet"]
    if metric.get("type", "").lower() == "cumulative":
        command.extend(["--group-by", "metric_time"])

    type_info = metric.get("type", "").lower()
    _logger.info(
        "Parsing metric %s %s\nFROM: %s",
        metric["name"],
        f"({type_info})" if type_info in ("derived", "ratio") else "",
        metric["path"],
    )
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True,
            encoding="utf-8", env=os.environ, errors="ignore",
        )
    except FileNotFoundError:
        _logger.warning(
            "`mf` command not found, make sure MetricFlow is installed to sync metrics",
        )
        return None
    except subprocess.CalledProcessError as exc:
        _logger.warning(
            "Could not generate SQL for metric %s\nstderr: %s",
            metric["name"], exc.stderr,
        )
        return None

    raw = result.stdout.strip()
    sql = clean_mf_sql_output(raw)
    if not sql:
        _logger.warning("mf returned no usable SQL for metric %s. Raw output:\n%s", metric["name"], raw)
        return None

    _logger.debug("mf raw output:\n%s", raw)
    _logger.debug("Cleaned SQL:\n%s", sql)
    return sql


def _deduplicate_models(models: List[ModelSchema]) -> List[ModelSchema]:
    """Khử trùng lặp các model bằng unique_id."""
    seen: set = set()
    unique: list = []
    for m in models:
        if m["unique_id"] not in seen:
            seen.add(m["unique_id"])
            unique.append(m)
    return unique


def _build_single_model_sql(model: ModelSchema, column_rename_map: Dict[str, str]) -> str:
    """Tạo câu SELECT ánh xạ các cột gốc sang alias cho virtual dataset đơn bảng."""
    model_cols = model.get("columns", {})
    cols_list = list(model_cols.values()) if isinstance(model_cols, dict) else model_cols
    select_items = [
        f"{col['name']} AS {column_rename_map[col['name']]}"
        if col["name"] in column_rename_map
        else col["name"]
        for col in cols_list
    ]
    select_clause = ",\n    ".join(select_items)
    return f"SELECT\n    {select_clause}\nFROM {model.get('database', '')}.{model.get('schema', '')}.{model['name']}"


def _build_multi_model_descriptions(models: List[ModelSchema]) -> Dict[str, str]:
    """Xây dựng mô tả cho các cột từ manifest khi metric phụ thuộc nhiều bảng."""
    column_descriptions: Dict[str, str] = {}
    for m in models:
        model_cols = m.get("columns", {})
        if isinstance(model_cols, dict):
            for col_name, col_meta in model_cols.items():
                desc = col_meta.get("description", "") if isinstance(col_meta, dict) else ""
                column_descriptions[f"{m['name']}__{col_name}"] = desc
                if col_name not in column_descriptions:
                    column_descriptions[col_name] = desc
    return column_descriptions


def get_sl_metric(
    metric: Dict[str, Any],
    model_map: Dict[ModelKey, ModelSchema],
    dialect: MFSQLEngine,
    configs: Dict[str, Any],
    target_config: Dict[str, Any] = None,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Tính toán một SL (Semantic Layer) metric sử dụng ``mf`` CLI.
    """
    sql = _execute_mf_query(metric)
    if not sql:
        return None

    models = get_models_from_sql(sql, dialect, model_map)
    if not models:
        _logger.warning("get_models_from_sql returned None/empty for metric %s", metric["name"])
        return None

    models = _deduplicate_models(models)
    _logger.debug(
        "Metric %s: %d unique models after dedup: %s",
        metric["name"], len(models), [m["unique_id"] for m in models],
    )

    model_sql = None
    model_id = None
    column_rename_map: Dict[str, str] = {}
    column_descriptions: Dict[str, str] = {}
    column_labels: Dict[str, str] = {}
    select_columns: List[str] = []

    if len(models) == 1:
        model_id = models[0]["unique_id"]
        if metric.get("meta", {}).get("ref_dataset"):
            column_rename_map, column_descriptions, column_labels, select_columns = (
                _build_single_model_column_metadata(models[0])
            )
            model_sql = _build_single_model_sql(models[0], column_rename_map)
    else:
        table_fqns = [f"{m['database']}.{m['schema']}.{m['name']}" for m in models]
        try:
            model_sql, column_rename_map, column_labels, select_columns = (
                handle_relation_derived_metric(table_fqns, configs, target_config)
            )
        except Exception as exc:
            _logger.warning("Cannot generate derived metric SQL for %s: %s", metric["name"], exc)
            return None

        _logger.debug(
            "Metric %s: multi-model SQL generated (has FULL OUTER JOIN: %s):\n%s",
            metric["name"], "FULL OUTER JOIN" in (model_sql or ""), model_sql,
        )
        column_descriptions = _build_multi_model_descriptions(models)

    return MFMetricWithSQLSchema().load({
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
    })


# ---------------------------------------------------------------------------
# Helpers extracted from command.py
# ---------------------------------------------------------------------------


def classify_metrics(
    metric_configs: Dict[str, Any],
    selected_metrics: Tuple[str, ...],
    select_paths: List[str],
    dialect: str,
    mf_dialect: Optional[MFSQLEngine],
) -> Tuple[list, list]:
    """
    Phân loại metric configs thành OG metrics và SL metric configs.

    Returns:
        Tuple of (og_metrics, sl_metric_configs).
    """
    og_metrics: list = []
    sl_metric_configs: list = []

    for metric_config in metric_configs.values():
        if selected_metrics and metric_config["name"] not in selected_metrics:
            continue
        if select_paths:
            metric_path = Path(
                metric_config.get("original_file_path", ""),
            ).as_posix()
            if not any(metric_path.startswith(sp) for sp in select_paths):
                continue
        # dbt đang chuyển đổi từ `metric.meta` sang `metric.config.meta`
        metric_config["meta"] = metric_config.get("meta") or metric_config.get(
            "config", {},
        ).get("meta", {})

        superset_meta = metric_config["meta"].get("superset", {})
        if superset_meta.get("model") and superset_meta.get("expression"):
            sql = superset_meta.pop("expression")
            og_metrics.append(
                get_og_metric_from_config(metric_config, dialect, depends_on=[], sql=sql),
            )
        elif "calculation_method" in metric_config or "sql" in metric_config:
            og_metrics.append(get_og_metric_from_config(metric_config, dialect))
        elif mf_dialect is not None:
            sl_metric_configs.append(metric_config)

    return og_metrics, sl_metric_configs


def process_sl_metrics_concurrently(
    sl_metric_configs: list,
    model_map: Dict[ModelKey, ModelSchema],
    mf_dialect: Optional[MFSQLEngine],
    configs: Dict[str, Any],
    target_output: Dict[str, Any],
    max_workers: int = 5,
) -> list:
    """
    Xử lý các semantic layer metrics một cách song song bằng ThreadPoolExecutor.
    """
    if not sl_metric_configs:
        return []

    sl_metrics: list = []
    _logger.info(
        "Processing %d semantic layer metrics concurrently (max_workers=%d)...",
        len(sl_metric_configs),
        max_workers,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_metric = {
            executor.submit(
                get_sl_metric, m_cfg, model_map, mf_dialect, configs, target_output,
            ): m_cfg
            for m_cfg in sl_metric_configs
        }
        for future in concurrent.futures.as_completed(future_to_metric):
            m_cfg = future_to_metric[future]
            try:
                result = future.result()
                if result:
                    sl_metrics.append(result)
            except Exception as exc:
                _logger.warning("Error processing metric %s: %s", m_cfg["name"], exc)

    return sl_metrics


def merge_sl_metrics_into_models(
    sl_metrics: list,
    models: list,
    database_profile: str,
) -> None:
    """
    Tạo (hoặc merge) các virtual models từ SL metrics có ``ref_dataset``.

    Mutates *sl_metrics* in-place (gán ``model`` hoặc xóa ``column_rename_map``)
    và append virtual models vào *models*.
    """
    seen_virtual_models: Dict[str, dict] = {}
    virtual_metrics_to_add: list = []

    for sl_metric in sl_metrics:
        ref_dataset = sl_metric.get("meta", {}).get("ref_dataset")
        if not ref_dataset:
            continue

        _logger.debug("Metric detected: %s (type=%s)", sl_metric["name"], sl_metric.get("type"))
        virtual_unique_id = f"model.semantic_layer.{ref_dataset}"

        if ref_dataset not in seen_virtual_models:
            _create_virtual_model(
                sl_metric, ref_dataset, virtual_unique_id,
                database_profile, seen_virtual_models, models,
            )
        else:
            _merge_into_existing_model(
                sl_metric, ref_dataset, seen_virtual_models[ref_dataset],
            )

        if sl_metric.get("model"):
            virtual_metric = copy.deepcopy(sl_metric)
            virtual_metric["model"] = virtual_unique_id
            virtual_metrics_to_add.append(virtual_metric)
            sl_metric["column_rename_map"] = {}
        else:
            sl_metric["model"] = virtual_unique_id

    sl_metrics.extend(virtual_metrics_to_add)


def _create_virtual_model(
    sl_metric: dict,
    model_name: str,
    virtual_unique_id: str,
    database_profile: str,
    seen: Dict[str, dict],
    models: list,
) -> None:
    """Tạo một virtual model mới từ SL metric và thêm vào *models*."""
    columns = [
        {
            "name": col,
            "description": sl_metric.get("column_descriptions", {}).get(col, ""),
            "meta": {"superset": {"verbose_name": sl_metric.get("column_labels", {})[col]}}
            if col in sl_metric.get("column_labels", {})
            else {},
        }
        for col in sl_metric.get("select_columns", [])
    ]
    model = {
        "name": model_name,
        "unique_id": virtual_unique_id,
        "schema": None,
        "database": database_profile,
        "description": "",
        "meta": {},
        "columns": columns,
        "sql": sl_metric.get("model_sql"),
    }
    seen[model_name] = model
    models.append(model)
    _logger.info("Created virtual model '%s'", model_name)


def _merge_into_existing_model(
    sl_metric: dict,
    model_name: str,
    existing_model: dict,
) -> None:
    """Merge thêm columns và cập nhật SQL cho một virtual model đã tồn tại."""
    new_sql = sl_metric.get("model_sql")
    if new_sql:
        current_sql = existing_model.get("sql", "") or ""
        if not current_sql:
            existing_model["sql"] = new_sql
            _logger.debug("Set initial sql for %s (len: %d)", model_name, len(new_sql))
        elif "FULL OUTER JOIN" in new_sql and "FULL OUTER JOIN" not in current_sql:
            existing_model["sql"] = new_sql
            _logger.info("Overwrote sql for %s with FULL OUTER JOIN query", model_name)
        elif (
            "FULL OUTER JOIN" in new_sql
            and "FULL OUTER JOIN" in current_sql
            and new_sql.count("FULL OUTER JOIN") > current_sql.count("FULL OUTER JOIN")
        ):
            existing_model["sql"] = new_sql
            _logger.info("Overwrote sql for %s with MORE FULL OUTER JOINs", model_name)

    existing_cols = {c["name"]: c for c in existing_model["columns"]}
    for col_name in sl_metric.get("select_columns", []):
        if col_name not in existing_cols:
            col_meta = {}
            label = sl_metric.get("column_labels", {}).get(col_name)
            if label:
                col_meta = {"superset": {"verbose_name": label}}
            new_col = {
                "name": col_name,
                "description": sl_metric.get("column_descriptions", {}).get(col_name, ""),
                "meta": col_meta,
            }
            existing_model["columns"].append(new_col)
            existing_cols[col_name] = new_col
