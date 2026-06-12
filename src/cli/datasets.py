"""
Đồng bộ dbt datasets/metrics sang Superset.
"""

# pylint: disable=consider-using-f-string
from __future__ import annotations

import copy
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.engine.url import URL as SQLAlchemyURL
from sqlalchemy.engine.url import make_url
from yarl import URL

from api.clients.dbt import ModelSchema
from api.clients.superset import SupersetClient, SupersetMetricDefinition
from api.operators import OneToMany
from cli.lib import (
    create_engine_with_check,
    is_measurement_column,
)
from exceptions import CLIError, SupersetError
from lib import dict_merge, raise_cli_errors

DEFAULT_CERTIFICATION = {"details": "This table is produced by dbt"}

_logger = logging.getLogger(__name__)


def model_in_database(model: ModelSchema, url: SQLAlchemyURL) -> bool:
    """
    Trả về True nếu model nằm trong cùng một database với một SQLAlchemy URI.
    """
    if url.drivername == "bigquery":
        return model["database"] == url.host

    return model["database"] == url.database


def clean_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Loại bỏ các cột không tương thích khỏi metadata để tạo/cập nhật một cột/metric.
    """
    for key in (
        "autoincrement",
        "changed_on",
        "comment",
        "created_on",
        "default",
        "name",
        "nullable",
        "type_generic",
        "precision",
        "scale",
        "max_length",
        "info",
    ):
        if key in metadata:
            del metadata[key]

    return metadata


@raise_cli_errors
def create_dataset(
    client: SupersetClient,
    database: Dict[str, Any],
    model: ModelSchema,
) -> Dict[str, Any]:
    """
    Tạo một physical dataset (dataset vật lý) hoặc virtual dataset (dataset ảo).

    Virtual dataset được tạo ra khi database của bảng khác với main
    database, dành cho các hệ thống hỗ trợ truy vấn chéo database (như Trino, BigQuery, v.v.)
    """
    kwargs = {
        "database": database["id"],
        "catalog": model["database"],
        "schema": model["schema"] or "",
        "table_name": model.get("alias") or model["name"],
        "sql": model.get("sql"),
    }
    # Virtual datasets: catalog=None để Superset không validate physical table
    if model.get("sql") and not model.get("schema"):
        kwargs["catalog"] = None
    try:
        # thử tạo dataset với catalog
        return client.create_dataset(**kwargs)
    except SupersetError as ex:
        if not no_catalog_support(ex):
            raise ex
        del kwargs["catalog"]

    url = make_url(database["sqlalchemy_uri"])
    if not kwargs["sql"] and not model_in_database(model, url):
        engine = create_engine_with_check(url)
        quote = engine.dialect.identifier_preparer.quote
        source = ".".join(quote(model[key]) for key in ("database", "schema", "name"))
        kwargs["sql"] = f"SELECT * FROM {source}"

    return client.create_dataset(**kwargs)


def no_catalog_support(ex: SupersetError) -> bool:
    """
    Trả về True nếu lỗi là do không hỗ trợ catalog.

    Payload của lỗi sẽ trông giống như thế này:

        [
            {
                "message": json.dumps({"message": {"catalog": ["Unknown field."]}}),
                "error_type": "UNKNOWN_ERROR",
                "level": ErrorLevel.ERROR,
            },
        ]

    """
    for error in ex.errors:
        try:
            message = json.loads(error["message"])
            if "Unknown field." in message["message"]["catalog"]:
                return True
        except Exception:  # pylint: disable=broad-except
            pass

    return False


def get_or_create_dataset(
    client: SupersetClient,
    model: ModelSchema,
    database: Any,
) -> Dict[str, Any]:
    """
    Trả về dataset đã tồn tại hoặc tạo mới một dataset.
    """
    filters = {
        "database": OneToMany(database["id"]),
        "schema": model["schema"],
        "table_name": model.get("alias") or model["name"],
    }
    existing = client.get_datasets(**filters)

    if len(existing) > 1:
        raise CLIError("More than one dataset found", 1)

    if existing:
        dataset = existing[0]
        _logger.info("Updating dataset %s", model["unique_id"])
        return client.get_dataset(dataset["id"])

    _logger.info("Creating dataset %s", model["unique_id"])
    try:
        dataset = create_dataset(client, database, model)
        return client.get_dataset(dataset["id"])
    except Exception as excinfo:
        _logger.exception("Unable to create dataset")
        raise CLIError("Unable to create dataset", 1) from excinfo

def get_or_create_virtual_dataset(
    client: SupersetClient,
    model: ModelSchema,
    database: Any,
) -> Dict[str, Any]:
    """
    Trả về virtual dataset đã tồn tại hoặc tạo mới một cái.

    Các virtual dataset được định danh bởi database + table_name.
    Câu truy vấn SQL được thiết lập lúc khởi tạo, không dùng như một filter.
    """
    table_name = model.get("alias") or model["name"]
    filters = {
        "database": OneToMany(database["id"]),
        "table_name": table_name,
    }
    existing = client.get_datasets(**filters)

    if len(existing) > 1:
        raise CLIError("More than one dataset found", 1)

    if existing:
        dataset = existing[0]
        _logger.info("Updating virtual dataset %s", model["unique_id"])
        return client.get_dataset(dataset["id"])

    _logger.info("Creating virtual dataset %s (table=%s)", model["unique_id"], table_name)
    try:
        # Gọi trực tiếp REST API với catalog=None (giống SQLLab UI)
        payload = {
            "database": database["id"],
            "catalog": None,
            "schema": None,
            "table_name": table_name,
            "sql": model.get("sql"),
        }
        if "schema" in payload and not payload["schema"]:
            del payload["schema"]
        if "catalog" in payload and not payload["catalog"]:
            del payload["catalog"]
            
        result = client.create_resource("dataset", **payload)
        return client.get_dataset(result["id"])
    except Exception as excinfo:
        _logger.exception("Unable to create virtual dataset")
        raise CLIError("Unable to create virtual dataset", 1) from excinfo

def get_certification_info(
    model_kwargs: Dict[str, Any],
    certification: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Trả về thông tin chứng nhận (certification) cho một dataset.
    """
    try:
        certification_details = model_kwargs["extra"].pop("certification")
    except KeyError:
        certification_details = certification or DEFAULT_CERTIFICATION
    return certification_details


def compute_metrics(
    dataset_metrics: List[Any],
    dbt_metrics: List[Any],
    reload_columns: bool,
    merge_metadata: bool,
    metric_defaults: Dict[str, Any] | None = None,
) -> List[Any]:
    """
    Tính toán danh sách metric cuối cùng sẽ được sử dụng để cập nhật dataset

    reload_columns (mặc định): đồng bộ dữ liệu dbt & xóa metadata chỉ có trên Superset
    merge_metadata: đồng bộ dữ liệu dbt & giữ lại metadata chỉ có trên Superset
    nếu cả hai đều là false: giữ lại metadata trên Superset & đồng bộ metadata chỉ có ở dbt
    """
    current_dataset_metrics = {
        metric["metric_name"]: metric for metric in dataset_metrics
    }
    model_metrics = {metric["metric_name"]: metric for metric in dbt_metrics}
    final_dataset_metrics = []

    for name, metric_definition in model_metrics.items():
        if reload_columns or merge_metadata or name not in current_dataset_metrics:
            final_metric = {}
            if name in current_dataset_metrics:
                metric_definition["id"] = current_dataset_metrics[name]["id"]
            if metric_defaults:
                final_metric = copy.deepcopy(metric_defaults)
                dict_merge(final_metric, metric_definition)
            final_dataset_metrics.append(final_metric or metric_definition)

    # Giữ lại metadata của Superset
    if not reload_columns:
        for name, metric in current_dataset_metrics.items():
            if not merge_metadata or name not in model_metrics:
                # xóa dữ liệu không nằm trong payload cập nhật
                metric = clean_metadata(metric)
                if merge_metadata and metric_defaults:
                    final_metric = copy.deepcopy(metric_defaults)
                    dict_merge(metric, final_metric)
                final_dataset_metrics.append(metric)

    return final_dataset_metrics


def compute_columns(
    dataset_columns: List[Any],
    refreshed_columns_list: List[Any],
) -> List[Any]:
    """
    Làm mới danh sách các cột trong khi vẫn giữ lại cấu hình hiện tại
    """
    final_dataset_columns = []

    current_dataset_columns = {
        column["column_name"]: column for column in dataset_columns
    }
    refreshed_columns = {
        column["column_name"]: column for column in refreshed_columns_list
    }
    for name, column in refreshed_columns.items():
        if name in current_dataset_columns:
            cleaned_column = clean_metadata(current_dataset_columns[name])
            final_dataset_columns.append(cleaned_column)
        else:
            cleaned_column = clean_metadata(column)
            final_dataset_columns.append(cleaned_column)

    return final_dataset_columns


def compute_columns_metadata(  # pylint: disable=too-many-branches, too-many-arguments  # noqa: C901
    dbt_columns: List[Any],
    dataset_columns: List[Any],
    reload_columns: bool,
    merge_metadata: bool,
    column_defaults: Dict[str, Any],
    dbt_calc_columns: List[Dict[str, Any]],
) -> List[Any]:
    """
    Thêm dbt metadata vào các cột của dataset.

    reload_columns (mặc định): đồng bộ dữ liệu dbt & xóa metadata chỉ có trên Superset
    merge_metadata: đồng bộ dữ liệu dbt & giữ lại metadata chỉ có trên Superset
    nếu cả hai đều là false: giữ lại metadata trên Superset & đồng bộ metadata chỉ có ở dbt
    """
    dbt_metadata = {
        column["name"]: {
            key: column[key]
            for key in ("description", "meta", "label")
            if key in column
        }
        for column in dbt_columns
    }
    for column, definition in dbt_metadata.items():
        superset_overrides = definition.pop("meta", {}).get("superset", {})
        # ưu tiên của verbose_name: meta.superset.verbose_name > dbt label
        # Nếu không có cái nào được thiết lập, thì không thiết lập verbose_name (Superset mặc định hiển thị column_name)
        dbt_label = definition.pop("label", None)
        if "verbose_name" in superset_overrides:
            dbt_metadata[column]["verbose_name"] = superset_overrides.pop(
                "verbose_name",
            )
        elif dbt_label:
            dbt_metadata[column]["verbose_name"] = dbt_label
        for key, value in superset_overrides.items():
            dbt_metadata[column][key] = value
        if column_defaults:
            final_column = copy.deepcopy(column_defaults)
            dict_merge(final_column, dbt_metadata[column])
            dbt_metadata[column] = final_column

    dbt_calc_columns_by_name = {c["column_name"]: c for c in dbt_calc_columns}

    if column_defaults:
        for column, definition in dbt_calc_columns_by_name.items():
            final_column = copy.deepcopy(column_defaults)
            dict_merge(final_column, definition)
            dbt_calc_columns_by_name[column] = final_column
    if reload_columns and dbt_calc_columns_by_name:
        dataset_columns = [
            column
            for column in dataset_columns
            if not column.get("expression")
            or column["column_name"] in dbt_calc_columns_by_name
        ]

    for column in dataset_columns:
        name = column["column_name"]
        # cột thông thường
        if name in dbt_metadata:
            for key, value in dbt_metadata[name].items():
                if reload_columns or merge_metadata or not column.get(key):
                    # Trong chế độ merge_metadata, không ghi đè các giá trị
                    # hiện có của Superset bằng các giá trị dbt rỗng
                    if merge_metadata and not value and column.get(key):
                        continue
                    column[key] = value
        # cột tính toán (calculated column)
        elif name in dbt_calc_columns_by_name:
            for key, value in dbt_calc_columns_by_name[name].items():
                if reload_columns or merge_metadata or not column.get(key):
                    if merge_metadata and not value and column.get(key):
                        continue
                    column[key] = value
            del dbt_calc_columns_by_name[name]
        elif column_defaults and (reload_columns or merge_metadata):
            for key, value in column_defaults.items():
                column[key] = value

        # xóa dữ liệu không nằm trong payload cập nhật
        column = clean_metadata(column)

        # vì lý do nào đó, trường này đôi khi được gửi dưới dạng null
        # https://github.com/preset-io/backend-sdk/issues/163
        if "is_active" in column and column["is_active"] is None:
            del column["is_active"]

    # Thêm các cột tính toán (calc columns) mới vào danh sách
    if dbt_calc_columns_by_name:
        for definition in dbt_calc_columns_by_name.values():
            dataset_columns.append(definition)

    return dataset_columns


def compute_dataset_metadata(  # pylint: disable=too-many-arguments
    model: Dict[str, Any],
    certification: Optional[Dict[str, Any]],
    disallow_edits: bool,
    final_dataset_metrics: List[Any],
    base_url: Optional[URL],
    final_dataset_columns: List[Any],
) -> Dict[str, Any]:
    """
    Trả về metadata của dataset dựa trên thông tin model
    """
    # tải metadata đặc thù của Superset từ định nghĩa dbt model (model.meta.superset)
    model_kwargs = model.get("meta", {}).pop("superset", {})
    certification_details = get_certification_info(model_kwargs, certification)
    extra = {
        "unique_id": model["unique_id"],
        "depends_on": "ref('{name}')".format(**model),
        **model_kwargs.pop(
            "extra",
            {},
        ),
    }
    if certification_details:
        extra["certification"] = certification_details

    # cập nhật metadata của dataset
    update = {
        "description": model.get("description", ""),
        "extra": json.dumps(extra),
        "is_managed_externally": disallow_edits,
        "metrics": final_dataset_metrics,
        **model_kwargs,  # bao gồm các metadata phụ của model được định nghĩa trong model.meta.superset
    }
    # Bao gồm câu lệnh SQL cho virtual dataset để truy vấn được cập nhật khi re-sync
    if model.get("sql"):
        update["sql"] = model["sql"]
    if base_url:
        fragment = "!/model/{unique_id}".format(**model)
        update["external_url"] = str(base_url.with_fragment(fragment))
    if final_dataset_columns:
        update["columns"] = final_dataset_columns

    return update


def sync_datasets(  # pylint: disable=too-many-locals, too-many-arguments
    client: SupersetClient,
    models: List[ModelSchema],
    metrics: Dict[str, List[SupersetMetricDefinition]],
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
    certification: Optional[Dict[str, Any]] = None,
    reload_columns: bool = True,
    merge_metadata: bool = False,
    selective_metrics: bool = False,
) -> Tuple[List[Any], List[str]]:
    """
    Đọc dbt manifest và import các model thành dataset cùng với các metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None
    datasets = []
    failed_datasets = []

    for model in models:
        # lấy dataset tương ứng
        try:
            if model["schema"] is None:
                dataset = get_or_create_virtual_dataset(client, model, database)
            else:
                dataset = get_or_create_dataset(client, model, database)
        except CLIError:
            failed_datasets.append(model["unique_id"])
            continue

        default_configs = (
            model.get("meta", {}).get("superset", {}).pop("default_configs", {})
        )

        # tính toán các metrics
        dbt_metrics_for_model = metrics.get(model["unique_id"], [])
        # Khi sử dụng cờ --metrics để đồng bộ có chọn lọc, giữ lại các
        # metrics hiện tại trên các dataset không có bất kỳ metric nào được chọn
        effective_merge_metrics = merge_metadata or (
            selective_metrics and not dbt_metrics_for_model
        )
        effective_reload_metrics = reload_columns and not (
            selective_metrics and not dbt_metrics_for_model
        )
        final_dataset_metrics = compute_metrics(
            dataset["metrics"],
            dbt_metrics_for_model,
            effective_reload_metrics,
            effective_merge_metrics,
            metric_defaults=default_configs.get("metrics", {}),
        )

        # tính toán các cột
        final_dataset_columns = []
        if not reload_columns:
            if merge_metadata or model.get("sql") or model["schema"] is None:
                # merge_metadata: sử dụng các cột hiện có, dbt metadata sẽ được
                # áp dụng sau trong hàm compute_columns_metadata.
                # Virtual datasets: không có bảng vật lý (physical table) để làm mới từ đó.
                final_dataset_columns = [
                    clean_metadata(col) for col in dataset["columns"]
                ]
                # Đối với virtual dataset: thêm các cột mới từ model
                # chưa từng tồn tại trên Superset, để chúng xuất hiện mà
                # không cần phải "Sync columns from source" theo cách thủ công.
                if model.get("sql") or model["schema"] is None:
                    existing_col_names = {
                        col["column_name"] for col in final_dataset_columns
                    }
                    for dbt_col in model.get("columns", []):
                        col_name = dbt_col.get("name", "")
                        if col_name and col_name not in existing_col_names:
                            is_measurement = is_measurement_column(dbt_col)
                            new_col = {
                                "column_name": col_name,
                                "filterable": True,
                                "groupby": not is_measurement,
                            }
                            # Áp dụng mô tả (description)
                            if dbt_col.get("description"):
                                new_col["description"] = dbt_col["description"]
                            # Áp dụng verbose_name từ meta.superset hoặc từ label
                            superset_meta = dbt_col.get("meta", {}).get(
                                "superset", {},
                            )
                            if superset_meta.get("verbose_name"):
                                new_col["verbose_name"] = superset_meta[
                                    "verbose_name"
                                ]
                            elif dbt_col.get("label"):
                                new_col["verbose_name"] = dbt_col["label"]
                            final_dataset_columns.append(new_col)
                            _logger.debug(
                                "Added new column '%s' to virtual dataset",
                                col_name,
                            )
            else:
                try:
                    refreshed_columns_list = client.get_refreshed_dataset_columns(
                        dataset["id"],
                    )
                    final_dataset_columns = compute_columns(
                        dataset["columns"],
                        refreshed_columns_list,
                    )
                except SupersetError:
                    _logger.warning(
                        "Could not refresh columns for dataset %s, "
                        "falling back to existing columns",
                        model["unique_id"],
                    )
                    final_dataset_columns = [
                        clean_metadata(col) for col in dataset["columns"]
                    ]

        # lấy các cột được tính toán (calculated columns) từ model
        calculated_columns = (
            model.get("meta", {}).get("superset", {}).pop("calculated_columns", [])
        )

        # tính toán payload cập nhật
        update = compute_dataset_metadata(
            model,
            certification,
            disallow_edits,
            final_dataset_metrics,
            base_url,
            final_dataset_columns,
        )

        # Trong chế độ merge_metadata, không ghi đè mô tả của dataset
        # bằng chuỗi rỗng từ dbt
        if merge_metadata and not update.get("description") and dataset.get("description"):
            del update["description"]

        _logger.debug("Updating dataset %s with payload: %s", dataset["id"], update)

        try:
            client.update_dataset(
                dataset["id"], override_columns=reload_columns, **update
            )
        except SupersetError:
            failed_datasets.append(model["unique_id"])
            continue

        # cập nhật column metadata
        dbt_columns = model.get("columns")
        if dbt_columns or calculated_columns:
            current_dataset_columns = client.get_dataset(dataset["id"])["columns"]
            dataset_columns = compute_columns_metadata(
                dbt_columns,
                current_dataset_columns,
                reload_columns,
                merge_metadata,
                default_configs.get("columns", {}),
                calculated_columns,
            )
            try:
                client.update_dataset(
                    dataset["id"],
                    override_columns=reload_columns,
                    columns=dataset_columns,
                )
            except SupersetError:
                failed_datasets.append(model["unique_id"])
                continue

        datasets.append(dataset)

    return datasets, failed_datasets
