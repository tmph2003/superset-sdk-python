"""
Các hàm tiện ích (Helper functions).
"""

import ast
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, TypedDict, Union

import yaml
from jinja2 import Environment
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import NoSuchModuleError

from api.clients.dbt import MetricSchema, ModelSchema, OGMetricSchema
from exceptions import CLIError

_logger = logging.getLogger(__name__)


class ModelKey(NamedTuple):
    """
    Key for a model, used to map models from datasets.
    """

    schema: Optional[str]
    table: str


def build_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target nhất định.
    """
    type_ = target.get("type")

    if type_ == "postgres":
        return build_postgres_sqlalchemy_params(target)
    if type_ == "redshift":
        return build_redshift_sqlalchemy_params(target)
    if type_ == "bigquery":
        return build_bigquery_sqlalchemy_params(target)
    if type_ == "snowflake":
        return build_snowflake_sqlalchemy_params(target)
    if type_ == "trino":
        return build_trino_sqlalchemy_params(target)

    raise NotImplementedError(
        f"Unable to build a SQLAlchemy URI for a target of type {type_}. Please file an "
        "issue at https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
        f"title=Backend+for+{type_}.",
    )


def build_postgres_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target Postgres.
    """
    if "search_path" in target:
        _logger.warning("Specifying a search path is not supported in Apache Superset")

    username = target["user"]
    password = target.get("password") or target.get("pass")
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    query = {"sslmode": target["sslmode"]} if "sslmode" in target else None

    return {
        "sqlalchemy_uri": str(
            URL(
                drivername="postgresql+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database=dbname,
                query=query,
            ),
        ),
    }


def build_redshift_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target Redshift.
    """
    if "search_path" in target:
        _logger.warning("Specifying a search path is not supported in Apache Superset")

    username = target["user"]
    password = target.get("password") or target.get("pass")
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    query = {"sslmode": target["sslmode"]} if "sslmode" in target else None

    return {
        "sqlalchemy_uri": str(
            URL(
                drivername="redshift+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database=dbname,
                query=query,
            ),
        ),
    }


def build_bigquery_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target BigQuery.

    Hiện tại chỉ hỗ trợ cấu hình thông qua ``keyfile``.
    """
    parameters: Dict[str, Any] = {}

    parameter_map = {
        "priority": "priority",
        "location": "location",
        "maximum_bytes_billed": "maximum_bytes_billed",
    }
    query = {
        kwarg: str(target[key]) for kwarg, key in parameter_map.items() if key in target
    }
    if "priority" in query:
        query["priority"] = query["priority"].upper()
    parameters["sqlalchemy_uri"] = str(
        URL(
            drivername="bigquery",
            host=target["project"],
            database="",
            query=query,
        ),
    )

    if "keyfile" not in target:
        raise Exception(
            "Only service account auth is supported, you MUST pass `keyfile`.",
        )

    with open(target["keyfile"], encoding="utf-8") as input_:
        credentials_info = json.load(input_)
        parameters["encrypted_extra"] = json.dumps(
            {"credentials_info": credentials_info},
        )

    return parameters


def build_snowflake_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target Snowflake.
    """
    username = target["user"]
    password = target.get("password", "") or None
    database = target["database"]
    host = target["account"]
    query = {"role": target["role"], "warehouse": target["warehouse"]}

    parameters = {
        "sqlalchemy_uri": str(
            URL(
                drivername="snowflake",
                username=username,
                password=password,
                host=host,
                database=database,
                query=query,
            ),
        ),
    }

    authenticator = target.get("authenticator")
    if authenticator:
        if authenticator == "externalbrowser":
            raise NotImplementedError("SSO not supported")
        if authenticator.startswith("http"):
            raise NotImplementedError("SSO not supported")
        parameters["extra"] = json.dumps(
            {
                "engine_params": {
                    "connect_args": {
                        "passcode": authenticator,
                    },
                },
            },
        )

    if "private_key_path" in target:
        with open(target["private_key_path"], encoding="utf-8") as input_:
            pk_body = input_.read()

        parameters["encrypted_extra"] = json.dumps(
            {
                "auth_method": "keypair",
                "auth_params": {
                    "privatekey_body": pk_body,
                    "privatekey_pass": target.get("private_key_passphrase", ""),
                },
            },
        )

    return parameters


def build_trino_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xây dựng SQLAlchemy URI cho một target Trino.
    """
    username = target.get("user", "")
    password = target.get("password", "")
    host = target["host"]
    port = target.get("port", 8080)
    catalog = target.get("database", "")

    auth = ""
    if username and password:
        auth = f"{username}:{password}@"
    elif username:
        auth = f"{username}@"

    return {
        "sqlalchemy_uri": f"trino://{auth}{host}:{port}/{catalog}"
    }


def create_engine_with_check(url: URL) -> Engine:
    """
    Trả về một SQLAlchemy engine hoặc văng lỗi nếu thiếu gói phụ thuộc (dependency) bắt buộc.
    """
    try:
        return create_engine(url)
    except NoSuchModuleError as exc:
        string_url = str(url)
        dialect = string_url.split("://", maxsplit=1)[0]
        # TODO: Xử lý thêm nhiều DB engine yêu cầu cài đặt gói bổ sung
        if dialect == "snowflake":
            raise CLIError(
                (
                    "Missing required package. "
                    'Please run ``pip install "preset-cli[snowflake]"`` to install it.'
                ),
                1,
            ) from exc
        raise NotImplementedError(
            f"Unable to build a SQLAlchemy Engine for the {dialect} connection. Please file an "
            "issue at https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
            f"title=Missing+package+for+{dialect}.",
        ) from exc


def env_var(var: str, default: Optional[str] = None) -> str:
    """
    Phiên bản rút gọn của ``env_var`` từ dbt.

    Chúng ta cần hàm này để load profile cùng với các giá trị bảo mật (secrets).
    """
    if var not in os.environ and not default:
        raise Exception(f"Env var required but not provided: '{var}'")
    return os.environ.get(var, default or "")


def as_number(value: str) -> Union[int, float]:
    """
    Phiên bản rút gọn của ``as_number`` từ dbt.
    """
    try:
        return int(value)
    except ValueError:
        return float(value)


class Target(TypedDict):
    """
    Thông tin về cấu hình kết nối tới kho dữ liệu (warehouse connection).
    """

    profile_name: str
    name: str
    schema: str
    type: str
    threads: int


def load_profiles(
    path: Path,
    project_name: str,
    profile_name: str,
    target_name: Optional[str],
) -> Dict[str, Any]:
    """
    Tải file và áp dụng (render) các template Jinja2.

    Chỉ render Jinja2 cho target được chọn, tránh crash khi target khác
    có env_var không có default value.
    """
    with open(path, encoding="utf-8") as input_:
        profiles = yaml.load(input_, Loader=yaml.SafeLoader)

    if profile_name not in profiles:
        raise Exception(f"Profile {profile_name} not found in {path}")
    project = profiles[profile_name]
    outputs = project["outputs"]

    env = Environment()
    env.filters["as_bool"] = bool
    env.filters["as_native"] = ast.literal_eval
    env.filters["as_number"] = as_number
    env.filters["as_text"] = str

    context = {
        "env_var": env_var,
        "project_name": project_name,
        "profile_name": profile_name,
        "target": {},  # Sẽ cập nhật sau khi xác định target
    }

    def apply_templating(config: Any) -> Any:
        """
        Áp dụng render Jinja2 cho các giá trị trong dict một cách đệ quy.
        """
        if isinstance(config, dict):
            for key, value in config.items():
                config[key] = apply_templating(value)
        elif isinstance(config, list):
            config = [apply_templating(el) for el in config]
        elif isinstance(config, str):
            template = env.from_string(config)
            config = yaml.load(template.render(**context), Loader=yaml.SafeLoader)

        return config

    # Render trường 'target' trước để xử lý trường hợp target_name=None
    if target_name is None:
        raw_target = project.get("target", "")
        if isinstance(raw_target, str) and "{{" in raw_target:
            template = env.from_string(raw_target)
            target_name = yaml.load(
                template.render(**context), Loader=yaml.SafeLoader,
            )
        else:
            target_name = raw_target

    if target_name not in outputs:
        raise Exception(f"Target {target_name} not found in the outputs of {path}")

    # Cập nhật context với target đã chọn
    target = outputs[target_name]
    context["target"] = target

    # Chỉ render Jinja2 cho target được chọn (không render tất cả targets)
    profiles[profile_name]["outputs"][target_name] = apply_templating(target)

    # Render trường 'target' (tên target mặc định) trong profile
    if isinstance(project.get("target"), str):
        project["target"] = apply_templating(project["target"])

    return profiles


# pylint: disable=R0911
def filter_models(models: List[ModelSchema], condition: str) -> List[ModelSchema]:
    """
    Lọc danh sách các model dbt theo một điều kiện lựa chọn (select condition).

    Hiện tại chỉ hỗ trợ một phần của cú pháp.

    Xem thêm tại https://docs.getdbt.com/reference/node-selection/syntax.
    """
    # lọc theo tag
    if condition.startswith("tag:"):
        tag = condition.split(":", 1)[1]
        return [model for model in models if tag in model["tags"]]

    if condition.startswith("config"):
        filtered_models = []
        config_key, config_value = re.split(r"[.:]", condition)[1:]
        for model in models:
            if model.get("config", {}).get(config_key) == config_value:
                filtered_models.append(model)
        return filtered_models

    # lọc đơn giản theo tên
    model_names = {model["name"]: model for model in models}
    if condition in model_names:
        return [model_names[condition]]

    # tập tin (file)
    file_path = Path(condition)
    if file_path.is_file() and file_path.stem in model_names:
        return [model_names[file_path.stem]]

    # đường dẫn/thư mục (path/directory)
    if file_path.is_dir() or (
        str(file_path).endswith("/*") and (file_path := file_path.parent)
    ):
        sql_files = [file for file in file_path.rglob("*.sql") if file.is_file()]
        return [
            model_names[file.stem] for file in sql_files if file.stem in model_names
        ]

    # toán tử plus và n-plus
    if "+" in condition:
        return filter_plus_operator(models, condition)

    # toán tử at (@) -- dựa theo tài liệu, có vẻ toán tử này chỉ có thể được dùng trước tên model
    # (https://docs.getdbt.com/reference/node-selection/graph-operators#the-at-operator)
    if condition.startswith("@"):
        return filter_at_operator(models, condition)

    raise NotImplementedError(
        f"Unable to parse the selection {condition}. Please file an issue at "
        "https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
        f"title=dbt+select+{condition}.",
    )


def filter_plus_operator(
    models: List[ModelSchema],
    condition: str,
) -> List[ModelSchema]:
    """
    Lọc danh sách các model sử dụng toán tử plus (+) hoặc n-plus (n+).
    """
    model_ids = {model["unique_id"]: model for model in models}
    model_names = {model["name"]: model for model in models}

    match = re.match(r"^(\d*\+)?(.*?)(\+\d*)?$", condition)
    # pylint: disable=invalid-name
    up, name, down = match.groups()  # type: ignore
    base_model = model_names[name]
    selected_models: Dict[str, ModelSchema] = {}

    if up:
        degrees = None if len(up) == 1 else int(up[:-1])
        queue = [(base_model, 0)]
        while queue:
            model, degree = queue.pop(0)
            id_ = model["unique_id"]
            if id_ not in selected_models:
                selected_models[id_] = model
                if degrees is None or degree < degrees:
                    queue.extend(
                        (model_ids[parent_id], degree + 1)
                        for parent_id in model.get("depends_on", [])
                        if parent_id in model_ids
                    )

    if down:
        degrees = None if len(down) == 1 else int(down[1:])
        queue = [(base_model, 0)]
        while queue:
            model, degree = queue.pop(0)
            id_ = model["unique_id"]
            if id_ not in selected_models:
                selected_models[id_] = model
                if degrees is None or degree < degrees:
                    queue.extend(
                        (model_ids[child_id], degree + 1)
                        for child_id in model.get("children", [])
                        if child_id in model_ids
                    )

    return list(selected_models.values())


def filter_at_operator(models: List[ModelSchema], condition: str) -> List[ModelSchema]:
    """
    Lọc danh sách các model sử dụng toán tử at (@).
    """
    model_ids = {model["unique_id"]: model for model in models}
    model_names = {model["name"]: model for model in models}

    base_model = model_names[condition[1:]]
    selected_models: Dict[str, ModelSchema] = {}

    queue = [base_model]
    while queue:
        model = queue.pop(0)
        id_ = model["unique_id"]
        if id_ not in selected_models:
            selected_models[id_] = model

            # thêm các children
            queue.extend(
                model_ids[child_id]
                for child_id in model.get("children", [])
                if child_id in model_ids
            )

            # thêm các parents (phụ thuộc) của các children thuộc model được chọn
            if model != base_model:
                queue.extend(
                    model_ids[parent_id]
                    for parent_id in model.get("depends_on", [])
                    if parent_id in model_ids
                )

    return list(selected_models.values())


def apply_select(
    models: List[ModelSchema],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
) -> List[ModelSchema]:
    """
    Áp dụng lọc dbt node (https://docs.getdbt.com/reference/node-selection/syntax).
    """
    model_ids = {model["unique_id"]: model for model in models}
    selected: Dict[str, ModelSchema]
    if not select:
        selected = {model["unique_id"]: model for model in models}
    else:
        selected = {}
        for selection in select:
            ids = set.intersection(
                *[
                    {model["unique_id"] for model in filter_models(models, condition)}
                    for condition in selection.split(",")
                ]
            )
            selected.update({id_: model_ids[id_] for id_ in ids})

    for selection in exclude:
        for id_ in set.intersection(
            *[
                {model["unique_id"] for model in filter_models(models, condition)}
                for condition in selection.split(",")
            ]
        ):
            if id_ in selected:
                del selected[id_]

    return list(selected.values())


def list_failed_models(failed_models: List[str]) -> str:
    """
    Liệt kê các model đã đồng bộ thất bại.
    """
    error_message = "Below model(s) failed to sync:"
    for failed_model in failed_models:
        error_message += f"\n - {failed_model}"

    return error_message


def get_og_metric_from_config(
    metric_config: Dict[str, Any],
    dialect: str,
    depends_on: Optional[List[str]] = None,
    sql: Optional[str] = None,
) -> OGMetricSchema:
    """
    Trả về một og metric từ cấu hình (config), tuân theo dbt Cloud schema.
    """
    metric_schema = OGMetricSchema()
    if depends_on is not None:
        metric_config["dependsOn"] = depends_on
        metric_config.pop("depends_on", None)
    else:
        metric_config["dependsOn"] = metric_config.pop("depends_on")["nodes"]

    if sql is not None:
        metric_config["expression"] = sql
        metric_config["calculation_method"] = "derived"
        metric_config.pop("type", None)
        metric_config.pop("sql", None)

    metric_config["uniqueId"] = metric_config.pop("unique_id")
    metric_config["dialect"] = dialect

    return metric_schema.load(metric_config)


def parse_metric_meta(metric: MetricSchema) -> Dict[str, Any]:
    """
    Phân tích (parse) thông tin meta của metric.
    """
    kwargs = metric.get("meta", {}).pop("superset", {})
    metric_name_override = kwargs.pop("metric_name", None)
    return {
        "meta": metric.get("meta", {}),
        "kwargs": kwargs,
        "metric_name_override": metric_name_override,
    }


def build_model_fqn_lookup(
    configs: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Tạo một mapping từ ``catalog.schema.table`` sang manifest model node.

    Mapping này được dùng bởi nhiều module khác nhau (như ``relations``, ``metricflow``) cần
    tra cứu metadata của model dựa trên tên đầy đủ (fully-qualified table name).
    """
    lookup: Dict[str, Dict[str, Any]] = {}
    for node in configs.get("nodes", {}).values():
        if node.get("resource_type") == "model":
            fqn = f"{node['database']}.{node['schema']}.{node['name']}"
            lookup[fqn] = node
    return lookup


def is_measurement_column(column: Dict[str, Any]) -> bool:
    """
    Trả về True nếu một cột được đánh dấu (flagged) là measurement.

    Hàm sẽ kiểm tra cả ``column.meta.is_measurement`` lẫn
    ``column.config.meta.is_measurement`` (dbt có thể đặt meta ở cả hai nơi này).
    """
    return bool(
        column.get("meta", {}).get("is_measurement")
        or column.get("config", {}).get("meta", {}).get("is_measurement")
    )
