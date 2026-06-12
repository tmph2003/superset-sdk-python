"""
A client for interacting with the Superset API.

Supports CRUD operations for databases and datasets.
"""

# pylint: disable=consider-using-f-string, too-many-lines

import json
import logging
from typing import (
    Any,
    Dict,
    List,
    Optional,
    TypedDict,
    Union,
)

import prison
from yarl import URL

__version__ = "1.0.0"
from api.operators import Equal, Operator
from auth.main import Auth
from lib import validate_response

_logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 100


class SupersetMetricDefinition(TypedDict, total=False):
    """
    Definition of a Superset metric.

    Used in the PUT API for datasets.
    """

    id: int
    expression: str
    metric_name: str
    metric_type: str
    verbose_name: str
    description: str
    extra: str
    warning_text: str
    d3format: str
    currency: str
    uuid: str


class SupersetClient:  # pylint: disable=too-many-public-methods

    """
    A client for running queries against Superset.
    """

    def __init__(self, baseurl: Union[str, URL], auth: Auth):
        # convert to URL if necessary
        self.baseurl = URL(baseurl)
        self.auth = auth

        self.session = auth.session
        self.session.headers.update(auth.get_headers())
        self.session.headers["Referer"] = str(self.baseurl)
        self.session.headers["User-Agent"] = f"Apache Superset Client ({__version__})"


    def get_resource(self, resource_name: str, resource_id: int) -> Any:
        """
        Return a single resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)

        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()["result"]

        return resource

    def get_resources(self, resource_name: str, **kwargs: Any) -> List[Any]:
        """
        Return one or more of a resource, possibly filtered.
        """
        resources = []
        operations = {
            k: v if isinstance(v, Operator) else Equal(v) for k, v in kwargs.items()
        }

        # paginate endpoint until no results are returned
        page = 0
        while True:
            query = prison.dumps(
                {
                    "filters": [
                        dict(col=col, opr=value.operator, value=value.value)
                        for col, value in operations.items()
                    ],
                    "order_column": "changed_on_delta_humanized",
                    "order_direction": "desc",
                    "page": page,
                    "page_size": MAX_PAGE_SIZE,
                },
            )
            url = self.baseurl / "api/v1" / resource_name / "" % {"q": query}

            _logger.debug("GET %s", url)
            response = self.session.get(url)
            validate_response(response)

            payload = response.json()

            if not payload["result"]:
                break

            resources.extend(payload["result"])
            page += 1

        return resources

    def create_resource(self, resource_name: str, **kwargs: Any) -> Any:
        """
        Create a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / ""

        _logger.debug("POST %s\n%s", url, json.dumps(kwargs, indent=4))
        response = self.session.post(url, json=kwargs)
        validate_response(response)

        resource = response.json()

        return resource

    def update_resource(
        self,
        resource_name: str,
        resource_id: int,
        query_args: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Update a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)
        if query_args:
            url %= query_args

        response = self.session.put(url, json=kwargs)
        validate_response(response)

        resource = response.json()

        return resource

    def get_resource_endpoint_info(self, resource_name: str, **kwargs: Any) -> Any:
        """
        Get resource endpoint info (such as available columns) possibly filtered.
        """
        query = prison.dumps({"keys": list(kwargs["keys"])} if "keys" in kwargs else {})

        url = self.baseurl / "api/v1" / resource_name / "_info" % {"q": query}
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        endpoint_info = response.json()

        return endpoint_info

    def validate_key_in_resource_schema(
        self, resource_name: str, key_name: str, **kwargs: Any
    ) -> Any:
        """
        Validate if a key is present in a resource schema.
        """
        schema_validation = {}

        endpoint_info = self.get_resource_endpoint_info(resource_name, **kwargs)

        for key in kwargs.get("keys", ["add_columns", "edit_columns"]):
            schema_columns = [column["name"] for column in endpoint_info.get(key, [])]
            schema_validation[key] = key_name in schema_columns

        return schema_validation

    def get_database(self, database_id: int) -> Any:
        """
        Return a single database.
        """
        database = self.get_resource("database", database_id)
        if "sqlalchemy_uri" in database:
            return database

        url = self.baseurl / "api/v1/database" / str(database_id) / "connection"
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()["result"]

        return resource

    def get_databases(self, **kwargs: str) -> List[Any]:
        """
        Return databases, possibly filtered.
        """
        return self.get_resources("database", **kwargs)

    def create_database(self, **kwargs: Any) -> Any:
        """
        Create a database.
        """
        return self.create_resource("database", **kwargs)

    def update_database(self, database_id: int, **kwargs: Any) -> Any:
        """
        Update a database.
        """
        return self.update_resource("database", database_id, **kwargs)

    def get_dataset(self, dataset_id: int) -> Any:
        """
        Return a single dataset.
        """
        return self.get_resource("dataset", dataset_id)

    def get_refreshed_dataset_columns(self, dataset_id: int) -> List[Any]:
        """
        Return dataset columns.
        """
        url = self.baseurl / "datasource/external_metadata/table" / str(dataset_id)
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()
        return resource

    def get_datasets(self, **kwargs: str) -> List[Any]:
        """
        Return datasets, possibly filtered.
        """
        return self.get_resources("dataset", **kwargs)

    def create_dataset(self, **kwargs: Any) -> Any:
        """
        Create a dataset.
        """
        if "sql" not in kwargs:
            return self.create_resource("dataset", **kwargs)

        # Check if the dataset creation supports sql directly
        not_legacy = self.validate_key_in_resource_schema(
            "dataset",
            "sql",
            keys=["add_columns"],
        )
        not_legacy = not_legacy["add_columns"]
        if not_legacy:
            return self.create_resource("dataset", **kwargs)

        # run query to determine columns types
        payload = self._run_query(
            database_id=kwargs["database"],
            sql=kwargs["sql"],
            schema=kwargs.get("schema", ""),
            limit=1,
        )

        # now add the virtual dataset
        columns = payload["columns"]
        for column in columns:
            column["column_name"] = column["name"]
            column["groupby"] = True
            # Superset <= 1.4 returns ``is_date`` instead of ``is_dttm``
            if column.get("is_dttm") or column.get("is_date"):
                column["type_generic"] = 2
            elif column["type"] is None:
                column["type"] = "UNKNOWN"
                column["type_generic"] = 1
            elif column["type"].lower() == "string":
                column["type_generic"] = 1
            else:
                column["type_generic"] = 0
        payload = {
            "sql": kwargs["sql"],
            "dbId": kwargs["database"],
            "schema": kwargs.get("schema", ""),
            "datasourceName": kwargs["table_name"],
            "columns": columns,
        }
        data = {"data": json.dumps(payload)}

        url = self.baseurl / "superset/sqllab_viz/"
        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, data=data)
        validate_response(response)

        payload = response.json()

        # Superset <= 1.4 returns ``{"table_id": dataset_id}`` rather than the dataset payload
        return payload["data"] if "data" in payload else {"id": payload["table_id"]}

    def update_dataset(
        self,
        dataset_id: int,
        override_columns: bool = False,
        **kwargs: Any,
    ) -> Any:
        """
        Update a dataset.
        """
        query_args = {"override_columns": "true" if override_columns else "false"}
        return self.update_resource("dataset", dataset_id, query_args, **kwargs)

