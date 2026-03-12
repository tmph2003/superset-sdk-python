"""
A simple client for interacting with the dbt API.

References:

    - https://docs.getdbt.com/dbt-cloud/api-v2
    - https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/metadata/schema/metadata-schema-seeds
    - https://github.com/dbt-labs/dbt-cloud-openapi-spec/blob/master/openapi-v3.yaml
"""

# pylint: disable=invalid-name, too-few-public-methods

import logging
from enum import Enum
from typing import Any, Type
from marshmallow import INCLUDE, Schema, fields

from superset_cli import __version__

_logger = logging.getLogger(__name__)

class PostelSchema(Schema):
    """
    Be liberal in what you accept, and conservative in what you send.

    A schema that allows unknown fields. This way if the API returns new fields that
    the client is not expecting no errors will be thrown when validating the payload.
    """

    class Meta:
        """
        Allow unknown fields.
        """

        unknown = INCLUDE


def PostelEnumField(enum: Type[Enum], *args: Any, **kwargs: Any) -> fields.Field:
    """
    Lenient replacement for ``EnumField``.

    This allows us to keep track of the enums expected in a field, while still
    accepting any unexpected new values that are introduced.
    """
    if issubclass(enum, str):
        return fields.String(*args, **kwargs)

    if issubclass(enum, int):
        return fields.Integer(*args, **kwargs)

    return fields.Raw(*args, **kwargs)


class ModelSchema(PostelSchema):
    """
    Schema for a model.
    """

    depends_on = fields.List(fields.String(), data_key="dependsOn")
    children = fields.List(fields.String(), data_key="childrenL1")
    database = fields.String()
    schema = fields.String()
    description = fields.String()
    meta = fields.Raw()
    name = fields.String()
    unique_id = fields.String(data_key="uniqueId")
    tags = fields.List(fields.String())
    columns = fields.Raw(allow_none=True)
    config = fields.Dict(fields.String(), fields.Raw(allow_none=True))


class FilterSchema(PostelSchema):
    """
    Schema for a metric filter.
    """

    field = fields.String()
    operator = fields.String()
    value = fields.String()


class MetricSchema(PostelSchema):
    """
    Base schema for a dbt metric.
    """

    name = fields.String()
    label = fields.String()
    description = fields.String()
    meta = fields.Raw()


class OGMetricSchema(MetricSchema):
    """
    Schema for an OG metric.
    """

    depends_on = fields.List(fields.String(), data_key="dependsOn")
    filters = fields.List(fields.Nested(FilterSchema))
    sql = fields.String()
    type = fields.String()
    unique_id = fields.String(data_key="uniqueId")
    # dbt >= 1.3
    calculation_method = fields.String()
    expression = fields.String()
    dialect = fields.String()
    skip_parsing = fields.Boolean(allow_none=True)


class MFMetricType(str, Enum):
    """
    Type of the MetricFlow metric.
    """

    SIMPLE = "SIMPLE"
    RATIO = "RATIO"
    CUMULATIVE = "CUMULATIVE"
    DERIVED = "DERIVED"


class MFMetricSchema(MetricSchema):
    """
    Schema for a MetricFlow metric.
    """

    type = PostelEnumField(MFMetricType)


class MFSQLEngine(str, Enum):
    """
    Databases supported by MetricFlow.
    """

    BIGQUERY = "BIGQUERY"
    DUCKDB = "DUCKDB"
    REDSHIFT = "REDSHIFT"
    POSTGRES = "POSTGRES"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"
    TRINO = "TRINO"


class MFMetricWithSQLSchema(MFMetricSchema):
    """
    MetricFlow metric with dialect and SQL, as well as model.
    """

    sql = fields.String()
    dialect = PostelEnumField(MFSQLEngine)
    model = fields.String()