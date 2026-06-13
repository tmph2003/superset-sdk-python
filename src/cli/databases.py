"""
Đồng bộ dbt database sang Superset.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from api.clients.superset import SupersetClient
from cli.lib import load_profiles
from exceptions import DatabaseNotFoundError

_logger = logging.getLogger(__name__)


def sync_database(  # pylint: disable=too-many-locals, too-many-arguments
    client: SupersetClient,
    profiles_path: Path,
    project_name: str,
    profile_name: str,
    target_name: Optional[str],
    disallow_edits: bool,  # pylint: disable=unused-argument
    external_url_prefix: str,
) -> Any:
    """
    Đọc target database từ một tệp dbt profiles.yml và tìm database tương ứng trên Superset.
    """
    profiles = load_profiles(profiles_path, project_name, profile_name, target_name)
    project = profiles[profile_name]
    outputs = project["outputs"]
    if target_name is None:
        target_name = project["target"]
    target = outputs[target_name]

    # Đọc thêm siêu dữ liệu (metadata) cần được áp dụng cho DB
    meta = target.get("meta", {}).get("superset", {})

    database_name = meta.pop("database_name", f"{project_name}_{target_name}")
    databases = client.get_databases(database_name=database_name)
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    if databases:
        _logger.info("Found an existing database connection, using it")
        database = databases[0]
        database["sqlalchemy_uri"] = client.get_database(database["id"])[
            "sqlalchemy_uri"
        ]
        return database

    raise DatabaseNotFoundError()
