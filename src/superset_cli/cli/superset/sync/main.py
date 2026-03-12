"""
Commands for syncing metastores to and from Superset.
"""

import click

from superset_cli.cli.superset.sync.dbt.command import dbt_core
from superset_cli.cli.superset.sync.native.command import native


@click.group()
def sync() -> None:
    """
    Sync metadata between Superset and an external repository.
    """


sync.add_command(native)
sync.add_command(dbt_core, name="dbt-core")
# for backwards compatibility
sync.add_command(dbt_core, name="dbt")
