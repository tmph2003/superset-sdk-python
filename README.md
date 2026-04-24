# Semantic Layer Tool
This tool is a command line interface (CLI) to interact with your Apache Superset instances. Currently it can be used to sync resources (databases, datasets, charts, dashboards) from source control, either in native format or from a `dbt <https://www.getdbt.com/>`_ project. It can also be used to run SQL against any database in any instance. In the future, the CLI will also allow you to manage your instances and users.


## Installation

```
# Redirect to repo folder
pip install .
```
## Usage
The following commands are currently available:

- ``superset-cli sql``: run SQL interactively or programmatically against an analytical database.
- ``superset-cli export-assets`` (alternatively, ``superset-cli export``): export resources (databases, datasets, charts, dashboards) into a directory as YAML files.
- ``superset-cli export-ownership``: export resource ownership (UUID -> email) into a YAML file.
- ``superset-cli export-rls``: export RLS rules into a YAML file.
- ``superset-cli export-roles``: export user roles into a YAML file.
- ``superset-cli export-users``: export users (name, username, email, roles) into a YAML file.
- ``superset-cli sync native`` (alternatively, ``superset-cli import-assets``): synchronize the workspace from a directory of templated configuration files.
- ``superset-cli sync dbt-core``: synchronize the workspace from a dbt Core project.

#### Synchronizing to and from dbt
The CLI also allows you to synchronize models, and metrics from a dbt project.

```bash
% superset-cli  --username admin --password admin https://superset-dev.sunhouse.com.vn/ sync dbt-core --select models/gold/ --project=sunhouse_etl_pipeline --profile=sunhouse_etl_pipeline --target=dev --profiles=profiles.yml target/manifest.json
```

Running this command will:

1. Read the dbt profile and create the ``$target`` database for the specified project in the Apache Superset instance.
2. Every source in the project will be created as a dataset in the Apache Superset instance.
3. Every model in the project will be created as a dataset in the Apache Superset instance.
4. Any `metrics <https://docs.getdbt.com/docs/building-a-dbt-project/metrics>`_ will be added to the corresponding datasets.
5. Every dashboard built on top of the dbt sources and/or models will be synchronized back to dbt as an `exposure <https://docs.getdbt.com/docs/building-a-dbt-project/exposures>`_.

Descriptions, labels and other metadata is also synced from dbt models to the corresponding fields in the dataset. It's also possible to specify values for Superset-only fields directly in the model definition, under ``model.meta.superset.{{field_name}}``. For example, to specify the cache timeout for a dataset:

```yaml

    models:
      - name: my_dbt_model
        meta:
          superset:
            cache_timeout: 250 # Setting the dataset cache timeout to 250. 
```