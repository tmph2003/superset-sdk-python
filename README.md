# Semantic Layer Tool

A lightweight, heavily optimized Python CLI tool designed to synchronize models and metrics from a [dbt](https://www.getdbt.com/) project directly into an Apache Superset instance.

This project has been aggressively simplified to focus **exclusively** on dbt synchronization, ensuring maximum speed, minimal dependency footprint, and high maintainability.

---

## 🚀 Features

- **Automated Database Setup:** Automatically provisions the `$target` database connection in Superset using your local `profiles.yml`.
- **Dataset Synchronization:** Converts every dbt source and model matched by your `--select` criteria into an interactive Dataset within Superset.
- **Metric Integration:** Extracts and attaches metrics (both standard metrics and Semantic Layer/MetricFlow configurations) directly to their corresponding Superset datasets.
- **Metadata Propagation:** Synchronizes column descriptions, labels, and advanced metadata from your dbt models to Superset.
- **Superset-specific Metadata:** Pass Superset-exclusive settings (e.g. `cache_timeout`) via the `meta` tag in your dbt YML files.

---

## 📦 Installation

This tool requires Python 3.8+.

```bash
# Clone the repository
git clone https://github.com/tmph2003/superset-sdk-python.git
cd superset-sdk-python

# Install the package locally
pip install -e .
```

---

## 💻 Usage

The CLI acts as a bridge between your compiled dbt `manifest.json` and your Superset instance API.

### Basic Sync Command

```bash
superset-cli https://superset-dev.sunhouse.com.vn/ target/manifest.json \
  --username admin \
  --password admin \
  --project=sunhouse_etl_pipeline \
  --profile=sunhouse_etl_pipeline \
  --target=dev \
  --profiles=profiles.yml \
  --select models/gold/ \
  --merge-metadata \
  --max-workers=3
```

### Advanced Metadata Customization

The tool allows you to pass Superset-exclusive settings via the `meta` tag in your dbt configurations. This works for both Database connections (`profiles.yml`) and Datasets (`models/*.yml`).

#### 1. Customizing the Database Connection (`profiles.yml`)
When using `--import-db`, the CLI reads your dbt target to create the Superset database. You can customize the name of the database or enable SQL Lab features by adding a `meta` block to your target in `profiles.yml`:

```yaml
sunhouse_etl_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: my_gcp_project
      dataset: my_dbt_dataset
      # Pass Superset-specific overrides here
      meta:
        superset:
          database_name: "Sunhouse Data Warehouse" # Overrides the default "{project}_{target}" name
          cache_timeout: 86400                     # Database-level cache timeout
          expose_in_sqllab: true                   # Enable SQL Lab access
```

#### 2. Customizing Datasets (`models/*.yml`)
Similarly, you can specify values for Superset-only fields directly in your dbt model definitions under the `model.meta.superset.{{field_name}}` key:

```yaml
models:
  - name: my_dbt_model
    meta:
      superset:
        cache_timeout: 250 # Sets the dataset cache timeout to 250 seconds in Superset.
        filter_select_enabled: true
```

### Command Options

Run `superset-cli --help` for a full list of configuration options:

- `--jwt-token`: Authenticate via JWT token instead of username/password.
- `--import-db`: Import (or update) the database connection to Superset automatically.
- `--select` / `-s`: Select specific models or paths to sync (e.g. `models/gold/`).
- `--exclude` / `-x`: Exclude specific models from syncing.
- `--metrics` / `-m`: Select specific metrics to sync (comma-separated).
- `--merge-metadata`: Update Superset configurations based on dbt metadata while preserving Superset-only metrics.
- `--preserve-metadata`: Completely preserve existing column and metric configurations defined in Superset.
- `--disallow-edits`: Mark resources as managed externally to prevent users from editing them in the Superset UI.
- `--max-workers`: Control the number of parallel workers used for processing Semantic Layer metrics (default: 3).

---

## 🛠️ Development Guide

If you wish to contribute or customize this tool, understanding the highly flattened, modular architecture is critical.

### Project Structure Overview

```text
superset-sdk-python/
├── pyproject.toml      # Modern Python build system metadata & dependencies
├── setup.cfg           # Core packaging configs and entry point definitions
├── README.md           # Documentation
├── tests/              # Pytest unit tests (run using `pytest -v`)
└── src/                # The core source code
```

### Module Guide (`src/`)

The core codebase is divided into specialized layers to handle API interactions, authentication, and CLI orchestration.

#### 1. CLI Execution Layer (`src/cli/`)
This is where the orchestration of the synchronization happens.
- **`command.py`**: The main entry point. Defines the Click CLI arguments, sets up logging, parses inputs, and directs the overall sync flow.
- **`databases.py`**: Handles importing or updating the main database connection inside Superset using credentials found in dbt's `profiles.yml`.
- **`datasets.py`**: The heavy lifter for models. Syncs physical tables/views into Superset Datasets, applies columns, data types, descriptions, and metrics.
- **`metrics.py`**: Extracts traditional dbt metric definitions, parses their aggregations, and structures them for Superset.
- **`metricflow.py`**: Specifically handles advanced MetricFlow / dbt Semantic Layer definitions (e.g. derived metrics, ratios) using concurrent workers.
- **`relations.py`**: Manages entity relationships, generating CTEs and complex join conditions based on Semantic Layer constraints.
- **`lib.py`**: Shared CLI utility functions, such as profile loading and model filtering logic.

#### 2. API Interaction Layer (`src/api/`)
Contains code for interpreting files and talking to external APIs.
- **`clients/superset.py`**: A robust REST API client for Superset. Performs paginated GETs, POSTs, and PUTs to manipulate databases and datasets.
- **`clients/dbt.py`**: Uses `marshmallow` schemas to safely and cleanly parse the massive dbt `manifest.json` files.
- **`operators.py`**: Simple filtering operators (e.g. `Equal`, `OneToMany`) used to build queries for the Superset API.

#### 3. Authentication Layer (`src/auth/`)
Handles the nuances of logging in and maintaining sessions.
- **`main.py`**: A generic base class that automatically intercepts `401 Unauthorized` responses and re-authenticates.
- **`superset.py`**: Implements Superset-specific login flows, including Username/Password login (which fetches a CSRF token) and JWT Token login.
- **`token.py`**: A lightweight implementation for simple Bearer token injection.

#### 4. Shared Utilities (`src/`)
- **`lib.py`**: Generic helpers like logging initialization and error payload validation (SIP-40 compliance).
- **`exceptions.py`**: Custom exception classes (`CLIError`, `SupersetError`, `DatabaseNotFoundError`) for clean error handling.

### Running Tests

We use `pytest` for unit testing. The test suite is fast and covers metric parsing, dataset sync logic, and API serialization.

```bash
# Run all tests with verbosity
pytest -v
```