# Semantic Layer Tool (superset-cli)

A command-line interface for interacting with Apache Superset instances, enabling resource synchronization (databases, datasets, charts, dashboards) and SQL execution.

## Project Overview

*   **Main Purpose:** Synchronize Superset assets from source control (native YAML or dbt Core/MetricFlow) and provide interactive/programmatic SQL access to analytical databases.
*   **Key Technologies:**
    *   **Language:** Python 3.8+
    *   **CLI Framework:** Click
    *   **Data Modeling/SQL:** SQLAlchemy, SQLGlot, Pandas
    *   **Integrations:** dbt Core (Semantic Layer / MetricFlow), yarl (URL handling)
    *   **Architecture:** Modular design with separate layers for API interaction (`src/superset_cli/api`), authentication (`src/superset_cli/auth`), and CLI commands (`src/superset_cli/cli`).

## Building and Running

### Installation
```bash
# From the project root
pip install .
```

### Core Commands
The primary entry point is `superset-cli`.
```bash
# Basic usage pattern
superset-cli <instance_url> [options] <command>

# Key Commands:
# - sql: Run SQL against analytical databases
# - sync native: Sync assets from a directory of YAML files
# - sync dbt-core: Sync assets from a dbt Core project
# - export: Export assets to YAML files
```

### Development Workflow
*   **Environment Setup:** Uses `pyenv` and `Makefile`. Run `make .python-version` to set up the virtualenv and install dependencies.
*   **Code Quality:** Run `make check` to execute `pre-commit` hooks (includes linting, formatting, and other checks).
*   **Spellcheck:** Run `make spellcheck` to verify documentation and source files.
*   **Testing:** (TODO: Test suite location and execution command to be verified; current structure suggests `pytest` but no tests were found in the root).

## Development Conventions

*   **Code Style:** Follows `flake8` and `black` (88 character line limit).
*   **Dependency Management:** Managed via `requirements.in` and compiled to `requirements.txt` using `pip-compile`.
*   **Project Structure:** Scaffolded using `PyScaffold`.
*   **Sync Logic:** Prioritizes correct grain and join logic when syncing from dbt. Idempotency is a core requirement for sync operations.
*   **Metadata:** Superset-specific fields can be defined in dbt model definitions under `model.meta.superset`.
