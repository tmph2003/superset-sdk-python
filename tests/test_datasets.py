"""
Tests for superset_cli.cli.datasets — dataset sync helpers.

Covers:
  - model_in_database
  - clean_metadata
  - no_catalog_support
  - get_certification_info
  - compute_metrics
  - compute_columns
  - compute_columns_metadata
  - compute_dataset_metadata
"""

import json

import pytest
from sqlalchemy.engine.url import make_url

from cli.datasets import (
    clean_metadata,
    compute_columns,
    compute_columns_metadata,
    compute_dataset_metadata,
    compute_metrics,
    get_certification_info,
    model_in_database,
    no_catalog_support,
    DEFAULT_CERTIFICATION,
)
from exceptions import ErrorLevel, SupersetError


# ─── model_in_database ───────────────────────────────────────────────────
class TestModelInDatabase:
    def test_postgres_match(self):
        model = {"database": "analytics"}
        url = make_url("postgresql+psycopg2://user:pass@host:5432/analytics")
        assert model_in_database(model, url) is True

    def test_postgres_no_match(self):
        model = {"database": "other_db"}
        url = make_url("postgresql+psycopg2://user:pass@host:5432/analytics")
        assert model_in_database(model, url) is False

    def test_bigquery_match(self):
        model = {"database": "my-project-123"}
        url = make_url("bigquery://my-project-123/")
        assert model_in_database(model, url) is True

    def test_bigquery_no_match(self):
        model = {"database": "other-project"}
        url = make_url("bigquery://my-project-123/")
        assert model_in_database(model, url) is False


# ─── clean_metadata ──────────────────────────────────────────────────────
class TestCleanMetadata:
    def test_removes_known_keys(self):
        metadata = {
            "column_name": "amount",
            "type": "FLOAT",
            "autoincrement": True,
            "changed_on": "2024-01-01",
            "comment": "test",
            "created_on": "2024-01-01",
            "default": None,
            "name": "amount",
            "nullable": True,
            "type_generic": 0,
            "precision": 10,
            "scale": 2,
            "max_length": None,
            "info": {},
        }
        result = clean_metadata(metadata)
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
            assert key not in result
        assert "column_name" in result
        assert "type" in result

    def test_preserves_non_removable_keys(self):
        metadata = {"column_name": "id", "filterable": True, "groupby": True}
        result = clean_metadata(metadata)
        assert result == metadata


# ─── no_catalog_support ──────────────────────────────────────────────────
class TestNoCatalogSupport:
    def test_catalog_unknown_field(self):
        exc = SupersetError(
            errors=[
                {
                    "message": json.dumps(
                        {"message": {"catalog": ["Unknown field."]}}
                    ),
                    "error_type": "UNKNOWN_ERROR",
                    "level": ErrorLevel.ERROR,
                },
            ],
        )
        assert no_catalog_support(exc) is True

    def test_other_error(self):
        exc = SupersetError(
            errors=[
                {
                    "message": "Some other error",
                    "error_type": "UNKNOWN_ERROR",
                    "level": ErrorLevel.ERROR,
                },
            ],
        )
        assert no_catalog_support(exc) is False

    def test_empty_errors(self):
        exc = SupersetError(errors=[])
        assert no_catalog_support(exc) is False


# ─── get_certification_info ──────────────────────────────────────────────
class TestGetCertificationInfo:
    def test_default_certification(self):
        model_kwargs = {"extra": {}}
        result = get_certification_info(model_kwargs)
        assert result == DEFAULT_CERTIFICATION

    def test_custom_certification(self):
        custom = {"details": "Custom cert"}
        model_kwargs = {"extra": {}}
        result = get_certification_info(model_kwargs, custom)
        assert result == custom

    def test_certification_from_extra(self):
        cert = {"details": "From extra"}
        model_kwargs = {"extra": {"certification": cert}}
        result = get_certification_info(model_kwargs)
        assert result == cert


# ─── compute_metrics ─────────────────────────────────────────────────────
class TestComputeMetrics:
    def test_reload_columns_replaces_all(self):
        dataset_metrics = [
            {"metric_name": "existing", "expression": "SUM(old)", "id": 1},
        ]
        dbt_metrics = [
            {"metric_name": "revenue", "expression": "SUM(amount)"},
        ]
        result = compute_metrics(
            dataset_metrics, dbt_metrics, reload_columns=True, merge_metadata=False
        )
        names = [m["metric_name"] for m in result]
        assert "revenue" in names
        assert "existing" not in names

    def test_merge_metadata_preserves_existing(self):
        dataset_metrics = [
            {"metric_name": "existing", "expression": "SUM(old)", "id": 1},
        ]
        dbt_metrics = [
            {"metric_name": "revenue", "expression": "SUM(amount)"},
        ]
        result = compute_metrics(
            dataset_metrics, dbt_metrics, reload_columns=False, merge_metadata=True
        )
        names = [m["metric_name"] for m in result]
        assert "revenue" in names
        assert "existing" in names

    def test_preserve_metadata_adds_new(self):
        """When both reload and merge are False, only add metrics not in Superset."""
        dataset_metrics = [
            {"metric_name": "existing", "expression": "SUM(old)", "id": 1},
        ]
        dbt_metrics = [
            {"metric_name": "new_metric", "expression": "SUM(new)"},
        ]
        result = compute_metrics(
            dataset_metrics, dbt_metrics, reload_columns=False, merge_metadata=False
        )
        names = [m["metric_name"] for m in result]
        assert "new_metric" in names
        assert "existing" in names

    def test_update_existing_metric_id(self):
        """When a dbt metric matches an existing one, preserve the Superset id."""
        dataset_metrics = [
            {"metric_name": "revenue", "expression": "SUM(old)", "id": 42},
        ]
        dbt_metrics = [
            {"metric_name": "revenue", "expression": "SUM(amount)"},
        ]
        result = compute_metrics(
            dataset_metrics, dbt_metrics, reload_columns=True, merge_metadata=False
        )
        assert result[0]["id"] == 42
        assert result[0]["expression"] == "SUM(amount)"

    def test_metric_defaults(self):
        dbt_metrics = [
            {"metric_name": "revenue", "expression": "SUM(amount)"},
        ]
        defaults = {"d3format": ",.2f"}
        result = compute_metrics(
            [], dbt_metrics, reload_columns=True, merge_metadata=False,
            metric_defaults=defaults,
        )
        assert result[0].get("d3format") == ",.2f"


# ─── compute_columns ─────────────────────────────────────────────────────
class TestComputeColumns:
    def test_preserve_existing_config(self):
        dataset_columns = [
            {"column_name": "id", "filterable": True, "groupby": True, "verbose_name": "ID"},
        ]
        refreshed_columns = [
            {"column_name": "id", "filterable": False, "groupby": False},
        ]
        result = compute_columns(dataset_columns, refreshed_columns)
        assert len(result) == 1
        # Existing config should be preserved (from dataset_columns)
        assert result[0]["verbose_name"] == "ID"

    def test_add_new_columns(self):
        dataset_columns = [
            {"column_name": "id", "filterable": True},
        ]
        refreshed_columns = [
            {"column_name": "id", "filterable": True},
            {"column_name": "new_col", "filterable": True},
        ]
        result = compute_columns(dataset_columns, refreshed_columns)
        assert len(result) == 2

    def test_remove_deleted_columns(self):
        """Columns not in refreshed should be removed."""
        dataset_columns = [
            {"column_name": "id", "filterable": True},
            {"column_name": "deleted_col", "filterable": True},
        ]
        refreshed_columns = [
            {"column_name": "id", "filterable": True},
        ]
        result = compute_columns(dataset_columns, refreshed_columns)
        names = [c["column_name"] for c in result]
        assert "deleted_col" not in names


# ─── compute_columns_metadata ────────────────────────────────────────────
class TestComputeColumnsMetadata:
    def test_reload_applies_dbt_metadata(self):
        dbt_columns = [
            {
                "name": "amount",
                "description": "Sale amount",
                "meta": {"superset": {"verbose_name": "Amount (VND)"}},
            },
        ]
        dataset_columns = [
            {"column_name": "amount", "filterable": True},
        ]
        result = compute_columns_metadata(
            dbt_columns, dataset_columns,
            reload_columns=True, merge_metadata=False,
            column_defaults={}, dbt_calc_columns=[],
        )
        assert result[0]["description"] == "Sale amount"
        assert result[0]["verbose_name"] == "Amount (VND)"

    def test_merge_preserves_superset_only(self):
        dbt_columns = [
            {"name": "amount", "description": "dbt desc", "meta": {}},
        ]
        dataset_columns = [
            {
                "column_name": "amount",
                "filterable": True,
                "verbose_name": "Existing Name",
            },
        ]
        result = compute_columns_metadata(
            dbt_columns, dataset_columns,
            reload_columns=False, merge_metadata=True,
            column_defaults={}, dbt_calc_columns=[],
        )
        assert result[0]["description"] == "dbt desc"

    def test_dbt_label_as_verbose_name(self):
        """dbt label should be used as verbose_name if no superset override."""
        dbt_columns = [
            {"name": "amount", "description": "", "label": "Doanh số", "meta": {}},
        ]
        dataset_columns = [
            {"column_name": "amount", "filterable": True},
        ]
        result = compute_columns_metadata(
            dbt_columns, dataset_columns,
            reload_columns=True, merge_metadata=False,
            column_defaults={}, dbt_calc_columns=[],
        )
        assert result[0]["verbose_name"] == "Doanh số"

    def test_calculated_columns_added(self):
        dbt_columns = []
        dataset_columns = [
            {"column_name": "id", "filterable": True},
        ]
        calc_columns = [
            {
                "column_name": "profit_margin",
                "expression": "revenue - cost",
                "filterable": True,
            },
        ]
        result = compute_columns_metadata(
            dbt_columns, dataset_columns,
            reload_columns=True, merge_metadata=False,
            column_defaults={}, dbt_calc_columns=calc_columns,
        )
        names = [c["column_name"] for c in result]
        assert "profit_margin" in names

    def test_column_defaults_applied(self):
        dbt_columns = [
            {"name": "amount", "description": "desc", "meta": {}},
        ]
        dataset_columns = [
            {"column_name": "amount", "filterable": True},
        ]
        defaults = {"filterable": True, "groupby": True}
        result = compute_columns_metadata(
            dbt_columns, dataset_columns,
            reload_columns=True, merge_metadata=False,
            column_defaults=defaults, dbt_calc_columns=[],
        )
        assert result[0]["groupby"] is True


# ─── compute_dataset_metadata ────────────────────────────────────────────
class TestComputeDatasetMetadata:
    def test_basic(self):
        model = {
            "name": "fact_sale",
            "unique_id": "model.project.fact_sale",
            "description": "Sales fact table",
            "meta": {},
        }
        result = compute_dataset_metadata(
            model,
            certification=None,
            disallow_edits=False,
            final_dataset_metrics=[],
            base_url=None,
            final_dataset_columns=[],
        )
        assert result["description"] == "Sales fact table"
        assert result["is_managed_externally"] is False
        extra = json.loads(result["extra"])
        assert extra["unique_id"] == "model.project.fact_sale"
        assert extra["depends_on"] == "ref('fact_sale')"

    def test_with_sql(self):
        model = {
            "name": "virtual_ds",
            "unique_id": "model.project.virtual_ds",
            "description": "",
            "meta": {},
            "sql": "SELECT * FROM fact_a JOIN fact_b",
        }
        result = compute_dataset_metadata(
            model,
            certification=None,
            disallow_edits=False,
            final_dataset_metrics=[],
            base_url=None,
            final_dataset_columns=[],
        )
        assert result["sql"] == "SELECT * FROM fact_a JOIN fact_b"

    def test_disallow_edits(self):
        model = {
            "name": "fact_sale",
            "unique_id": "model.project.fact_sale",
            "description": "",
            "meta": {},
        }
        result = compute_dataset_metadata(
            model,
            certification=None,
            disallow_edits=True,
            final_dataset_metrics=[],
            base_url=None,
            final_dataset_columns=[],
        )
        assert result["is_managed_externally"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
