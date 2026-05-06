"""
Integration tests using the real manifest.json from the dbt project.

These tests verify that the actual dbt manifest can be parsed, models
can be loaded and filtered, metrics can be extracted, and the
build_model_fqn_lookup utility works against production data.

Covers:
  - Manifest loading and basic structure validation
  - Model parsing via ModelSchema
  - Metric parsing and classification
  - Model filtering by tag, config, name
  - apply_select with real model data
  - build_model_fqn_lookup against real manifest
  - is_measurement_column against real columns
  - Semantic model extraction
"""

import json
import os
from pathlib import Path

import pytest

from superset_cli.api.clients.dbt import (
    MFMetricType,
    ModelSchema,
)
from superset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    build_model_fqn_lookup,
    filter_models,
    is_measurement_column,
)
from superset_cli.cli.superset.sync.dbt.metrics import is_derived


# Path to the manifest.json in the tests directory
MANIFEST_PATH = Path(__file__).parent / "manifest.json"

# Skip all tests if manifest.json is not present
pytestmark = pytest.mark.skipif(
    not MANIFEST_PATH.exists(),
    reason="manifest.json not found in tests directory",
)


@pytest.fixture(scope="module")
def manifest():
    """Load the real manifest.json once per module."""
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def models(manifest):
    """Parse all model nodes from the manifest."""
    model_schema = ModelSchema()
    parsed = []
    for node_config in manifest["nodes"].values():
        if node_config["resource_type"] == "model":
            unique_id = node_config["uniqueId"] = node_config["unique_id"]
            node_config["childrenL1"] = manifest["child_map"].get(unique_id, [])
            node_config["dependsOn"] = node_config.get("depends_on", {}).get(
                "nodes", []
            )
            cols = node_config.get("columns", {})
            if isinstance(cols, dict):
                node_config["columns"] = list(cols.values())
            parsed.append(model_schema.load(node_config))
    return parsed


# ─── Manifest structure ──────────────────────────────────────────────────
class TestManifestStructure:
    def test_has_required_keys(self, manifest):
        required_keys = {"nodes", "metrics", "sources", "child_map"}
        assert required_keys <= set(manifest.keys())

    def test_has_nodes(self, manifest):
        assert len(manifest["nodes"]) > 0

    def test_has_metrics(self, manifest):
        assert len(manifest["metrics"]) > 0

    def test_has_semantic_models(self, manifest):
        assert "semantic_models" in manifest
        assert len(manifest["semantic_models"]) > 0


# ─── Model parsing ────────────────────────────────────────────────────────
class TestModelParsing:
    def test_models_not_empty(self, models):
        assert len(models) > 0

    def test_model_has_required_fields(self, models):
        """Every parsed model should have name, unique_id, schema, database."""
        for model in models[:10]:  # Sample first 10
            assert "name" in model
            assert "unique_id" in model
            assert "schema" in model or model.get("schema") is None
            assert "database" in model

    def test_unique_ids_are_unique(self, models):
        ids = [m["unique_id"] for m in models]
        assert len(ids) == len(set(ids))

    def test_model_count_realistic(self, models, manifest):
        """Model count should match number of model nodes in manifest."""
        expected = sum(
            1
            for n in manifest["nodes"].values()
            if n["resource_type"] == "model"
        )
        assert len(models) == expected


# ─── Metric classification ───────────────────────────────────────────────
class TestMetricClassification:
    def test_metric_types(self, manifest):
        """Verify that all metrics have a known type."""
        known_types = {"simple", "derived", "ratio", "cumulative"}
        for metric in manifest["metrics"].values():
            assert metric["type"] in known_types, (
                f"Unknown metric type: {metric['type']} for {metric['name']}"
            )

    def test_derived_metrics_detected(self, manifest):
        """Check that is_derived correctly identifies derived metrics."""
        for metric in manifest["metrics"].values():
            if metric["type"] == "derived":
                assert is_derived(metric) is True

    def test_simple_metrics_not_derived(self, manifest):
        for metric in manifest["metrics"].values():
            if metric["type"] == "simple":
                assert is_derived(metric) is False

    def test_all_metrics_have_labels(self, manifest):
        """Every metric should have a human-readable label."""
        for metric in manifest["metrics"].values():
            assert metric.get("label"), f"Metric {metric['name']} missing label"


# ─── Model filtering ─────────────────────────────────────────────────────
class TestModelFiltering:
    def test_filter_by_name(self, models):
        """Filter by exact model name should return that model."""
        first_model = models[0]
        result = filter_models(models, first_model["name"])
        assert len(result) == 1
        assert result[0]["name"] == first_model["name"]

    def test_filter_by_tag_returns_subset(self, models):
        """Filtering by tag should return <= total models."""
        # Find a tag that at least one model has
        tags_found = set()
        for m in models:
            tags_found.update(m.get("tags", []))

        if tags_found:
            tag = list(tags_found)[0]
            result = filter_models(models, f"tag:{tag}")
            assert 0 < len(result) <= len(models)

    def test_filter_by_config_materialized(self, models):
        """Filter by materialization type."""
        # Find a materialization type
        mat_types = set()
        for m in models:
            if m.get("config", {}).get("materialized"):
                mat_types.add(m["config"]["materialized"])

        if mat_types:
            mat = list(mat_types)[0]
            result = filter_models(models, f"config.materialized:{mat}")
            assert len(result) > 0
            for m in result:
                assert m["config"]["materialized"] == mat


# ─── apply_select ─────────────────────────────────────────────────────────
class TestApplySelectIntegration:
    def test_no_filter_returns_all(self, models):
        result = apply_select(models, select=(), exclude=())
        assert len(result) == len(models)

    def test_exclude_reduces_count(self, models):
        tags_found = set()
        for m in models:
            tags_found.update(m.get("tags", []))

        if tags_found:
            tag = list(tags_found)[0]
            result = apply_select(models, select=(), exclude=(f"tag:{tag}",))
            assert len(result) < len(models)


# ─── build_model_fqn_lookup ──────────────────────────────────────────────
class TestBuildModelFqnLookupIntegration:
    def test_lookup_count(self, manifest):
        lookup = build_model_fqn_lookup(manifest)
        model_count = sum(
            1
            for n in manifest["nodes"].values()
            if n["resource_type"] == "model"
        )
        assert len(lookup) == model_count

    def test_lookup_keys_format(self, manifest):
        """Keys should be in catalog.schema.table format."""
        lookup = build_model_fqn_lookup(manifest)
        for key in list(lookup.keys())[:10]:
            parts = key.split(".")
            assert len(parts) == 3, f"Bad FQN format: {key}"

    def test_lookup_values_are_nodes(self, manifest):
        lookup = build_model_fqn_lookup(manifest)
        for node in list(lookup.values())[:5]:
            assert node["resource_type"] == "model"
            assert "name" in node


# ─── is_measurement_column (real data) ───────────────────────────────────
class TestIsMeasurementColumnIntegration:
    def test_measurement_flag_detection(self, manifest):
        """Check that is_measurement works on real manifest columns."""
        for node in list(manifest["nodes"].values())[:100]:
            if node["resource_type"] != "model":
                continue
            columns = node.get("columns", {})
            if isinstance(columns, dict):
                columns = list(columns.values())
            for col in columns:
                # Just verify it doesn't crash on real data
                result = is_measurement_column(col)
                assert isinstance(result, bool)


# ─── Semantic models ─────────────────────────────────────────────────────
class TestSemanticModels:
    def test_semantic_models_have_measures(self, manifest):
        for sm in manifest["semantic_models"].values():
            assert "measures" in sm
            assert "dimensions" in sm
            assert "entities" in sm

    def test_semantic_model_names(self, manifest):
        for sm in manifest["semantic_models"].values():
            assert sm.get("name"), "Semantic model missing name"

    def test_semantic_models_reference_valid_models(self, manifest):
        """Each semantic model should reference a model that exists in nodes."""
        for sm in manifest["semantic_models"].values():
            model_ref = sm.get("model")
            if model_ref:
                # model_ref is like "ref('fact_agg_sale_by_month')"
                # depends_on.nodes should have the actual node reference
                deps = sm.get("depends_on", {}).get("nodes", [])
                for dep in deps:
                    if dep.startswith("model."):
                        assert dep in manifest["nodes"], (
                            f"Semantic model {sm['name']} references "
                            f"non-existent node {dep}"
                        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
