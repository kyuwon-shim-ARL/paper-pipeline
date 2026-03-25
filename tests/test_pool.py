"""Tests for T2/T3: pool manifest and merge-pool."""

import json
import tempfile
from pathlib import Path

import pytest

from paper_pipeline.pool import (
    create_manifest,
    load_manifest,
    merge_manifests,
    save_manifest,
    validate_manifest,
)
from paper_pipeline.store import PaperStore


@pytest.fixture
def tmp_store(tmp_path):
    store = PaperStore(str(tmp_path / "papers"))
    # Pre-populate with some papers
    for i in range(3):
        doi = f"10.1234/test{i:03d}"
        store.save_layer(doi, "L0", {"doi": doi, "title": f"Paper {i}", "publication_year": 2025})
    return store


@pytest.fixture
def sample_papers():
    return [
        {"doi": "10.1234/test000", "title": "Paper 0"},
        {"doi": "10.1234/test001", "title": "Paper 1"},
        {"doi": "10.1234/test002", "title": "Paper 2"},
    ]


class TestCreateManifest:
    def test_basic_creation(self, tmp_store, sample_papers):
        manifest = create_manifest("session-1", sample_papers, tmp_store, goal="Test goal")
        assert manifest["schema_version"] == 1
        assert manifest["session_id"] == "session-1"
        assert manifest["total_papers"] == 3
        assert manifest["validated_goal"] == "Test goal"
        assert all(p["in_store"] for p in manifest["papers"])

    def test_orphan_detection(self, tmp_store):
        papers = [{"doi": "10.9999/orphan", "title": "Orphan Paper"}]
        manifest = create_manifest("session-1", papers, tmp_store)
        assert manifest["papers"][0]["in_store"] is False

    def test_papers_without_doi_skipped(self, tmp_store):
        papers = [{"title": "No DOI"}, {"doi": "10.1234/test000", "title": "Has DOI"}]
        manifest = create_manifest("session-1", papers, tmp_store)
        assert manifest["total_papers"] == 1


class TestLoadManifest:
    def test_load_v1(self, tmp_path):
        v1 = {
            "schema_version": 1,
            "session_id": "s1",
            "created_at": "2026-01-01T00:00:00Z",
            "validated_goal": "",
            "search_params_summary": {},
            "papers": [{"doi": "10.1234/a", "title": "A", "added_at": "2026-01-01T00:00:00Z", "in_store": True}],
            "total_papers": 1,
            "store_path": "data/papers",
        }
        path = tmp_path / "v1.json"
        path.write_text(json.dumps(v1))
        loaded = load_manifest(path)
        assert loaded["schema_version"] == 1
        assert loaded["papers"][0]["doi"] == "10.1234/a"

    def test_load_v0_auto_migration(self, tmp_path):
        """v0 pool files (no schema_version) should be auto-migrated to v1."""
        v0 = {
            "papers": [
                {"doi": "10.1234/a", "title": "A", "openalex_id": "W123", "cited_by_count": 50},
                {"doi": "10.1234/b", "title": "B", "openalex_id": "W456"},
            ],
            "total_papers": 2,
        }
        path = tmp_path / "v0.json"
        path.write_text(json.dumps(v0))

        loaded = load_manifest(path)
        assert loaded["schema_version"] == 1
        assert loaded["total_papers"] == 2
        # v0 migration strips full metadata, keeps doi+title
        assert loaded["papers"][0]["doi"] == "10.1234/a"
        assert "openalex_id" not in loaded["papers"][0]
        assert "in_store" in loaded["papers"][0]

    def test_v0_original_file_unchanged(self, tmp_path):
        """v0 migration is read-only — original file should not be modified."""
        v0 = {"papers": [{"doi": "10.1234/a", "title": "A"}], "total_papers": 1}
        path = tmp_path / "v0.json"
        path.write_text(json.dumps(v0))
        original_content = path.read_text()

        load_manifest(path)
        assert path.read_text() == original_content


class TestValidateManifest:
    def test_all_valid(self, tmp_store, sample_papers):
        manifest = create_manifest("s1", sample_papers, tmp_store)
        result = validate_manifest(manifest, tmp_store)
        assert result["orphans"] == []
        assert result["valid"] == 3

    def test_orphan_detected(self, tmp_store):
        manifest = {
            "schema_version": 1,
            "papers": [
                {"doi": "10.1234/test000", "in_store": True},
                {"doi": "10.9999/missing", "in_store": False},
            ],
        }
        result = validate_manifest(manifest, tmp_store)
        assert "10.9999/missing" in result["orphans"]
        assert result["valid"] == 1


class TestMergeManifests:
    def test_dedup_by_doi(self, tmp_store):
        m1 = create_manifest("s1", [{"doi": "10.1234/test000", "title": "P0"}], tmp_store)
        m2 = create_manifest("s2", [{"doi": "10.1234/test000", "title": "P0"}, {"doi": "10.1234/test001", "title": "P1"}], tmp_store)

        merged = merge_manifests([m1, m2], store=tmp_store)
        assert merged["total_papers"] == 2  # dedup

    def test_session_tracking(self, tmp_store):
        m1 = create_manifest("s1", [{"doi": "10.1234/test000", "title": "P0"}], tmp_store)
        m2 = create_manifest("s2", [{"doi": "10.1234/test001", "title": "P1"}], tmp_store)

        merged = merge_manifests([m1, m2])
        assert "s1" in merged["search_params_summary"]["merged_sessions"]
        assert "s2" in merged["search_params_summary"]["merged_sessions"]

    def test_strict_mode_raises_on_orphan(self, tmp_store):
        manifest = {
            "schema_version": 1,
            "session_id": "s1",
            "papers": [{"doi": "10.9999/orphan", "title": "Orphan"}],
        }
        with pytest.raises(ValueError, match="orphan"):
            merge_manifests([manifest], store=tmp_store, strict=True)

    def test_all_flag_glob(self, tmp_path, tmp_store):
        """Test --all discovery pattern by creating files and loading them."""
        pool_dir = tmp_path / "outputs"
        pool_dir.mkdir()

        for i in range(3):
            m = create_manifest(f"s{i}", [{"doi": f"10.1234/test{i:03d}", "title": f"P{i}"}], tmp_store)
            save_manifest(m, pool_dir / f"lit_pool_s{i}.json")

        # Simulate --all: glob for lit_pool_*.json
        from glob import glob
        files = sorted(glob(str(pool_dir / "lit_pool_*.json")))
        assert len(files) == 3

        manifests = [load_manifest(f) for f in files]
        merged = merge_manifests(manifests, store=tmp_store)
        assert merged["total_papers"] == 3
