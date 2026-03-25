"""Tests for T1: provenance field on L0.json."""

import json
import tempfile
from pathlib import Path

import pytest

from paper_pipeline.store import PaperStore


@pytest.fixture
def tmp_store(tmp_path):
    """Create a temporary PaperStore."""
    return PaperStore(str(tmp_path / "papers"))


def _make_paper(doi="10.1234/test001", title="Test Paper"):
    return {"doi": doi, "title": title, "publication_year": 2025}


def _make_provenance(session_id="lit-20260325-120000", source="sweep"):
    return {
        "session_id": session_id,
        "timestamp": "2026-03-25T12:00:00+00:00",
        "source": source,
        "search_params": {"queries": ["test"], "filters": {}},
        "cluster_id": None,
        "seed_source": None,
    }


class TestProvenanceAppendOnly:
    """Provenance array should be append-only across saves."""

    def test_first_save_creates_provenance(self, tmp_store):
        paper = _make_paper()
        prov = _make_provenance()
        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov)

        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert "provenance" in loaded
        assert len(loaded["provenance"]) == 1
        assert loaded["provenance"][0]["session_id"] == "lit-20260325-120000"

    def test_second_save_appends(self, tmp_store):
        paper = _make_paper()
        prov1 = _make_provenance(session_id="session-1")
        prov2 = _make_provenance(session_id="session-2")

        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov1)
        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov2)

        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert len(loaded["provenance"]) == 2
        assert loaded["provenance"][0]["session_id"] == "session-1"
        assert loaded["provenance"][1]["session_id"] == "session-2"

    def test_save_without_provenance_preserves_existing(self, tmp_store):
        paper = _make_paper()
        prov = _make_provenance()

        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov)
        # Save again without provenance_entry
        tmp_store.save_layer(paper["doi"], "L0", paper)

        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert len(loaded["provenance"]) == 1
        assert loaded["provenance"][0]["session_id"] == "lit-20260325-120000"


class TestProvenanceAutoPatch:
    """Loading L0 without provenance should auto-patch with empty array."""

    def test_legacy_l0_gets_empty_provenance(self, tmp_store):
        """Simulate a legacy L0 file without provenance field."""
        paper = _make_paper()
        paper_dir = tmp_store.get_paper_dir(paper["doi"])
        paper_dir.mkdir(parents=True, exist_ok=True)

        # Write L0 directly without provenance
        l0_path = paper_dir / "metadata.json"
        with open(l0_path, "w") as f:
            json.dump({"doi": paper["doi"], "title": "Legacy"}, f)

        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert "provenance" in loaded
        assert loaded["provenance"] == []

    def test_new_l0_without_provenance_entry_gets_empty(self, tmp_store):
        paper = _make_paper()
        tmp_store.save_layer(paper["doi"], "L0", paper)

        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert loaded["provenance"] == []


class TestProvenanceSeedSource:
    """seed_source field should propagate correctly."""

    def test_seed_source_stored(self, tmp_store):
        paper = _make_paper()
        prov = _make_provenance()
        prov["seed_source"] = "handoff"

        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov)
        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert loaded["provenance"][0]["seed_source"] == "handoff"

    def test_seed_source_null_default(self, tmp_store):
        paper = _make_paper()
        prov = _make_provenance()
        # seed_source is None by default in _make_provenance

        tmp_store.save_layer(paper["doi"], "L0", paper, provenance_entry=prov)
        loaded = tmp_store.load_layer(paper["doi"], "L0")
        assert loaded["provenance"][0]["seed_source"] is None
