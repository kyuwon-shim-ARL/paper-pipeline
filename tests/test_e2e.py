"""E2E tests for the reference management pipeline."""

import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from paper_pipeline.store import PaperStore
from paper_pipeline.pool import create_manifest, save_manifest, load_manifest, merge_manifests, validate_manifest
from paper_pipeline.bibtex import export_bib


@pytest.fixture
def store(tmp_path):
    return PaperStore(str(tmp_path / "papers"))


def _save_paper(store, doi, title="Test", year=2025, extra=None):
    """Helper to save a paper with standard OpenAlex-like metadata."""
    data = {
        "doi": doi,
        "title": title,
        "publication_year": year,
        "authorships": [{"author": {"display_name": "John Smith"}}],
        "primary_location": {"source": {"display_name": "Journal X", "type": "journal"}},
        "biblio": {"volume": "1", "issue": "1", "first_page": "1", "last_page": "10"},
    }
    if extra:
        data.update(extra)
    provenance_entry = {
        "session_id": f"test-session",
        "timestamp": "2026-03-25T12:00:00+00:00",
        "source": "sweep",
        "search_params": {"queries": ["test"]},
        "cluster_id": None,
        "seed_source": None,
    }
    store.save_layer(doi, "L0", data, provenance_entry=provenance_entry)
    return data


class TestE2E1SingleSessionPipeline:
    """E2E-1: sweep → manifest → export-bib single session."""

    @patch("paper_pipeline.bibtex._call_doi2bib", return_value=None)
    def test_single_session_flow(self, mock_doi2bib, store, tmp_path):
        # 1. Simulate sweep: save papers to store
        papers = []
        for i in range(5):
            doi = f"10.1234/e2e1_{i:03d}"
            p = _save_paper(store, doi, title=f"Paper {i}")
            papers.append(p)

        # 2. Create manifest
        manifest = create_manifest("e2e-session-1", papers, store, goal="E2E test")
        manifest_path = tmp_path / "pool.json"
        save_manifest(manifest, manifest_path)

        assert manifest["total_papers"] == 5
        assert all(p["in_store"] for p in manifest["papers"])

        # 3. Export BibTeX
        bib_path = tmp_path / "refs.bib"
        stats = export_bib(manifest, store, bib_path)

        assert stats["success"] == 5
        assert stats["fallback"] == 5  # all via OpenAlex fallback since doi2bib mocked
        content = bib_path.read_text()
        assert content.count("@article{") == 5


class TestE2E2MultiSessionProvenanceConcat:
    """E2E-2: Two sessions → each manifest → merge → provenance concat check."""

    def test_multi_session_provenance(self, store, tmp_path):
        doi_shared = "10.1234/shared"

        # Session 1: save shared paper
        prov1 = {
            "session_id": "session-1",
            "timestamp": "2026-03-25T10:00:00+00:00",
            "source": "sweep",
            "search_params": {"queries": ["alpha"]},
            "cluster_id": None,
            "seed_source": None,
        }
        store.save_layer(doi_shared, "L0", {
            "doi": doi_shared, "title": "Shared Paper", "publication_year": 2025,
            "authorships": [{"author": {"display_name": "A B"}}],
            "primary_location": {"source": {"display_name": "J", "type": "journal"}},
            "biblio": {},
        }, provenance_entry=prov1)

        # Session 2: save same paper again with different provenance
        prov2 = {
            "session_id": "session-2",
            "timestamp": "2026-03-25T14:00:00+00:00",
            "source": "sweep",
            "search_params": {"queries": ["beta"]},
            "cluster_id": None,
            "seed_source": None,
        }
        store.save_layer(doi_shared, "L0", {
            "doi": doi_shared, "title": "Shared Paper", "publication_year": 2025,
            "authorships": [{"author": {"display_name": "A B"}}],
            "primary_location": {"source": {"display_name": "J", "type": "journal"}},
            "biblio": {},
        }, provenance_entry=prov2)

        # Verify provenance concatenated
        l0 = store.load_layer(doi_shared, "L0")
        assert len(l0["provenance"]) == 2
        assert l0["provenance"][0]["session_id"] == "session-1"
        assert l0["provenance"][1]["session_id"] == "session-2"

        # Create manifests and merge
        m1 = create_manifest("session-1", [{"doi": doi_shared, "title": "Shared"}], store)
        m2 = create_manifest("session-2", [{"doi": doi_shared, "title": "Shared"}], store)

        merged = merge_manifests([m1, m2], store=store)
        assert merged["total_papers"] == 1  # dedup by DOI


class TestE2E3FallbackMock:
    """E2E-3: doi2bib failure → OpenAlex fallback → BibTeX generated."""

    @patch("paper_pipeline.bibtex._call_doi2bib", return_value=None)
    def test_fallback_produces_valid_bibtex(self, mock_doi2bib, store, tmp_path):
        doi = "10.1234/fallback"
        _save_paper(store, doi, title="Fallback Test Paper")

        manifest = create_manifest("fb-session", [{"doi": doi, "title": "Fallback Test"}], store)
        bib_path = tmp_path / "fallback.bib"
        stats = export_bib(manifest, store, bib_path)

        assert stats["success"] == 1
        assert stats["fallback"] == 1
        content = bib_path.read_text()
        assert "@article{" in content
        assert "Fallback Test Paper" in content


class TestE2E4OrphanStrict:
    """E2E-4: Orphan DOI manifest → merge --strict → error."""

    def test_orphan_strict_error(self, store, tmp_path):
        # Create manifest with an orphan DOI (not in store)
        manifest = {
            "schema_version": 1,
            "session_id": "orphan-session",
            "papers": [{"doi": "10.9999/not_in_store", "title": "Ghost Paper"}],
            "total_papers": 1,
        }

        with pytest.raises(ValueError, match="orphan"):
            merge_manifests([manifest], store=store, strict=True)


class TestE2E5TimeoutPartialFlush:
    """E2E-5: export-bib --timeout with many DOIs → partial flush."""

    @patch("paper_pipeline.bibtex._call_doi2bib", return_value=None)
    def test_timeout_preserves_successes(self, mock_doi2bib, store, tmp_path):
        # Save many papers
        dois = []
        for i in range(10):
            doi = f"10.1234/timeout_{i:03d}"
            _save_paper(store, doi, title=f"Timeout Paper {i}")
            dois.append(doi)

        manifest = create_manifest("timeout-session", [{"doi": d} for d in dois], store)
        bib_path = tmp_path / "timeout.bib"

        # Run with very short timeout (0 seconds)
        stats = export_bib(manifest, store, bib_path, timeout=0, max_concurrent=2)

        # First batch should complete, rest timeout
        assert stats["success"] + stats["failed"] == 10

        # If any succeeded, the .bib file should exist with content
        if stats["success"] > 0:
            content = bib_path.read_text()
            assert "@article{" in content

        # Failed DOIs should be recorded
        if stats["failed"] > 0:
            failed_path = tmp_path / "failed_dois.txt"
            assert failed_path.exists()
            failed_content = failed_path.read_text()
            assert "timeout_global" in failed_content
