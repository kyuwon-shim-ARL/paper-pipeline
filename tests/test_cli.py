"""Tests for CLI commands — provenance and search-local (T9, T10)."""

import json
import tempfile
from unittest.mock import patch, MagicMock
from argparse import Namespace

from paper_pipeline.cli import cmd_provenance, cmd_search_local


def _make_store_with_paper(tmpdir, doi="10.1038/test", title="Test Paper",
                           provenance=None, fulltext=None):
    """Create a PaperStore with one paper for testing."""
    from paper_pipeline.store import PaperStore

    store = PaperStore(base_dir=tmpdir)
    l0_data = {
        "doi": doi,
        "title": title,
        "publication_year": 2024,
        "provenance": provenance or [],
    }
    store.save_layer(doi, "L0", l0_data)

    if fulltext:
        store.save_content(doi, "fulltext", fulltext)

    return store


# --- T9: cmd_provenance ---

def test_provenance_shows_entries(capsys):
    """provenance command prints provenance history."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = _make_store_with_paper(
            tmpdir,
            provenance=[
                {"session_id": "s1", "timestamp": "2024-01-01", "source": "search"},
                {"session_id": "s2", "timestamp": "2024-01-02", "source": "sweep"},
            ],
        )
        args = Namespace(doi="10.1038/test", data_dir=tmpdir)
        cmd_provenance(args)

        output = capsys.readouterr().out
        assert "Test Paper" in output
        assert "s1" in output
        assert "s2" in output
        assert "Total entries: 2" in output


def test_provenance_no_paper(capsys):
    """provenance command exits with error when paper not found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        from paper_pipeline.store import PaperStore
        PaperStore(base_dir=tmpdir)  # empty store

        args = Namespace(doi="10.9999/nonexistent", data_dir=tmpdir)
        import pytest
        with pytest.raises(SystemExit):
            cmd_provenance(args)


# --- T10: cmd_search_local ---

def test_search_local_title_match(capsys):
    """search-local finds papers by title match."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = _make_store_with_paper(tmpdir, title="Urban Microbiome Analysis")
        args = Namespace(keyword="microbiome", data_dir=tmpdir)
        cmd_search_local(args)

        output = capsys.readouterr().out
        assert "1 matches" in output
        assert "Urban Microbiome" in output


def test_search_local_fulltext_match(capsys):
    """search-local finds papers by fulltext content."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = _make_store_with_paper(
            tmpdir,
            title="Paper About Something",
            fulltext="This paper discusses antimicrobial resistance patterns in urban environments.",
        )
        args = Namespace(keyword="antimicrobial", data_dir=tmpdir)
        cmd_search_local(args)

        output = capsys.readouterr().out
        assert "1 matches" in output
        assert "fulltext" in output


def test_search_local_no_match(capsys):
    """search-local returns 0 matches when keyword not found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = _make_store_with_paper(tmpdir, title="Something Else")
        args = Namespace(keyword="nonexistent_keyword", data_dir=tmpdir)
        cmd_search_local(args)

        output = capsys.readouterr().out
        assert "0 matches" in output
