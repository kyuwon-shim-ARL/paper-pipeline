"""Tests for PaperFetcher — mock-based tests for fetch chain, PDF download, Europe PMC."""

import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock

from paper_pipeline.fetcher import PaperFetcher, ContentResult


def _make_fetcher(pdf_dir=None):
    """Create fetcher with mocked session."""
    if pdf_dir is None:
        pdf_dir = tempfile.mkdtemp()
    return PaperFetcher(email="test@example.com", pdf_dir=pdf_dir)


# --- fetch_content fallback chain ---

def test_fetch_content_europe_pmc_success():
    """fetch_content returns PMC XML when Europe PMC succeeds."""
    fetcher = _make_fetcher()
    with patch.object(fetcher, "fetch_europe_pmc_fulltext", return_value="<article>xml</article>"):
        result = fetcher.fetch_content("https://doi.org/10.1038/test")

    assert result.source == "europe_pmc"
    assert result.content_type == "pmc_xml"
    assert result.data == "<article>xml</article>"


def test_fetch_content_openalex_oa_fallback():
    """fetch_content falls back to OpenAlex OA URL when PMC fails."""
    fetcher = _make_fetcher()
    with patch.object(fetcher, "fetch_europe_pmc_fulltext", return_value=None):
        with patch.object(fetcher, "_download_pdf_to", return_value="/fake/path.pdf"):
            result = fetcher.fetch_content(
                "10.1038/test",
                work_data={"pdf_url": "https://example.com/paper.pdf"},
            )

    assert result.source == "openalex_oa"
    assert result.content_type == "pdf"


def test_fetch_content_unpaywall_fallback():
    """fetch_content falls back to Unpaywall when PMC and OA fail."""
    fetcher = _make_fetcher()
    with patch.object(fetcher, "fetch_europe_pmc_fulltext", return_value=None):
        with patch.object(fetcher, "fetch_unpaywall_url", return_value="https://unpaywall.com/paper.pdf"):
            with patch.object(fetcher, "_download_pdf_to", return_value="/fake/unpaywall.pdf"):
                result = fetcher.fetch_content("10.1038/test")

    assert result.source == "unpaywall"
    assert result.content_type == "pdf"


def test_fetch_content_metadata_only():
    """fetch_content returns metadata_only when all sources fail."""
    fetcher = _make_fetcher()
    with patch.object(fetcher, "fetch_europe_pmc_fulltext", return_value=None):
        with patch.object(fetcher, "fetch_unpaywall_url", return_value=None):
            with patch.object(fetcher, "_get_biorxiv_pdf_url", return_value=None):
                with patch.object(fetcher, "_get_crossref_pdf_url", return_value=None):
                    result = fetcher.fetch_content("10.9999/nonexistent")

    assert result.content_type == "metadata_only"
    assert result.source == "none"


# --- download_pdf ---

def test_download_pdf_success():
    """download_pdf writes PDF with atomic rename."""
    fetcher = _make_fetcher()
    with tempfile.TemporaryDirectory() as tmpdir:
        save_path = os.path.join(tmpdir, "test.pdf")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/pdf"}
        mock_resp.iter_content = MagicMock(return_value=[b"%PDF-1.4 fake pdf content"])

        with patch.object(fetcher.session, "get", return_value=mock_resp):
            with patch.object(fetcher, "_rate_limit"):
                result = fetcher.download_pdf("https://example.com/paper.pdf", save_path)

        assert result is True
        assert os.path.exists(save_path)
        with open(save_path, "rb") as f:
            assert f.read().startswith(b"%PDF-")


def test_download_pdf_html_rejection():
    """download_pdf rejects HTML content type."""
    fetcher = _make_fetcher()
    with tempfile.TemporaryDirectory() as tmpdir:
        save_path = os.path.join(tmpdir, "test.pdf")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "text/html"}

        with patch.object(fetcher.session, "get", return_value=mock_resp):
            with patch.object(fetcher, "_rate_limit"):
                result = fetcher.download_pdf("https://example.com/paper.pdf", save_path)

        assert result is False


def test_download_pdf_non_pdf_header():
    """download_pdf rejects files that don't start with %PDF-."""
    fetcher = _make_fetcher()
    with tempfile.TemporaryDirectory() as tmpdir:
        save_path = os.path.join(tmpdir, "test.pdf")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/pdf"}
        mock_resp.iter_content = MagicMock(return_value=[b"<html>not a pdf</html>"])

        with patch.object(fetcher.session, "get", return_value=mock_resp):
            with patch.object(fetcher, "_rate_limit"):
                result = fetcher.download_pdf("https://example.com/paper.pdf", save_path)

        assert result is False
        assert not os.path.exists(save_path)


# --- fetch_europe_pmc_fulltext ---

def test_fetch_europe_pmc_success():
    """Europe PMC fetch succeeds with PMCID lookup + XML fetch."""
    fetcher = _make_fetcher()

    search_resp = MagicMock()
    search_resp.status_code = 200
    search_resp.json.return_value = {
        "resultList": {"result": [{"pmcid": "PMC123456"}]}
    }

    xml_resp = MagicMock()
    xml_resp.status_code = 200
    xml_resp.text = "<?xml version='1.0'?><article>content</article>"

    with patch.object(fetcher.session, "get", side_effect=[search_resp, xml_resp]):
        with patch.object(fetcher, "_rate_limit"):
            result = fetcher.fetch_europe_pmc_fulltext("10.1038/test")

    assert result is not None
    assert "<article>" in result


def test_fetch_europe_pmc_no_pmcid():
    """Europe PMC returns None when no PMCID found."""
    fetcher = _make_fetcher()

    search_resp = MagicMock()
    search_resp.status_code = 200
    search_resp.json.return_value = {
        "resultList": {"result": [{"doi": "10.1038/test"}]}  # no pmcid
    }

    with patch.object(fetcher.session, "get", return_value=search_resp):
        with patch.object(fetcher, "_rate_limit"):
            result = fetcher.fetch_europe_pmc_fulltext("10.1038/test")

    assert result is None


def test_fetch_europe_pmc_http_error():
    """Europe PMC returns None on HTTP error."""
    fetcher = _make_fetcher()

    error_resp = MagicMock()
    error_resp.status_code = 500

    with patch.object(fetcher.session, "get", return_value=error_resp):
        with patch.object(fetcher, "_rate_limit"):
            result = fetcher.fetch_europe_pmc_fulltext("10.1038/test")

    assert result is None


# --- fetch_unpaywall_url ---

def test_fetch_unpaywall_success():
    """Unpaywall returns PDF URL on success."""
    fetcher = _make_fetcher()

    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "best_oa_location": {"url_for_pdf": "https://example.com/paper.pdf"}
    }

    with patch.object(fetcher.session, "get", return_value=resp):
        with patch.object(fetcher, "_rate_limit"):
            result = fetcher.fetch_unpaywall_url("10.1038/test")

    assert result == "https://example.com/paper.pdf"


def test_fetch_unpaywall_no_email():
    """Unpaywall returns None when email is empty."""
    fetcher = _make_fetcher()
    fetcher.email = ""
    result = fetcher.fetch_unpaywall_url("10.1038/test")
    assert result is None


# --- _rate_limit ---

def test_rate_limit_basic():
    """rate_limit doesn't crash and records timestamps."""
    fetcher = _make_fetcher()
    fetcher._rate_limit("europe_pmc")
    assert "europe_pmc" in fetcher._last_request


# --- _download_pdf_to (decoupled from store) ---

def test_download_pdf_to_with_save_dir():
    """_download_pdf_to uses save_dir when provided."""
    fetcher = _make_fetcher()
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch.object(fetcher, "download_pdf", return_value=True):
            result = fetcher._download_pdf_to("https://example.com/paper.pdf", "10.1038/test", save_dir=tmpdir)

    assert result is not None
    assert "content/source.pdf" in result


def test_download_pdf_to_default_resolver():
    """_download_pdf_to uses inline doi_to_dirname when no path_resolver."""
    fetcher = _make_fetcher()
    with patch.object(fetcher, "download_pdf", return_value=True):
        result = fetcher._download_pdf_to("https://example.com/paper.pdf", "10.1038/test")

    assert result is not None
    assert "by-doi" in result
    assert "10.1038__test" in result
