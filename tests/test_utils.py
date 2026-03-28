"""Tests for utils.py — clean_doi function."""

from paper_pipeline.utils import clean_doi


def test_clean_doi_normal():
    """Normal DOI passes through unchanged."""
    assert clean_doi("10.1021/acs.jcim.3c00160") == "10.1021/acs.jcim.3c00160"


def test_clean_doi_https_prefix():
    """https://doi.org/ prefix is removed."""
    assert clean_doi("https://doi.org/10.1021/acs.jcim.3c00160") == "10.1021/acs.jcim.3c00160"


def test_clean_doi_http_prefix():
    """http://doi.org/ prefix is removed."""
    assert clean_doi("http://doi.org/10.1038/s41586-024-07345-x") == "10.1038/s41586-024-07345-x"


def test_clean_doi_trailing_slash():
    """Trailing slash is removed."""
    assert clean_doi("10.1021/acs.jcim.3c00160/") == "10.1021/acs.jcim.3c00160"


def test_clean_doi_whitespace():
    """Leading/trailing whitespace is stripped."""
    assert clean_doi("  10.1021/acs.jcim.3c00160  ") == "10.1021/acs.jcim.3c00160"


def test_clean_doi_prefix_and_whitespace():
    """Combined prefix + whitespace + trailing slash."""
    assert clean_doi("  https://doi.org/10.1021/xxx/  ") == "10.1021/xxx"


def test_clean_doi_empty():
    """Empty string returns empty."""
    assert clean_doi("") == ""
