"""Tests for PaperExtractor — XML fixtures, regex segmentation, fallback routing."""

from unittest.mock import patch, MagicMock
from paper_pipeline.extractor import PaperExtractor, ExtractionResult


# --- Fixtures ---

JATS_XML = """<?xml version="1.0"?>
<article>
  <front>
    <article-meta>
      <abstract>
        <p>This is the abstract text.</p>
      </abstract>
    </article-meta>
  </front>
  <body>
    <sec sec-type="intro">
      <title>Introduction</title>
      <p>Introduction paragraph one.</p>
      <p>Introduction paragraph two.</p>
    </sec>
    <sec sec-type="methods">
      <title>Methods</title>
      <p>Methods paragraph.</p>
    </sec>
    <sec sec-type="results">
      <title>Results</title>
      <p>Results paragraph.</p>
    </sec>
    <sec sec-type="discussion">
      <title>Discussion</title>
      <p>Discussion paragraph.</p>
    </sec>
  </body>
</article>
"""

TEI_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader>
    <profileDesc>
      <abstract>
        <p>TEI abstract text here.</p>
      </abstract>
    </profileDesc>
  </teiHeader>
  <text>
    <body>
      <div>
        <head>Introduction</head>
        <p>TEI introduction paragraph.</p>
      </div>
      <div>
        <head>Methods</head>
        <p>TEI methods paragraph.</p>
      </div>
    </body>
  </text>
</TEI>
"""


# --- T8: extract_from_europe_pmc_xml ---

def test_extract_from_europe_pmc_xml_sections():
    """JATS XML extraction produces correct sections."""
    extractor = _make_extractor()
    result = extractor.extract_from_europe_pmc_xml(JATS_XML)

    assert result.extraction_method == "europe_pmc_xml"
    assert "Introduction" in result.sections
    assert "Methods" in result.sections
    assert "Results" in result.sections
    assert "Discussion" in result.sections
    assert "Introduction paragraph one." in result.sections["Introduction"]


def test_extract_from_europe_pmc_xml_abstract():
    """JATS XML extraction captures abstract."""
    extractor = _make_extractor()
    result = extractor.extract_from_europe_pmc_xml(JATS_XML)

    assert result.abstract == "This is the abstract text."


def test_extract_from_europe_pmc_xml_invalid():
    """Invalid XML returns empty result."""
    extractor = _make_extractor()
    result = extractor.extract_from_europe_pmc_xml("not valid xml <<<<")

    assert result.extraction_method == "europe_pmc_xml"
    assert result.sections == {}


# --- T8: _parse_tei_xml_string ---

def test_parse_tei_xml_string_sections():
    """TEI XML parsing extracts sections correctly."""
    extractor = _make_extractor()
    sections = extractor._parse_tei_xml_string(TEI_XML)

    assert "Introduction" in sections
    assert "Methods" in sections
    assert "TEI introduction paragraph." in sections["Introduction"]


def test_parse_tei_xml_string_invalid():
    """Invalid TEI XML returns empty dict."""
    extractor = _make_extractor()
    sections = extractor._parse_tei_xml_string("<<<invalid")

    assert sections == {}


# --- T8: _regex_segment ---

def test_regex_segment_with_headers():
    """Markdown with headers is split into sections."""
    extractor = _make_extractor()
    md = "# Introduction\nSome intro text.\n\n## Methods\nSome methods.\n\n## Results\nFindings."
    sections = extractor._regex_segment(md)

    assert "Introduction" in sections
    assert "Methods" in sections
    assert "Results" in sections


def test_regex_segment_no_headers():
    """Markdown without headers goes into Preamble."""
    extractor = _make_extractor()
    md = "Just some plain text without any markdown headers."
    sections = extractor._regex_segment(md)

    assert "Preamble" in sections
    assert "plain text" in sections["Preamble"]


def test_regex_segment_mixed():
    """Markdown with preamble then headers."""
    extractor = _make_extractor()
    md = "Preamble text here.\n\n# Section One\nContent one.\n\n## Section Two\nContent two."
    sections = extractor._regex_segment(md)

    assert "Preamble" in sections
    assert "Section One" in sections
    assert "Section Two" in sections


# --- T8: extract() fallback routing ---

def test_extract_pmc_xml_route():
    """extract() routes to PMC XML when content_type is pmc_xml."""
    extractor = _make_extractor()
    content = MagicMock()
    content.content_type = "pmc_xml"
    content.data = JATS_XML

    result = extractor.extract(content)
    assert result.extraction_method == "europe_pmc_xml"


def test_extract_pdf_grobid_fallback():
    """extract() falls back from GROBID to pymupdf when GROBID returns empty."""
    extractor = _make_extractor()
    # Set instance dict directly to preserve class-level cached_property descriptor
    extractor.__dict__["grobid_available"] = True

    content = MagicMock()
    content.content_type = "pdf"
    content.data = None
    content.pdf_path = "/fake/path.pdf"

    with patch.object(extractor, "extract_from_pdf_grobid", return_value=ExtractionResult(extraction_method="grobid")):
        with patch.object(extractor, "extract_from_pdf_pymupdf", return_value=ExtractionResult(
            sections={"Intro": "text", "Methods": "text"},
            full_text="some text",
            extraction_method="pymupdf4llm",
        )):
            result = extractor.extract(content, pdf_path="/fake/path.pdf")
            assert result.extraction_method == "pymupdf4llm"


def test_extract_no_content():
    """extract() returns 'none' when no content available."""
    extractor = _make_extractor()
    content = MagicMock()
    content.content_type = "metadata_only"
    content.data = None
    content.pdf_path = None

    result = extractor.extract(content)
    assert result.extraction_method == "none"


# --- T2: GROBID cached_property ---

def test_grobid_check_not_called_on_init():
    """PaperExtractor.__init__ does not call GROBID (lazy check)."""
    with patch("paper_pipeline.extractor.requests.get") as mock_get:
        extractor = PaperExtractor()
        mock_get.assert_not_called()


def test_grobid_available_cached():
    """grobid_available is a cached_property — value is computed once then cached in __dict__."""
    import functools

    # Verify it's a cached_property descriptor
    assert isinstance(
        PaperExtractor.__dict__["grobid_available"],
        functools.cached_property,
    )

    # Verify caching: once set in __dict__, descriptor is bypassed
    extractor = _make_extractor()
    # After _make_extractor, grobid_available is not yet in __dict__
    # (it was not accessed during __init__)
    assert "grobid_available" not in extractor.__dict__

    # First access populates __dict__
    _ = extractor.grobid_available
    assert "grobid_available" in extractor.__dict__

    # Second access returns same value from __dict__ (cached)
    val1 = extractor.grobid_available
    val2 = extractor.grobid_available
    assert val1 is val2


# --- Helper ---

def _make_extractor():
    """Create extractor without GROBID check."""
    with patch("paper_pipeline.extractor.requests.get", side_effect=Exception("no grobid")):
        extractor = PaperExtractor()
    return extractor
