"""
Paper Extractor - Structured text extraction from PDF/XML.

Extraction priority:
    1. Europe PMC JATS XML (already structured, best)
    2. GROBID TEI XML (ML-based, F1=0.87-0.95)
    3. pymupdf4llm (fast but lower structure accuracy, fallback)

GROBID TEI parsing has 2-tier fallback:
    - grobidmonkey (if installed)
    - xml.etree.ElementTree direct parsing (always available)
"""

import functools
import re
import xml.etree.ElementTree as ET

import defusedxml.ElementTree as SafeET
import requests
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class ExtractionResult:
    """Result of text extraction from a paper."""
    sections: dict[str, str] = field(default_factory=dict)
    abstract: Optional[str] = None
    full_text: str = ""
    tables: list[str] = field(default_factory=list)
    figure_captions: list[str] = field(default_factory=list)
    extraction_method: str = ""  # "europe_pmc_xml", "grobid", "pymupdf4llm"


class PaperExtractor:
    """Extract structured text from papers using multiple backends.

    Usage:
        extractor = PaperExtractor()
        result = extractor.extract(content_result, pdf_path="/path/to/paper.pdf")
    """

    def __init__(self, grobid_url: str = "http://localhost:8070"):
        """Initialize extractor.

        Args:
            grobid_url: GROBID service URL
        """
        self.grobid_url = grobid_url
        self.docling_available = self._check_docling()

    @functools.cached_property
    def grobid_available(self) -> bool:
        """Check if GROBID service is running (lazy, cached).

        Returns:
            True if GROBID is alive
        """
        try:
            resp = requests.get(f"{self.grobid_url}/api/isalive", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False

    def _check_docling(self) -> bool:
        """Check if docling is installed."""
        try:
            from docling.document_converter import DocumentConverter  # noqa: F401
            return True
        except ImportError:
            return False

    def extract(self, content_result, pdf_path: Optional[str] = None) -> ExtractionResult:
        """Extract text based on ContentResult type.

        Args:
            content_result: ContentResult from PaperFetcher
            pdf_path: Path to PDF file (for GROBID/pymupdf4llm)

        Returns:
            ExtractionResult with sections and full text
        """
        if content_result.content_type == "pmc_xml" and content_result.data:
            return self.extract_from_europe_pmc_xml(content_result.data)

        effective_pdf = pdf_path or content_result.pdf_path
        if content_result.content_type == "pdf" and effective_pdf:
            if self.grobid_available:
                result = self.extract_from_pdf_grobid(effective_pdf)
                if result and result.sections:
                    return result
            pymupdf_result = self.extract_from_pdf_pymupdf(effective_pdf)
            if pymupdf_result and pymupdf_result.full_text and len(pymupdf_result.sections) >= 2:
                return pymupdf_result
            if self.docling_available:
                docling_result = self.extract_from_pdf_docling(effective_pdf)
                if docling_result and docling_result.full_text and len(docling_result.sections) >= 2:
                    return docling_result
            return pymupdf_result

        # No content to extract
        return ExtractionResult(extraction_method="none")

    def extract_from_europe_pmc_xml(self, xml_content: str) -> ExtractionResult:
        """Extract sections from Europe PMC JATS XML.

        Parses <sec sec-type="..."> elements:
            intro → Introduction
            methods → Methods
            results → Results
            discussion → Discussion

        Args:
            xml_content: JATS XML string

        Returns:
            ExtractionResult with structured sections
        """
        try:
            root = SafeET.fromstring(xml_content)
        except ET.ParseError:
            return ExtractionResult(extraction_method="europe_pmc_xml")

        sections = {}
        abstract_text = None
        tables = []
        figure_captions = []

        # Section type mapping
        sec_type_map = {
            "intro": "Introduction",
            "introduction": "Introduction",
            "methods": "Methods",
            "materials": "Methods",
            "materials|methods": "Methods",
            "results": "Results",
            "discussion": "Discussion",
            "conclusions": "Conclusions",
            "conclusion": "Conclusions",
        }

        # Extract abstract
        for abstract_el in root.iter("abstract"):
            parts = []
            for p in abstract_el.iter("p"):
                text = "".join(p.itertext()).strip()
                if text:
                    parts.append(text)
            if parts:
                abstract_text = " ".join(parts)

        # Extract sections
        for body in root.iter("body"):
            for sec in body.findall(".//sec"):
                sec_type = sec.get("sec-type", "").lower()
                # Also try title text
                title_el = sec.find("title")
                title_text = "".join(title_el.itertext()).strip() if title_el is not None else ""

                # Determine section name
                section_name = sec_type_map.get(sec_type)
                if not section_name and title_text:
                    # Try matching title text
                    title_lower = title_text.lower()
                    for key, name in sec_type_map.items():
                        if key in title_lower:
                            section_name = name
                            break
                    if not section_name:
                        section_name = title_text

                if not section_name:
                    section_name = "Other"

                # Extract paragraphs
                paragraphs = []
                for p in sec.findall("p"):
                    text = "".join(p.itertext()).strip()
                    if text:
                        paragraphs.append(text)

                if paragraphs:
                    if section_name in sections:
                        sections[section_name] += "\n\n" + "\n\n".join(paragraphs)
                    else:
                        sections[section_name] = "\n\n".join(paragraphs)

        # Extract tables
        for table_wrap in root.iter("table-wrap"):
            caption = table_wrap.find(".//caption")
            if caption is not None:
                cap_text = "".join(caption.itertext()).strip()
                if cap_text:
                    tables.append(cap_text)

        # Extract figure captions
        for fig in root.iter("fig"):
            caption = fig.find(".//caption")
            if caption is not None:
                cap_text = "".join(caption.itertext()).strip()
                if cap_text:
                    figure_captions.append(cap_text)

        # Build full text
        full_text_parts = []
        if abstract_text:
            full_text_parts.append(f"## Abstract\n{abstract_text}")
        for name, content in sections.items():
            full_text_parts.append(f"## {name}\n{content}")

        return ExtractionResult(
            sections=sections,
            abstract=abstract_text,
            full_text="\n\n".join(full_text_parts),
            tables=tables,
            figure_captions=figure_captions,
            extraction_method="europe_pmc_xml",
        )

    def extract_from_pdf_grobid(self, pdf_path: str) -> ExtractionResult:
        """Extract sections from PDF via GROBID.

        Steps:
            1. Send PDF to GROBID processFulltextDocument
            2. Parse returned TEI XML via _parse_tei_xml (2-tier fallback)

        Args:
            pdf_path: Path to PDF file

        Returns:
            ExtractionResult with sections
        """
        # Send to GROBID
        tei_xml = self._grobid_process_fulltext(pdf_path)
        if not tei_xml:
            return ExtractionResult(extraction_method="grobid")

        # Parse TEI XML
        sections = self._parse_tei_xml_string(tei_xml)

        # Extract abstract from TEI
        abstract = self._extract_tei_abstract(tei_xml)

        full_text_parts = []
        if abstract:
            full_text_parts.append(f"## Abstract\n{abstract}")
        for name, content in sections.items():
            full_text_parts.append(f"## {name}\n{content}")

        return ExtractionResult(
            sections=sections,
            abstract=abstract,
            full_text="\n\n".join(full_text_parts),
            extraction_method="grobid",
        )

    def extract_from_pdf_docling(self, pdf_path: str, timeout: int = 120) -> ExtractionResult:
        """Extract text from PDF via docling with timeout protection.

        Falls back gracefully on ImportError, TimeoutError, empty output, or
        fewer than 2 sections (indicating poor extraction quality).

        Args:
            pdf_path: Path to PDF file
            timeout: Max seconds to wait for docling (default 120)

        Returns:
            ExtractionResult with sections and full text, or error result
        """
        try:
            from docling.document_converter import DocumentConverter  # noqa: F401
        except ImportError:
            return ExtractionResult(extraction_method="docling_unavailable")

        try:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._run_docling, pdf_path)
                result = future.result(timeout=timeout)
            if not result or len(result.full_text) == 0:
                return ExtractionResult(extraction_method="docling_empty")
            if len(result.sections) < 2:
                return ExtractionResult(extraction_method="docling_poor")
            return result
        except concurrent.futures.TimeoutError:
            return ExtractionResult(extraction_method="docling_timeout")
        except Exception:
            return ExtractionResult(extraction_method="docling_error")

    def _run_docling(self, pdf_path: str) -> ExtractionResult:
        """Internal docling worker — runs in thread pool for timeout support.

        Detects scanned PDFs via pymupdf text length heuristic and enables
        OCR mode automatically.

        Args:
            pdf_path: Path to PDF file

        Returns:
            ExtractionResult with sections and full text
        """
        from docling.document_converter import DocumentConverter

        # Scan detection: if all pages have < 100 chars, treat as scanned
        is_scan = False
        try:
            import pymupdf
            doc_pymupdf = pymupdf.open(pdf_path)
            is_scan = all(len(page.get_text()) < 100 for page in doc_pymupdf)
            doc_pymupdf.close()
        except Exception:
            pass

        if is_scan:
            try:
                from docling.datamodel.pipeline_options import PdfPipelineOptions
                pipeline_options = PdfPipelineOptions(do_ocr=True)
                converter = DocumentConverter(
                    format_options={"pdf": {"pipeline_options": pipeline_options}}
                )
            except Exception:
                converter = DocumentConverter()
        else:
            converter = DocumentConverter()

        result = converter.convert(pdf_path)
        md_text = result.document.export_to_markdown()
        sections = self._regex_segment(md_text)
        return ExtractionResult(
            sections=sections,
            full_text=md_text,
            extraction_method="docling",
        )

    def extract_from_pdf_pymupdf(self, pdf_path: str) -> ExtractionResult:
        """Extract text from PDF via pymupdf4llm (fallback).

        Uses pymupdf4llm for Markdown conversion, then regex-based section splitting.

        Args:
            pdf_path: Path to PDF file

        Returns:
            ExtractionResult with best-effort sections
        """
        try:
            import pymupdf4llm
            md_text = pymupdf4llm.to_markdown(pdf_path)
        except Exception as e:
            print(f"pymupdf4llm failed: {e}")
            return ExtractionResult(extraction_method="pymupdf4llm")

        sections = self._regex_segment(md_text)

        return ExtractionResult(
            sections=sections,
            full_text=md_text,
            extraction_method="pymupdf4llm",
        )

    def _grobid_process_fulltext(self, pdf_path: str) -> Optional[str]:
        """Send PDF to GROBID and get TEI XML response.

        Args:
            pdf_path: Path to PDF

        Returns:
            TEI XML string or None
        """
        url = f"{self.grobid_url}/api/processFulltextDocument"
        try:
            with open(pdf_path, "rb") as f:
                resp = requests.post(
                    url,
                    files={"input": f},
                    data={
                        "consolidateHeader": "1",
                        "consolidateCitations": "0",
                        "includeRawAffiliations": "1",
                    },
                    timeout=60,
                )
            if resp.status_code == 200:
                return resp.text
        except requests.RequestException as e:
            print(f"GROBID request failed: {e}")
        return None

    def _parse_tei_xml_string(self, tei_xml: str) -> dict[str, str]:
        """Parse TEI XML string to section dict. 2-tier fallback.

        Tier 1: grobidmonkey (if installed)
        Tier 2: ElementTree direct parsing (always available)

        Args:
            tei_xml: TEI XML string

        Returns:
            Dict of section_name -> text
        """
        # Tier 1: Try grobidmonkey
        try:
            from grobidmonkey import reader
            import tempfile
            import os

            # grobidmonkey needs a file path
            fd, tmp_path = tempfile.mkstemp(suffix=".tei.xml")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(tei_xml)
                monkey = reader.MonkeyReader("monkey")
                essay = monkey.readEssay(tmp_path)
                if essay:
                    return {k: "\n\n".join(v) for k, v in essay.items()}
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        except (ImportError, Exception):
            pass  # grobidmonkey not installed or failed -> fallback

        # Tier 2: ElementTree direct parsing
        try:
            root = SafeET.fromstring(tei_xml)
            ns = {"tei": "http://www.tei-c.org/ns/1.0"}

            sections = {}
            for div in root.findall(".//tei:body/tei:div", ns):
                head = div.find("tei:head", ns)
                section_name = (
                    head.text if head is not None and head.text else "Unnamed"
                )
                paragraphs = []
                for p in div.findall("tei:p", ns):
                    text = "".join(p.itertext()).strip()
                    if text:
                        paragraphs.append(text)
                if paragraphs:
                    sections[section_name] = "\n\n".join(paragraphs)
            return sections
        except ET.ParseError:
            return {}

    def _extract_tei_abstract(self, tei_xml: str) -> Optional[str]:
        """Extract abstract from GROBID TEI XML.

        Args:
            tei_xml: TEI XML string

        Returns:
            Abstract text or None
        """
        try:
            root = SafeET.fromstring(tei_xml)
            ns = {"tei": "http://www.tei-c.org/ns/1.0"}
            abstract_el = root.find(".//tei:profileDesc/tei:abstract", ns)
            if abstract_el is not None:
                parts = []
                for p in abstract_el.findall(".//tei:p", ns):
                    text = "".join(p.itertext()).strip()
                    if text:
                        parts.append(text)
                if parts:
                    return " ".join(parts)
        except ET.ParseError:
            pass
        return None

    def _regex_segment(self, md_text: str) -> dict[str, str]:
        """Segment Markdown text into sections using regex (fallback).

        Args:
            md_text: Markdown text from pymupdf4llm

        Returns:
            Dict of section_name -> text (best-effort)
        """
        # Match markdown headers
        pattern = r"^#{1,3}\s+(.+)$"
        sections = {}
        current_section = "Preamble"
        current_text = []

        for line in md_text.split("\n"):
            match = re.match(pattern, line)
            if match:
                # Save previous section
                if current_text:
                    text = "\n".join(current_text).strip()
                    if text:
                        sections[current_section] = text
                current_section = match.group(1).strip()
                current_text = []
            else:
                current_text.append(line)

        # Save last section
        if current_text:
            text = "\n".join(current_text).strip()
            if text:
                sections[current_section] = text

        return sections

