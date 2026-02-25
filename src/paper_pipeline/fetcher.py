"""
Paper Fetcher - Multi-source paper content acquisition.

Fallback chain:
    1. Europe PMC full-text XML (best: structured, no GROBID needed)
    2. OpenAlex OA PDF URL
    3. Unpaywall API
    4. bioRxiv/medRxiv (DOI 10.1101/)
    5. CrossRef publisher links
"""

import os
import time
import tempfile
import requests
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class ContentResult:
    """Result of content fetching attempt."""
    source: str = ""          # "europe_pmc", "unpaywall", "openalex_oa", "biorxiv", "crossref"
    content_type: str = ""    # "pmc_xml", "pdf", "abstract_only", "metadata_only"
    data: Optional[str] = None   # XML/text content or None (PDF saved to file)
    pdf_path: Optional[str] = None


class PaperFetcher:
    """Multi-source paper content fetcher with per-service rate limiting.

    Usage:
        fetcher = PaperFetcher(email="your@email.com")
        result = fetcher.fetch_content("10.1038/s41586-024-xxx")
    """

    # Per-service rate limits (seconds)
    RATE_LIMITS = {
        "europe_pmc": 0.2,
        "unpaywall": 0.1,
        "pdf_download": 2.0,
        "biorxiv": 0.5,
        "crossref": 0.1,
    }

    def __init__(self, email: str, ncbi_api_key: Optional[str] = None,
                 pdf_dir: str = "data/papers"):
        """Initialize fetcher.

        Args:
            email: Required for Unpaywall API and polite access
            ncbi_api_key: Optional NCBI API key for higher Europe PMC limits
            pdf_dir: Base directory for PDF storage
        """
        self.email = email or os.environ.get("PAPER_PIPELINE_EMAIL", "")
        self.ncbi_api_key = ncbi_api_key or os.environ.get("NCBI_API_KEY")
        self.pdf_dir = Path(pdf_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"PaperPipeline/1.0 (mailto:{self.email})"
        })

        # Per-service last request timestamps
        self._last_request: dict[str, float] = {}

        # Stats
        self.stats = {
            "total_attempts": 0,
            "successes": 0,
            "failures": 0,
            "by_source": {},
        }

    def _rate_limit(self, service: str) -> None:
        """Enforce per-service rate limiting."""
        delay = self.RATE_LIMITS.get(service, 0.1)
        last = self._last_request.get(service, 0)
        elapsed = time.time() - last
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request[service] = time.time()

    def fetch_content(self, doi: str, work_data: Optional[dict] = None,
                      save_dir: Optional[str] = None) -> ContentResult:
        """Fetch paper content with multi-source fallback.

        Args:
            doi: DOI string
            work_data: Optional pre-fetched OpenAlex work data (has pdf_url)
            save_dir: Optional directory to save PDF (for GROBID later)

        Returns:
            ContentResult with source, type, and data/path
        """
        self.stats["total_attempts"] += 1
        clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")

        # 1. Europe PMC full-text XML (best)
        xml = self.fetch_europe_pmc_fulltext(clean_doi)
        if xml:
            self._record_success("europe_pmc")
            return ContentResult(
                source="europe_pmc",
                content_type="pmc_xml",
                data=xml,
            )

        # 2. OpenAlex OA URL (if work_data provided)
        if work_data:
            pdf_url = work_data.get("pdf_url") or work_data.get("oa_url")
            if pdf_url:
                pdf_path = self._download_pdf_to(pdf_url, clean_doi, save_dir)
                if pdf_path:
                    self._record_success("openalex_oa")
                    return ContentResult(
                        source="openalex_oa",
                        content_type="pdf",
                        pdf_path=pdf_path,
                    )

        # 3. Unpaywall
        unpaywall_url = self.fetch_unpaywall_url(clean_doi)
        if unpaywall_url:
            pdf_path = self._download_pdf_to(unpaywall_url, clean_doi, save_dir)
            if pdf_path:
                self._record_success("unpaywall")
                return ContentResult(
                    source="unpaywall",
                    content_type="pdf",
                    pdf_path=pdf_path,
                )

        # 4. bioRxiv/medRxiv (DOI 10.1101/)
        if clean_doi.startswith("10.1101/"):
            biorxiv_url = self._get_biorxiv_pdf_url(clean_doi)
            if biorxiv_url:
                pdf_path = self._download_pdf_to(biorxiv_url, clean_doi, save_dir)
                if pdf_path:
                    self._record_success("biorxiv")
                    return ContentResult(
                        source="biorxiv",
                        content_type="pdf",
                        pdf_path=pdf_path,
                    )

        # 5. CrossRef publisher links
        crossref_url = self._get_crossref_pdf_url(clean_doi)
        if crossref_url:
            pdf_path = self._download_pdf_to(crossref_url, clean_doi, save_dir)
            if pdf_path:
                self._record_success("crossref")
                return ContentResult(
                    source="crossref",
                    content_type="pdf",
                    pdf_path=pdf_path,
                )

        # Fallback: abstract only or metadata only
        self.stats["failures"] += 1
        if work_data and work_data.get("abstract"):
            return ContentResult(
                source="openalex",
                content_type="abstract_only",
                data=work_data["abstract"],
            )

        return ContentResult(
            source="none",
            content_type="metadata_only",
        )

    def fetch_europe_pmc_fulltext(self, doi: str) -> Optional[str]:
        """Fetch full-text XML from Europe PMC.

        Steps:
            1. Search by DOI to get PMCID
            2. Fetch full-text XML by PMCID

        Args:
            doi: Clean DOI string

        Returns:
            XML string or None
        """
        self._rate_limit("europe_pmc")

        # Step 1: Search for PMCID
        search_url = (
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/search"
            f"?query=DOI:{doi}&format=json&resultType=core"
        )
        try:
            resp = self.session.get(search_url, timeout=10)
            if resp.status_code != 200:
                return None

            data = resp.json()
            results = data.get("resultList", {}).get("result", [])
            if not results:
                return None

            # Find PMCID
            pmcid = None
            for r in results:
                if r.get("pmcid"):
                    pmcid = r["pmcid"]
                    break

            if not pmcid:
                return None

        except (requests.RequestException, ValueError):
            return None

        # Step 2: Fetch full-text XML
        self._rate_limit("europe_pmc")
        fulltext_url = (
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
        )
        try:
            resp = self.session.get(fulltext_url, timeout=15)
            if resp.status_code == 200 and resp.text.strip().startswith(("<?xml", "<!DOCTYPE", "<article")):
                return resp.text
        except requests.RequestException:
            pass

        return None

    def fetch_unpaywall_url(self, doi: str) -> Optional[str]:
        """Get best OA PDF URL from Unpaywall.

        Args:
            doi: Clean DOI string

        Returns:
            PDF URL string or None
        """
        if not self.email:
            return None

        self._rate_limit("unpaywall")
        url = f"https://api.unpaywall.org/v2/{doi}?email={self.email}"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return None

            data = resp.json()
            best_loc = data.get("best_oa_location") or {}
            return best_loc.get("url_for_pdf") or best_loc.get("url")

        except (requests.RequestException, ValueError):
            return None

    def download_pdf(self, url: str, save_path: str) -> bool:
        """Download PDF to specified path. Uses temp file + rename for atomicity.

        Args:
            url: PDF URL
            save_path: Target file path

        Returns:
            True if download succeeded
        """
        self._rate_limit("pdf_download")
        save_path_obj = Path(save_path)
        save_path_obj.parent.mkdir(parents=True, exist_ok=True)

        try:
            resp = self.session.get(url, timeout=30, stream=True)
            if resp.status_code != 200:
                return False

            # Check content type
            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type and "pdf" not in content_type:
                return False

            # Write to temp file
            fd, tmp_path = tempfile.mkstemp(
                dir=str(save_path_obj.parent), suffix=".pdf.tmp"
            )
            try:
                with os.fdopen(fd, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Verify it looks like a PDF
                with open(tmp_path, "rb") as f:
                    header = f.read(5)
                if header != b"%PDF-":
                    os.unlink(tmp_path)
                    return False

                os.replace(tmp_path, str(save_path_obj))
                return True

            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                return False

        except requests.RequestException:
            return False

    def _download_pdf_to(self, url: str, doi: str,
                         save_dir: Optional[str] = None) -> Optional[str]:
        """Download PDF to paper's content directory.

        Args:
            url: PDF URL
            doi: Clean DOI for directory name
            save_dir: Override save directory

        Returns:
            Path string if successful, None otherwise
        """
        from paper_pipeline.store import doi_to_dirname

        if save_dir:
            pdf_path = os.path.join(save_dir, "content", "source.pdf")
        else:
            dirname = doi_to_dirname(doi)
            pdf_path = str(self.pdf_dir / "by-doi" / dirname / "content" / "source.pdf")

        if self.download_pdf(url, pdf_path):
            return pdf_path
        return None

    def _get_biorxiv_pdf_url(self, doi: str) -> Optional[str]:
        """Get PDF URL from bioRxiv/medRxiv API.

        Args:
            doi: Clean DOI (must start with 10.1101/)

        Returns:
            PDF URL or None
        """
        self._rate_limit("biorxiv")
        # bioRxiv API: https://api.biorxiv.org/details/biorxiv/{doi}
        api_url = f"https://api.biorxiv.org/details/biorxiv/{doi}"
        try:
            resp = self.session.get(api_url, timeout=10)
            if resp.status_code != 200:
                # Try medRxiv
                api_url = f"https://api.biorxiv.org/details/medrxiv/{doi}"
                resp = self.session.get(api_url, timeout=10)
                if resp.status_code != 200:
                    return None

            data = resp.json()
            collection = data.get("collection", [])
            if collection:
                # Get latest version
                latest = collection[-1]
                jatsxml = latest.get("jatsxml", "")
                if jatsxml:
                    # Convert JATS XML URL to PDF URL
                    return jatsxml.replace(".source.xml", ".full.pdf")
        except (requests.RequestException, ValueError):
            pass
        return None

    def _get_crossref_pdf_url(self, doi: str) -> Optional[str]:
        """Get PDF URL from CrossRef metadata.

        Args:
            doi: Clean DOI

        Returns:
            PDF URL or None
        """
        self._rate_limit("crossref")
        url = f"https://api.crossref.org/works/{doi}"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return None

            data = resp.json()
            message = data.get("message", {})
            links = message.get("link", [])
            for link in links:
                if link.get("content-type") == "application/pdf":
                    return link.get("URL")

        except (requests.RequestException, ValueError):
            pass
        return None

    def _record_success(self, source: str) -> None:
        """Record successful fetch for stats."""
        self.stats["successes"] += 1
        self.stats["by_source"][source] = self.stats["by_source"].get(source, 0) + 1

    def get_stats(self) -> dict:
        """Get fetcher statistics."""
        return {
            **self.stats,
            "success_rate": (
                self.stats["successes"] / max(self.stats["total_attempts"], 1)
            ),
        }
