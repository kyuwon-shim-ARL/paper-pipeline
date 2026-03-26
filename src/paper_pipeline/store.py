"""
Paper Store - Hierarchical storage for paper data (L0/L2 layers).

Storage layout:
    data/papers/
    ├── index.json
    ├── by-doi/{doi_dirname}/
    │   ├── metadata.json      (L0)
    │   ├── sections.json      (L2)
    │   ├── content/           (git-ignored)
    │   │   ├── fulltext.md
    │   │   ├── raw_abstract.txt
    │   │   ├── grobid.tei.xml
    │   │   └── source.pdf
    │   └── README.md
    └── by-collection/
        └── {name}/collection.json
"""

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


LAYER_FILES = {
    "L0": "metadata.json",
    "L2": "sections.json",
}

CONTENT_FILES = {
    "fulltext": "fulltext.md",
    "abstract": "raw_abstract.txt",
    "pdf": "source.pdf",
    "grobid_tei": "grobid.tei.xml",
}


def doi_to_dirname(doi: str) -> str:
    """Convert DOI to safe directory name.

    Handles special characters: /, ., (, ), ;, etc.

    Args:
        doi: DOI string (with or without https://doi.org/ prefix)

    Returns:
        Safe directory name string

    Example:
        >>> doi_to_dirname("10.1038/s41586-024-07345-x")
        '10_1038__s41586-024-07345-x'
    """
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    safe = doi.replace("/", "__")
    safe = re.sub(r"[^a-zA-Z0-9._\-]", lambda m: f"_{ord(m.group()):02x}_", safe)
    return safe


class PaperStore:
    """Hierarchical paper storage with L0/L2 layers.

    Usage:
        store = PaperStore("data/papers")
        store.save_layer("10.1038/xxx", "L0", {"title": "...", ...})
        data = store.load_layer("10.1038/xxx", "L0")
    """

    def __init__(self, base_dir: str = "data/papers"):
        """Initialize store. Creates directories and loads index.

        Args:
            base_dir: Base directory for paper storage
        """
        self.base_dir = Path(base_dir)
        self.doi_dir = self.base_dir / "by-doi"
        self.collection_dir = self.base_dir / "by-collection"
        self.summaries_dir = self.base_dir / "summaries"
        self.index_path = self.base_dir / "index.json"

        # Create directories
        self.doi_dir.mkdir(parents=True, exist_ok=True)
        self.collection_dir.mkdir(parents=True, exist_ok=True)
        self.summaries_dir.mkdir(parents=True, exist_ok=True)

        # Load or create index
        self.index = self._load_index()

    def _load_index(self) -> dict:
        """Load index.json or create empty index."""
        if self.index_path.exists():
            with open(self.index_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {
            "version": "1.0",
            "last_updated": "",
            "paper_count": 0,
            "papers": {},
        }

    def _save_index(self) -> None:
        """Save index.json atomically (write to temp, then rename)."""
        self.index["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        self.index["paper_count"] = len(self.index["papers"])

        fd, tmp_path = tempfile.mkstemp(
            dir=str(self.base_dir), suffix=".json.tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self.index, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, str(self.index_path))
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def get_paper_dir(self, doi: str) -> Path:
        """Get full directory path for a paper.

        Args:
            doi: DOI string

        Returns:
            Path to paper directory
        """
        dirname = doi_to_dirname(doi)
        return self.doi_dir / dirname

    def save_layer(
        self,
        doi: str,
        layer: str,
        data: dict,
        provenance_entry: Optional[dict] = None,
    ) -> Path:
        """Save a specific layer for a paper (upsert).

        Args:
            doi: DOI string
            layer: Layer name ("L0", "L2")
            data: JSON-serializable dict
            provenance_entry: Optional provenance metadata to append (L0 only).
                Required keys: session_id, timestamp, source.
                Optional keys: search_params, cluster_id, seed_source.

        Returns:
            Path to saved file

        Raises:
            ValueError: If layer is not valid
        """
        if layer not in LAYER_FILES:
            raise ValueError(f"Invalid layer: {layer}. Must be one of {list(LAYER_FILES.keys())}")

        paper_dir = self.get_paper_dir(doi)
        paper_dir.mkdir(parents=True, exist_ok=True)

        file_path = paper_dir / LAYER_FILES[layer]

        # For L0: handle provenance append
        if layer == "L0" and provenance_entry is not None:
            # Load existing L0 to preserve provenance history
            existing = None
            if file_path.exists():
                with open(file_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)

            # Get existing provenance array (auto-patch if missing)
            prev_provenance = []
            if existing and "provenance" in existing:
                prev_provenance = existing["provenance"]

            # Append new entry
            prev_provenance.append(provenance_entry)
            data["provenance"] = prev_provenance
        elif layer == "L0":
            # Ensure provenance key exists (even if empty)
            if "provenance" not in data:
                # Preserve existing provenance if updating L0 without new entry
                if file_path.exists():
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                    data["provenance"] = existing.get("provenance", [])
                else:
                    data["provenance"] = []

        # Atomic write
        fd, tmp_path = tempfile.mkstemp(dir=str(paper_dir), suffix=".json.tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, str(file_path))
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        # Update index
        clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        self._update_index_entry(clean_doi, layer, data if layer == "L0" else None)
        self._save_index()

        return file_path

    def load_layer(self, doi: str, layer: str) -> Optional[dict]:
        """Load a specific layer for a paper.

        Auto-patches L0 data: adds empty provenance array if missing.

        Args:
            doi: DOI string
            layer: Layer name ("L0", "L2")

        Returns:
            Layer data dict or None if not found
        """
        if layer not in LAYER_FILES:
            raise ValueError(f"Invalid layer: {layer}")

        file_path = self.get_paper_dir(doi) / LAYER_FILES[layer]
        if not file_path.exists():
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Auto-patch: ensure L0 always has provenance field
        if layer == "L0" and "provenance" not in data:
            data["provenance"] = []

        return data

    def has_layer(self, doi: str, layer: str) -> bool:
        """Check if a layer exists for a paper.

        Args:
            doi: DOI string
            layer: Layer name

        Returns:
            True if layer file exists
        """
        if layer not in LAYER_FILES:
            return False
        return (self.get_paper_dir(doi) / LAYER_FILES[layer]).exists()

    def save_content(self, doi: str, content_type: str, data: str | bytes) -> Path:
        """Save raw content to content/ subdirectory (git-ignored).

        Args:
            doi: DOI string
            content_type: Content type ("fulltext", "abstract", "pdf", "grobid_tei")
            data: Content data (str for text, bytes for binary)

        Returns:
            Path to saved file
        """
        if content_type not in CONTENT_FILES:
            raise ValueError(f"Invalid content type: {content_type}. Must be one of {list(CONTENT_FILES.keys())}")

        content_dir = self.get_paper_dir(doi) / "content"
        content_dir.mkdir(parents=True, exist_ok=True)

        file_path = content_dir / CONTENT_FILES[content_type]
        mode = "wb" if isinstance(data, bytes) else "w"
        encoding = None if isinstance(data, bytes) else "utf-8"

        with open(file_path, mode, encoding=encoding) as f:
            f.write(data)

        return file_path

    def load_content(self, doi: str, content_type: str) -> Optional[str | bytes]:
        """Load raw content from content/ subdirectory.

        Args:
            doi: DOI string
            content_type: Content type

        Returns:
            Content data or None
        """
        if content_type not in CONTENT_FILES:
            return None

        file_path = self.get_paper_dir(doi) / "content" / CONTENT_FILES[content_type]
        if not file_path.exists():
            return None

        if content_type == "pdf":
            with open(file_path, "rb") as f:
                return f.read()
        else:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()

    def _update_index_entry(self, doi: str, layer: str, l0_data: Optional[dict] = None) -> None:
        """Update index entry for a paper."""
        dirname = doi_to_dirname(doi)

        if doi not in self.index["papers"]:
            self.index["papers"][doi] = {
                "paper_id": dirname,
                "openalex_id": "",
                "title": "",
                "year": None,
                "layers": {"L0": False, "L2": False},
                "oa_status": None,
                "content_source": None,
                "content_available": False,
                "extraction_method": None,
                "collections": [],
                "added_date": datetime.now().strftime("%Y-%m-%d"),
            }

        entry = self.index["papers"][doi]
        entry["layers"][layer] = True

        # Populate from L0 metadata
        if l0_data and layer == "L0":
            entry["openalex_id"] = l0_data.get("openalex_id", "")
            entry["title"] = l0_data.get("title", "")
            entry["year"] = l0_data.get("publication_year")
            entry["oa_status"] = l0_data.get("oa_status")

    def update_content_info(self, doi: str, content_source: str, extraction_method: str) -> None:
        """Update content source and extraction method in index.

        Args:
            doi: DOI string
            content_source: e.g., "europe_pmc_xml", "unpaywall", "openalex_oa"
            extraction_method: e.g., "europe_pmc_xml", "grobid", "pymupdf4llm"
        """
        clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        if clean_doi in self.index["papers"]:
            self.index["papers"][clean_doi]["content_source"] = content_source
            self.index["papers"][clean_doi]["content_available"] = True
            self.index["papers"][clean_doi]["extraction_method"] = extraction_method
            self._save_index()

    def list_papers(self, filters: Optional[dict] = None) -> list[dict]:
        """List papers with optional filtering.

        Args:
            filters: Filter dict with keys:
                - year_min: Minimum publication year
                - year_max: Maximum publication year
                - oa_only: Only OA papers
                - has_layer: Required layer (e.g., "L2")
                - collection: Collection name

        Returns:
            List of index entries matching filters
        """
        papers = list(self.index["papers"].values())

        if not filters:
            return papers

        if filters.get("year_min"):
            papers = [p for p in papers if (p.get("year") or 0) >= filters["year_min"]]
        if filters.get("year_max"):
            papers = [p for p in papers if (p.get("year") or 9999) <= filters["year_max"]]
        if filters.get("oa_only"):
            papers = [p for p in papers if p.get("oa_status") and p["oa_status"] != "closed"]
        if filters.get("has_layer"):
            layer = filters["has_layer"]
            papers = [p for p in papers if p.get("layers", {}).get(layer, False)]
        if filters.get("collection"):
            coll = filters["collection"]
            papers = [p for p in papers if coll in (p.get("collections") or [])]

        return papers

    def create_collection(self, name: str, dois: list[str]) -> Path:
        """Create a named collection of papers.

        Args:
            name: Collection name
            dois: List of DOI strings

        Returns:
            Path to collection.json
        """
        coll_dir = self.collection_dir / name
        coll_dir.mkdir(parents=True, exist_ok=True)

        coll_data = {
            "name": name,
            "created": datetime.now().strftime("%Y-%m-%d"),
            "dois": [d.replace("https://doi.org/", "").replace("http://doi.org/", "") for d in dois],
            "count": len(dois),
        }

        coll_path = coll_dir / "collection.json"
        with open(coll_path, "w", encoding="utf-8") as f:
            json.dump(coll_data, f, ensure_ascii=False, indent=2)

        # Update index entries
        for doi in coll_data["dois"]:
            if doi in self.index["papers"]:
                colls = self.index["papers"][doi].get("collections", [])
                if name not in colls:
                    colls.append(name)
                    self.index["papers"][doi]["collections"] = colls
        self._save_index()

        return coll_path

    def get_collection(self, name: str) -> Optional[list[str]]:
        """Get DOI list from a collection.

        Args:
            name: Collection name

        Returns:
            List of DOI strings or None
        """
        coll_path = self.collection_dir / name / "collection.json"
        if not coll_path.exists():
            return None

        with open(coll_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("dois", [])

    def list_collections(self) -> list[str]:
        """List all collection names.

        Returns:
            List of collection name strings
        """
        if not self.collection_dir.exists():
            return []
        return [
            d.name
            for d in self.collection_dir.iterdir()
            if d.is_dir() and (d / "collection.json").exists()
        ]

    def generate_readme(self, doi: str) -> Optional[Path]:
        """Generate README.md for a paper from L0 metadata.

        Args:
            doi: DOI string

        Returns:
            Path to README.md or None if no L0
        """
        l0 = self.load_layer(doi, "L0")
        if not l0:
            return None

        clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        paper_dir = self.get_paper_dir(doi)

        lines = [
            f"# {l0.get('title', 'Unknown Title')}",
            "",
            f"- **DOI**: {clean_doi}",
            f"- **Year**: {l0.get('publication_year', 'N/A')}",
            f"- **Journal**: {l0.get('journal', 'N/A')}",
            f"- **Citations**: {l0.get('cited_by_count', 0)}",
            f"- **OA Status**: {l0.get('oa_status', 'N/A')}",
            "",
        ]

        # Layers status
        layers_status = []
        for layer in ["L0", "L2"]:
            status = "available" if self.has_layer(doi, layer) else "not generated"
            layers_status.append(f"- {layer}: {status}")
        lines.append("## Analysis Layers")
        lines.append("")
        lines.extend(layers_status)
        lines.append("")

        # Abstract excerpt
        abstract = l0.get("abstract")
        if abstract:
            lines.append("## Abstract")
            lines.append("")
            lines.append(abstract[:500] + ("..." if len(abstract or "") > 500 else ""))
            lines.append("")

        readme_path = paper_dir / "README.md"
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return readme_path

    def update_index(self) -> None:
        """Rebuild index from disk state. Scans all paper directories."""
        new_papers = {}

        if not self.doi_dir.exists():
            return

        for paper_dir in self.doi_dir.iterdir():
            if not paper_dir.is_dir():
                continue

            # Detect layers
            layers = {}
            for layer, filename in LAYER_FILES.items():
                layers[layer] = (paper_dir / filename).exists()

            # Try to load L0 for metadata
            l0_path = paper_dir / "metadata.json"
            l0_data = {}
            if l0_path.exists():
                with open(l0_path, "r", encoding="utf-8") as f:
                    l0_data = json.load(f)

            doi = l0_data.get("doi", "")
            if not doi:
                # Try to reverse dirname → DOI (best effort)
                doi = paper_dir.name.replace("__", "/")

            content_dir = paper_dir / "content"
            has_content = content_dir.exists() and any(content_dir.iterdir()) if content_dir.exists() else False

            # Preserve existing entry data
            existing = self.index.get("papers", {}).get(doi, {})

            new_papers[doi] = {
                "paper_id": paper_dir.name,
                "openalex_id": l0_data.get("openalex_id", existing.get("openalex_id", "")),
                "title": l0_data.get("title", existing.get("title", "")),
                "year": l0_data.get("publication_year", existing.get("year")),
                "layers": layers,
                "oa_status": l0_data.get("oa_status", existing.get("oa_status")),
                "content_source": existing.get("content_source"),
                "content_available": has_content,
                "extraction_method": existing.get("extraction_method"),
                "collections": existing.get("collections", []),
                "added_date": existing.get("added_date", datetime.now().strftime("%Y-%m-%d")),
            }

        self.index["papers"] = new_papers
        self._save_index()

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.

        Returns:
            Dict with paper count, layer counts, collection counts
        """
        papers = self.index.get("papers", {})
        layer_counts = {"L0": 0, "L2": 0}
        for p in papers.values():
            for layer, has in p.get("layers", {}).items():
                if has:
                    layer_counts[layer] += 1

        return {
            "total_papers": len(papers),
            "layer_counts": layer_counts,
            "collections": self.list_collections(),
            "content_available": sum(1 for p in papers.values() if p.get("content_available")),
        }
