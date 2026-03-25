"""
BibTeX Generation - doi2bib wrapper with OpenAlex fallback.

Primary: calls doi2bib CLI via subprocess (concurrent, batched).
Fallback: generates BibTeX from OpenAlex L0 metadata when doi2bib fails.
"""

import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from unidecode import unidecode
except ImportError:
    def unidecode(s):
        """Fallback: strip non-ASCII."""
        return s.encode("ascii", "ignore").decode("ascii")


DOI2BIB_CMD = os.environ.get("PAPER_PIPELINE_DOI2BIB_CMD", "doi2bib")


def _extract_lastname(display_name: str) -> str:
    """Extract last name from author display_name.

    Rules:
        1. If comma present ("Smith, John") -> token before comma
        2. If no comma ("John Smith") -> last space-separated token
        3. No author -> "unknown"
    """
    if not display_name or not display_name.strip():
        return "unknown"
    name = display_name.strip()
    if "," in name:
        return name.split(",")[0].strip()
    parts = name.split()
    return parts[-1] if parts else "unknown"


def _make_citation_key(authors: list[dict], year: Optional[int], title: str) -> str:
    """Generate citation key: {lastname}{year}{first_title_word} (lowercase ASCII).

    Args:
        authors: List of authorship dicts with author.display_name
        year: Publication year
        title: Paper title

    Returns:
        Citation key string (lowercase ASCII)
    """
    # Last name
    if authors:
        display_name = authors[0].get("author", {}).get("display_name", "")
        lastname = _extract_lastname(display_name)
    else:
        lastname = "unknown"

    # Year
    year_str = str(year) if year else "nd"

    # First meaningful title word
    title_clean = re.sub(r"[^a-zA-Z0-9\s]", "", title or "")
    words = title_clean.split()
    # Skip common short words
    skip = {"a", "an", "the", "of", "in", "on", "for", "and", "to", "with"}
    first_word = "untitled"
    for w in words:
        if w.lower() not in skip:
            first_word = w.lower()
            break

    key = f"{unidecode(lastname).lower()}{year_str}{first_word}"
    # Ensure only alphanumeric + hyphen
    key = re.sub(r"[^a-z0-9]", "", key)
    return key or "unknown"


def _resolve_key_conflicts(keys_to_dois: dict[str, list[str]]) -> dict[str, str]:
    """Resolve citation key conflicts by appending a/b/c... suffixes.

    DOIs are sorted lexicographically for determinism.

    Args:
        keys_to_dois: Mapping of base key -> list of DOIs sharing that key

    Returns:
        Mapping of DOI -> final unique citation key
    """
    doi_to_key = {}
    for base_key, dois in keys_to_dois.items():
        if len(dois) == 1:
            doi_to_key[dois[0]] = base_key
        else:
            sorted_dois = sorted(dois)
            for i, doi in enumerate(sorted_dois):
                if i < 26:
                    suffix = chr(ord("a") + i)
                else:
                    # aa, ab, ac, ...
                    suffix = chr(ord("a") + (i // 26) - 1) + chr(ord("a") + (i % 26))
                doi_to_key[doi] = f"{base_key}{suffix}"
    return doi_to_key


def _determine_entry_type(l0: dict) -> str:
    """Determine BibTeX entry type from OpenAlex metadata."""
    source_type = ""
    primary_loc = l0.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    source_type = (source.get("type") or "").lower()

    work_type = (l0.get("type") or "").lower()

    if source_type == "journal":
        return "article"
    elif source_type == "conference":
        return "inproceedings"
    elif work_type in ("book", "book-chapter"):
        return "inbook" if work_type == "book-chapter" else "book"
    else:
        return "misc"


def _bibtex_from_openalex(doi: str, l0: dict, citation_key: str) -> Optional[str]:
    """Generate BibTeX entry from OpenAlex L0 metadata.

    Args:
        doi: DOI string
        l0: L0 metadata dict from PaperStore
        citation_key: Pre-computed citation key

    Returns:
        BibTeX entry string, or None if required fields missing
    """
    title = l0.get("title")
    year = l0.get("publication_year")

    if not title and not year:
        return None

    entry_type = _determine_entry_type(l0)

    # Authors
    authorships = l0.get("authorships", [])
    if authorships:
        author_names = []
        for a in authorships:
            name = a.get("author", {}).get("display_name", "")
            if name:
                author_names.append(name)
        author_str = " and ".join(author_names) if author_names else "{Unknown}"
    else:
        author_str = "{Unknown}"

    # Build fields
    fields = [f"  title = {{{title or 'Unknown'}}}"]
    fields.append(f"  author = {{{author_str}}}")
    if year:
        fields.append(f"  year = {{{year}}}")

    # Source-dependent fields
    primary_loc = l0.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    source_name = source.get("display_name", "")

    if entry_type == "article" and source_name:
        fields.append(f"  journal = {{{source_name}}}")
    elif entry_type == "inproceedings" and source_name:
        fields.append(f"  booktitle = {{{source_name}}}")

    if doi:
        fields.append(f"  doi = {{{doi}}}")

    # Biblio fields
    biblio = l0.get("biblio") or {}
    if biblio.get("volume"):
        fields.append(f"  volume = {{{biblio['volume']}}}")
    if biblio.get("issue"):
        fields.append(f"  number = {{{biblio['issue']}}}")
    first_page = biblio.get("first_page")
    last_page = biblio.get("last_page")
    if first_page:
        pages = f"{first_page}--{last_page}" if last_page else first_page
        fields.append(f"  pages = {{{pages}}}")

    # Check for incomplete fields
    missing = []
    if not title:
        missing.append("title")
    if not year:
        missing.append("year")
    if author_str == "{Unknown}":
        missing.append("author")
    if missing:
        fields.append(f"  note = {{[INCOMPLETE: missing {', '.join(missing)}]}}")

    fields_str = ",\n".join(fields)
    return f"@{entry_type}{{{citation_key},\n{fields_str}\n}}"


def _call_doi2bib(doi: str) -> Optional[str]:
    """Call doi2bib CLI for a single DOI.

    Returns:
        BibTeX string if successful, None on failure
    """
    try:
        result = subprocess.run(
            [DOI2BIB_CMD, doi],
            capture_output=True,
            text=True,
            timeout=10,
        )
        stdout = result.stdout.strip()
        if result.returncode != 0:
            return None
        if not stdout:
            return None
        if "@" not in stdout:
            return None
        return stdout
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return None
    except Exception:
        return None


def _read_existing_keys(bib_path: Path) -> set[str]:
    """Read existing citation keys from a .bib file."""
    keys = set()
    if not bib_path.exists():
        return keys
    content = bib_path.read_text(encoding="utf-8")
    for match in re.finditer(r"@\w+\{([^,]+),", content):
        keys.add(match.group(1).strip())
    return keys


def export_bib(
    manifest: dict,
    store,
    output_path: str | Path,
    timeout: int = 300,
    max_concurrent: int = 5,
) -> dict:
    """Export BibTeX for all DOIs in a pool manifest.

    Primary: doi2bib CLI (concurrent). Fallback: OpenAlex L0 metadata.
    Partial flush: successful entries are appended immediately.

    Args:
        manifest: Pool manifest dict (schema_version 1)
        store: PaperStore instance for OpenAlex fallback
        output_path: Path for output .bib file
        timeout: Global timeout in seconds (default 300 = 5 min)
        max_concurrent: Max concurrent doi2bib calls

    Returns:
        Dict with statistics:
            total: total DOIs attempted
            success: successfully generated
            failed: failed DOIs
            fallback: count using OpenAlex fallback
            skipped: already in existing .bib
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing_keys = _read_existing_keys(output_path)
    papers = manifest.get("papers", [])
    dois = [p["doi"] for p in papers if p.get("doi")]

    # Pre-compute citation keys
    keys_to_dois: dict[str, list[str]] = {}
    doi_to_l0: dict[str, dict] = {}

    for doi in dois:
        l0 = store.load_layer(doi, "L0")
        if not l0:
            continue
        doi_to_l0[doi] = l0
        base_key = _make_citation_key(
            l0.get("authorships", []),
            l0.get("publication_year"),
            l0.get("title", ""),
        )
        keys_to_dois.setdefault(base_key, []).append(doi)

    doi_to_key = _resolve_key_conflicts(keys_to_dois)

    stats = {"total": len(dois), "success": 0, "failed": 0, "fallback": 0, "skipped": 0}
    failed_dois = []
    start_time = time.time()

    # Open file for appending
    with open(output_path, "a", encoding="utf-8") as bib_file:

        def process_doi(doi: str) -> Optional[str]:
            """Process a single DOI: doi2bib -> fallback -> return bibtex."""
            key = doi_to_key.get(doi)
            if not key:
                return None

            if key in existing_keys:
                return "SKIP"

            # Try doi2bib first
            bibtex = _call_doi2bib(doi)
            if bibtex:
                # Replace the citation key in doi2bib output
                bibtex = re.sub(r"(@\w+\{)[^,]+,", f"\\1{key},", bibtex, count=1)
                return bibtex

            # Fallback to OpenAlex
            l0 = doi_to_l0.get(doi)
            if l0:
                bibtex = _bibtex_from_openalex(doi, l0, key)
                if bibtex:
                    return "FALLBACK:" + bibtex

            return None

        # Process in batches with thread pool
        remaining_dois = [d for d in dois if d in doi_to_key]
        batch_size = max_concurrent

        for batch_start in range(0, len(remaining_dois), batch_size):
            # Check global timeout
            elapsed = time.time() - start_time
            if elapsed > timeout:
                # Timeout: record remaining as failed
                for doi in remaining_dois[batch_start:]:
                    failed_dois.append({"doi": doi, "reason": "timeout_global"})
                    stats["failed"] += 1
                print(f"[WARN] Global timeout ({timeout}s) reached. "
                      f"{len(remaining_dois) - batch_start} DOIs not processed.")
                break

            batch = remaining_dois[batch_start:batch_start + batch_size]

            with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = {executor.submit(process_doi, doi): doi for doi in batch}
                for future in as_completed(futures):
                    doi = futures[future]
                    try:
                        result = future.result()
                        if result == "SKIP":
                            stats["skipped"] += 1
                        elif result and result.startswith("FALLBACK:"):
                            bibtex = result[len("FALLBACK:"):]
                            bib_file.write(bibtex + "\n\n")
                            bib_file.flush()
                            existing_keys.add(doi_to_key[doi])
                            stats["success"] += 1
                            stats["fallback"] += 1
                        elif result:
                            bib_file.write(result + "\n\n")
                            bib_file.flush()
                            existing_keys.add(doi_to_key[doi])
                            stats["success"] += 1
                        else:
                            failed_dois.append({"doi": doi, "reason": "all_methods_failed"})
                            stats["failed"] += 1
                    except Exception as e:
                        failed_dois.append({"doi": doi, "reason": str(e)})
                        stats["failed"] += 1

    # Write failed DOIs
    if failed_dois:
        failed_path = output_path.parent / "failed_dois.txt"
        with open(failed_path, "w", encoding="utf-8") as f:
            for entry in failed_dois:
                f.write(f"{entry['doi']}\t{entry['reason']}\n")
        print(f"Failed DOIs written to {failed_path}")

    return stats
