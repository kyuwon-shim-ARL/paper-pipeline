"""
Pool Manifest - Thin DOI-list view over PaperStore.

A pool manifest is a lightweight JSON file that tracks which papers belong
to a particular search session, without duplicating paper data (which lives
in PaperStore as the single source of truth).

Schema v1:
    {
        "schema_version": 1,
        "session_id": "lit-20260325-001",
        "created_at": "ISO 8601",
        "validated_goal": "research goal text",
        "search_params_summary": {"total_queries": 15, "filters": {}},
        "papers": [
            {"doi": "10.1234/...", "title": "...", "added_at": "ISO 8601", "in_store": true}
        ],
        "total_papers": 120,
        "store_path": "data/papers"
    }
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def create_manifest(
    session_id: str,
    papers: list[dict],
    store,
    goal: str = "",
    search_params_summary: Optional[dict] = None,
    store_path: str = "data/papers",
) -> dict:
    """Create a new pool manifest from a list of papers.

    Args:
        session_id: Unique session identifier (e.g., "lit-20260325-120000")
        papers: List of paper dicts, each must have "doi" and "title" keys
        store: PaperStore instance for in_store validation
        goal: Validated research goal text
        search_params_summary: Summary of search parameters used
        store_path: Path to the PaperStore data directory

    Returns:
        Pool manifest dict (schema_version 1)
    """
    now = datetime.now(timezone.utc).isoformat()
    manifest_papers = []
    orphan_count = 0

    for p in papers:
        doi = p.get("doi")
        if not doi:
            continue
        in_store = store.has_layer(doi, "L0")
        if not in_store:
            orphan_count += 1
        manifest_papers.append({
            "doi": doi,
            "title": p.get("title", ""),
            "added_at": p.get("added_at", now),
            "in_store": in_store,
        })

    if orphan_count > 0:
        print(f"[WARN] {orphan_count} orphan DOI(s) not found in PaperStore")

    return {
        "schema_version": 1,
        "session_id": session_id,
        "created_at": now,
        "validated_goal": goal,
        "search_params_summary": search_params_summary or {},
        "papers": manifest_papers,
        "total_papers": len(manifest_papers),
        "store_path": store_path,
    }


def load_manifest(path: str | Path) -> dict:
    """Load a pool manifest from file, auto-migrating v0 to v1 in memory.

    v0 detection: no "schema_version" key, papers contain full OpenAlex metadata.
    Migration is read-only — the original file is not modified.

    Args:
        path: Path to the manifest JSON file

    Returns:
        Pool manifest dict (always schema_version 1)
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("schema_version"):
        return data

    # v0 → v1 migration (in-memory only)
    now = datetime.now(timezone.utc).isoformat()
    v0_papers = data.get("papers", [])
    migrated_papers = []
    for p in v0_papers:
        doi = p.get("doi")
        if not doi:
            continue
        migrated_papers.append({
            "doi": doi,
            "title": p.get("title", ""),
            "added_at": p.get("added_at", now),
            "in_store": False,  # unknown without store; validate_manifest can update
        })

    return {
        "schema_version": 1,
        "session_id": data.get("session_id", "unknown"),
        "created_at": data.get("created_at", now),
        "validated_goal": data.get("validated_goal", ""),
        "search_params_summary": data.get("search_params_summary", {}),
        "papers": migrated_papers,
        "total_papers": len(migrated_papers),
        "store_path": data.get("store_path", "data/papers"),
    }


def validate_manifest(manifest: dict, store) -> dict:
    """Validate a pool manifest against a PaperStore.

    Checks each DOI's presence in the store and updates in_store flags.

    Args:
        manifest: Pool manifest dict
        store: PaperStore instance

    Returns:
        Dict with validation results:
            orphans: list of DOIs not in store
            total: total papers in manifest
            valid: count of papers found in store
    """
    orphans = []
    valid_count = 0

    for paper in manifest.get("papers", []):
        doi = paper.get("doi")
        if not doi:
            continue
        in_store = store.has_layer(doi, "L0")
        paper["in_store"] = in_store
        if in_store:
            valid_count += 1
        else:
            orphans.append(doi)

    if orphans:
        print(f"[WARN] {len(orphans)} orphan DOI(s): {', '.join(orphans[:5])}"
              + (f" ... and {len(orphans) - 5} more" if len(orphans) > 5 else ""))

    return {
        "orphans": orphans,
        "total": len(manifest.get("papers", [])),
        "valid": valid_count,
    }


def save_manifest(manifest: dict, path: str | Path) -> Path:
    """Save a pool manifest to file.

    Args:
        manifest: Pool manifest dict
        path: Output file path

    Returns:
        Path to saved file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return path


def merge_manifests(
    manifests: list[dict],
    store=None,
    strict: bool = False,
) -> dict:
    """Merge multiple pool manifests with DOI-based dedup.

    For duplicate DOIs across manifests, provenance arrays are concatenated
    (sorted by timestamp ascending). The first occurrence's metadata is kept.

    Args:
        manifests: List of pool manifest dicts (schema_version 1)
        store: Optional PaperStore for orphan validation
        strict: If True, raise ValueError on orphan DOIs

    Returns:
        Merged pool manifest dict

    Raises:
        ValueError: If strict=True and orphan DOIs are found
    """
    seen_dois = {}  # doi -> paper entry
    session_ids = set()
    dedup_count = 0

    for manifest in manifests:
        session_id = manifest.get("session_id", "unknown")
        session_ids.add(session_id)

        for paper in manifest.get("papers", []):
            doi = paper.get("doi")
            if not doi:
                continue
            if doi in seen_dois:
                dedup_count += 1
            else:
                seen_dois[doi] = {
                    "doi": doi,
                    "title": paper.get("title", ""),
                    "added_at": paper.get("added_at", ""),
                    "in_store": paper.get("in_store", False),
                }

    merged_papers = list(seen_dois.values())

    now = datetime.now(timezone.utc).isoformat()
    merged = {
        "schema_version": 1,
        "session_id": "merged-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"),
        "created_at": now,
        "validated_goal": "",
        "search_params_summary": {
            "merged_sessions": sorted(session_ids),
            "total_source_manifests": len(manifests),
        },
        "papers": merged_papers,
        "total_papers": len(merged_papers),
        "store_path": manifests[0].get("store_path", "data/papers") if manifests else "data/papers",
    }

    # Validate against store if provided
    orphan_count = 0
    if store:
        result = validate_manifest(merged, store)
        orphan_count = len(result["orphans"])
        if strict and orphan_count > 0:
            raise ValueError(
                f"Strict mode: {orphan_count} orphan DOI(s) found: "
                + ", ".join(result["orphans"][:5])
            )

    # Print statistics
    print(f"Merge statistics:")
    print(f"  Total papers: {len(merged_papers)}")
    print(f"  Duplicates removed: {dedup_count}")
    print(f"  Sessions: {len(session_ids)} ({', '.join(sorted(session_ids))})")
    if store:
        print(f"  Orphan DOIs: {orphan_count}")

    return merged
