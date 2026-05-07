"""
Zotero storage → PaperStore sync adapter.

Reads Zotero's local SQLite database (read-only) to find PDF attachments
keyed by DOI, then copies/symlinks them into paper-pipeline's PaperStore
under content/source.pdf with a minimal Zotero-derived L0 if none exists.

CLI:
    paper-pipeline zotero-sync
    paper-pipeline zotero-sync --collection my-papers
    paper-pipeline zotero-sync --doi 10.1038/xxx --symlink

Python:
    from paper_pipeline.store import PaperStore
    from paper_pipeline.zotero_sync import ZoteroSync
    sync = ZoteroSync(zotero_dir="~/Zotero", store=PaperStore("data/papers"))
    stats = sync.sync_all()

Design notes:
- Read-only sqlite open to coexist with a running Zotero instance.
- DOI is the join key. Items without DOI are skipped (paper-pipeline indexes by DOI).
- Existing L0 is never overwritten — Zotero stub only fills the gap.
- Existing source.pdf is skipped unless --force.
"""

import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from paper_pipeline.store import PaperStore
from paper_pipeline.utils import clean_doi


WANTED_FIELDS = ("DOI", "title", "date", "publicationTitle", "abstractNote")


@dataclass
class ZoteroAttachment:
    doi: str
    pdf_path: Path
    title: str = ""
    year: Optional[int] = None
    publication: str = ""
    abstract: str = ""
    parent_key: str = ""


class ZoteroSync:
    def __init__(
        self,
        zotero_dir: str | Path = "~/Zotero",
        store: Optional[PaperStore] = None,
        copy_mode: str = "copy",
    ):
        self.zotero_dir = Path(zotero_dir).expanduser()
        self.db_path = self.zotero_dir / "zotero.sqlite"
        self.storage_dir = self.zotero_dir / "storage"
        self.store = store
        if copy_mode not in ("copy", "symlink"):
            raise ValueError(f"copy_mode must be 'copy' or 'symlink', got {copy_mode!r}")
        self.copy_mode = copy_mode

        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Zotero database not found at {self.db_path}. "
                f"Install Zotero or pass --zotero <path>."
            )

    def iter_pdf_attachments(self) -> Iterator[ZoteroAttachment]:
        """Yield PDF attachments whose parent item has a DOI."""
        # mode=ro is the polite way to coexist with a running Zotero.
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            conn.row_factory = sqlite3.Row

            field_map = {
                row["fieldName"]: row["fieldID"]
                for row in conn.execute("SELECT fieldID, fieldName FROM fields")
            }

            sql = """
                SELECT
                    att.itemID  AS att_id,
                    att_item.key AS att_key,
                    att.path    AS att_path,
                    parent.itemID AS parent_id,
                    parent.key  AS parent_key
                FROM itemAttachments att
                JOIN items att_item ON att.itemID = att_item.itemID
                JOIN items parent   ON att.parentItemID = parent.itemID
                WHERE att.contentType = 'application/pdf'
                  AND att.parentItemID IS NOT NULL
            """

            for row in conn.execute(sql):
                fields = self._fetch_item_fields(conn, row["parent_id"], field_map)
                doi_raw = (fields.get("DOI") or "").strip()
                if not doi_raw:
                    continue
                doi = clean_doi(doi_raw)
                if not doi:
                    continue

                pdf_path = self._resolve_pdf_path(row["att_path"], row["att_key"])
                if pdf_path is None or not pdf_path.exists():
                    continue

                yield ZoteroAttachment(
                    doi=doi,
                    pdf_path=pdf_path,
                    title=fields.get("title", "").strip(),
                    year=self._parse_year(fields.get("date")),
                    publication=fields.get("publicationTitle", "").strip(),
                    abstract=fields.get("abstractNote", "").strip(),
                    parent_key=row["parent_key"],
                )
        finally:
            conn.close()

    @staticmethod
    def _fetch_item_fields(
        conn: sqlite3.Connection, item_id: int, field_map: dict
    ) -> dict[str, str]:
        wanted_ids = [field_map[name] for name in WANTED_FIELDS if name in field_map]
        if not wanted_ids:
            return {}
        placeholders = ",".join("?" * len(wanted_ids))
        sql = f"""
            SELECT fields.fieldName AS name, idv.value AS value
            FROM itemData id_
            JOIN fields ON id_.fieldID = fields.fieldID
            JOIN itemDataValues idv ON id_.valueID = idv.valueID
            WHERE id_.itemID = ?
              AND id_.fieldID IN ({placeholders})
        """
        return {
            row["name"]: row["value"]
            for row in conn.execute(sql, (item_id, *wanted_ids))
        }

    def _resolve_pdf_path(self, att_path: Optional[str], att_key: str) -> Optional[Path]:
        if not att_path:
            return None
        if att_path.startswith("storage:"):
            return self.storage_dir / att_key / att_path[len("storage:"):]
        p = Path(att_path).expanduser()
        return p if p.is_absolute() else None

    @staticmethod
    def _parse_year(date_str: Optional[str]) -> Optional[int]:
        if not date_str:
            return None
        for token in date_str.replace("/", "-").split():
            head = token[:4]
            if head.isdigit():
                return int(head)
        return None

    def sync_one(
        self, att: ZoteroAttachment, skip_existing_pdf: bool = True
    ) -> dict:
        if self.store is None:
            raise RuntimeError("ZoteroSync.store is not set")

        paper_dir = self.store.get_paper_dir(att.doi)
        content_dir = paper_dir / "content"
        content_dir.mkdir(parents=True, exist_ok=True)
        target = content_dir / "source.pdf"

        if target.exists() and skip_existing_pdf:
            pdf_action = "skipped"
        else:
            if target.exists() or target.is_symlink():
                target.unlink()
            if self.copy_mode == "symlink":
                target.symlink_to(att.pdf_path.resolve())
            else:
                shutil.copy2(att.pdf_path, target)
            pdf_action = self.copy_mode

        l0_written = False
        if not self.store.has_layer(att.doi, "L0"):
            l0 = {
                "doi": att.doi,
                "title": att.title,
                "publication_year": att.year,
                "journal": att.publication,
                "abstract": att.abstract,
                "openalex_id": "",
                "oa_status": None,
                "_source": "zotero",
                "_zotero_key": att.parent_key,
            }
            now = datetime.now(timezone.utc)
            provenance = {
                "session_id": f"zotero-{now.strftime('%Y%m%d-%H%M%S')}",
                "timestamp": now.isoformat(),
                "source": "zotero_sync",
                "search_params": {},
                "cluster_id": None,
                "seed_source": None,
            }
            self.store.save_layer(att.doi, "L0", l0, provenance_entry=provenance)
            l0_written = True

        self.store.update_content_info(
            att.doi, content_source="zotero", extraction_method="raw"
        )

        return {
            "doi": att.doi,
            "pdf_action": pdf_action,
            "pdf_size": target.stat().st_size if target.exists() else 0,
            "l0_written": l0_written,
        }

    def sync_all(
        self,
        doi_filter: Optional[set[str]] = None,
        skip_existing_pdf: bool = True,
    ) -> dict:
        results = []
        seen = set()
        for att in self.iter_pdf_attachments():
            if doi_filter is not None and att.doi not in doi_filter:
                continue
            if att.doi in seen:
                continue
            seen.add(att.doi)
            try:
                results.append(self.sync_one(att, skip_existing_pdf=skip_existing_pdf))
            except Exception as e:
                results.append({"doi": att.doi, "error": str(e)})

        return {
            "total_attachments_with_doi": len(seen),
            "synced": sum(1 for r in results if "error" not in r),
            "errors": sum(1 for r in results if "error" in r),
            "results": results,
        }
