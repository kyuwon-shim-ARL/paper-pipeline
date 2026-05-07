"""Tests for Zotero → PaperStore sync adapter."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from paper_pipeline.store import PaperStore
from paper_pipeline.zotero_sync import ZoteroSync


# Subset of Zotero schema sufficient for sync paths.
ZOTERO_SCHEMA_SQL = """
CREATE TABLE fields (
    fieldID INTEGER PRIMARY KEY,
    fieldName TEXT NOT NULL,
    fieldFormatID INTEGER
);

CREATE TABLE items (
    itemID INTEGER PRIMARY KEY,
    itemTypeID INTEGER,
    key TEXT
);

CREATE TABLE itemAttachments (
    itemID INTEGER PRIMARY KEY,
    parentItemID INTEGER,
    contentType TEXT,
    path TEXT
);

CREATE TABLE itemDataValues (
    valueID INTEGER PRIMARY KEY,
    value TEXT
);

CREATE TABLE itemData (
    itemID INTEGER,
    fieldID INTEGER,
    valueID INTEGER
);
"""


def _make_zotero_fixture(tmp_path: Path) -> Path:
    """Build a tiny Zotero library with two parent items.

    Item A: has DOI + valid attached PDF.
    Item B: has DOI but no PDF file on disk (path points to missing file).
    Item C: no DOI (must be skipped).
    """
    zdir = tmp_path / "Zotero"
    zdir.mkdir()
    storage = zdir / "storage"
    storage.mkdir()

    db = zdir / "zotero.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(ZOTERO_SCHEMA_SQL)

    # Fields
    fields = [
        (1, "DOI"),
        (2, "title"),
        (3, "date"),
        (4, "publicationTitle"),
        (5, "abstractNote"),
    ]
    conn.executemany("INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)", fields)

    # Parent items A, B, C
    conn.executemany(
        "INSERT INTO items(itemID, itemTypeID, key) VALUES (?, ?, ?)",
        [
            (1, 2, "AAAA1111"),  # parent A
            (2, 2, "BBBB2222"),  # parent B
            (3, 2, "CCCC3333"),  # parent C — no DOI
            (10, 14, "ATTA0001"),  # attachment for A
            (11, 14, "ATTB0002"),  # attachment for B (file missing)
            (12, 14, "ATTC0003"),  # attachment for C
        ],
    )

    # itemDataValues
    values = [
        (101, "10.1038/test-a"),
        (102, "Paper A"),
        (103, "2024-05-01"),
        (104, "Nature"),
        (105, "Abstract A"),
        (201, "10.1016/j.test-b"),
        (202, "Paper B"),
        (303, "2023"),  # C has no DOI, only date
        (304, "Paper C"),
    ]
    conn.executemany(
        "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)", values
    )

    # itemData: (itemID, fieldID, valueID)
    item_data = [
        (1, 1, 101), (1, 2, 102), (1, 3, 103), (1, 4, 104), (1, 5, 105),
        (2, 1, 201), (2, 2, 202),
        (3, 2, 304), (3, 3, 303),  # parent C: no DOI field
    ]
    conn.executemany(
        "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)", item_data
    )

    # Attachments
    conn.executemany(
        "INSERT INTO itemAttachments(itemID, parentItemID, contentType, path) "
        "VALUES (?, ?, ?, ?)",
        [
            (10, 1, "application/pdf", "storage:paper-a.pdf"),
            (11, 2, "application/pdf", "storage:paper-b.pdf"),  # file will be missing
            (12, 3, "application/pdf", "storage:paper-c.pdf"),
        ],
    )

    conn.commit()
    conn.close()

    # Drop a real PDF for attachment A only.
    a_dir = storage / "ATTA0001"
    a_dir.mkdir()
    (a_dir / "paper-a.pdf").write_bytes(b"%PDF-1.4\n%fake-pdf-content\n")

    # Attachment C also has its file present, but parent C has no DOI →
    # the sync should skip it. Drop file to confirm we don't accidentally read it.
    c_dir = storage / "ATTC0003"
    c_dir.mkdir()
    (c_dir / "paper-c.pdf").write_bytes(b"%PDF-1.4\n%c\n")

    return zdir


def test_iter_pdf_attachments_filters_correctly(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    sync = ZoteroSync(zotero_dir=zdir)

    atts = list(sync.iter_pdf_attachments())
    # Only paper A passes: B has missing PDF, C has no DOI.
    assert len(atts) == 1
    a = atts[0]
    assert a.doi == "10.1038/test-a"
    assert a.title == "Paper A"
    assert a.year == 2024
    assert a.publication == "Nature"
    assert a.abstract == "Abstract A"
    assert a.parent_key == "AAAA1111"
    assert a.pdf_path.exists()


def test_sync_all_writes_pdf_and_l0(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    store = PaperStore(str(tmp_path / "papers"))
    sync = ZoteroSync(zotero_dir=zdir, store=store)

    stats = sync.sync_all()

    assert stats["synced"] == 1
    assert stats["errors"] == 0
    assert stats["total_attachments_with_doi"] == 1

    # PDF present
    pdf_path = store.get_paper_dir("10.1038/test-a") / "content" / "source.pdf"
    assert pdf_path.exists()
    assert pdf_path.read_bytes().startswith(b"%PDF-")

    # L0 written from Zotero metadata
    l0 = store.load_layer("10.1038/test-a", "L0")
    assert l0["title"] == "Paper A"
    assert l0["publication_year"] == 2024
    assert l0["journal"] == "Nature"
    assert l0["_source"] == "zotero"
    assert l0["_zotero_key"] == "AAAA1111"
    # Provenance recorded
    assert any(p.get("source") == "zotero_sync" for p in l0.get("provenance", []))

    # Index marks content available
    entry = store.index["papers"]["10.1038/test-a"]
    assert entry["content_available"] is True
    assert entry["content_source"] == "zotero"


def test_sync_skips_existing_pdf_unless_force(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    store = PaperStore(str(tmp_path / "papers"))
    sync = ZoteroSync(zotero_dir=zdir, store=store)

    # First sync: action == "copy"
    first = sync.sync_all()
    assert first["results"][0]["pdf_action"] == "copy"

    # Second sync: should skip
    second = sync.sync_all()
    assert second["results"][0]["pdf_action"] == "skipped"

    # With force: copy again
    third = sync.sync_all(skip_existing_pdf=False)
    assert third["results"][0]["pdf_action"] == "copy"


def test_sync_does_not_overwrite_existing_l0(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    store = PaperStore(str(tmp_path / "papers"))

    # Pre-write a richer L0 (e.g. from OpenAlex)
    store.save_layer(
        "10.1038/test-a",
        "L0",
        {"title": "From OpenAlex", "openalex_id": "W123", "publication_year": 2024},
    )

    sync = ZoteroSync(zotero_dir=zdir, store=store)
    stats = sync.sync_all()
    assert stats["synced"] == 1
    assert stats["results"][0]["l0_written"] is False

    l0 = store.load_layer("10.1038/test-a", "L0")
    # Existing L0 preserved, not overwritten by Zotero stub
    assert l0["title"] == "From OpenAlex"
    assert l0["openalex_id"] == "W123"
    assert l0.get("_source") != "zotero"


def test_symlink_mode(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    store = PaperStore(str(tmp_path / "papers"))
    sync = ZoteroSync(zotero_dir=zdir, store=store, copy_mode="symlink")

    sync.sync_all()
    pdf_path = store.get_paper_dir("10.1038/test-a") / "content" / "source.pdf"
    assert pdf_path.is_symlink()


def test_doi_filter(tmp_path):
    zdir = _make_zotero_fixture(tmp_path)
    store = PaperStore(str(tmp_path / "papers"))
    sync = ZoteroSync(zotero_dir=zdir, store=store)

    stats = sync.sync_all(doi_filter={"10.1038/no-such-paper"})
    assert stats["synced"] == 0
    assert stats["total_attachments_with_doi"] == 0


def test_missing_zotero_db_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        ZoteroSync(zotero_dir=tmp_path / "no-zotero")
