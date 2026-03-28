"""Tests for public API surface (__init__.py) and structural invariants."""

import ast
import importlib


def test_all_exports_importable():
    """Verify all __all__ symbols can be imported."""
    import paper_pipeline

    expected = {
        "PaperDiscovery",
        "PaperFetcher",
        "PaperExtractor",
        "PaperStore",
        "ContentResult",
        "ExtractionResult",
    }
    assert set(paper_pipeline.__all__) == expected

    for name in paper_pipeline.__all__:
        obj = getattr(paper_pipeline, name)
        assert obj is not None, f"{name} is None"


def test_no_store_import_in_fetcher():
    """Verify fetcher.py does not import store.py (structural decoupling)."""
    import pathlib

    fetcher_path = pathlib.Path(__file__).parent.parent / "src" / "paper_pipeline" / "fetcher.py"
    with open(fetcher_path) as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "store" in node.module:
            assert False, f"fetcher.py imports store at line {node.lineno}"
        if isinstance(node, ast.Import):
            for alias in node.names:
                if "store" in alias.name:
                    assert False, f"fetcher.py imports store at line {node.lineno}"


def test_version_exists():
    """Verify __version__ is defined."""
    import paper_pipeline

    assert hasattr(paper_pipeline, "__version__")
    assert isinstance(paper_pipeline.__version__, str)
