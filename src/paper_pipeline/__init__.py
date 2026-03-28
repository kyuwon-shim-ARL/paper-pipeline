"""Paper Pipeline - OpenAlex paper search, acquisition, analysis, and storage."""

__version__ = "0.2.0"

from paper_pipeline.discovery import PaperDiscovery
from paper_pipeline.extractor import ExtractionResult, PaperExtractor
from paper_pipeline.fetcher import ContentResult, PaperFetcher
from paper_pipeline.store import PaperStore

__all__ = [
    "PaperDiscovery",
    "PaperFetcher",
    "PaperExtractor",
    "PaperStore",
    "ContentResult",
    "ExtractionResult",
]
