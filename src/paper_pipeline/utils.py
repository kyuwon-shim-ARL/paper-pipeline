"""Shared utilities for paper-pipeline."""


def clean_doi(doi: str) -> str:
    """Remove URL prefix and strip whitespace from DOI.

    Args:
        doi: DOI string, possibly with https://doi.org/ prefix

    Returns:
        Clean DOI string

    Example:
        >>> clean_doi("https://doi.org/10.1038/s41586-024-07345-x")
        '10.1038/s41586-024-07345-x'
    """
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return doi.strip().rstrip("/")
