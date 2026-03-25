"""Tests for expand_citations() forward citation expansion."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from paper_pipeline.discovery import PaperDiscovery


@pytest.fixture
def discovery():
    """Create PaperDiscovery instance with mock email."""
    return PaperDiscovery(email="test@example.com")


@pytest.fixture
def seed_papers():
    """Sample seed papers with OpenAlex IDs."""
    return [
        {
            "doi": "10.1234/seed1",
            "openalex_id": "https://openalex.org/W1234",
            "title": "Seed Paper 1",
        },
        {
            "doi": "10.1234/seed2",
            "openalex_id": "W5678",  # Without URL prefix
            "title": "Seed Paper 2",
        },
    ]


def test_expand_citations_basic(discovery, seed_papers):
    """Test basic citation expansion without filters."""
    mock_work = {
        "id": "https://openalex.org/W9999",
        "doi": "https://doi.org/10.1234/citing1",
        "title": "Citing Paper 1",
        "publication_year": 2023,
        "cited_by_count": 10,
        "open_access": {"is_oa": True},
        "topics": [],
        "referenced_works": [],
    }

    with patch("paper_pipeline.discovery.Works") as MockWorks:
        # Setup mock chain
        mock_paginate = MagicMock()
        mock_paginate.paginate.return_value = [[mock_work]]
        mock_filter = MagicMock(return_value=mock_paginate)
        MockWorks.return_value.filter = mock_filter

        results = discovery.expand_citations(seed_papers, max_per_seed=500)

        # Verify filter was called with correct cites parameter
        assert mock_filter.call_count == 2  # Two seed papers
        # First call should use full URL
        first_call_kwargs = mock_filter.call_args_list[0][1]
        assert "cites" in first_call_kwargs
        assert first_call_kwargs["cites"].startswith("https://openalex.org/")

        assert len(results) >= 1
        assert results[0]["doi"] == "10.1234/citing1"


def test_expand_citations_deduplication(discovery, seed_papers):
    """Test that duplicate papers are filtered out."""
    # Create a citing paper that's already in seeds
    duplicate_work = {
        "id": "https://openalex.org/W1234",  # Same as seed1
        "doi": "https://doi.org/10.1234/seed1",
        "title": "Seed Paper 1",
        "publication_year": 2020,
        "cited_by_count": 50,
        "open_access": {"is_oa": True},
        "topics": [],
        "referenced_works": [],
    }

    new_work = {
        "id": "https://openalex.org/W9999",
        "doi": "https://doi.org/10.1234/new1",
        "title": "New Citing Paper",
        "publication_year": 2023,
        "cited_by_count": 5,
        "open_access": {"is_oa": True},
        "topics": [],
        "referenced_works": [],
    }

    with patch("paper_pipeline.discovery.Works") as MockWorks:
        mock_paginate = MagicMock()
        mock_paginate.paginate.return_value = [[duplicate_work, new_work]]
        mock_filter = MagicMock(return_value=mock_paginate)
        MockWorks.return_value.filter = mock_filter

        results = discovery.expand_citations(seed_papers, max_per_seed=500)

        # Only the new paper should be in results
        assert len(results) == 1
        assert results[0]["doi"] == "10.1234/new1"


def test_expand_citations_with_text_filter(discovery, seed_papers):
    """Test citation expansion with text filter."""
    mock_work = {
        "id": "https://openalex.org/W9999",
        "doi": "https://doi.org/10.1234/citing1",
        "title": "Citing Paper with Novel Method",
        "publication_year": 2023,
        "cited_by_count": 10,
        "open_access": {"is_oa": True},
        "topics": [],
        "referenced_works": [],
    }

    with patch("paper_pipeline.discovery.Works") as MockWorks:
        # Setup mock chain with search method
        mock_paginate = MagicMock()
        mock_paginate.paginate.return_value = [[mock_work]]
        mock_search = MagicMock(return_value=mock_paginate)
        mock_filter_result = MagicMock()
        mock_filter_result.search = mock_search
        mock_filter = MagicMock(return_value=mock_filter_result)
        MockWorks.return_value.filter = mock_filter

        results = discovery.expand_citations(
            seed_papers,
            max_per_seed=500,
            text_filter="we propose OR novel method",
        )

        # Verify search was called with text filter
        assert mock_search.called
        assert mock_search.call_args[0][0] == "we propose OR novel method"
        assert len(results) >= 1


def test_expand_citations_with_year_filters(discovery, seed_papers):
    """Test citation expansion with year range filters."""
    mock_work = {
        "id": "https://openalex.org/W9999",
        "doi": "https://doi.org/10.1234/citing1",
        "title": "Recent Citing Paper",
        "publication_year": 2023,
        "cited_by_count": 10,
        "open_access": {"is_oa": True},
        "topics": [],
        "referenced_works": [],
    }

    with patch("paper_pipeline.discovery.Works") as MockWorks:
        # Setup mock chain: filter(cites=...) returns object that has filter(publication_year=...)
        mock_paginate = MagicMock()
        mock_paginate.paginate.return_value = [[mock_work]]

        # Second filter call (for year) returns paginate
        mock_year_filter_result = MagicMock()
        mock_year_filter_result.filter.return_value = mock_paginate

        # First filter call (for cites) returns object with filter method
        mock_cites_filter = MagicMock(return_value=mock_year_filter_result)
        MockWorks.return_value.filter = mock_cites_filter

        results = discovery.expand_citations(
            seed_papers,
            max_per_seed=500,
            year_min=2020,
            year_max=2024,
        )

        # Verify both filter calls were made
        assert mock_cites_filter.called
        # Verify year filter was applied
        assert mock_year_filter_result.filter.called
        year_filter_calls = mock_year_filter_result.filter.call_args_list
        assert any("publication_year" in call[1] for call in year_filter_calls)


def test_expand_citations_no_openalex_ids(discovery):
    """Test handling papers without OpenAlex IDs."""
    papers_no_ids = [
        {"doi": "10.1234/paper1", "title": "Paper Without OA ID"},
        {"doi": "10.1234/paper2", "title": "Another Paper"},
    ]

    results = discovery.expand_citations(papers_no_ids, max_per_seed=500)

    # Should return empty list with warning message
    assert results == []


def test_expand_citations_exception_handling(discovery, seed_papers):
    """Test that exceptions during fetch are handled gracefully."""
    with patch("paper_pipeline.discovery.Works") as MockWorks:
        mock_filter = MagicMock(side_effect=Exception("API Error"))
        MockWorks.return_value.filter = mock_filter

        # Should not raise exception, just continue
        results = discovery.expand_citations(seed_papers, max_per_seed=500)

        # Should return empty or partial results
        assert isinstance(results, list)
