"""
Paper Discovery module using PyAlex for OpenAlex search.

Replaces custom 246-line OpenAlexClient with PyAlex library.
PyAlex handles: search, filter, cursor pagination, abstract restoration, rate limiting.
"""

import sys
from collections import Counter

import pyalex
from pyalex import Works, Authors
from typing import Optional
from tqdm import tqdm


class PaperDiscovery:
    """PyAlex-based paper search with progress tracking.

    Usage:
        discovery = PaperDiscovery(email="your@email.com")
        papers = discovery.search("urban microbiome", max_results=50)
    """

    def __init__(self, email: str):
        """Initialize with email for OpenAlex polite pool (10 req/sec).

        Args:
            email: Email for polite pool access
        """
        pyalex.config.email = email
        pyalex.config.max_retries = 3
        pyalex.config.retry_backoff_factor = 0.1
        pyalex.config.retry_http_codes = [429, 500, 503]

    def search(
        self,
        query: str,
        max_results: int = 50,
        filters: Optional[dict] = None,
        sort_by: str = "cited_by_count",
        sort_order: str = "desc",
    ) -> list[dict]:
        """Search papers via PyAlex. Abstracts are automatically restored.

        Args:
            query: Search query string
            max_results: Maximum number of results to return
            filters: OpenAlex filter dict (e.g., {"publication_year": ">2020", "is_oa": True})
            sort_by: Sort field name
            sort_order: "desc" or "asc"

        Returns:
            List of normalized paper dicts
        """
        q = Works().search(query)
        if filters:
            q = q.filter(**filters)
        q = q.sort(**{sort_by: sort_order})

        results = []
        per_page = min(200, max_results)
        with tqdm(total=max_results, desc="Searching papers", unit="paper") as pbar:
            for page in q.paginate(per_page=per_page, n_max=max_results):
                for work in page:
                    results.append(self._normalize_work(work))
                    pbar.update(1)
                    if len(results) >= max_results:
                        break
                if len(results) >= max_results:
                    break

        return results[:max_results]

    def multi_search(
        self,
        queries: list[str],
        max_per_query: int = 500,
        filters: Optional[dict] = None,
        sort_by: str = "relevance_score",
        sort_order: str = "desc",
        hybrid: bool = False,
    ) -> list[dict]:
        """Search multiple queries and deduplicate by DOI.

        Args:
            queries: List of search query strings
            max_per_query: Max results per individual query
            filters: OpenAlex filters applied to all queries
            sort_by: Sort field (default relevance_score for discovery)
            sort_order: Sort order
            hybrid: If True, search each query twice (relevance_score + cited_by_count)
                    to capture both recent relevant papers and foundational literature

        Returns:
            Deduplicated list of normalized paper dicts
        """
        seen_dois: dict[str, bool] = {}
        seen_titles: dict[str, bool] = {}
        combined: list[dict] = []

        if hybrid:
            sort_strategies = ["relevance_score", "cited_by_count"]
        else:
            sort_strategies = [sort_by]

        total_queries = len(queries) * len(sort_strategies)
        step = 0

        for query in queries:
            for sort in sort_strategies:
                step += 1
                papers = self.search(
                    query,
                    max_results=max_per_query,
                    filters=filters,
                    sort_by=sort,
                    sort_order=sort_order,
                )
                new_count = 0
                for paper in papers:
                    doi = paper.get("doi", "").strip().lower()
                    title = (paper.get("title") or "").strip().lower()
                    if doi:
                        if doi in seen_dois:
                            continue
                        seen_dois[doi] = True
                    elif title:
                        if title in seen_titles:
                            continue
                        seen_titles[title] = True
                    else:
                        continue
                    combined.append(paper)
                    new_count += 1
                sort_label = f" [{sort}]" if hybrid else ""
                print(
                    f"Query {step}/{total_queries}: '{query}'{sort_label} -> {len(papers)} results ({new_count} new)",
                    file=sys.stderr,
                )

        return combined

    def expand_references(
        self,
        papers: list[dict],
        max_refs: int = 500,
    ) -> list[dict]:
        """Discover additional papers by following referenced_works.

        Collects all referenced_works OpenAlex IDs from input papers,
        removes IDs already in the input set,
        fetches metadata for the most-frequently-referenced new IDs.

        Args:
            papers: List of paper dicts (must have 'openalex_id' and 'referenced_works')
            max_refs: Maximum number of reference papers to fetch

        Returns:
            List of NEW paper dicts not in the input set
        """
        # Build set of known OpenAlex IDs
        known_ids: set[str] = set()
        for p in papers:
            oa_id = p.get("openalex_id", "")
            if oa_id:
                # Normalize: strip URL prefix if present
                clean_id = oa_id.replace("https://openalex.org/", "")
                known_ids.add(clean_id)

        # Collect all referenced work IDs and count frequency
        ref_counter: Counter = Counter()
        for p in papers:
            for ref_id in p.get("referenced_works", []):
                clean = ref_id.replace("https://openalex.org/", "")
                if clean and clean not in known_ids:
                    ref_counter[clean] += 1

        if not ref_counter:
            print("No new references found to expand.", file=sys.stderr)
            return []

        # Take the top max_refs most-frequently-referenced IDs
        top_refs = [rid for rid, _ in ref_counter.most_common(max_refs)]
        print(
            f"Expanding references: {len(ref_counter)} unique refs, fetching top {len(top_refs)}",
            file=sys.stderr,
        )

        # Fetch in batches of 50 (OpenAlex pipe-separated filter limit)
        results: list[dict] = []
        batch_size = 50
        with tqdm(total=len(top_refs), desc="Fetching references", unit="paper", file=sys.stderr) as pbar:
            for i in range(0, len(top_refs), batch_size):
                batch = top_refs[i : i + batch_size]
                id_filter = "|".join(batch)
                try:
                    for page in Works().filter(openalex_id=id_filter).paginate(
                        per_page=min(200, len(batch)), n_max=len(batch)
                    ):
                        for work in page:
                            results.append(self._normalize_work(work))
                            pbar.update(1)
                except Exception as e:
                    print(f"Reference batch fetch failed: {e}", file=sys.stderr)
                    pbar.update(len(batch))
                    continue

        return results

    def expand_citations(
        self,
        papers: list[dict],
        max_per_seed: int = 500,
        text_filter: str | None = None,
        year_min: int | None = None,
        year_max: int | None = None,
    ) -> list[dict]:
        """Discover papers that cite the input papers (forward citation expansion).

        Uses OpenAlex filter=cites:{oa_id} to find citing papers.
        Optionally filters by text search and year range.

        Args:
            papers: List of paper dicts (must have 'openalex_id')
            max_per_seed: Maximum citing papers to fetch per seed paper
            text_filter: Optional OpenAlex fulltext.search query (e.g., "we propose OR novel method")
            year_min: Minimum publication year filter
            year_max: Maximum publication year filter

        Returns:
            List of NEW paper dicts not in the input set
        """
        # Build set of known DOIs and OpenAlex IDs
        known_dois: set[str] = set()
        known_ids: set[str] = set()
        for p in papers:
            doi = p.get("doi", "").strip().lower()
            if doi:
                known_dois.add(doi)
            oa_id = p.get("openalex_id", "")
            if oa_id:
                # Normalize: strip URL prefix if present
                clean_id = oa_id.replace("https://openalex.org/", "")
                known_ids.add(clean_id)

        # Collect seed OpenAlex IDs (needs full URL format for cites filter)
        seed_ids: list[str] = []
        for p in papers:
            oa_id = p.get("openalex_id", "")
            if oa_id:
                # Ensure full URL format for cites filter
                if not oa_id.startswith("https://openalex.org/"):
                    oa_id = f"https://openalex.org/{oa_id}"
                seed_ids.append(oa_id)

        if not seed_ids:
            print("No papers with openalex_id found to expand citations.", file=sys.stderr)
            return []

        print(
            f"Expanding citations from {len(seed_ids)} seed papers (max {max_per_seed} per seed)",
            file=sys.stderr,
        )

        results: list[dict] = []
        with tqdm(total=len(seed_ids), desc="Fetching citations", unit="seed", file=sys.stderr) as pbar:
            for seed_id in seed_ids:
                try:
                    # Build query with cites filter
                    q = Works().filter(cites=seed_id)

                    # Add text filter if provided
                    if text_filter:
                        q = q.search(text_filter)

                    # Add year filters if provided
                    if year_min is not None or year_max is not None:
                        year_filter = []
                        if year_min is not None:
                            year_filter.append(f">{year_min - 1}")
                        if year_max is not None:
                            year_filter.append(f"<{year_max + 1}")
                        if year_filter:
                            q = q.filter(publication_year=",".join(year_filter))

                    # Paginate and collect results
                    for page in q.paginate(per_page=200, n_max=max_per_seed):
                        for work in page:
                            # Dedup by DOI
                            doi = (work.get("doi") or "").replace("https://doi.org/", "").replace("http://doi.org/", "").strip().lower()
                            oa_id = (work.get("id") or "").replace("https://openalex.org/", "")

                            if doi and doi in known_dois:
                                continue
                            if oa_id and oa_id in known_ids:
                                continue

                            if doi:
                                known_dois.add(doi)
                            if oa_id:
                                known_ids.add(oa_id)

                            results.append(self._normalize_work(work))

                except Exception as e:
                    print(f"Citation fetch failed for {seed_id}: {e}", file=sys.stderr)
                finally:
                    pbar.update(1)

        print(f"Found {len(results)} new citing papers", file=sys.stderr)
        return results

    def search_by_doi(self, doi: str) -> Optional[dict]:
        """Search single paper by DOI.

        Args:
            doi: DOI string (with or without https://doi.org/ prefix)

        Returns:
            Normalized paper dict or None
        """
        clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        try:
            work = Works()[f"https://doi.org/{clean_doi}"]
            if work:
                return self._normalize_work(work)
        except Exception:
            pass
        return None

    def search_by_dois(self, dois: list[str], batch_size: int = 30) -> list[dict]:
        """Batch search by DOIs. Splits into batches to avoid URL length limits.

        Args:
            dois: List of DOI strings
            batch_size: Number of DOIs per batch (max 30 to avoid URL limit)

        Returns:
            List of normalized paper dicts
        """
        results = []
        for i in tqdm(range(0, len(dois), batch_size), desc="Fetching DOIs", unit="batch"):
            batch = dois[i : i + batch_size]
            doi_filter = "|".join(
                d.replace("https://doi.org/", "").replace("http://doi.org/", "")
                for d in batch
            )
            try:
                for page in Works().filter(doi=doi_filter).paginate(
                    per_page=batch_size, n_max=batch_size
                ):
                    for work in page:
                        results.append(self._normalize_work(work))
            except Exception as e:
                print(f"Batch fetch failed: {e}")
                continue
        return results

    def search_by_topic(self, topic_id: str, max_results: int = 50, **filters) -> list[dict]:
        """Search papers by OpenAlex topic ID.

        Args:
            topic_id: OpenAlex topic ID (e.g., "T12345")
            max_results: Maximum results
            **filters: Additional OpenAlex filters

        Returns:
            List of normalized paper dicts
        """
        q = Works().filter(topics={"id": topic_id}, **filters)
        q = q.sort(cited_by_count="desc")

        results = []
        for page in q.paginate(per_page=min(200, max_results), n_max=max_results):
            for work in page:
                results.append(self._normalize_work(work))
        return results[:max_results]

    def get_oa_pdf_url(self, work: dict) -> Optional[str]:
        """Extract OA PDF URL from work data with fallback chain.

        Priority: primary_location → open_access.oa_url → locations

        Args:
            work: Raw PyAlex work dict

        Returns:
            PDF URL string or None
        """
        # 1. primary_location pdf_url
        primary = work.get("primary_location") or {}
        if primary.get("pdf_url"):
            return primary["pdf_url"]

        # 2. open_access.oa_url
        oa = work.get("open_access") or {}
        if oa.get("oa_url"):
            return oa["oa_url"]

        # 3. Scan all locations
        for loc in work.get("locations") or []:
            if loc.get("pdf_url"):
                return loc["pdf_url"]

        return None

    @staticmethod
    def _reconstruct_abstract(inverted_index: dict | None) -> str | None:
        """Reconstruct abstract text from OpenAlex inverted index format.

        Args:
            inverted_index: Dict mapping words to position lists,
                e.g. {"This": [0], "study": [1, 48]}

        Returns:
            Reconstructed abstract string, or None if input is empty/None
        """
        if not inverted_index:
            return None
        word_positions: list[tuple[int, str]] = []
        for word, positions in inverted_index.items():
            for pos in positions:
                word_positions.append((pos, word))
        word_positions.sort(key=lambda x: x[0])
        return " ".join(w for _, w in word_positions) if word_positions else None

    def _normalize_work(self, work: dict) -> dict:
        """Convert PyAlex work to internal schema.

        Reconstructs abstract from OpenAlex abstract_inverted_index.

        Args:
            work: Raw PyAlex work dict

        Returns:
            Normalized paper dict
        """
        doi_raw = work.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "").replace("http://doi.org/", "")

        primary_loc = work.get("primary_location") or {}
        source = primary_loc.get("source") or {}

        # Try direct abstract first, fall back to inverted index reconstruction
        abstract = work.get("abstract") or self._reconstruct_abstract(
            work.get("abstract_inverted_index")
        )

        return {
            "doi": doi,
            "openalex_id": work.get("id", ""),
            "title": work.get("title", ""),
            "abstract": abstract,
            "publication_year": work.get("publication_year"),
            "publication_date": work.get("publication_date"),
            "journal": source.get("display_name"),
            "cited_by_count": work.get("cited_by_count", 0),
            "is_oa": (work.get("open_access") or {}).get("is_oa", False),
            "oa_status": (work.get("open_access") or {}).get("oa_status"),
            "oa_url": (work.get("open_access") or {}).get("oa_url"),
            "pdf_url": self.get_oa_pdf_url(work),
            "authorships": work.get("authorships", []),
            "topics": [t.get("display_name") for t in (work.get("topics") or [])[:5]],
            "type": work.get("type"),
            "referenced_works": [
                url.replace("https://openalex.org/", "")
                for url in (work.get("referenced_works") or [])
            ],
        }
