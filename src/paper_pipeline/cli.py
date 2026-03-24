"""
Paper Pipeline CLI - Batch operations for paper search, fetch, and management.

Usage:
    paper-pipeline search "urban microbiome" --max 50 --oa-only
    paper-pipeline fetch --collection metasub --email user@example.com
    paper-pipeline status
    paper-pipeline collection create metasub --dois "10.1038/xxx" "10.1016/yyy"
    paper-pipeline grobid-status
    paper-pipeline ask "What are the main AMR patterns?" --collection metasub
"""

import argparse
import json
import sys
from pathlib import Path

from paper_pipeline.discovery import PaperDiscovery
from paper_pipeline.fetcher import PaperFetcher, ContentResult
from paper_pipeline.extractor import PaperExtractor
from paper_pipeline.store import PaperStore


def cmd_search(args):
    """Search for papers on OpenAlex."""
    discovery = PaperDiscovery(email=args.email)

    filters = {}
    if args.oa_only:
        filters["is_oa"] = True
    if args.year_from:
        filters["publication_year"] = f">{args.year_from - 1}"

    papers = discovery.search(
        args.query,
        max_results=args.max,
        filters=filters if filters else None,
        sort_by=args.sort,
    )

    store = PaperStore(args.data_dir)

    saved = 0
    for paper in papers:
        if paper.get("doi"):
            store.save_layer(paper["doi"], "L0", paper)
            saved += 1

    print(f"\nFound {len(papers)} papers, saved {saved} with DOI as L0")
    print(f"Store: {store.get_stats()}")

    if args.collection:
        dois = [p["doi"] for p in papers if p.get("doi")]
        store.create_collection(args.collection, dois)
        print(f"Created collection '{args.collection}' with {len(dois)} papers")


def cmd_fetch(args):
    """Fetch full-text content for papers."""
    store = PaperStore(args.data_dir)
    fetcher = PaperFetcher(email=args.email, pdf_dir=args.data_dir)
    extractor = PaperExtractor()

    # Get DOIs to fetch
    if args.collection:
        dois = store.get_collection(args.collection)
        if not dois:
            print(f"Collection '{args.collection}' not found")
            return
    elif args.doi:
        dois = [args.doi]
    else:
        # Fetch all papers without content
        papers = store.list_papers({"has_layer": "L0"})
        dois = [
            doi for doi, entry in store.index.get("papers", {}).items()
            if not entry.get("content_available")
        ]

    print(f"Fetching content for {len(dois)} papers...")

    for i, doi in enumerate(dois, 1):
        print(f"[{i}/{len(dois)}] {doi}... ", end="", flush=True)

        # Load L0 for work_data
        l0 = store.load_layer(doi, "L0")

        result = fetcher.fetch_content(doi, work_data=l0)
        print(f"{result.source} ({result.content_type})")

        # Save raw content
        if result.content_type == "pmc_xml" and result.data:
            store.save_content(doi, "fulltext", result.data)
        elif result.content_type == "abstract_only" and result.data:
            store.save_content(doi, "abstract", result.data)

        # Extract text
        if result.content_type in ("pmc_xml", "pdf"):
            extraction = extractor.extract(result, pdf_path=result.pdf_path)
            if extraction.full_text:
                store.save_content(doi, "fulltext", extraction.full_text)
            if extraction.sections:
                store.save_layer(doi, "L2", {
                    "sections": extraction.sections,
                    "extraction_method": extraction.extraction_method,
                    "tables": extraction.tables,
                    "figure_captions": extraction.figure_captions,
                })
            # Save GROBID TEI if available
            if extraction.extraction_method == "grobid" and result.pdf_path:
                tei_path = Path(result.pdf_path).parent / "grobid.tei.xml"
                if tei_path.exists():
                    store.save_content(doi, "grobid_tei", tei_path.read_text())

            store.update_content_info(doi, result.source, extraction.extraction_method)

    print(f"\nFetch complete. Stats: {json.dumps(fetcher.get_stats(), indent=2)}")


def cmd_status(args):
    """Show store status."""
    store = PaperStore(args.data_dir)
    stats = store.get_stats()

    print(f"Paper Pipeline Store Status")
    print(f"{'='*40}")
    print(f"Total papers: {stats['total_papers']}")
    print(f"Content available: {stats['content_available']}")
    print(f"\nLayer counts:")
    for layer, count in stats["layer_counts"].items():
        print(f"  {layer}: {count}")
    print(f"\nCollections: {', '.join(stats['collections']) if stats['collections'] else '(none)'}")


def cmd_collection(args):
    """Manage collections."""
    store = PaperStore(args.data_dir)

    if args.action == "create":
        if not args.name or not args.dois:
            print("Usage: collection create <name> --dois <doi1> <doi2> ...")
            return
        path = store.create_collection(args.name, args.dois)
        print(f"Created collection '{args.name}' at {path}")

    elif args.action == "list":
        collections = store.list_collections()
        if collections:
            for name in collections:
                dois = store.get_collection(name)
                print(f"  {name}: {len(dois) if dois else 0} papers")
        else:
            print("No collections found")

    elif args.action == "show":
        if not args.name:
            print("Usage: collection show <name>")
            return
        dois = store.get_collection(args.name)
        if dois:
            print(f"Collection '{args.name}': {len(dois)} papers")
            for doi in dois:
                l0 = store.load_layer(doi, "L0")
                title = l0.get("title", "Unknown") if l0 else "No L0"
                print(f"  - {doi}: {title[:60]}")
        else:
            print(f"Collection '{args.name}' not found")


def cmd_grobid_status(args):
    """Check GROBID service status."""
    extractor = PaperExtractor()
    if extractor.grobid_available:
        print(f"GROBID is running at {extractor.grobid_url}")
    else:
        print(f"GROBID is NOT available at {extractor.grobid_url}")
        print("Start with: docker run --rm --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.2-crf")


def cmd_ask(args):
    """Answer questions using paper-qa2 over collected papers."""
    try:
        from paperqa import Docs
    except ImportError:
        print("paper-qa2 not installed. Run: uv pip install 'paper-pipeline[qa]'")
        return

    store = PaperStore(args.data_dir)

    # Get DOIs to query
    if args.collection:
        dois = store.get_collection(args.collection)
        if not dois:
            print(f"Collection '{args.collection}' not found")
            return
        print(f"Using {len(dois)} papers from collection '{args.collection}'")
    else:
        # Use all papers with content
        papers = store.list_papers({"has_layer": "L0"})
        dois = [
            doi for doi, entry in store.index.get("papers", {}).items()
            if entry.get("content_available")
        ]
        print(f"Using {len(dois)} papers with content from store")

    if not dois:
        print("No papers with content found. Run 'fetch' first.")
        return

    # Collect PDF paths
    pdf_paths = []
    for doi in dois:
        content_dir = store.get_paper_dir(doi) / "content"
        pdf_path = content_dir / "source.pdf"
        if pdf_path.exists():
            pdf_paths.append(str(pdf_path))

    print(f"Found {len(pdf_paths)} PDFs to index")

    if not pdf_paths:
        print("No PDFs found in store. paper-qa2 requires PDF files.")
        return

    # Initialize paper-qa2
    print("\nIndexing papers (this may take a while)...")
    docs = Docs()

    for i, pdf_path in enumerate(pdf_paths, 1):
        print(f"[{i}/{len(pdf_paths)}] Indexing {Path(pdf_path).parent.parent.name}...")
        docs.add(pdf_path)

    # Query
    print(f"\nQuerying: {args.question}")
    answer = docs.query(args.question)

    # Display results
    print("\n" + "=" * 60)
    print("ANSWER")
    print("=" * 60)
    print(answer.answer)
    print("\n" + "=" * 60)
    print("REFERENCES")
    print("=" * 60)
    for i, ref in enumerate(answer.references, 1):
        print(f"\n[{i}] {ref}")
    print("\n" + "=" * 60)
    print(f"Confidence: {answer.confidence if hasattr(answer, 'confidence') else 'N/A'}")
    print("=" * 60)


def cmd_sweep(args):
    """Comprehensive search: multi-query + citation expansion + dedup."""
    discovery = PaperDiscovery(email=args.email)

    filters = {}
    if args.oa_only:
        filters["is_oa"] = True
    if args.year_from:
        filters["publication_year"] = f">{args.year_from - 1}"

    # Step 1: Multi-query search (hybrid by default: relevance + citation sort)
    queries = [args.query] + (args.synonyms or [])
    papers = discovery.multi_search(
        queries,
        max_per_query=args.max_per_query,
        filters=filters if filters else None,
        sort_by=args.sort,
        hybrid=not args.no_hybrid,
    )
    print(f"\nStep 1: Multi-query search -> {len(papers)} unique papers")

    # Step 2: Citation expansion (optional)
    if args.expand_refs:
        new_papers = discovery.expand_references(papers, max_refs=args.max_refs)
        papers.extend(new_papers)
        print(f"Step 2: Citation expansion -> +{len(new_papers)} papers ({len(papers)} total)")

    # Step 3: Save to store
    store = PaperStore(args.data_dir)
    saved = 0
    for paper in papers:
        if paper.get("doi"):
            store.save_layer(paper["doi"], "L0", paper)
            saved += 1
    print(f"\nSaved {saved} papers with DOI to store")

    # Step 4: Create collection
    if args.collection:
        dois = [p["doi"] for p in papers if p.get("doi")]
        store.create_collection(args.collection, dois)
        print(f"Created collection '{args.collection}' with {len(dois)} papers")

    # Step 5: Export for PaperSift
    if args.export:
        export_data = []
        for p in papers:
            if p.get("doi"):
                export_data.append({
                    "doi": p["doi"],
                    "title": p.get("title", ""),
                    "year": p.get("publication_year"),
                    "topics": p.get("topics", []),
                    "cited_by_count": p.get("cited_by_count", 0),
                    "referenced_works": p.get("referenced_works", []),
                })
        export_path = Path(args.export)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        print(f"Exported {len(export_data)} papers to {args.export}")


def cmd_sota_expand(args):
    """Expand forward citations from seed papers to find SOTA work."""
    discovery = PaperDiscovery(email=args.email)

    # Load seed papers
    seeds_path = Path(args.seeds)
    if not seeds_path.exists():
        print(f"Error: Seeds file not found: {args.seeds}")
        return

    with open(seeds_path, "r", encoding="utf-8") as f:
        seeds = json.load(f)

    print(f"Loaded {len(seeds)} seed papers from {args.seeds}")

    # Expand citations
    papers = discovery.expand_citations(
        seeds,
        max_per_seed=args.max_per_seed,
        text_filter=args.text_filter,
        year_min=args.year_min,
        year_max=args.year_max,
    )
    print(f"\nForward citation expansion -> {len(papers)} new citing papers")

    # Save to store
    store = PaperStore(args.data_dir)
    saved = 0
    for paper in papers:
        if paper.get("doi"):
            store.save_layer(paper["doi"], "L0", paper)
            saved += 1
    print(f"\nSaved {saved} papers with DOI to store")

    # Create collection
    if args.collection:
        dois = [p["doi"] for p in papers if p.get("doi")]
        store.create_collection(args.collection, dois)
        print(f"Created collection '{args.collection}' with {len(dois)} papers")

    # Export for PaperSift
    if args.export:
        export_data = []
        for p in papers:
            if p.get("doi"):
                export_data.append({
                    "doi": p["doi"],
                    "title": p.get("title", ""),
                    "year": p.get("publication_year"),
                    "topics": p.get("topics", []),
                    "cited_by_count": p.get("cited_by_count", 0),
                    "referenced_works": p.get("referenced_works", []),
                })
        export_path = Path(args.export)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        print(f"Exported {len(export_data)} papers to {args.export}")


def main():
    parser = argparse.ArgumentParser(
        description="Paper Pipeline - Search, fetch, and manage academic papers"
    )
    parser.add_argument("--data-dir", default="data/papers", help="Data directory")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # search
    p_search = subparsers.add_parser("search", help="Search for papers")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--max", type=int, default=50, help="Max results")
    p_search.add_argument("--oa-only", action="store_true", help="OA papers only")
    p_search.add_argument("--year-from", type=int, help="Min publication year")
    p_search.add_argument("--email", default="", help="Email for polite pool")
    p_search.add_argument("--sort", default="relevance_score",
        choices=["relevance_score", "cited_by_count", "publication_date"],
        help="Sort order (default: relevance_score)")
    p_search.add_argument("--collection", help="Save results to collection")

    # fetch
    p_fetch = subparsers.add_parser("fetch", help="Fetch paper content")
    p_fetch.add_argument("--collection", help="Fetch papers in collection")
    p_fetch.add_argument("--doi", help="Fetch single paper by DOI")
    p_fetch.add_argument("--email", default="", help="Email for API access")

    # status
    subparsers.add_parser("status", help="Show store status")

    # collection
    p_coll = subparsers.add_parser("collection", help="Manage collections")
    p_coll.add_argument("action", choices=["create", "list", "show"])
    p_coll.add_argument("name", nargs="?", help="Collection name")
    p_coll.add_argument("--dois", nargs="+", help="DOI list")

    # grobid-status
    subparsers.add_parser("grobid-status", help="Check GROBID service")

    # sweep
    p_sweep = subparsers.add_parser("sweep", help="Comprehensive multi-query search with citation expansion")
    p_sweep.add_argument("query", help="Primary search query")
    p_sweep.add_argument("--synonyms", nargs="+", help="Additional synonym queries")
    p_sweep.add_argument("--max-per-query", type=int, default=500, help="Max results per query")
    p_sweep.add_argument("--sort", default="relevance_score",
        choices=["relevance_score", "cited_by_count", "publication_date"])
    p_sweep.add_argument("--expand-refs", action="store_true", help="Expand via citation chaining")
    p_sweep.add_argument("--no-hybrid", action="store_true",
        help="Disable hybrid sorting (default: search both relevance_score and cited_by_count)")
    p_sweep.add_argument("--max-refs", type=int, default=500, help="Max reference papers to fetch")
    p_sweep.add_argument("--oa-only", action="store_true")
    p_sweep.add_argument("--year-from", type=int)
    p_sweep.add_argument("--email", default="", help="Email for polite pool")
    p_sweep.add_argument("--collection", help="Save to collection")
    p_sweep.add_argument("--export", help="Export PaperSift-compatible JSON to path")

    # ask
    p_ask = subparsers.add_parser("ask", help="Answer questions using paper-qa2")
    p_ask.add_argument("question", help="Question to answer")
    p_ask.add_argument("--collection", help="Use papers from specific collection")
    p_ask.add_argument("--email", help="Email (unused, for consistency)")

    # sota-expand
    p_sota = subparsers.add_parser("sota-expand", help="Expand forward citations from seed papers")
    p_sota.add_argument("--seeds", required=True, help="Path to seeds JSON file (papers.json format)")
    p_sota.add_argument("--text-filter", help="Optional fulltext.search query (e.g., 'we propose OR novel method')")
    p_sota.add_argument("--max-per-seed", type=int, default=500, help="Max citing papers per seed paper")
    p_sota.add_argument("--year-min", type=int, help="Minimum publication year")
    p_sota.add_argument("--year-max", type=int, help="Maximum publication year")
    p_sota.add_argument("--email", default="", help="Email for polite pool")
    p_sota.add_argument("--collection", help="Save to collection")
    p_sota.add_argument("--export", help="Export PaperSift-compatible JSON to path")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        "search": cmd_search,
        "fetch": cmd_fetch,
        "status": cmd_status,
        "collection": cmd_collection,
        "grobid-status": cmd_grobid_status,
        "sweep": cmd_sweep,
        "ask": cmd_ask,
        "sota-expand": cmd_sota_expand,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
