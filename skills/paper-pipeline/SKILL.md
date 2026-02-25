# Paper Pipeline v2 SKILL

논문 검색, 수집, 구조화된 저장 및 분석을 위한 Claude Code 워크플로우.

## Overview

Paper Pipeline은 DOI/키워드 기반 논문 검색부터 L0-L3 계층화된 분석까지의 완전한 ETL 파이프라인입니다.

### 4-Layer System

| Layer | 내용 | 토큰/논문 | 생성 주체 |
|-------|------|-----------|-----------|
| L0 | 메타데이터 카드 | ~50 | Python (PyAlex 자동) |
| L1 | 구조화된 초록 분석 | ~200-400 | Claude Code 인라인 |
| L2 | 섹션별 요약 | ~500-800 | Claude Code 인라인 |
| L3 | 심층 분석 | ~1500-2500 | Claude Code on-demand |

## Quick Start

### CLI Commands

```bash
# 1. 논문 검색 (PyAlex 기반)
paper-pipeline search "urban microbiome metagenomics" --max 50 --oa-only

# 2. 원문 획득 (Europe PMC → Unpaywall → PDF)
paper-pipeline fetch --collection urban-microbiome --email your@email.com

# 3. 상태 확인
paper-pipeline status

# 4. 컬렉션 관리
paper-pipeline collection list
paper-pipeline collection show urban-microbiome
```

### Python API

```python
from paper_pipeline import PaperDiscovery, PaperFetcher, PaperExtractor, PaperStore

# 검색
discovery = PaperDiscovery(email="your@email.com")
papers = discovery.search("antimicrobial resistance", max_results=20, filters={"is_oa": True})

# 저장
store = PaperStore("data/papers")
for paper in papers:
    store.save_layer(paper["doi"], "L0", paper)

# 원문 획득
fetcher = PaperFetcher(email="your@email.com")
result = fetcher.fetch_content(paper["doi"], work_data=paper, save_dir=store.get_paper_dir(paper["doi"]) / "content")

# 텍스트 추출
extractor = PaperExtractor()
extraction = extractor.extract(result.content_type, data=result.data, pdf_path=result.pdf_path)

# 저장
if extraction.full_text:
    store.save_content(paper["doi"], "fulltext", extraction.full_text)
```

---

## Analysis Workflows

### L1 분석 생성 (초록 기반)

**언제 사용**: L0 메타데이터만 있고 초록 분석이 필요할 때

**입력**: `store.load_layer(doi, "L0")` → abstract 필드

**프롬프트 템플릿**:
```
다음 논문 초록을 분석하여 JSON 형식으로 구조화하세요:

제목: {title}
초록: {abstract}

출력 JSON 스키마:
{
  "objective": "1-2문장으로 연구 목적",
  "methods": "1-2문장으로 핵심 방법론 (사용된 도구/데이터셋 포함)",
  "key_findings": ["구체적 수치 포함 발견 1", "발견 2", "발견 3"],
  "significance": "1문장으로 학술적 의의"
}
```

**저장**: `store.save_layer(doi, "L1", analysis)`

---

### L2 분석 생성 (전체 텍스트 기반)

**언제 사용**: 원문이 있고 섹션별 요약이 필요할 때

**전제 조건**: `store.has_layer(doi, "L0")` AND `store.load_content(doi, "fulltext")`

**extraction_method 분기**:

| extraction_method | 데이터 소스 | 특징 |
|-------------------|-------------|------|
| `europe_pmc_xml` | sections.json 또는 fulltext.md | 섹션이 이미 구조화됨 |
| `grobid` | fulltext.md + grobid.tei.xml | TEI XML 참조 가능 |
| `pymupdf4llm` | fulltext.md | 섹션 경계 불명확할 수 있음 |

**프롬프트 템플릿**:
```
다음 논문의 전체 텍스트를 분석하여 섹션별 요약을 JSON으로 생성하세요:

제목: {title}
DOI: {doi}
추출 방법: {extraction_method}

전체 텍스트:
{full_text}

출력 JSON 스키마:
{
  "introduction": "연구 배경 및 맥락 (2-3문장)",
  "methods": "핵심 방법론 요약 (2-3문장, 도구/데이터셋 포함)",
  "results": ["주요 결과 1 (구체적 수치)", "주요 결과 2", "주요 결과 3"],
  "discussion": "핵심 해석 및 시사점 (2-3문장)",
  "limitations": ["한계점 1", "한계점 2"]
}

참고: extraction_method가 'pymupdf4llm'인 경우 섹션 구분이 불명확할 수 있으니
내용 기반으로 섹션을 추론하세요.
```

**저장**: `store.save_layer(doi, "L2", analysis)`

---

### L3 심층 분석 (On-Demand)

**언제 사용**: 특정 연구 질문에 대한 심층 분석이 필요할 때

**전제 조건**: L2 분석 완료

**프롬프트 템플릿**:
```
다음 논문에 대해 심층 분석을 수행하세요:

제목: {title}
DOI: {doi}
연구 질문: {research_question}

L2 요약:
{l2_summary}

전체 텍스트:
{full_text}

출력 JSON 스키마:
{
  "detailed_methodology": "재현 가능한 수준의 상세 방법론 기술",
  "quantitative_results": [
    {"metric": "지표명", "value": "값", "context": "설명 (p-value, CI 포함)"}
  ],
  "limitations_by_authors": ["저자가 명시한 한계 1", "한계 2"],
  "open_questions": ["후속 연구 필요 사항 1", "사항 2"],
  "relevance_to_query": "연구 질문에 대한 이 논문의 기여도 및 연결점",
  "connections_to_corpus": [
    {"doi": "관련 논문 DOI", "connection": "연결 근거"}
  ]
}
```

**저장**: `store.save_layer(doi, "L3", analysis)`

---

## Batch Analysis Workflow

### 캐시 확인 → 분석 생성 → 저장

```python
from paper_pipeline import PaperStore

store = PaperStore("data/papers")
collection_dois = store.get_collection("my-collection")

for doi in collection_dois:
    # L1 캐시 확인
    if store.has_layer(doi, "L1"):
        print(f"[SKIP] {doi} - L1 already exists")
        continue

    # L0 로드
    metadata = store.load_layer(doi, "L0")
    if not metadata or not metadata.get("abstract"):
        print(f"[SKIP] {doi} - No abstract available")
        continue

    # L1 분석 생성 (Claude Code 인라인)
    # ... 프롬프트 템플릿 사용 ...

    # 저장
    store.save_layer(doi, "L1", l1_analysis)
    print(f"[DONE] {doi} - L1 saved")
```

### 캐시 활용 패턴

```python
# 이미 분석된 논문은 즉시 로드 (LLM 호출 0회)
l2 = store.load_layer(doi, "L2")
if l2:
    # 즉시 사용 가능
    print(l2["results"])
else:
    # 분석 필요
    ...
```

---

## Graceful Degradation

### Content Acquisition

| Tier | Source | Coverage | 추출 방법 |
|------|--------|----------|-----------|
| A | Europe PMC XML | ~30-40% | XML 직접 파싱 (최선) |
| B | OA PDF + GROBID | ~40-55% | GROBID TEI XML |
| C | OA PDF + pymupdf4llm | ~40-55% | 마크다운 변환 |
| D | Abstract only | ~85% | L0+L1만 가능 |
| E | Metadata only | 100% | L0만 가능 |

### Analysis Capability by Content

| Content Available | 가능한 분석 |
|-------------------|-------------|
| Metadata only | L0 |
| Abstract | L0 + L1 |
| Full-text | L0 + L1 + L2 + L3 |

---

## Directory Structure

```
data/papers/
├── index.json                        # 전체 인덱스
├── by-doi/
│   └── 10.1038__nature06244/         # DOI별 디렉토리
│       ├── metadata.json             # L0
│       ├── abstract.json             # L1
│       ├── sections.json             # L2
│       ├── analysis.json             # L3
│       ├── README.md                 # 자동 생성
│       └── content/                  # git-ignored
│           ├── fulltext.md
│           ├── raw_abstract.txt
│           ├── source.pdf
│           ├── grobid.tei.xml
│           └── europe_pmc.xml
└── by-collection/
    └── my-collection/
        └── collection.json
```

---

## Environment Variables

| 변수 | 필수 | 설명 |
|------|------|------|
| `PAPER_PIPELINE_EMAIL` | Yes | OpenAlex/Unpaywall API용 이메일 |
| `NCBI_API_KEY` | No | Europe PMC 높은 rate limit |
| `ANTHROPIC_API_KEY` | No | paper-qa2 사용 시 |

---

## Tips

### Token Efficiency

1. **L0 먼저 저장**: 메타데이터는 토큰 비용 없이 저장 가능
2. **L1 배치 생성**: 초록 분석은 병렬 처리 가능
3. **L2 on-demand**: 필요한 논문만 전체 텍스트 분석
4. **L3 질문 기반**: 연구 질문이 명확할 때만 심층 분석

### Best Practices

1. `--oa-only` 플래그로 접근 가능한 논문만 검색
2. Europe PMC XML이 있으면 GROBID 불필요
3. 컬렉션으로 프로젝트별 논문 그룹 관리
4. `store.get_stats()`로 저장소 상태 정기 확인
