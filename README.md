# Toronto Event Source Pipeline

A data pipeline to identify high-quality event sources in Toronto and the Greater Toronto Area (GTA) from the [Web Data Commons](http://webdatacommons.org/structureddata/schemaorg) Event dataset.

built for [CivicTechTO](https://github.com/CivicTechTO).

## Overview

This project analyzes terabytes of N-Quads data to find domains that publish structured event data (Schema.org/Event) located in Toronto. It uses a multi-stage filtering process to efficiently identify relevant domains without needing to process the entire internet's worth of data.

## Features

- **Tri-State Domain Classification**: Efficiently categorizes domains as `INCLUDE`, `EXCLUDE`, or `UNKNOWN` based on TLDs and keywords before deep processing.
- **Geo-Filtering**: Multi-strategy location matching using postal codes (M*, L*), bounding boxes, and city names.
- **Performance Optimized**: Uses `orjson` and `regex` for high-performance parsing of massive N-Quads files with `tqdm` progress bars.
- **Smart Prioritization**: Identifies which part files contain the most relevant data to minimize download/processing time.

## Installation

This project uses [`uv`](https://github.com/astral-sh/uv) for fast dependency management.

```bash
# Clone repo
git clone https://github.com/CivicTechTO/toronto-events.git
cd toronto-events

# Install dependencies
uv sync
```

## Usage

The entire pipeline can be run via the orchestration script:

```bash
# Run the full pipeline
uv run python scripts/run_pipeline.py
```

### Advanced Options

```bash
# Process specific part files only
uv run python scripts/run_pipeline.py --parts part_101.gz part_14.gz

# Limit events for testing
uv run python scripts/run_pipeline.py --limit 1000

# Skip domain analysis (if Phase 1 is already complete)
uv run python scripts/run_pipeline.py --skip-phase1
```

## Pipeline Architecture

### Phase 1: Domain Signal Analysis (`analyze_domains.py`)
Analyzes the WDC domain graph metadata to assign initial signals:
- **Positive**: `.ca` TLD, "toronto" in domain, known institutions (e.g., `utoronto.ca`).
- **Negative**: Foreign regional TLDs (e.g., `.co.uk`, `.fr`) → **Excluded**.
- **Neutral**: Generic TLDs (`.com`, `.org`) → **Processed** if they contain geo-data.

### Phase 2: Prioritization (`identify_relevant_parts.py`)
Determines which source `part_*.gz` files contain the highest density of candidate domains to optimize download order.

### Phase 3: Extraction (`extract_events.py`)
Streams N-Quads data, filtering out "Negative" signal domains immediately. Parses `Schema.org/Event` objects and reconstructs their properties (location, dates, organization).

### Phase 4 & 5: Scoring (`score_domains.py`)
Validates event locations against Toronto/GTA geography (Postal Codes M*/L*, Lat/Lon bounding box).
Produces a final classification:
- **INCLUDE**: Confirmed or likely Toronto source.
- **EXCLUDE**: Explicitly non-Toronto geo data (e.g., an event in London, UK).
- **UNKNOWN**: Insufficient data.

## Development

- **Dependencies**: Managed via `pyproject.toml`.
- **Scripts**: All processing logic resides in `scripts/`.
- **Validation UI**: A mini web-app in `validation_ui/` for manual review of domain classifications.

## License

MIT
