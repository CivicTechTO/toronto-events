# Architecture

This document describes the technical architecture of the Toronto Events pipeline.

## Overview

The Toronto Events pipeline processes terabytes of structured web data to identify high-quality event sources in Toronto and the Greater Toronto Area (GTA). The pipeline uses a multi-phase approach to efficiently filter and classify domains without processing the entire dataset.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Web Data Commons (WDC)                        │
│              Schema.org/Event Dataset (2024-12)                 │
│                    ~20GB N-Quads Data                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Download
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Phase 1: Domain Analysis                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ analyze_domains.py                                        │  │
│  │ - Tri-state classification (POSITIVE/NEUTRAL/NEGATIVE)    │  │
│  │ - TLD analysis (.ca, .co.uk, .com)                       │  │
│  │ - Keyword matching (toronto, gta, etc.)                  │  │
│  │ - Known institution detection                            │  │
│  │ Output: domain_signals.csv                               │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Phase 2: Part Prioritization                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ identify_relevant_parts.py                                │  │
│  │ - Determines which part_*.gz files to download first      │  │
│  │ - Prioritizes by density of candidate domains             │  │
│  │ Output: prioritized_parts.csv                            │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Phase 3: Event Extraction                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ extract_events.py                                         │  │
│  │ - Streams N-Quads from part_*.gz files                   │  │
│  │ - Skips NEGATIVE signal domains immediately              │  │
│  │ - Parses Schema.org/Event entities                       │  │
│  │ - Reconstructs event properties & locations              │  │
│  │ Output: extracted_events.ndjson                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Phase 4 & 5: Geo-Filtering & Scoring            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ score_domains.py + geo_filter.py                          │  │
│  │ - Multi-strategy location matching:                       │  │
│  │   * Postal codes (M*, L*)                                │  │
│  │   * Lat/lon bounding boxes                               │  │
│  │   * City/locality names                                  │  │
│  │ - Tri-state classification (INCLUDE/EXCLUDE/UNKNOWN)     │  │
│  │ - Confidence scoring                                      │  │
│  │ Output: domain_scores.csv                                │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Phase 6: Output Generation                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ generate_outputs.py                                       │  │
│  │ - Creates final deliverables                             │  │
│  │ Outputs:                                                  │  │
│  │   * toronto_event_sources.csv                            │  │
│  │   * toronto_event_samples.ndjson                         │  │
│  │   * manual_review_queue.csv                              │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Validation UI    │
                    │ (Optional)       │
                    └──────────────────┘
```

## Core Components

### 1. Domain Signal Analysis (`analyze_domains.py`)

**Purpose**: Efficiently categorize domains before deep processing

**Tri-State Classification**:
- **POSITIVE**: Strong Toronto signals (`.ca` TLD, "toronto" keyword, known institutions)
- **NEGATIVE**: Foreign regional TLD (`.co.uk`, `.fr`, `.de`) → Skip immediately
- **NEUTRAL**: Generic TLD (`.com`, `.org`) → Process with geo verification

**Why Tri-State?**
- Avoids false negatives (e.g., `eventbrite.com` hosts Toronto events)
- Optimizes performance by skipping obvious non-Toronto domains
- Prioritizes processing of high-confidence domains

### 2. N-Quads Parser (`src/toronto_events/core/nquads_parser.py`)

**Purpose**: Stream and parse large N-Quads files efficiently

**Features**:
- Memory-efficient streaming (processes files incrementally)
- Handles gzipped files natively
- Extracts RDF quads: `subject predicate object graph`
- Domain extraction from graph URIs

**Performance**:
- Uses `regex` library (faster than stdlib `re`)
- Processes ~100K quads/second on modern hardware

### 3. Geo-Filter (`src/toronto_events/core/geo_filter.py`)

**Purpose**: Multi-strategy location matching for Toronto/GTA

**Strategies**:

1. **Postal Code Matching** (Highest confidence)
   - Toronto: M* prefixes (M1A through M9W)
   - GTA regions: L* prefixes (specific codes for Mississauga, Brampton, etc.)

2. **Bounding Box** (High confidence)
   - Toronto Core: 43.58°N - 43.86°N, -79.64°W - -79.10°W
   - GTA Extended: 43.40°N - 44.30°N, -80.20°W - -78.80°W

3. **Locality Matching** (Medium confidence)
   - City names: Toronto, Mississauga, Brampton, etc.
   - Neighborhoods: Scarborough, Etobicoke, North York, etc.

4. **Region Matching** (Low confidence)
   - Province: Ontario, ON

**Anti-Pattern Detection**:
- Identifies non-Toronto locations (e.g., "London, UK")
- Marks domains with 100% non-Toronto events as EXCLUDE

### 4. Event Extractor (`extract_events.py`)

**Purpose**: Parse Schema.org/Event entities from N-Quads

**Entity Reconstruction**:
- Follows RDF graph references (blank nodes)
- Extracts nested structures (Location, Organizer, PostalAddress)
- Handles multiple Event subtypes (MusicEvent, SportsEvent, etc.)

**Optimization**:
- Skips domains with NEGATIVE signal immediately
- Groups quads by subject for efficient parsing
- Processes in batches to manage memory

## Data Flow

### Input Data

1. **Event_lookup.csv** (~13MB)
   - Maps domains to part files
   - Format: `domain,tld,part_file`

2. **Event_domain_stats.csv** (~76MB)
   - Domain-level statistics
   - Format: `domain,num_quads,num_entities`

3. **part_*.gz** (133 files, ~20GB total)
   - N-Quads format: `subject predicate object graph .`
   - Contains Schema.org/Event entities

### Intermediate Data

1. **domain_signals.csv**
   - Tri-state domain classification
   - Priority scores

2. **extracted_events.ndjson**
   - Parsed event entities with locations
   - JSON Lines format for streaming

3. **domain_scores.csv**
   - Per-domain geo-matching statistics
   - Final classification

### Output Data

1. **toronto_event_sources.csv**
   - Ranked list of INCLUDE domains
   - Metrics: event count, GTA%, confidence score

2. **toronto_event_samples.ndjson**
   - Sample events from each domain
   - For validation and downstream use

3. **manual_review_queue.csv**
   - UNKNOWN classification domains
   - For human validation

## Performance Considerations

### Memory Management

- **Streaming Processing**: Files processed incrementally, not loaded entirely
- **Batch Processing**: Events processed in batches (100K quads)
- **Early Filtering**: NEGATIVE domains skipped before parsing

### Optimization Techniques

1. **`orjson`**: 2-3x faster than stdlib `json`
2. **`regex`**: Faster than stdlib `re` for complex patterns
3. **`tqdm`**: Progress bars for long operations
4. **Domain batching**: Group by domain to minimize I/O

### Scalability

Current pipeline can process:
- ~133 part files (20GB) in ~2-4 hours
- ~10M+ events
- ~50K+ unique domains

Bottlenecks:
- Download bandwidth (20GB data)
- Disk I/O (reading compressed files)
- CPU (regex parsing)

## Extension Points

### Adding New Geo Strategies

Edit `src/toronto_events/core/geo_filter.py`:

```python
def check_custom_strategy(self, location: LocationInfo) -> bool:
    """Your custom location matching logic."""
    # Implementation here
    return True
```

### Supporting New Event Types

Edit event type patterns in `extract_events.py`:

```python
EVENT_TYPES = {
    'http://schema.org/Event',
    'http://schema.org/YourNewEventType',  # Add here
}
```

### Custom Scoring

Edit `score_domains.py` to adjust confidence scoring:

```python
def compute_classification(self, ...):
    # Adjust weights for different signals
    score += self.postal_code_matches * 0.30  # Adjust this
```

## Technology Stack

- **Python 3.11+**: Core language
- **orjson**: High-performance JSON encoding/decoding
- **regex**: Advanced regex with better performance
- **tqdm**: Progress bars
- **uv**: Fast dependency management

## Future Enhancements

1. **Incremental Updates**: Download only new/changed parts
2. **Parallel Processing**: Multi-process extraction
3. **Machine Learning**: Classify UNKNOWN domains automatically
4. **Real-time API**: Serve event data via REST API
5. **Automated Testing**: Unit and integration tests
