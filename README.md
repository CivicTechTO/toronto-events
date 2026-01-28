# Toronto Event Source Pipeline

> A data pipeline to identify high-quality event sources in Toronto and the Greater Toronto Area (GTA) from the [Web Data Commons](http://webdatacommons.org/structureddata) Event dataset.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

Built for [CivicTechTO](https://github.com/CivicTechTO) to enable better civic event discovery and aggregation.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Architecture](#pipeline-architecture)
- [Project Structure](#project-structure)
- [Validation UI](#validation-ui)
- [Output Files](#output-files)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project analyzes terabytes of structured web data (N-Quads with Schema.org markup) to find domains that publish event information for Toronto and the GTA. It uses a sophisticated multi-stage filtering process to efficiently identify relevant domains without needing to process the entire internet's worth of data.

### Why This Matters

Toronto lacks a comprehensive, open-source list of event sources. This pipeline:
- **Discovers** event sources automatically from web data
- **Validates** locations using multiple geographic strategies
- **Ranks** sources by confidence and data quality
- **Enables** civic tech projects to build better event aggregators

### The Challenge

The Web Data Commons Event dataset contains:
- 133 compressed files (~20GB total)
- 10M+ events from 50K+ domains worldwide
- Only a tiny fraction are Toronto-specific

Our pipeline efficiently identifies the ~1-2% of relevant Toronto sources.

## ✨ Features

### 🎯 Tri-State Domain Classification
Efficiently categorizes domains as `INCLUDE`, `EXCLUDE`, or `UNKNOWN` based on:
- Top-level domains (TLDs) - `.ca` vs `.co.uk`
- Keywords - "toronto", "gta", municipality names
- Known institutions - Universities, venues, cultural organizations

### 🌍 Multi-Strategy Geo-Filtering
Identifies Toronto/GTA events using:
- **Postal Codes**: M* (Toronto), L* (GTA regions)
- **Bounding Boxes**: Lat/lon coordinates
- **City Names**: Toronto, Mississauga, Brampton, etc.
- **Neighborhoods**: Scarborough, Etobicoke, North York, etc.

### ⚡ Performance Optimized
- Uses `orjson` for 2-3x faster JSON processing
- Uses `regex` for high-performance pattern matching  
- Streaming N-Quads parser for memory efficiency
- Progress bars with `tqdm` for long operations

### 📊 Smart Prioritization
Identifies which data files contain the most Toronto-relevant domains, optimizing download and processing time.

### 🖥️ Manual Validation UI
Web-based interface for human review of uncertain classifications, with:
- Keyboard shortcuts for fast validation
- Auto-save to browser storage
- Export/import of validation decisions

## 🚀 Quick Start

```bash
# 1. Install uv (fast Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone the repository
git clone https://github.com/CivicTechTO/toronto-events.git
cd toronto-events

# 3. Install dependencies
uv sync

# 4. Download metadata and sample data
uv run python scripts/download_wdc_events.py --metadata-only

# 5. Run a quick test (1 file, 1000 events)
uv run python scripts/download_wdc_events.py --parts part_101.gz
uv run python scripts/run_pipeline.py --parts part_101.gz --limit 1000

# 6. Check your results!
cat data/processed/toronto_event_sources.csv
```

See [examples/TUTORIAL.md](examples/TUTORIAL.md) for a detailed walkthrough.

## 📦 Installation

### Requirements

- **Python 3.11+** (3.12 recommended)
- **~25GB disk space** (20GB for data + 5GB for processing)
- **Stable internet** (for downloading data)

### Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone repository
git clone https://github.com/CivicTechTO/toronto-events.git
cd toronto-events

# Install dependencies
uv sync

# Verify installation
uv run python --version
```

**Alternative**: Traditional pip/venv setup:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

**Note**: The scripts use `sys.path` manipulation for imports. For a cleaner setup, install the package in editable mode with `uv pip install -e .` or `pip install -e .`.

## 📖 Usage

### Full Pipeline

Process the complete dataset (~2-4 hours):

```bash
# Download all data (~20GB, can take hours)
uv run python scripts/download_wdc_events.py

# Run complete pipeline
uv run python scripts/run_pipeline.py
```

### Selective Processing

Process specific part files only:

```bash
# Download specific parts
uv run python scripts/download_wdc_events.py --parts part_0.gz part_14.gz part_101.gz

# Process those parts
uv run python scripts/run_pipeline.py --parts part_0.gz part_14.gz part_101.gz
```

### Testing & Development

Quick test runs for development:

```bash
# Limit to 100 events
uv run python scripts/run_pipeline.py --limit 100

# Skip already-completed phases
uv run python scripts/run_pipeline.py --skip-phase1
```

### Advanced Options

```bash
# Run specific pipeline phases
uv run python scripts/analyze_domains.py      # Phase 1: Domain signals
uv run python scripts/identify_relevant_parts.py  # Phase 2: Prioritization
uv run python scripts/extract_events.py --limit 1000  # Phase 3: Extraction
uv run python scripts/score_domains.py         # Phase 5: Scoring
uv run python scripts/generate_outputs.py      # Phase 6: Outputs

# Download options
uv run python scripts/download_wdc_events.py --metadata-only  # Metadata only
uv run python scripts/download_wdc_events.py --resume        # Resume interrupted download
```

## 🏗️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Web Data Commons Event Dataset (2024-12)              │
│  133 part files, ~20GB N-Quads, 10M+ events            │
└─────────────────┬───────────────────────────────────────┘
                  │
    ┌─────────────▼──────────────┐
    │ Phase 1: Domain Analysis   │  Tri-state classification
    │ (analyze_domains.py)       │  based on TLD, keywords,
    └─────────────┬──────────────┘  known institutions
                  │
    ┌─────────────▼──────────────┐
    │ Phase 2: Prioritization    │  Identify which files
    │ (identify_relevant_parts)  │  to download first
    └─────────────┬──────────────┘
                  │
    ┌─────────────▼──────────────┐
    │ Phase 3: Event Extraction  │  Parse N-Quads, skip
    │ (extract_events.py)        │  negative domains,
    └─────────────┬──────────────┘  reconstruct events
                  │
    ┌─────────────▼──────────────┐
    │ Phase 4 & 5: Geo & Scoring │  Multi-strategy location
    │ (score_domains.py)         │  matching, confidence
    └─────────────┬──────────────┘  scoring
                  │
    ┌─────────────▼──────────────┐
    │ Phase 6: Generate Outputs  │  Final deliverables:
    │ (generate_outputs.py)      │  CSV + NDJSON + queue
    └─────────────┬──────────────┘
                  │
         ┌────────▼─────────┐
         │ Validation UI    │  Manual review
         │ (optional)       │  of UNKNOWN domains
         └──────────────────┘
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical documentation.

## 📁 Project Structure

```
toronto-events/
├── src/
│   └── toronto_events/          # Python package (reusable modules)
│       ├── core/                # Core functionality
│       │   ├── nquads_parser.py # N-Quads streaming parser
│       │   └── geo_filter.py    # Geographic filtering
│       ├── pipeline/            # Pipeline orchestration (future)
│       └── utils/               # Utility functions (future)
├── scripts/                     # Executable pipeline scripts
│   ├── run_pipeline.py         # Main pipeline orchestrator
│   ├── download_wdc_events.py  # Data downloader
│   ├── analyze_domains.py      # Phase 1: Domain signals
│   ├── identify_relevant_parts.py  # Phase 2: Part prioritization
│   ├── extract_events.py       # Phase 3: Event extraction
│   ├── score_domains.py        # Phase 5: Scoring & classification
│   ├── generate_outputs.py     # Phase 6: Output generation
│   ├── prepare_validation_data.py  # Prepare UI data
│   └── apply_validations.py    # Apply manual validations
├── validation_ui/               # Web-based validation interface
│   ├── index.html              # Main UI
│   ├── app.js                  # UI logic
│   ├── styles.css              # Styling
│   └── domains.json            # Data to validate
├── data/                        # Data directory (gitignored)
│   ├── raw/                    # Downloaded WDC files
│   ├── intermediate/           # Processing artifacts
│   └── processed/              # Final outputs ✨
├── docs/                        # Additional documentation
│   └── VALIDATION_UI.md        # Validation UI guide
├── examples/                    # Usage examples
│   └── TUTORIAL.md             # Step-by-step tutorial
├── README.md                    # This file
├── ARCHITECTURE.md              # Technical architecture
├── CONTRIBUTING.md              # Contribution guidelines
└── pyproject.toml              # Project configuration
```

## 🖥️ Validation UI

For domains classified as `UNKNOWN`, use the manual validation interface:

```bash
# 1. Prepare validation data
uv run python scripts/prepare_validation_data.py

# 2. Open UI (use a local server for best results)
python -m http.server 8000
# Then visit: http://localhost:8000/validation_ui/

# 3. Review domains using keyboard shortcuts:
#    A = Accept    R = Reject    U = Uncertain    N = Next

# 4. Export validations and apply
uv run python scripts/apply_validations.py
```

Features:
- ⌨️ **Keyboard shortcuts** for fast validation
- 💾 **Auto-save** to browser local storage  
- 📊 **Progress tracking** with visual indicators
- 📤 **Export** validations for pipeline integration

See [docs/VALIDATION_UI.md](docs/VALIDATION_UI.md) for detailed documentation.

## 📊 Output Files

All outputs are saved to `data/processed/`:

### toronto_event_sources.csv
**Main deliverable** - Ranked list of Toronto event sources

```csv
domain,classification,confidence_score,total_events,gta_events,gta_percentage,match_reasons
rcmusic.com,INCLUDE,0.95,247,247,100.0,postal_code|known_institution
torontopubliclibrary.ca,INCLUDE,0.92,183,183,100.0,known_institution|locality
eventbrite.com,INCLUDE,0.45,892,421,47.2,locality|postal_code
```

**Columns**:
- `domain` - Website domain name
- `classification` - INCLUDE, EXCLUDE, or UNKNOWN
- `confidence_score` - 0.0-1.0 confidence level
- `total_events` - Total events found
- `gta_events` - Events matched to GTA
- `gta_percentage` - Percentage of GTA events
- `match_reasons` - Why it matched (postal, coords, locality, etc.)

### toronto_event_samples.ndjson
Sample events from each INCLUDE domain (JSON Lines format)

```json
{"domain":"rcmusic.com","name":"Summer Concert Series","location":{"locality":"Toronto","postal_code":"M5B 1W8"},"start_date":"2024-07-15"}
{"domain":"ago.ca","name":"Art Gallery Exhibition","location":{"locality":"Toronto"},"start_date":"2024-08-01"}
```

### manual_review_queue.csv
Domains classified as UNKNOWN, needing human review

```csv
domain,confidence_score,event_count,gta_percentage,reason
example.com,0.35,12,58.3,low_sample_size
another-site.org,0.42,45,60.0,ambiguous_location
```

## 📚 Documentation

- **[examples/TUTORIAL.md](examples/TUTORIAL.md)** - Step-by-step tutorial for beginners
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Technical architecture and design
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - How to contribute to the project
- **[docs/VALIDATION_UI.md](docs/VALIDATION_UI.md)** - Validation UI documentation

## 🤝 Contributing

We welcome contributions! Whether you're:
- 🐛 Reporting bugs
- 💡 Suggesting features
- 📝 Improving documentation  
- 💻 Writing code

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Quick Contribution Workflow

```bash
# 1. Fork and clone
git clone https://github.com/YOUR_USERNAME/toronto-events.git
cd toronto-events

# 2. Create a branch
git checkout -b feature/your-feature

# 3. Make changes and test
uv sync
# ... make your changes ...
uv run python scripts/run_pipeline.py --limit 100  # Test

# 4. Commit and push
git commit -m "Add: your feature description"
git push origin feature/your-feature

# 5. Create Pull Request on GitHub
```

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Web Data Commons** - For providing the Event dataset
- **CivicTechTO** - For supporting civic technology projects in Toronto
- **Contributors** - Everyone who has contributed to this project

## 📞 Contact & Support

- **Issues**: [GitHub Issues](https://github.com/CivicTechTO/toronto-events/issues)
- **Discussions**: [GitHub Discussions](https://github.com/CivicTechTO/toronto-events/discussions)
- **CivicTechTO**: [Website](https://civictech.ca) | [Slack](https://civictech.ca/slack)

---

Made with ❤️ by [CivicTechTO](https://github.com/CivicTechTO)
