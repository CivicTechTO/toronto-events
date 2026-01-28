# Quick Start Tutorial

This tutorial will guide you through running the Toronto Events pipeline from start to finish.

## Prerequisites

- Python 3.11 or higher
- At least 25GB of free disk space
- Stable internet connection (for downloading ~20GB of data)

## Installation

### 1. Install uv

[uv](https://github.com/astral-sh/uv) is a fast Python package manager.

**macOS/Linux**:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows**:
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Clone the Repository

```bash
git clone https://github.com/CivicTechTO/toronto-events.git
cd toronto-events
```

### 3. Install Dependencies

```bash
uv sync
```

This creates a virtual environment and installs all required packages.

## Quick Test Run

Before processing the full dataset, let's do a quick test with a small sample.

### Step 1: Download Metadata Only

```bash
uv run python scripts/download_wdc_events.py --metadata-only
```

This downloads (~90MB):
- `Event_lookup.csv` - Domain to file mapping
- `Event_domain_stats.csv` - Domain statistics
- `Event_sample.txt` - Sample data

**Expected output**:
```
Downloading metadata files...
✓ Event_lookup.csv (13 MB)
✓ Event_domain_stats.csv (76 MB)  
✓ Event_sample.txt (164 KB)
Complete!
```

### Step 2: Download One Part File

```bash
uv run python scripts/download_wdc_events.py --parts part_101.gz
```

This downloads one compressed data file (~150MB).

### Step 3: Run Test Pipeline

```bash
uv run python scripts/run_pipeline.py --parts part_101.gz --limit 1000
```

This runs the full pipeline on just 1,000 events.

**Pipeline phases**:
1. ✓ Domain signal analysis
2. ✓ Part prioritization (skipped)
3. ✓ Event extraction (limit: 1,000)
4. ✓ Domain scoring
5. ✓ Output generation

**Outputs** in `data/processed/`:
- `toronto_event_sources.csv` - Ranked Toronto domains
- `toronto_event_samples.ndjson` - Sample events
- `manual_review_queue.csv` - Domains needing review

## Full Pipeline Run

Ready to process the complete dataset? Here's how:

### Step 1: Download All Data

⚠️ **Warning**: This downloads ~20GB of data and takes 1-4 hours depending on your connection.

```bash
# Start the download
uv run python scripts/download_wdc_events.py

# Optional: Download specific parts only
uv run python scripts/download_wdc_events.py --parts part_0.gz part_1.gz part_2.gz
```

**Tip**: You can interrupt with Ctrl+C and resume later. Already-downloaded files are skipped.

### Step 2: Run Complete Pipeline

```bash
uv run python scripts/run_pipeline.py
```

**Processing time**: 2-4 hours on modern hardware

The pipeline will:
1. Analyze ~50,000 domains for Toronto signals
2. Extract and parse ~10M+ events
3. Apply geographic filtering
4. Score and classify domains
5. Generate final outputs

### Step 3: Review Results

Check `data/processed/` for output files:

```bash
ls -lh data/processed/
```

**Key files**:
- `toronto_event_sources.csv` - Your main deliverable!
- `toronto_event_samples.ndjson` - Example events from each source
- `manual_review_queue.csv` - Domains needing human validation

## Understanding the Output

### toronto_event_sources.csv

This is your ranked list of Toronto event sources.

**Columns**:
- `domain` - Website domain
- `classification` - INCLUDE, EXCLUDE, or UNKNOWN
- `confidence_score` - 0.0 to 1.0 (higher = more confident)
- `total_events` - Total events found
- `gta_events` - Events matched to GTA
- `gta_percentage` - Percent of GTA events
- `match_reasons` - Why it was matched (postal, coords, locality)

**Example row**:
```csv
domain,classification,confidence_score,total_events,gta_events,gta_percentage
rcmusic.com,INCLUDE,0.95,247,247,100.0,postal_code|known_institution
```

### Interpreting Classifications

**INCLUDE** - High confidence Toronto sources:
- Use these domains for your downstream application
- Safe to scrape or aggregate
- Verified Toronto/GTA locations

**EXCLUDE** - Not Toronto sources:
- Explicitly non-Toronto locations detected
- Skip these domains

**UNKNOWN** - Needs manual review:
- Insufficient data or ambiguous signals
- Review using Validation UI (see below)

## Advanced Usage

### Processing Specific Domains

If you're interested in specific domains, you can filter:

```bash
# Check if a domain is in the dataset
grep "eventbrite.com" data/raw/Event_lookup.csv

# Process only specific part files
uv run python scripts/run_pipeline.py --parts part_14.gz part_101.gz
```

### Skipping Phases

If you've already run parts of the pipeline:

```bash
# Skip domain analysis (use cached signals)
uv run python scripts/run_pipeline.py --skip-phase1

# Skip both analysis and prioritization
uv run python scripts/run_pipeline.py --skip-phase1 --skip-phase2
```

### Testing Changes

When modifying the pipeline:

```bash
# Quick test with 100 events
uv run python scripts/run_pipeline.py --limit 100

# Test on specific part
uv run python scripts/run_pipeline.py --parts part_0.gz --limit 1000
```

## Using the Validation UI

For domains classified as UNKNOWN, use the manual validation interface.

### 1. Prepare Validation Data

```bash
uv run python scripts/prepare_validation_data.py
```

This creates `validation_ui/domains.json` with domains to review.

### 2. Open the UI

```bash
# Simple way (might not work in all browsers)
open validation_ui/index.html

# Better: Use a local web server
python -m http.server 8000
# Then visit: http://localhost:8000/validation_ui/
```

### 3. Review and Export

- Use keyboard shortcuts (A=Accept, R=Reject, U=Uncertain)
- Review domain details and sample events
- Export validations when done

### 4. Apply Validations

```bash
uv run python scripts/apply_validations.py
```

This updates classifications based on your manual review.

See [docs/VALIDATION_UI.md](../docs/VALIDATION_UI.md) for detailed documentation.

## Common Issues & Solutions

### Out of Memory

**Problem**: Script crashes with memory error

**Solution**: Process fewer events at a time:
```bash
# Process one part at a time
for i in {0..132}; do
  uv run python scripts/run_pipeline.py --parts part_$i.gz
done
```

### Download Interrupted

**Problem**: Download stops partway through

**Solution**: Just run the download command again:
```bash
uv run python scripts/download_wdc_events.py
```
Already-downloaded files are automatically skipped.

### Slow Processing

**Problem**: Pipeline takes too long

**Solution**:
1. Close other applications to free CPU/memory
2. Process subset of parts first
3. Use SSD instead of HDD for data directory

### Import Errors

**Problem**: `ModuleNotFoundError` when running scripts

**Solution**: Always use `uv run`:
```bash
# ✗ Wrong
python scripts/run_pipeline.py

# ✓ Correct  
uv run python scripts/run_pipeline.py
```

### No Results Found

**Problem**: Output files are empty or have few results

**Possible causes**:
1. Downloaded wrong part files (check `Event_lookup.csv`)
2. Too restrictive filtering (check `domain_signals.csv`)
3. Geo-filter too strict (review `geo_filter.py` settings)

**Debug**:
```bash
# Check how many domains have each signal
uv run python scripts/analyze_domains.py
# Look at the summary output

# Check extracted events
head data/intermediate/extracted_events.ndjson
```

## Next Steps

Now that you have Toronto event sources, you can:

1. **Build an Event Aggregator**: Scrape events from these domains
2. **Create an Events API**: Serve Toronto events to applications
3. **Analyze Event Patterns**: Study what types of events happen in Toronto
4. **Monitor Sources**: Track which sources are most active

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/CivicTechTO/toronto-events/issues)
- **Documentation**: Check the `docs/` directory
- **CivicTechTO**: Join our community meetings

## Learn More

- [ARCHITECTURE.md](../ARCHITECTURE.md) - Technical architecture details
- [CONTRIBUTING.md](../CONTRIBUTING.md) - How to contribute
- [docs/VALIDATION_UI.md](../docs/VALIDATION_UI.md) - Validation UI guide
