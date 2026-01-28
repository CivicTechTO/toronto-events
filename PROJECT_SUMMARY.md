# Project Summary

## What is Toronto Events?

Toronto Events is a data pipeline that analyzes Web Data Commons structured web data to identify high-quality event sources for Toronto and the Greater Toronto Area (GTA).

## Key Deliverables

1. **toronto_event_sources.csv** - Ranked list of Toronto event sources
2. **toronto_event_samples.ndjson** - Sample events from each source
3. **manual_review_queue.csv** - Domains needing human validation

## Tech Stack

- **Python 3.11+** - Core language
- **orjson** - High-performance JSON
- **regex** - Advanced pattern matching
- **tqdm** - Progress indicators
- **uv** - Fast dependency management

## Documentation

- **README.md** - Main documentation
- **ARCHITECTURE.md** - Technical architecture
- **CONTRIBUTING.md** - Contribution guidelines
- **examples/TUTORIAL.md** - Step-by-step tutorial
- **docs/VALIDATION_UI.md** - Validation UI guide

## Quick Links

- **Repository**: https://github.com/CivicTechTO/toronto-events
- **Issues**: https://github.com/CivicTechTO/toronto-events/issues
- **CivicTechTO**: https://civictech.ca

## For Maintainers

### Project Structure

```
toronto-events/
├── src/toronto_events/      # Reusable Python modules
│   └── core/               # Core functionality (parsers, filters)
├── scripts/                # Executable pipeline scripts  
├── validation_ui/          # Web interface for validation
├── docs/                   # Additional documentation
├── examples/               # Usage examples and tutorials
└── data/                   # Data directory (gitignored)
```

### Running the Pipeline

```bash
# Quick test
uv run python examples/quick_test.py

# Full pipeline
uv run python scripts/run_pipeline.py
```

### Release Checklist

- [ ] Update version in `src/toronto_events/__init__.py`
- [ ] Update version in `pyproject.toml`
- [ ] Update CHANGELOG.md (if exists)
- [ ] Tag release in git
- [ ] Update documentation as needed

## For Contributors

### Getting Started

1. Fork the repository
2. Clone your fork
3. Create a feature branch
4. Make your changes
5. Test locally
6. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### Development Commands

```bash
# Install dependencies
uv sync

# Run quick test
uv run python scripts/run_pipeline.py --limit 100

# Test specific script
uv run python scripts/analyze_domains.py --help

# Format/lint (add these tools as needed)
# black .
# ruff check .
```

## License

MIT License - See [LICENSE](LICENSE) file for details.
