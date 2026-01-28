# Contributing to Toronto Events

Thank you for your interest in contributing to the Toronto Events project! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Code Style](#code-style)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)

## Code of Conduct

This project is part of CivicTechTO. We are committed to providing a welcoming and inclusive environment for all contributors. Please be respectful and constructive in your interactions.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/toronto-events.git
   cd toronto-events
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/CivicTechTO/toronto-events.git
   ```

## Development Setup

This project uses [`uv`](https://github.com/astral-sh/uv) for fast dependency management.

```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync

# Verify installation
uv run python --version
```

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with:
- A clear, descriptive title
- Steps to reproduce the problem
- Expected vs. actual behavior
- Your environment (OS, Python version, etc.)
- Any relevant logs or error messages

### Suggesting Enhancements

We welcome suggestions! Please create an issue with:
- A clear description of the enhancement
- Use cases and benefits
- Any implementation ideas (optional)

### Code Contributions

1. **Create a branch** for your work:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write clear, readable code
   - Add comments for complex logic
   - Update documentation as needed

3. **Test your changes**:
   - Run the pipeline on a subset of data
   - Verify scripts work as expected
   - Check for any breaking changes

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Clear, concise commit message"
   ```

## Code Style

- **Python**: Follow PEP 8 guidelines
- **Naming**: Use descriptive variable and function names
- **Documentation**: Add docstrings to functions and classes
- **Comments**: Explain the "why", not just the "what"
- **Line length**: Aim for 100 characters max, 120 absolute max

### Example

```python
def calculate_geo_score(latitude: float, longitude: float) -> float:
    """
    Calculate geographic relevance score for Toronto/GTA.
    
    Args:
        latitude: Event latitude coordinate
        longitude: Event longitude coordinate
    
    Returns:
        Score from 0.0 to 1.0, where 1.0 is within Toronto core
    """
    # Implementation here
    pass
```

## Testing

While we don't have automated tests yet, please verify your changes:

1. **Run on sample data**:
   ```bash
   uv run python scripts/run_pipeline.py --limit 100
   ```

2. **Check specific scripts**:
   ```bash
   uv run python scripts/your_modified_script.py --help
   ```

3. **Validate outputs**: Ensure generated files are in expected format

## Submitting Changes

1. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a Pull Request** on GitHub with:
   - Clear title describing the change
   - Description of what changed and why
   - Reference to any related issues
   - Screenshots if UI changes

3. **Respond to feedback**:
   - Address review comments
   - Update your PR as needed
   - Keep discussions constructive

## Project Structure

```
toronto-events/
├── src/toronto_events/    # Reusable Python modules
│   ├── core/             # Core functionality (parsers, filters)
│   ├── pipeline/         # Pipeline orchestration
│   └── utils/            # Utility functions
├── scripts/              # Executable pipeline scripts
├── validation_ui/        # Web interface for manual validation
├── docs/                 # Additional documentation
├── examples/             # Usage examples and tutorials
└── data/                 # Data directory (mostly .gitignored)
```

## Questions?

- Open an issue for questions about the project
- Join CivicTechTO meetings to connect with the team
- Check existing documentation in the `docs/` directory

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
