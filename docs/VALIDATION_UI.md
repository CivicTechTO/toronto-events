# Validation UI Documentation

The Validation UI is a web-based interface for manually reviewing and validating domain classifications.

## Overview

After running the pipeline, some domains are classified as `UNKNOWN` - they lack clear signals to confidently include or exclude them. The Validation UI helps human reviewers quickly classify these domains.

## Features

- **Card-based Interface**: Review one domain at a time
- **Keyboard Shortcuts**: Fast validation with keyboard
- **Auto-save**: Progress saved to local storage
- **Export**: Export validations to apply back to pipeline
- **Categories**: Confirmed, Likely, Possible, Review

## Getting Started

### 1. Prepare Validation Data

Run the pipeline to generate domains needing review:

```bash
uv run python scripts/prepare_validation_data.py
```

This creates `validation_ui/domains.json` with domains to review.

### 2. Open the UI

Open `validation_ui/index.html` in a modern web browser:

```bash
# On macOS
open validation_ui/index.html

# On Linux
xdg-open validation_ui/index.html

# Or use a local web server (recommended)
python -m http.server 8000
# Then visit http://localhost:8000/validation_ui/
```

### 3. Review Domains

The UI presents each domain with:
- **Domain name**: The website being reviewed
- **Sample events**: Event names from this domain
- **Locations**: Cities/addresses found in events
- **Metadata**: Event count, GTA percentage, confidence score

## Keyboard Shortcuts

- **A**: Accept (mark as Toronto/GTA)
- **R**: Reject (mark as non-Toronto)
- **U**: Uncertain (needs more review)
- **N**: Next (skip to next domain)
- **←/→**: Navigate between tabs

## Classification Categories

### Confirmed (Accept)
Domains that clearly publish Toronto/GTA events:
- Multiple events with Toronto locations
- Known Toronto venues/organizations
- High confidence geo signals

**Examples**:
- `rcmusic.com` (Royal Conservatory of Music)
- `torontoevents.ca`
- `harbourfrontcentre.com`

### Likely (Accept with caveat)
Domains that probably publish Toronto events but need verification:
- Some Toronto events, some unclear
- Generic event platforms with Toronto content
- Low sample size

**Examples**:
- `eventbrite.com` (has Toronto events but also worldwide)
- `ticketmaster.ca` (multi-city but includes Toronto)

### Possible (Uncertain)
Domains that might publish Toronto events:
- Ambiguous location data
- Very low sample size
- Mixed signals

**Examples**:
- Local businesses without clear location data
- Personal blogs mentioning Toronto
- Aggregators with partial data

### Review (Reject or needs more investigation)
Domains that are clearly not Toronto or need deeper review:
- Foreign locations only
- No relevant Toronto content
- Broken/spam domains

**Examples**:
- `londonevents.co.uk`
- `newyorktimes.com`
- Obvious spam domains

## Validation Workflow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Load domains.json (from prepare_validation_data.py) │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Review each domain:                                  │
│    - Read domain name                                   │
│    - Check sample events                                │
│    - Verify locations                                   │
│    - Consider confidence score                          │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│ 3. Classify using keyboard shortcuts:                   │
│    - A: Accept as Toronto/GTA source                    │
│    - R: Reject as non-Toronto                           │
│    - U: Mark as uncertain                               │
│    - N: Skip for now                                    │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│ 4. Auto-save to browser local storage                   │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Export validations.json when complete                │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│ 6. Apply validations to pipeline:                       │
│    uv run python scripts/apply_validations.py           │
└─────────────────────────────────────────────────────────┘
```

## Data Format

### Input: domains.json

```json
[
  {
    "domain": "example.com",
    "confidence": 0.45,
    "event_count": 12,
    "gta_percentage": 67,
    "sample_events": [
      "Summer Concert Series",
      "Toronto Film Festival Event"
    ],
    "sample_locations": [
      "Toronto",
      "123 Queen St, Toronto"
    ],
    "classification": "unknown"
  }
]
```

### Output: validation_state.json (auto-saved)

```json
{
  "example.com": {
    "decision": "accept",
    "timestamp": "2024-01-15T10:30:00Z",
    "notes": ""
  },
  "another.com": {
    "decision": "reject",
    "timestamp": "2024-01-15T10:31:00Z",
    "notes": "Events in London, UK"
  }
}
```

## Tips for Effective Validation

### 1. Check Sample Events
- Do event names mention Toronto or GTA cities?
- Are they generic or location-specific?

### 2. Verify Locations
- Look for postal codes (M*, L* prefixes)
- Check city names (Toronto, Mississauga, etc.)
- Watch for foreign locations

### 3. Consider Context
- Is this a venue, organization, or aggregator?
- Known Toronto institutions → Accept
- Generic platforms → Likely (if has Toronto content)

### 4. Use External Resources
- Visit the domain to verify
- Google the organization name
- Check social media for location

### 5. When in Doubt
- Mark as Uncertain (U)
- Add notes for future review
- Don't force a decision

## Applying Validations

After completing reviews, export and apply:

```bash
# 1. Click "Export Validations" button in UI
#    This downloads validation_state.json

# 2. Move to validation_ui directory
mv ~/Downloads/validation_state.json validation_ui/

# 3. Apply validations to pipeline
uv run python scripts/apply_validations.py
```

This updates `domain_scores.csv` with manual classifications.

## Troubleshooting

### UI doesn't load
- Ensure `domains.json` exists in `validation_ui/`
- Check browser console for errors (F12)
- Try using a local web server instead of file://

### Validations not saving
- Check browser local storage is enabled
- Try a different browser
- Export regularly to avoid data loss

### Missing domains
- Re-run `prepare_validation_data.py`
- Check that pipeline has UNKNOWN classifications
- Verify `domain_scores.csv` exists

### Browser compatibility
- Requires modern browser (Chrome 90+, Firefox 88+, Safari 14+)
- JavaScript must be enabled
- Local storage must be allowed

## Advanced Usage

### Filtering Domains

Edit `prepare_validation_data.py` to customize which domains appear:

```python
# Only show high-event-count domains
if domain_score.total_events < 10:
    continue

# Only show specific confidence range
if not (0.3 <= domain_score.confidence_score <= 0.7):
    continue
```

### Customizing UI

Files to edit:
- `index.html`: Structure and layout
- `styles.css`: Visual styling
- `app.js`: Logic and keyboard shortcuts

### Batch Validation

For many domains, consider:
1. Sort by confidence score
2. Review highest/lowest first
3. Use keyboard shortcuts for speed
4. Export after each session

## Integration with Pipeline

The validation workflow integrates with the pipeline:

```
Pipeline → manual_review_queue.csv → prepare_validation_data.py 
   → validation_ui → validation_state.json → apply_validations.py 
   → Updated domain_scores.csv → generate_outputs.py → Final outputs
```

This allows human judgment to override automated classification where needed.
