#!/usr/bin/env python3
"""
Prepare domain data for the validation UI.

Converts domain_scores.csv to JSON format for the browser-based validator.
"""

import csv
import json
import argparse
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_INTERMEDIATE = PROJECT_ROOT / "data" / "intermediate"
VALIDATION_UI = PROJECT_ROOT / "validation_ui"

MAX_EVENTS_PER_DOMAIN = 50


def load_events_by_domain(events_path: Path,
                          domain_set: set[str]) -> dict[str, list[dict]]:
    """Load events grouped by domain, only for domains in domain_set.

    Streams the NDJSON file line-by-line and caps at MAX_EVENTS_PER_DOMAIN
    per domain. Only keeps events for domains we actually need.
    """
    events_by_domain = defaultdict(list)

    if not events_path.exists():
        print(f"Warning: Events file not found: {events_path}")
        return events_by_domain

    with open(events_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                domain = event.get('domain', '')
                if domain not in domain_set:
                    continue
                if len(events_by_domain[domain]) < MAX_EVENTS_PER_DOMAIN:
                    events_by_domain[domain].append({
                        'name': event.get('name', 'Untitled'),
                        'url': event.get('url') or event.get('source_url', ''),
                        'start_date': event.get('start_date', ''),
                        'end_date': event.get('end_date', ''),
                        'location': event.get('location_name', ''),
                        'has_location': event.get('has_location', False),
                        'has_dates': event.get('has_dates', False)
                    })
            except json.JSONDecodeError:
                continue

    return events_by_domain


def load_domain_scores(path: Path) -> list[dict]:
    """Load domain scores from CSV."""
    domains = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get('domain'):
                continue
            domains.append({
                'domain': row['domain'],
                'classification': row.get('classification', 'review'),
                'confidence_score': float(row.get('confidence_score', 0)),
                'total_events': int(row.get('total_events', 0)),
                'gta_events': int(row.get('gta_events', 0)),
                'gta_percentage': float(row.get('gta_percentage', 0)),
                'postal_matches': int(row.get('postal_matches', 0)),
                'coord_matches': int(row.get('coord_matches', 0)),
                'locality_matches': int(row.get('locality_matches', 0)),
                'sample_events': row.get('sample_events', ''),
                'match_reasons': row.get('match_reasons', '')
            })
    return domains


def main():
    parser = argparse.ArgumentParser(description='Prepare domain data for validation UI')
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=DATA_INTERMEDIATE / 'domain_scores.csv',
        help='Path to domain_scores.csv'
    )
    parser.add_argument(
        '--events', '-e',
        type=Path,
        default=DATA_INTERMEDIATE / 'events' / 'extracted_events.ndjson',
        help='Path to extracted_events.ndjson'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=VALIDATION_UI / 'domains.json',
        help='Output path for domains.json'
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}")
        print("Run the scoring pipeline first to generate domain_scores.csv")
        return 1

    print(f"Loading domains from: {args.input}")
    domains = load_domain_scores(args.input)

    # Build domain set so we only load events for known domains
    domain_set = {d['domain'] for d in domains if d.get('domain')}

    print(f"Loading events from: {args.events}")
    events_by_domain = load_events_by_domain(args.events, domain_set)
    print(f"  Loaded events for {len(events_by_domain)} domains")

    # Merge events into domains
    for domain in domains:
        domain['events'] = events_by_domain.get(domain['domain'], [])

    # Free the events dict — no longer needed after merge
    del events_by_domain

    # Sort by confidence score descending within each classification
    domains.sort(key=lambda d: (-d['confidence_score'], d['domain']))

    # Ensure output directory exists
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Write as streaming JSON array to avoid building the full serialized
    # string in memory at once
    print(f"Writing {len(domains)} domains to: {args.output}")
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write('[\n')
        for i, domain in enumerate(domains):
            if i > 0:
                f.write(',\n')
            json.dump(domain, f)
        f.write('\n]')

    # Summary
    by_class = {}
    for d in domains:
        cls = d['classification']
        by_class[cls] = by_class.get(cls, 0) + 1
    
    print("\nSummary:")
    for cls in ['confirmed', 'likely', 'possible', 'review']:
        print(f"  {cls}: {by_class.get(cls, 0)}")
    
    return 0


if __name__ == '__main__':
    exit(main())

