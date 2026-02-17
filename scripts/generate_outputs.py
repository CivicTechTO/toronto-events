#!/usr/bin/env python3
"""
Phase 6: Generate Final Outputs

Generate the final output files for the Toronto event source pipeline:
1. toronto_event_sources.csv - Ranked list of Toronto event sources
2. toronto_event_samples.ndjson - Sample events from each domain
3. manual_review_queue.csv - Domains needing human review
"""

import csv
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_INTERMEDIATE = PROJECT_ROOT / "data" / "intermediate"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"


def load_domain_scores(path: Path) -> list[dict]:
    """Load domain scores from CSV."""
    domains = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            domains.append(row)
    return domains


def _score_event(event: dict) -> int:
    """Score an event for sample selection (higher = better)."""
    score = 0
    if event.get('location'):
        score += 2
    if event.get('start_date'):
        score += 1
    if event.get('name'):
        score += 1
    return score


def load_event_samples(path: Path, domain_set: set[str],
                       samples_per_domain: int = 3) -> dict[str, list[dict]]:
    """Stream events and keep only the best N samples per domain.

    Instead of loading all events into memory, this streams line-by-line
    and maintains a small bounded buffer per domain.
    """
    domain_samples: dict[str, list[tuple[int, dict]]] = defaultdict(list)

    if not path.exists():
        return {}

    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            event = json.loads(line)
            domain = event.get('domain', '').lower()

            if domain not in domain_set:
                continue

            score = _score_event(event)
            buf = domain_samples[domain]

            if len(buf) < samples_per_domain:
                buf.append((score, event))
            else:
                # Replace the lowest-scored sample if this one is better
                min_idx = min(range(len(buf)), key=lambda i: buf[i][0])
                if score > buf[min_idx][0]:
                    buf[min_idx] = (score, event)

    # Strip scores and sort best-first
    return {
        domain: [ev for _, ev in sorted(buf, key=lambda x: x[0], reverse=True)]
        for domain, buf in domain_samples.items()
    }


def generate_event_sources(domains: list[dict], output_path: Path):
    """Generate toronto_event_sources.csv - the primary deliverable."""
    confirmed = [d for d in domains if d['classification'] == 'confirmed']
    likely = [d for d in domains if d['classification'] == 'likely']
    possible = [d for d in domains if d['classification'] == 'possible']
    
    # Combine in order of classification
    ranked = confirmed + likely + possible
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'rank', 'domain', 'classification', 'confidence_score',
            'gta_events', 'total_events', 'gta_percentage',
            'location_signals', 'sample_events'
        ])
        
        for i, d in enumerate(ranked, 1):
            # Summarize location signals
            signals = []
            if int(d.get('postal_matches', 0)) > 0:
                signals.append(f"postal:{d['postal_matches']}")
            if int(d.get('coord_matches', 0)) > 0:
                signals.append(f"coord:{d['coord_matches']}")
            if int(d.get('locality_matches', 0)) > 0:
                signals.append(f"locality:{d['locality_matches']}")
            
            writer.writerow([
                i,
                d['domain'],
                d['classification'],
                d['confidence_score'],
                d['gta_events'],
                d['total_events'],
                d['gta_percentage'],
                ', '.join(signals) or 'domain_name_only',
                d.get('sample_events', '')[:100],
            ])
    
    logger.info(f"Saved {len(ranked)} event sources to {output_path}")
    return len(ranked)


def generate_event_samples(domains: list[dict], event_samples: dict[str, list[dict]],
                          output_path: Path):
    """Generate toronto_event_samples.ndjson - sample events for testing."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample_count = 0

    with open(output_path, 'w', encoding='utf-8') as f:
        for d in domains:
            if d['classification'] in ('confirmed', 'likely', 'possible'):
                domain = d['domain'].lower()

                for event in event_samples.get(domain, []):
                    sample = {
                        'domain': domain,
                        'classification': d['classification'],
                        'event_name': event.get('name'),
                        'start_date': event.get('start_date'),
                        'end_date': event.get('end_date'),
                        'source_url': event.get('source_url'),
                    }

                    # Add location if present
                    if event.get('location'):
                        loc = event['location']
                        sample['location'] = {
                            'name': loc.get('name'),
                            'locality': loc.get('address_locality'),
                            'region': loc.get('address_region'),
                            'postal_code': loc.get('postal_code'),
                        }

                    f.write(json.dumps(sample) + '\n')
                    sample_count += 1

    logger.info(f"Saved {sample_count} event samples to {output_path}")
    return sample_count


def generate_review_queue(domains: list[dict], output_path: Path):
    """Generate manual_review_queue.csv - domains needing human review."""
    review = [d for d in domains if d['classification'] == 'review']
    
    # Sort by total events (larger domains first as they're more important)
    review.sort(key=lambda d: int(d.get('total_events', 0)), reverse=True)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'domain', 'total_events', 'gta_events', 'confidence_score',
            'review_reason', 'sample_events', 'recommended_action'
        ])
        
        for d in review:
            # Determine review reason
            reasons = []
            if int(d.get('total_events', 0)) > 10:
                reasons.append("high_volume")
            if float(d.get('confidence_score', 0)) > 0:
                reasons.append("partial_match")
            if not reasons:
                reasons.append("low_signal")
            
            # Recommend action
            if float(d.get('gta_percentage', 0)) > 0:
                action = "investigate"
            else:
                action = "likely_exclude"
            
            writer.writerow([
                d['domain'],
                d['total_events'],
                d['gta_events'],
                d['confidence_score'],
                ', '.join(reasons),
                d.get('sample_events', '')[:80],
                action,
            ])
    
    logger.info(f"Saved {len(review)} domains to review queue at {output_path}")
    return len(review)


def main():
    parser = argparse.ArgumentParser(
        description="Generate final output files"
    )
    parser.add_argument(
        '--scores', '-s',
        type=Path,
        default=DATA_INTERMEDIATE / "domain_scores.csv",
        help='Path to domain scores from Phase 5'
    )
    parser.add_argument(
        '--events', '-e',
        type=Path,
        default=DATA_INTERMEDIATE / "events" / "extracted_events.ndjson",
        help='Path to extracted events'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        default=DATA_PROCESSED,
        help='Output directory for final files'
    )
    
    args = parser.parse_args()
    
    if not args.scores.exists():
        logger.error(f"Domain scores not found: {args.scores}")
        logger.info("Run score_domains.py first")
        return 1
    
    # Load data
    logger.info("Loading domain scores...")
    domains = load_domain_scores(args.scores)

    # Build set of relevant domains for bounded event sampling
    relevant_domains = {
        d['domain'].lower() for d in domains
        if d['classification'] in ('confirmed', 'likely', 'possible')
    }

    logger.info("Streaming events (keeping best 3 samples per domain)...")
    event_samples = load_event_samples(args.events, relevant_domains,
                                       samples_per_domain=3)

    # Generate outputs
    sources_count = generate_event_sources(
        domains,
        args.output_dir / "toronto_event_sources.csv"
    )

    samples_count = generate_event_samples(
        domains, event_samples,
        args.output_dir / "toronto_event_samples.ndjson"
    )
    
    review_count = generate_review_queue(
        domains,
        args.output_dir / "manual_review_queue.csv"
    )
    
    # Summary
    print("\n" + "=" * 60)
    print("OUTPUT GENERATION SUMMARY")
    print("=" * 60)
    print(f"\nGenerated {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"\nOutput directory: {args.output_dir}")
    print(f"\nFiles created:")
    print(f"  1. toronto_event_sources.csv  - {sources_count} ranked domains")
    print(f"  2. toronto_event_samples.ndjson - {samples_count} sample events")
    print(f"  3. manual_review_queue.csv    - {review_count} domains to review")
    
    # Classification summary
    by_class = defaultdict(int)
    for d in domains:
        by_class[d['classification']] += 1
    
    print(f"\nDomain classifications:")
    for cls in ['confirmed', 'likely', 'possible', 'review']:
        print(f"  {cls}: {by_class[cls]}")
    
    return 0


if __name__ == '__main__':
    exit(main())
