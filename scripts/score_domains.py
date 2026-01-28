#!/usr/bin/env python3
"""
Phase 5: Domain Scoring & Classification

Score and classify domains with tri-state output:
  - INCLUDE: Toronto domain signals, Toronto geo, or Toronto text
  - EXCLUDE: Explicitly non-Toronto geo data
  - UNKNOWN: No clear signals (needs manual review)

Domain signals from Phase 1 boost confidence but geo is authoritative.
"""

import csv
import orjson  # Faster than stdlib json
import argparse
import logging
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from toronto_events.core.geo_filter import GeoFilter

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


@dataclass
class DomainScore:
    """Scoring data for a single domain."""
    domain: str
    total_events: int = 0
    gta_events: int = 0
    
    # Location signal counts
    postal_code_matches: int = 0
    coordinate_matches: int = 0
    locality_matches: int = 0
    region_only_matches: int = 0
    events_with_location: int = 0
    events_with_dates: int = 0
    
    # Sample data
    sample_event_names: list = field(default_factory=list)
    sample_localities: set = field(default_factory=set)
    match_reasons: set = field(default_factory=set)
    
    # Computed scores
    confidence_score: float = 0.0
    classification: str = "unclassified"
    
    @property
    def gta_percentage(self) -> float:
        if self.total_events == 0:
            return 0.0
        return (self.gta_events / self.total_events) * 100
    
    @property
    def has_strong_location_signal(self) -> bool:
        """Has at least one strong location signal (postal/coord)."""
        return self.postal_code_matches > 0 or self.coordinate_matches > 0
    
    def compute_classification(self, domain_signal: str = 'neutral', domain_priority: float = 0.0):
        """
        Compute confidence score and tri-state classification.
        
        Classification rules:
          - INCLUDE: positive domain signal, OR Toronto geo match
          - EXCLUDE: has geo data but 0% Toronto matches
          - UNKNOWN: no clear signals either way
        
        Args:
            domain_signal: Signal from Phase 1 ('positive', 'neutral')
            domain_priority: Priority score from Phase 1 (domain name analysis)
        """
        score = 0.0
        
        # Strong location signals
        if self.postal_code_matches > 0:
            score += min(0.30, self.postal_code_matches * 0.10)
        if self.coordinate_matches > 0:
            score += min(0.30, self.coordinate_matches * 0.10)
        
        # Locality matches
        if self.locality_matches > 0:
            score += min(0.20, self.locality_matches * 0.05)
        
        # GTA percentage
        if self.total_events > 0:
            gta_pct = self.gta_events / self.total_events
            score += gta_pct * 0.20
        
        # Event volume (more events = more confidence)
        if self.gta_events >= 10:
            score += 0.10
        elif self.gta_events >= 5:
            score += 0.05
        
        # Domain name signal boost from Phase 1
        if domain_priority >= 50:
            score += 0.10
        elif domain_priority >= 20:
            score += 0.05
        
        self.confidence_score = min(1.0, score)
        
        # Tri-state classification
        has_geo_data = self.events_with_location > 0
        has_toronto_geo = self.gta_events > 0
        has_positive_domain_signal = domain_signal == 'positive'
        
        # INCLUDE: domain signal OR geo confirms Toronto
        if has_positive_domain_signal or has_toronto_geo:
            if self.confidence_score >= 0.5 and self.has_strong_location_signal:
                self.classification = "include_confirmed"
            elif self.confidence_score >= 0.3:
                self.classification = "include_likely"
            else:
                self.classification = "include_possible"
        # EXCLUDE: has geo data but nothing matches Toronto
        elif has_geo_data and self.total_events >= 3 and self.gta_percentage == 0:
            self.classification = "exclude"
        # UNKNOWN: not enough data to decide
        else:
            self.classification = "unknown"


class DomainScorer:
    """Score domains based on their event content."""
    
    def __init__(self, domain_signals: dict[str, tuple[str, float]] = None):
        """
        Initialize scorer.
        
        Args:
            domain_signals: Dict mapping domain -> (signal, priority_score) from Phase 1
        """
        self.domain_signals = domain_signals or {}
        self.geo_filter = GeoFilter()
        self.domain_scores = {}
    
    def process_events(self, events_path: Path):
        """
        Process events file and score domains.
        
        Args:
            events_path: Path to extracted_events.ndjson
        """
        logger.info(f"Processing events from {events_path}")
        
        domain_data = defaultdict(lambda: DomainScore(domain=""))
        
        # Count lines for progress bar
        with open(events_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        with open(events_path, 'r', encoding='utf-8') as f:
            iterator = f
            if tqdm:
                iterator = tqdm(f, total=total_lines, desc="Scoring events",
                               unit="events", dynamic_ncols=True)
            
            for line in iterator:
                if not line.strip():
                    continue
                
                event = orjson.loads(line)
                domain = event.get('domain', '').lower()
                
                if not domain:
                    continue
                
                if domain_data[domain].domain == "":
                    domain_data[domain].domain = domain
                
                score = domain_data[domain]
                score.total_events += 1
                
                # Check for location
                if event.get('location'):
                    score.events_with_location += 1
                
                # Check for dates
                if event.get('start_date') or event.get('end_date'):
                    score.events_with_dates += 1
                
                # Geo-filter the event
                geo_result = self.geo_filter.filter_event(event)
                
                if geo_result.confidence >= 0.3:
                    score.gta_events += 1
                    score.match_reasons.add(geo_result.match_reason)
                    
                    # Track match type
                    if 'postal_code' in geo_result.match_reason:
                        score.postal_code_matches += 1
                    elif 'coordinates' in geo_result.match_reason:
                        score.coordinate_matches += 1
                    elif 'locality' in geo_result.match_reason:
                        score.locality_matches += 1
                    elif 'region' in geo_result.match_reason:
                        score.region_only_matches += 1
                    
                    # Track locality
                    if geo_result.match_details.get('locality'):
                        score.sample_localities.add(geo_result.match_details['locality'])
                
                # Sample event names
                event_name = event.get('name', '')
                if event_name and len(score.sample_event_names) < 3:
                    # Clean up name
                    name = event_name[:80].replace('\n', ' ').strip()
                    if name and name not in score.sample_event_names:
                        score.sample_event_names.append(name)
        
        # Compute classifications
        for domain, score in domain_data.items():
            signal, priority = self.domain_signals.get(domain, ('neutral', 0.0))
            score.compute_classification(signal, priority)
        
        self.domain_scores = dict(domain_data)
        logger.info(f"Scored {len(self.domain_scores)} domains")
    
    def get_ranked_domains(self, min_confidence: float = 0.0) -> list[DomainScore]:
        """Get domains ranked by confidence score."""
        domains = [s for s in self.domain_scores.values() 
                   if s.confidence_score >= min_confidence]
        domains.sort(key=lambda s: (s.confidence_score, s.gta_events), reverse=True)
        return domains


def load_domain_signals(path: Path) -> dict[str, tuple[str, float]]:
    """Load domain signals and scores from Phase 1."""
    signals = {}
    
    if not path.exists():
        return signals
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = row['domain'].lower()
            signal = row.get('signal', 'neutral')
            try:
                priority = float(row.get('score', 0))
            except ValueError:
                priority = 0.0
            signals[domain] = (signal, priority)
    
    return signals


def save_rankings(domains: list[DomainScore], output_path: Path):
    """Save domain rankings to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'domain', 'classification', 'confidence_score',
            'total_events', 'gta_events', 'gta_percentage',
            'postal_matches', 'coord_matches', 'locality_matches',
            'sample_events', 'match_reasons'
        ])
        
        for s in domains:
            writer.writerow([
                s.domain,
                s.classification,
                f"{s.confidence_score:.2f}",
                s.total_events,
                s.gta_events,
                f"{s.gta_percentage:.1f}",
                s.postal_code_matches,
                s.coordinate_matches,
                s.locality_matches,
                ' | '.join(s.sample_event_names[:2]),
                ' | '.join(list(s.match_reasons)[:3]),
            ])
    
    logger.info(f"Saved {len(domains)} domain rankings to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Score and classify domains by Toronto/GTA event content"
    )
    parser.add_argument(
        '--events', '-e',
        type=Path,
        default=DATA_INTERMEDIATE / "events" / "extracted_events.ndjson",
        help='Path to extracted events file'
    )
    parser.add_argument(
        '--signals', '-s',
        type=Path,
        default=DATA_INTERMEDIATE / "domain_signals.csv",
        help='Path to domain signals from Phase 1'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=DATA_INTERMEDIATE / "domain_scores.csv",
        help='Output path for domain scores'
    )
    parser.add_argument(
        '--min-confidence',
        type=float,
        default=0.0,
        help='Minimum confidence to include in output'
    )
    
    args = parser.parse_args()
    
    if not args.events.exists():
        logger.error(f"Events file not found: {args.events}")
        return 1
    
    # Load domain signals
    signals = load_domain_signals(args.signals)
    logger.info(f"Loaded {len(signals)} domain signals")
    
    # Score domains
    scorer = DomainScorer(domain_signals=signals)
    scorer.process_events(args.events)
    
    # Get rankings
    ranked = scorer.get_ranked_domains(min_confidence=args.min_confidence)
    
    # Save
    save_rankings(ranked, args.output)
    
    # Summary
    print("\n" + "=" * 60)
    print("DOMAIN SCORING SUMMARY")
    print("=" * 60)
    
    # Count by classification
    classifications = defaultdict(int)
    for d in ranked:
        classifications[d.classification] += 1
    
    print("\nClassification breakdown:")
    for cls in ['include_confirmed', 'include_likely', 'include_possible', 'exclude', 'unknown']:
        count = classifications.get(cls, 0)
        print(f"  {cls}: {count}")
    
    # Top domains
    print(f"\nTop 15 domains by confidence:")
    print(f"{'Domain':<35} {'Class':<12} {'Conf':<6} {'GTA':<6} {'Total':<6}")
    print("-" * 70)
    
    for d in ranked[:15]:
        print(f"{d.domain[:34]:<35} {d.classification:<12} "
              f"{d.confidence_score:.2f}  {d.gta_events:<6} {d.total_events:<6}")
    
    return 0


if __name__ == '__main__':
    exit(main())
