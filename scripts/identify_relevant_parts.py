#!/usr/bin/env python3
"""
Phase 2: Identify Relevant Part Files

Analyze domain signals to determine which part files contain the most
Toronto/GTA-relevant data. Creates a priority download/processing list.
"""

import csv
import argparse
import logging
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass

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
class PartFileStats:
    """Statistics for a single part file."""
    part_file: str
    total_candidates: int = 0
    total_priority_score: float = 0.0
    high_priority_count: int = 0  # score >= 50
    toronto_keyword_count: int = 0
    sample_domains: list = None
    
    def __post_init__(self):
        if self.sample_domains is None:
            self.sample_domains = []
    
    @property
    def avg_priority_score(self) -> float:
        if self.total_candidates == 0:
            return 0.0
        return self.total_priority_score / self.total_candidates


def analyze_part_files(signals_path: Path) -> dict[str, PartFileStats]:
    """
    Analyze domain signals to build part file statistics.
    
    Only counts positive-signal domains for prioritization.
    
    Returns:
        Dict mapping part_file names to PartFileStats
    """
    stats = defaultdict(lambda: PartFileStats(part_file=""))
    
    with open(signals_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Only count positive-signal domains for prioritization
            signal = row.get('signal', 'neutral')
            if signal == 'negative':
                continue
            
            part_file = row['part_file']
            priority_score = float(row.get('score', 0))
            reasons = row.get('reasons', '').split('|')
            domain = row['domain']
            
            if stats[part_file].part_file == "":
                stats[part_file].part_file = part_file
            
            stats[part_file].total_candidates += 1
            stats[part_file].total_priority_score += priority_score
            
            if priority_score >= 50:
                stats[part_file].high_priority_count += 1
            
            if any('keyword:toronto' in r for r in reasons):
                stats[part_file].toronto_keyword_count += 1
            
            # Keep sample of high-scoring domains
            if priority_score >= 40 and len(stats[part_file].sample_domains) < 5:
                stats[part_file].sample_domains.append(domain)
    
    return dict(stats)


def rank_part_files(stats: dict[str, PartFileStats]) -> list[PartFileStats]:
    """
    Rank part files by priority for processing.
    
    Ranking criteria:
    1. High priority domain count (most important)
    2. Toronto keyword count
    3. Total priority score
    """
    ranked = list(stats.values())
    
    ranked.sort(key=lambda s: (
        s.high_priority_count,
        s.toronto_keyword_count,
        s.total_priority_score,
    ), reverse=True)
    
    return ranked


def save_ranking(ranked: list[PartFileStats], output_path: Path):
    """Save part file ranking to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'part_file', 'total_candidates', 'high_priority_count',
            'toronto_keyword_count', 'avg_priority_score', 'sample_domains'
        ])
        
        for s in ranked:
            writer.writerow([
                s.part_file,
                s.total_candidates,
                s.high_priority_count,
                s.toronto_keyword_count,
                f"{s.avg_priority_score:.1f}",
                '|'.join(s.sample_domains[:3]),
            ])
    
    logger.info(f"Saved {len(ranked)} part file rankings to {output_path}")


def print_summary(ranked: list[PartFileStats]):
    """Print summary of part file analysis."""
    print("\n" + "=" * 70)
    print("PART FILE PRIORITY RANKING")
    print("=" * 70)
    
    # Count totals
    total_candidates = sum(s.total_candidates for s in ranked)
    total_high_priority = sum(s.high_priority_count for s in ranked)
    total_toronto = sum(s.toronto_keyword_count for s in ranked)
    
    print(f"\nTotal candidates: {total_candidates:,}")
    print(f"High priority (score >= 50): {total_high_priority:,}")
    print(f"Toronto keyword matches: {total_toronto:,}")
    print(f"Part files with candidates: {len(ranked)}")
    
    # Top 20 part files
    print(f"\n{'Part File':<15} {'Candidates':<12} {'High Pri':<10} {'Toronto':<10} {'Avg Score':<10}")
    print("-" * 60)
    
    for s in ranked[:20]:
        print(f"{s.part_file:<15} {s.total_candidates:<12} {s.high_priority_count:<10} "
              f"{s.toronto_keyword_count:<10} {s.avg_priority_score:<10.1f}")
    
    # Recommended download strategy
    print("\n" + "=" * 70)
    print("RECOMMENDED DOWNLOAD STRATEGY")
    print("=" * 70)
    
    # Parts with high-priority domains
    essential = [s for s in ranked if s.high_priority_count > 0]
    print(f"\n1. ESSENTIAL ({len(essential)} files) - Contains high-priority domains:")
    for s in essential[:10]:
        samples = ', '.join(s.sample_domains[:2])
        print(f"   {s.part_file}: {s.high_priority_count} high-pri domains (e.g., {samples})")
    if len(essential) > 10:
        print(f"   ... and {len(essential) - 10} more")
    
    # Parts with Toronto keywords
    toronto_parts = [s for s in ranked 
                     if s.toronto_keyword_count > 0 and s.high_priority_count == 0]
    print(f"\n2. HIGH VALUE ({len(toronto_parts)} files) - Toronto keyword matches:")
    for s in toronto_parts[:5]:
        print(f"   {s.part_file}: {s.toronto_keyword_count} Toronto domains")
    
    # Parts with only .ca domains
    ca_only = [s for s in ranked 
               if s.toronto_keyword_count == 0 and s.high_priority_count == 0]
    print(f"\n3. EXPLORATORY ({len(ca_only)} files) - Canadian domains only")


def main():
    parser = argparse.ArgumentParser(
        description="Identify and rank relevant part files for processing"
    )
    parser.add_argument(
        '--signals', '-s',
        type=Path,
        default=DATA_INTERMEDIATE / "domain_signals.csv",
        help='Path to domain_signals.csv from Phase 1'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=DATA_INTERMEDIATE / "part_file_priority.csv",
        help='Output path for part file ranking'
    )
    
    args = parser.parse_args()
    
    if not args.signals.exists():
        logger.error(f"Domain signals file not found: {args.signals}")
        logger.info("Run analyze_domains.py first")
        return 1
    
    # Analyze part files
    logger.info(f"Analyzing domain signals from {args.signals}")
    stats = analyze_part_files(args.signals)
    
    # Rank by priority
    ranked = rank_part_files(stats)
    
    # Save and print
    save_ranking(ranked, args.output)
    print_summary(ranked)
    
    return 0


if __name__ == '__main__':
    exit(main())
