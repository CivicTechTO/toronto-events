#!/usr/bin/env python3
"""
Phase 1: Domain-Level Filtering

Filter Web Data Commons Event domains to identify Toronto/GTA candidates
using only the metadata files (no full data download required).

Filtering strategies:
1. Canadian TLD (.ca domains)
2. Toronto/GTA keywords in domain name
3. Known Toronto institution domains
"""

import csv
import re
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field
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
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_INTERMEDIATE = PROJECT_ROOT / "data" / "intermediate"

# Toronto/GTA keywords to search for in domain names
TORONTO_KEYWORDS = {
    'toronto', 'gta', 'yyz', 'the6ix', '6ix',
}

# GTA municipality names
GTA_CITIES = {
    'mississauga', 'brampton', 'vaughan', 'markham', 'richmondhill',
    'scarborough', 'etobicoke', 'northyork', 'eastyork',
    'oakville', 'burlington', 'hamilton', 'pickering', 'ajax',
    'whitby', 'oshawa', 'newmarket', 'aurora', 'kingcity',
    'caledon', 'milton', 'halton', 'peel', 'york', 'durham',
}

# Known Toronto institutions and venues (curated list)
# These are high-value domains we want to capture even without keyword matching
KNOWN_TORONTO_DOMAINS = {
    # Government
    'toronto.ca',
    'ontario.ca',
    
    # Universities
    'utoronto.ca',
    'yorku.ca',
    'ryerson.ca',  # Now TMU
    'torontomu.ca',
    'senecacollege.ca',
    'georgebrown.ca',
    'humber.ca',
    'centennialcollege.ca',
    
    # Major venues
    'rcmusic.com',  # Roy Thomson Hall
    'masseyhall.com',
    'theex.com',  # CNE
    'rogerscentre.com',
    'scotiabankarena.com',
    'budweiserstage.com',
    
    # Cultural institutions
    'ago.ca',  # Art Gallery of Ontario
    'rom.on.ca',  # Royal Ontario Museum
    'tiff.net',  # Toronto Film Festival
    'ontarioplace.com',
    'harbourfrontcentre.com',
    'nfrb.ca',  # National Ballet
    'coc.ca',  # Canadian Opera Company
    
    # Sports
    'mlse.com',
    'nhl.com/mapleleafs',
    'rfraptors.com',
    'bluejays.com',
    'torontofc.ca',
    'argonauts.ca',
    
    # Events / Festivals
    'caribana.com',
    'pride toronto.com',
    'luminatofestival.com',
    'salsaintoronto.com',
    'nuitblanche.com',
    'tasteoftoronto.com',
    'junctionartsfestival.com',
}

# Provinces/regions that might indicate Canadian but not Toronto
NON_GTA_CANADIAN_REGIONS = {
    'vancouver', 'calgary', 'edmonton', 'winnipeg', 
    'montreal', 'quebec', 'ottawa', 'halifax',
    'victoria', 'saskatoon', 'regina',
}


@dataclass
class DomainCandidate:
    """Represents a candidate domain for Toronto events."""
    domain: str
    tld: str
    part_file: str
    match_reasons: list = field(default_factory=list)
    priority_score: float = 0.0
    
    def add_reason(self, reason: str, score_boost: float = 0.0):
        """Add a match reason and boost priority score."""
        self.match_reasons.append(reason)
        self.priority_score += score_boost


def normalize_domain(domain: str) -> str:
    """Normalize domain for keyword matching."""
    # Remove common prefixes and TLD
    domain = domain.lower()
    domain = re.sub(r'^(www\.|m\.)', '', domain)
    # Remove dots and hyphens for keyword matching
    return re.sub(r'[.\-]', '', domain)


def segment_domain(domain: str) -> list[str]:
    """
    Segment a domain name into words.
    e.g., 'hamiltonhealthsciences.ca' -> ['hamilton', 'health', 'sciences', 'ca']
    """
    # Remove TLD and common prefixes
    domain = domain.lower()
    domain = re.sub(r'^(www\.|m\.)', '', domain)
    
    # Split on dots and hyphens first
    parts = re.split(r'[.\-]', domain)
    
    # For each part, try to segment camelCase or concatenated words
    # This is a simple heuristic - split on common word boundaries
    words = []
    for part in parts:
        # Try to find word boundaries by looking for known keywords
        remaining = part
        found_words = []
        
        # Check for known city/keywords first (longer matches first)
        all_keywords = sorted(
            list(TORONTO_KEYWORDS) + list(GTA_CITIES) + list(NON_GTA_CANADIAN_REGIONS),
            key=len, reverse=True
        )
        
        for kw in all_keywords:
            if kw in remaining:
                # Only match if it's at a word boundary (start/end or surrounded by other chars)
                idx = remaining.find(kw)
                if idx != -1:
                    found_words.append(kw)
                    remaining = remaining[:idx] + remaining[idx+len(kw):]
        
        words.extend(found_words)
        # Add any remaining part
        if remaining:
            words.append(remaining)
    
    return words


def check_toronto_keywords(domain: str) -> list[str]:
    """Check if domain contains Toronto-related keywords."""
    # Get segmented words from domain
    words = segment_domain(domain)
    matches = []
    
    for keyword in TORONTO_KEYWORDS:
        if keyword in words:
            matches.append(f"keyword:{keyword}")
    
    for city in GTA_CITIES:
        if city in words:
            matches.append(f"gta_city:{city}")
    
    return matches


def check_known_domain(domain: str) -> bool:
    """Check if domain is in the known Toronto institutions list."""
    domain_lower = domain.lower()
    
    # Exact match
    if domain_lower in KNOWN_TORONTO_DOMAINS:
        return True
    
    # Check if it's a subdomain of a known domain
    for known in KNOWN_TORONTO_DOMAINS:
        if domain_lower.endswith('.' + known):
            return True
    
    return False


def check_non_toronto_canadian(domain: str) -> bool:
    """Check if domain appears to be Canadian but NOT Toronto."""
    normalized = normalize_domain(domain)
    
    for region in NON_GTA_CANADIAN_REGIONS:
        if region in normalized:
            return True
    
    return False


def calculate_priority_score(candidate: DomainCandidate) -> float:
    """Calculate priority score for ranking candidates."""
    score = 0.0
    
    # Known domain is highest priority
    if any('known_institution' in r for r in candidate.match_reasons):
        score += 100.0
    
    # Direct Toronto keyword match
    if any('keyword:toronto' in r for r in candidate.match_reasons):
        score += 50.0
    
    # GTA city in domain name
    gta_city_matches = [r for r in candidate.match_reasons if r.startswith('gta_city:')]
    score += len(gta_city_matches) * 30.0
    
    # Other Toronto keywords (gta, yyz, etc.)
    other_keywords = [r for r in candidate.match_reasons 
                      if r.startswith('keyword:') and 'toronto' not in r]
    score += len(other_keywords) * 20.0
    
    # Canadian TLD (moderate boost, but lower than direct matches)
    if any('canadian_tld' in r for r in candidate.match_reasons):
        score += 10.0
    
    # Penalty for non-GTA Canadian signals
    if any('non_gta_canadian' in r for r in candidate.match_reasons):
        score -= 50.0
    
    return max(score, 0.0)


def filter_domains(lookup_path: Path, stats_path: Path = None) -> list[DomainCandidate]:
    """
    Filter domains from the lookup CSV to find Toronto/GTA candidates.
    
    Args:
        lookup_path: Path to Event_lookup.csv
        stats_path: Optional path to Event_domain_stats.csv for additional info
    
    Returns:
        List of DomainCandidate objects
    """
    candidates = []
    stats = defaultdict(dict)
    
    # Load domain stats if available
    if stats_path and stats_path.exists():
        logger.info(f"Loading domain stats from {stats_path}")
        with open(stats_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 3:
                    domain = row[0]
                    stats[domain] = {
                        'quads': int(row[1]) if row[1].isdigit() else 0,
                        'entities': int(row[2]) if row[2].isdigit() else 0,
                    }
    
    # Process lookup file
    logger.info(f"Processing {lookup_path}")
    
    total_domains = 0
    canadian_domains = 0
    
    with open(lookup_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        for row in reader:
            if len(row) < 3:
                continue
            
            domain, tld, part_file = row[0], row[1], row[2]
            total_domains += 1
            
            candidate = None
            
            # Strategy 1: Canadian TLD
            if tld == 'ca':
                canadian_domains += 1
                candidate = DomainCandidate(domain=domain, tld=tld, part_file=part_file)
                candidate.add_reason('canadian_tld')
                
                # Check if it's a non-GTA Canadian domain
                if check_non_toronto_canadian(domain):
                    candidate.add_reason('non_gta_canadian')
            
            # Strategy 2: Toronto/GTA keywords (any TLD)
            keyword_matches = check_toronto_keywords(domain)
            if keyword_matches:
                if candidate is None:
                    candidate = DomainCandidate(domain=domain, tld=tld, part_file=part_file)
                for match in keyword_matches:
                    candidate.add_reason(match)
            
            # Strategy 3: Known institutions
            if check_known_domain(domain):
                if candidate is None:
                    candidate = DomainCandidate(domain=domain, tld=tld, part_file=part_file)
                candidate.add_reason('known_institution')
            
            # If we have a candidate, calculate score and add to list
            if candidate:
                candidate.priority_score = calculate_priority_score(candidate)
                
                # Add stats if available
                if domain in stats:
                    candidate.entity_count = stats[domain].get('entities', 0)
                
                candidates.append(candidate)
    
    logger.info(f"Processed {total_domains:,} total domains")
    logger.info(f"Found {canadian_domains:,} Canadian (.ca) domains")
    logger.info(f"Selected {len(candidates):,} candidate domains")
    
    # Sort by priority score (descending)
    candidates.sort(key=lambda c: c.priority_score, reverse=True)
    
    return candidates


def save_candidates(candidates: list[DomainCandidate], output_path: Path):
    """Save candidate domains to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'domain', 'tld', 'part_file', 'priority_score', 
            'match_reasons', 'entity_count'
        ])
        
        for c in candidates:
            writer.writerow([
                c.domain,
                c.tld,
                c.part_file,
                f"{c.priority_score:.1f}",
                '|'.join(c.match_reasons),
                getattr(c, 'entity_count', ''),
            ])
    
    logger.info(f"Saved {len(candidates)} candidates to {output_path}")


def print_summary(candidates: list[DomainCandidate]):
    """Print summary of candidate domains."""
    print("\n" + "=" * 60)
    print("CANDIDATE DOMAIN SUMMARY")
    print("=" * 60)
    
    # Count by match reason
    reason_counts = defaultdict(int)
    for c in candidates:
        for reason in c.match_reasons:
            reason_type = reason.split(':')[0]
            reason_counts[reason_type] += 1
    
    print("\nMatch reasons breakdown:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count:,}")
    
    # Top candidates
    print(f"\nTop 20 candidates by priority score:")
    for c in candidates[:20]:
        reasons = ', '.join(c.match_reasons[:3])
        if len(c.match_reasons) > 3:
            reasons += f" (+{len(c.match_reasons) - 3} more)"
        print(f"  {c.priority_score:6.1f}  {c.domain}  [{reasons}]")
    
    # Distribution by TLD
    tld_counts = defaultdict(int)
    for c in candidates:
        tld_counts[c.tld] += 1
    
    print(f"\nTop TLDs:")
    for tld, count in sorted(tld_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  .{tld}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Filter domains for Toronto/GTA event candidates"
    )
    parser.add_argument(
        '--lookup', '-l',
        type=Path,
        default=DATA_RAW / "Event_lookup.csv",
        help='Path to Event_lookup.csv'
    )
    parser.add_argument(
        '--stats', '-s',
        type=Path,
        default=DATA_RAW / "Event_domain_stats.csv",
        help='Path to Event_domain_stats.csv'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=DATA_INTERMEDIATE / "candidate_domains.csv",
        help='Output path for candidate domains'
    )
    parser.add_argument(
        '--min-score',
        type=float,
        default=0.0,
        help='Minimum priority score to include'
    )
    
    args = parser.parse_args()
    
    # Check input files exist
    if not args.lookup.exists():
        logger.error(f"Lookup file not found: {args.lookup}")
        logger.info("Run download_wdc_events.py --metadata-only to download")
        return 1
    
    # Filter domains
    candidates = filter_domains(
        lookup_path=args.lookup,
        stats_path=args.stats if args.stats.exists() else None
    )
    
    # Filter by minimum score
    if args.min_score > 0:
        candidates = [c for c in candidates if c.priority_score >= args.min_score]
        logger.info(f"Filtered to {len(candidates)} candidates with score >= {args.min_score}")
    
    # Save and print summary
    save_candidates(candidates, args.output)
    print_summary(candidates)
    
    return 0


if __name__ == '__main__':
    exit(main())
