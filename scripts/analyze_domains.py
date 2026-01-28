#!/usr/bin/env python3
"""
Phase 1: Domain Signal Analysis

Analyze Web Data Commons Event domains for Toronto/GTA signals.
Produces a tri-state classification:
  - positive: Domain signals suggest Toronto relevance
  - negative: Domain signals suggest non-Toronto (foreign regional TLD)
  - neutral: No clear signal from domain name (needs geo verification)

This replaces the old filter_domains.py which excluded domains outright.
"""

import csv
import regex as re  # Faster than stdlib re
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

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


class DomainSignal(Enum):
    """Tri-state domain signal classification."""
    POSITIVE = "positive"  # Toronto-related signals
    NEGATIVE = "negative"  # Foreign regional TLD (exclude)
    NEUTRAL = "neutral"    # Generic TLD, needs geo verification


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
KNOWN_TORONTO_DOMAINS = {
    # Government
    'toronto.ca',
    'ontario.ca',
    
    # Universities
    'utoronto.ca',
    'yorku.ca',
    'ryerson.ca',
    'torontomu.ca',
    'senecacollege.ca',
    'georgebrown.ca',
    'humber.ca',
    'centennialcollege.ca',
    
    # Major venues
    'rcmusic.com',
    'masseyhall.com',
    'theex.com',
    'rogerscentre.com',
    'scotiabankarena.com',
    'budweiserstage.com',
    
    # Cultural institutions
    'ago.ca',
    'rom.on.ca',
    'tiff.net',
    'ontarioplace.com',
    'harbourfrontcentre.com',
    'nfrb.ca',
    'coc.ca',
    
    # Sports
    'mlse.com',
    'bluejays.com',
    'torontofc.ca',
    'argonauts.ca',
    
    # Events / Festivals
    'caribana.com',
    'luminatofestival.com',
    'nuitblanche.com',
    'tasteoftoronto.com',
}

# Foreign regional TLDs that indicate non-Canadian location
# These get a NEGATIVE signal - exclude early
FOREIGN_REGIONAL_TLDS = {
    # Country-code TLDs for non-Canadian countries
    'co.nz', 'co.uk', 'com.au', 'co.za', 'co.jp', 'co.in', 'co.kr',
    'com.br', 'com.mx', 'com.ar', 'com.cn', 'com.tw', 'com.hk',
    'com.sg', 'com.my', 'com.ph', 'com.pk', 'com.tr', 'com.ua',
    # European country TLDs
    'de', 'fr', 'it', 'es', 'nl', 'be', 'at', 'ch', 'pl', 'cz',
    'se', 'no', 'dk', 'fi', 'pt', 'gr', 'ie', 'hu', 'ro', 'bg',
    # Other regional
    'ru', 'cn', 'jp', 'kr', 'in', 'br', 'mx', 'ar', 'au', 'nz',
    'za', 'ae', 'il', 'sg', 'my', 'th', 'ph', 'id', 'vn',
    # UK specific
    'uk', 'co.uk', 'org.uk', 'ac.uk',
}

# Generic TLDs that are NEUTRAL (need geo verification)
GENERIC_TLDS = {
    'com', 'org', 'net', 'info', 'biz', 'edu', 'gov',
    'io', 'co', 'app', 'dev', 'ai', 'me', 'tv', 'fm',
}

# Non-GTA Canadian regions (for scoring, not exclusion)
NON_GTA_CANADIAN_REGIONS = {
    'vancouver', 'calgary', 'edmonton', 'winnipeg',
    'montreal', 'quebec', 'ottawa', 'halifax',
    'victoria', 'saskatoon', 'regina',
}


@dataclass
class DomainAnalysis:
    """Analysis result for a single domain."""
    domain: str
    tld: str
    part_file: str
    signal: DomainSignal = DomainSignal.NEUTRAL
    score: float = 0.0
    reasons: list = field(default_factory=list)
    
    def add_reason(self, reason: str):
        """Add a reason for the signal classification."""
        self.reasons.append(reason)


def get_full_tld(domain: str) -> str:
    """
    Extract the full TLD including compound TLDs like co.uk.
    
    Examples:
        example.co.uk -> co.uk
        example.com -> com
        sub.example.co.nz -> co.nz
    """
    parts = domain.lower().split('.')
    if len(parts) >= 2:
        # Check for compound TLDs
        potential_compound = '.'.join(parts[-2:])
        if potential_compound in FOREIGN_REGIONAL_TLDS:
            return potential_compound
    return parts[-1] if parts else ''


def normalize_domain(domain: str) -> str:
    """Normalize domain for keyword matching."""
    domain = domain.lower()
    domain = re.sub(r'^(www\.|m\.)', '', domain)
    return re.sub(r'[\.\-]', '', domain)


def segment_domain(domain: str) -> list[str]:
    """Segment a domain name into words for keyword matching."""
    domain = domain.lower()
    domain = re.sub(r'^(www\.|m\.)', '', domain)
    
    parts = re.split(r'[\.\-]', domain)
    words = []
    
    all_keywords = sorted(
        list(TORONTO_KEYWORDS) + list(GTA_CITIES) + list(NON_GTA_CANADIAN_REGIONS),
        key=len, reverse=True
    )
    
    for part in parts:
        remaining = part
        found_words = []
        
        for kw in all_keywords:
            if kw in remaining:
                idx = remaining.find(kw)
                if idx != -1:
                    found_words.append(kw)
                    remaining = remaining[:idx] + remaining[idx+len(kw):]
        
        words.extend(found_words)
        if remaining:
            words.append(remaining)
    
    return words


def check_toronto_keywords(domain: str) -> list[str]:
    """Check if domain contains Toronto-related keywords."""
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
    """Check if domain is a known Toronto institution."""
    domain_lower = domain.lower()
    
    if domain_lower in KNOWN_TORONTO_DOMAINS:
        return True
    
    for known in KNOWN_TORONTO_DOMAINS:
        if domain_lower.endswith('.' + known):
            return True
    
    return False


def check_non_toronto_canadian(domain: str) -> list[str]:
    """Check if domain has non-Toronto Canadian city names."""
    words = segment_domain(domain)
    matches = []
    
    for region in NON_GTA_CANADIAN_REGIONS:
        if region in words:
            matches.append(f"non_gta_region:{region}")
    
    return matches


def is_foreign_regional_tld(domain: str, tld: str) -> bool:
    """Check if domain has a foreign regional TLD."""
    full_tld = get_full_tld(domain)
    
    # Check compound TLDs first
    if full_tld in FOREIGN_REGIONAL_TLDS:
        return True
    
    # Check single TLDs
    if tld.lower() in FOREIGN_REGIONAL_TLDS:
        return True
    
    return False


def calculate_score(analysis: DomainAnalysis) -> float:
    """Calculate priority score for ranking."""
    score = 0.0
    
    # Known institution is highest
    if any('known_institution' in r for r in analysis.reasons):
        score += 100.0
    
    # Direct Toronto keyword
    if any('keyword:toronto' in r for r in analysis.reasons):
        score += 50.0
    
    # GTA cities
    gta_matches = [r for r in analysis.reasons if r.startswith('gta_city:')]
    score += len(gta_matches) * 30.0
    
    # Other Toronto keywords
    other_kw = [r for r in analysis.reasons 
                if r.startswith('keyword:') and 'toronto' not in r]
    score += len(other_kw) * 20.0
    
    # Canadian TLD boost
    if any('canadian_tld' in r for r in analysis.reasons):
        score += 10.0
    
    # Penalty for non-GTA Canadian regions (they're still processed but lower priority)
    non_gta = [r for r in analysis.reasons if r.startswith('non_gta_region:')]
    score -= len(non_gta) * 25.0
    
    return max(score, 0.0)


def analyze_domains(lookup_path: Path, stats_path: Path = None) -> list[DomainAnalysis]:
    """
    Analyze all domains and classify with tri-state signals.
    
    Returns:
        List of DomainAnalysis objects for ALL domains
    """
    results = []
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
    
    logger.info(f"Processing {lookup_path}")
    
    counts = {
        'total': 0,
        'positive': 0,
        'negative': 0,
        'neutral': 0,
    }
    
    # Count total lines for progress bar
    with open(lookup_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1  # Subtract header
    
    with open(lookup_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        # Wrap with progress bar if tqdm available
        if tqdm:
            reader = tqdm(reader, total=total_lines, desc="Analyzing domains", 
                         unit="domains", dynamic_ncols=True)
        
        for row in reader:
            if len(row) < 3:
                continue
            
            domain, tld, part_file = row[0], row[1], row[2]
            counts['total'] += 1
            
            analysis = DomainAnalysis(domain=domain, tld=tld, part_file=part_file)
            
            # Check for NEGATIVE signal first (foreign regional TLD)
            if is_foreign_regional_tld(domain, tld):
                analysis.signal = DomainSignal.NEGATIVE
                analysis.add_reason(f"foreign_tld:{get_full_tld(domain)}")
                counts['negative'] += 1
                results.append(analysis)
                continue
            
            # Check for POSITIVE signals
            has_positive = False
            
            # Canadian TLD
            if tld.lower() == 'ca':
                analysis.add_reason('canadian_tld')
                has_positive = True
            
            # Toronto/GTA keywords
            for match in check_toronto_keywords(domain):
                analysis.add_reason(match)
                has_positive = True
            
            # Known institution
            if check_known_domain(domain):
                analysis.add_reason('known_institution')
                has_positive = True
            
            # Non-GTA Canadian (still positive but lower score)
            for match in check_non_toronto_canadian(domain):
                analysis.add_reason(match)
            
            if has_positive:
                analysis.signal = DomainSignal.POSITIVE
                counts['positive'] += 1
            else:
                analysis.signal = DomainSignal.NEUTRAL
                counts['neutral'] += 1
            
            # Calculate score
            analysis.score = calculate_score(analysis)
            
            results.append(analysis)
    
    logger.info(f"Analyzed {counts['total']:,} domains:")
    logger.info(f"  POSITIVE: {counts['positive']:,} (Toronto signals)")
    logger.info(f"  NEGATIVE: {counts['negative']:,} (foreign regional TLD)")
    logger.info(f"  NEUTRAL:  {counts['neutral']:,} (need geo verification)")
    
    # Sort by score descending, then by signal (positive first)
    signal_order = {DomainSignal.POSITIVE: 0, DomainSignal.NEUTRAL: 1, DomainSignal.NEGATIVE: 2}
    results.sort(key=lambda a: (-a.score, signal_order[a.signal]))
    
    return results


def save_results(results: list[DomainAnalysis], output_path: Path):
    """Save analysis results to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'domain', 'tld', 'part_file', 'signal', 'score', 'reasons'
        ])
        
        for a in results:
            writer.writerow([
                a.domain,
                a.tld,
                a.part_file,
                a.signal.value,
                f"{a.score:.1f}",
                '|'.join(a.reasons),
            ])
    
    logger.info(f"Saved {len(results)} domain analyses to {output_path}")


def print_summary(results: list[DomainAnalysis]):
    """Print analysis summary."""
    print("\n" + "=" * 60)
    print("DOMAIN SIGNAL ANALYSIS SUMMARY")
    print("=" * 60)
    
    # Count by signal
    by_signal = defaultdict(int)
    for a in results:
        by_signal[a.signal.value] += 1
    
    print(f"\nSignal distribution:")
    print(f"  POSITIVE: {by_signal['positive']:,} (will process, Toronto signals)")
    print(f"  NEUTRAL:  {by_signal['neutral']:,} (will process, needs geo verification)")
    print(f"  NEGATIVE: {by_signal['negative']:,} (will skip, foreign regional TLD)")
    
    # Top positive domains
    positive = [a for a in results if a.signal == DomainSignal.POSITIVE]
    print(f"\nTop 15 POSITIVE domains:")
    for a in positive[:15]:
        reasons = ', '.join(a.reasons[:3])
        print(f"  {a.score:6.1f}  {a.domain}  [{reasons}]")
    
    # Sample negative domains
    negative = [a for a in results if a.signal == DomainSignal.NEGATIVE]
    if negative:
        print(f"\nSample NEGATIVE domains (will be skipped):")
        for a in negative[:10]:
            print(f"  {a.domain}  [{a.reasons[0] if a.reasons else 'foreign_tld'}]")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze domains for Toronto/GTA signals (tri-state classification)"
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
        default=DATA_INTERMEDIATE / "domain_signals.csv",
        help='Output path for domain signals'
    )
    
    args = parser.parse_args()
    
    if not args.lookup.exists():
        logger.error(f"Lookup file not found: {args.lookup}")
        logger.info("Run download_wdc_events.py --metadata-only to download")
        return 1
    
    # Analyze domains
    results = analyze_domains(
        lookup_path=args.lookup,
        stats_path=args.stats if args.stats.exists() else None
    )
    
    # Save and print summary
    save_results(results, args.output)
    print_summary(results)
    
    return 0


if __name__ == '__main__':
    exit(main())
