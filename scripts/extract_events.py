#!/usr/bin/env python3
"""
Phase 3: Extract Events from N-Quads

Extract and reconstruct Event entities from N-Quads data files.
Uses domain signals from Phase 1 to skip foreign regional domains
while processing all positive and neutral signal domains.
"""

import csv
import orjson  # Faster than stdlib json
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Iterator, Optional
from collections import defaultdict

from nquads_parser import NQuadsParser, Quad, group_by_subject

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "wdc_events"
DATA_INTERMEDIATE = PROJECT_ROOT / "data" / "intermediate"


@dataclass
class LocationInfo:
    """Extracted location information."""
    location_type: Optional[str] = None  # Place, VirtualLocation, PostalAddress
    name: Optional[str] = None
    address_text: Optional[str] = None  # Unstructured address
    address_locality: Optional[str] = None  # City
    address_region: Optional[str] = None  # Province/State
    postal_code: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@dataclass
class ExtractedEvent:
    """Extracted event information."""
    source_url: str
    domain: str
    event_type: str = "Event"
    name: Optional[str] = None
    description: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    event_status: Optional[str] = None
    event_attendance_mode: Optional[str] = None
    location: Optional[LocationInfo] = None
    organizer_name: Optional[str] = None
    url: Optional[str] = None
    image: Optional[str] = None
    
    # Quality metrics
    property_count: int = 0
    has_location: bool = False
    has_dates: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        # Remove None values
        d = {k: v for k, v in d.items() if v is not None}
        return d


class EventExtractor:
    """Extract Event entities from N-Quads data."""
    
    # Schema.org predicates of interest
    EVENT_TYPE_PREDICATES = {
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    }
    
    EVENT_TYPES = {
        'http://schema.org/Event',
        'https://schema.org/Event',
        'http://schema.org/MusicEvent',
        'http://schema.org/SportsEvent',
        'http://schema.org/TheaterEvent',
        'http://schema.org/Festival',
        'http://schema.org/Course',
        'http://schema.org/ExhibitionEvent',
        'http://schema.org/BusinessEvent',
        'http://schema.org/EducationEvent',
        'http://schema.org/SocialEvent',
    }
    
    LOCATION_TYPES = {
        'http://schema.org/Place',
        'http://schema.org/VirtualLocation',
        'http://schema.org/PostalAddress',
    }
    
    # Property mappings
    EVENT_PROPERTIES = {
        'name': ['name', 'title'],
        'description': ['description', 'Description'],
        'start_date': ['startDate', 'startdate'],
        'end_date': ['endDate', 'enddate'],
        'event_status': ['eventStatus'],
        'event_attendance_mode': ['eventAttendanceMode'],
        'url': ['url'],
        'image': ['image'],
    }
    
    LOCATION_PROPERTIES = {
        'name': ['name'],
        'address_text': ['address', 'streetAddress'],
        'address_locality': ['addressLocality'],
        'address_region': ['addressRegion'],
        'postal_code': ['postalCode'],
        'country': ['addressCountry'],
        'latitude': ['latitude', 'lat'],
        'longitude': ['longitude', 'lng', 'lon'],
    }
    
    def __init__(self, excluded_domains: set[str] = None):
        """
        Initialize extractor.
        
        Args:
            excluded_domains: Domains to skip (negative signal from Phase 1).
                             If None, processes all domains.
        """
        self.excluded_domains = excluded_domains or set()
        self.stats = {
            'events_found': 0,
            'events_with_location': 0,
            'events_with_dates': 0,
            'domains_seen': set(),
            'domains_skipped': set(),
        }
    
    def _get_property(self, quads: list[Quad], prop_names: list[str]) -> Optional[str]:
        """Get the value of a property from quads."""
        for quad in quads:
            local = quad.predicate_local
            if local.lower() in [p.lower() for p in prop_names]:
                return quad.object_value
        return None
    
    def _extract_location(self, location_subject: str, 
                          all_quads: dict[str, list[Quad]]) -> Optional[LocationInfo]:
        """
        Extract location information by following references.
        
        Args:
            location_subject: Subject of the location node
            all_quads: Dict mapping subjects to their quads
        """
        if location_subject not in all_quads:
            return None
        
        loc_quads = all_quads[location_subject]
        
        # Determine location type
        loc_type = None
        for quad in loc_quads:
            if quad.predicate.endswith('#type') or quad.predicate.endswith('/type'):
                for lt in self.LOCATION_TYPES:
                    if lt in quad.object_value:
                        loc_type = lt.split('/')[-1]
                        break
        
        location = LocationInfo(location_type=loc_type)
        
        # Extract properties
        for field_name, prop_names in self.LOCATION_PROPERTIES.items():
            value = self._get_property(loc_quads, prop_names)
            if value:
                if field_name in ('latitude', 'longitude'):
                    try:
                        value = float(value)
                    except ValueError:
                        continue
                setattr(location, field_name, value)
        
        # Check for nested address
        address_ref = self._get_property(loc_quads, ['address'])
        if address_ref and address_ref.startswith('_:') or address_ref in all_quads:
            nested_loc = self._extract_location(address_ref, all_quads)
            if nested_loc:
                # Merge nested address info
                for field in ['address_locality', 'address_region', 'postal_code', 'country']:
                    if getattr(nested_loc, field) and not getattr(location, field):
                        setattr(location, field, getattr(nested_loc, field))
        
        return location
    
    def extract_from_quads(self, quads: list[Quad]) -> Iterator[ExtractedEvent]:
        """
        Extract events from a list of quads (typically from one part file).
        
        Args:
            quads: List of quads to process
        
        Yields:
            ExtractedEvent objects
        """
        # Group quads by subject
        subjects = defaultdict(list)
        for quad in quads:
            subjects[quad.subject].append(quad)
        
        # Find Event subjects
        for subject, subj_quads in subjects.items():
            is_event = False
            event_type = "Event"
            
            for quad in subj_quads:
                if '#type' in quad.predicate or '/type' in quad.predicate:
                    for et in self.EVENT_TYPES:
                        if et in quad.object_value:
                            is_event = True
                            event_type = et.split('/')[-1]
                            break
            
            if not is_event:
                continue
            
            # This is an Event - extract properties
            source_url = subj_quads[0].graph if subj_quads else ""
            domain = subj_quads[0].domain if subj_quads else ""
            
            self.stats['events_found'] += 1
            self.stats['domains_seen'].add(domain)
            
            event = ExtractedEvent(
                source_url=source_url,
                domain=domain,
                event_type=event_type,
            )
            
            # Extract simple properties
            for field_name, prop_names in self.EVENT_PROPERTIES.items():
                value = self._get_property(subj_quads, prop_names)
                if value:
                    setattr(event, field_name, value)
            
            # Count properties
            event.property_count = len([q for q in subj_quads 
                                        if '#type' not in q.predicate])
            
            # Extract location
            location_ref = self._get_property(subj_quads, ['location'])
            if location_ref:
                event.location = self._extract_location(location_ref, subjects)
                if event.location:
                    event.has_location = True
                    self.stats['events_with_location'] += 1
            
            # Check dates
            if event.start_date or event.end_date:
                event.has_dates = True
                self.stats['events_with_dates'] += 1
            
            # Extract organizer name
            organizer_ref = self._get_property(subj_quads, ['organizer'])
            if organizer_ref and organizer_ref in subjects:
                org_name = self._get_property(subjects[organizer_ref], ['name'])
                if org_name:
                    event.organizer_name = org_name
            
            yield event
    
    def process_part_file(self, part_path: Path, limit: int = None) -> Iterator[ExtractedEvent]:
        """
        Process a single part file and extract events.
        
        Args:
            part_path: Path to the gzipped part file
            limit: Maximum number of events to extract
        
        Yields:
            ExtractedEvent objects
        """
        parser = NQuadsParser()  # Process all domains, filter here instead
        
        # Collect all quads (grouping by domain for efficiency)
        domain_quads = defaultdict(list)
        
        for quad in parser.stream_file(part_path):
            # Skip excluded domains (negative signal)
            if quad.domain.lower() in self.excluded_domains:
                self.stats['domains_skipped'].add(quad.domain)
                continue
            
            domain_quads[quad.domain].append(quad)
            
            # Process in batches by domain to manage memory
            if len(domain_quads[quad.domain]) >= 100000:
                for event in self.extract_from_quads(domain_quads[quad.domain]):
                    yield event
                    if limit and self.stats['events_found'] >= limit:
                        return
                domain_quads[quad.domain] = []
        
        # Process remaining quads
        for domain, quads in domain_quads.items():
            if quads:
                for event in self.extract_from_quads(quads):
                    yield event
                    if limit and self.stats['events_found'] >= limit:
                        return
        
        logger.info(f"Parser stats: {parser.get_stats()}")
    
    def get_stats(self) -> dict:
        """Get extraction statistics."""
        stats = self.stats.copy()
        stats['domains_seen'] = len(stats['domains_seen'])
        stats['domains_skipped'] = len(stats['domains_skipped'])
        return stats


def load_domain_signals(path: Path) -> tuple[set[str], dict[str, float]]:
    """
    Load domain signals from Phase 1 (analyze_domains.py) output.
    
    Returns:
        Tuple of (excluded_domains, domain_scores)
        - excluded_domains: Set of domains with negative signal (skip these)
        - domain_scores: Dict mapping domain -> priority score
    """
    excluded = set()
    scores = {}
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = row['domain'].lower()
            signal = row.get('signal', 'neutral')
            
            if signal == 'negative':
                excluded.add(domain)
            else:
                try:
                    scores[domain] = float(row.get('score', 0))
                except ValueError:
                    scores[domain] = 0.0
    
    return excluded, scores


def main():
    parser = argparse.ArgumentParser(
        description="Extract events from N-Quads data files"
    )
    parser.add_argument(
        '--part', '-p',
        type=str,
        help='Specific part file to process (e.g., part_101.gz)'
    )
    parser.add_argument(
        '--data-dir', '-d',
        type=Path,
        default=DATA_RAW,
        help='Directory containing part files'
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
        default=DATA_INTERMEDIATE / "events",
        help='Output directory for extracted events'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Maximum events to extract (for testing)'
    )
    parser.add_argument(
        '--no-filter',
        action='store_true',
        help='Process all domains (ignore domain signals)'
    )
    
    args = parser.parse_args()
    
    # Load domain signals
    excluded_domains = None
    domain_scores = {}
    if not args.no_filter and args.signals.exists():
        logger.info(f"Loading domain signals from {args.signals}")
        excluded_domains, domain_scores = load_domain_signals(args.signals)
        logger.info(f"Loaded signals: {len(excluded_domains):,} excluded (negative), "
                    f"{len(domain_scores):,} to process (positive/neutral)")
    
    # Create extractor - pass excluded domains to skip
    extractor = EventExtractor(excluded_domains=excluded_domains)
    
    # Determine which files to process
    if args.part:
        part_files = [args.data_dir / args.part]
    else:
        part_files = sorted(args.data_dir.glob("part_*.gz"))
    
    if not part_files:
        logger.error(f"No part files found in {args.data_dir}")
        return 1
    
    logger.info(f"Processing {len(part_files)} part file(s)")
    
    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    
    # Process files
    all_events = []
    
    for part_path in part_files:
        if not part_path.exists():
            logger.warning(f"File not found: {part_path}")
            continue
        
        logger.info(f"\nProcessing {part_path.name}...")
        
        events = list(extractor.process_part_file(part_path, limit=args.limit))
        logger.info(f"Extracted {len(events)} events from {part_path.name}")
        
        all_events.extend(events)
        
        if args.limit and len(all_events) >= args.limit:
            break
    
    # Save events
    output_path = args.output / "extracted_events.ndjson"
    with open(output_path, 'w', encoding='utf-8') as f:
        for event in all_events:
            f.write(orjson.dumps(event.to_dict()).decode() + '\n')
    
    logger.info(f"\nSaved {len(all_events)} events to {output_path}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    stats = extractor.get_stats()
    print(f"Total events: {stats['events_found']:,}")
    print(f"Events with location: {stats['events_with_location']:,}")
    print(f"Events with dates: {stats['events_with_dates']:,}")
    print(f"Unique domains: {stats['domains_seen']:,}")
    print(f"Domains skipped (negative signal): {stats['domains_skipped']:,}")
    
    # Sample events
    print("\nSample events:")
    for event in all_events[:5]:
        loc_str = ""
        if event.location:
            if event.location.address_locality:
                loc_str = f" @ {event.location.address_locality}"
            elif event.location.name:
                loc_str = f" @ {event.location.name}"
        print(f"  - {event.name or '(unnamed)'}{loc_str} [{event.domain}]")
    
    return 0


if __name__ == '__main__':
    exit(main())
