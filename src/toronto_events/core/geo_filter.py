#!/usr/bin/env python3
"""
Phase 4: Geo-Filtering for Toronto/GTA

Multi-strategy location matching to identify events in the Greater Toronto Area.

Strategies:
1. Postal code matching (M*, L* prefixes)
2. Geographic bounding box (lat/lon)
3. City/locality name matching
4. Region/province matching
"""

import re
import json
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

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


# ============================================================================
# GTA Geographic Constants
# ============================================================================

# Toronto postal codes start with M
# GTA postal codes include L (Peel, York, Durham, Halton regions)
GTA_POSTAL_PREFIXES = {
    # Toronto proper (all M codes)
    'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9',
    
    # Peel Region (Mississauga, Brampton, Caledon)
    'L4T', 'L4V', 'L4W', 'L4X', 'L4Y', 'L4Z',  # Mississauga
    'L5A', 'L5B', 'L5C', 'L5E', 'L5G', 'L5H', 'L5J', 'L5K', 'L5L', 'L5M', 'L5N', 'L5P', 'L5R', 'L5S', 'L5T', 'L5V', 'L5W',  # Mississauga
    'L6P', 'L6R', 'L6S', 'L6T', 'L6V', 'L6W', 'L6X', 'L6Y', 'L6Z',  # Brampton
    'L7C',  # Caledon
    
    # York Region (Vaughan, Richmond Hill, Markham, etc.)
    'L3P', 'L3R', 'L3S', 'L3T',  # Markham
    'L4B', 'L4C', 'L4E', 'L4G', 'L4H', 'L4J', 'L4K', 'L4L', 'L4S',  # Various York
    'L6A', 'L6B', 'L6C', 'L6E', 'L6G',  # Newmarket/Aurora area
    
    # Durham Region (Pickering, Ajax, Whitby, Oshawa)
    'L1H', 'L1J', 'L1K', 'L1L', 'L1M', 'L1N', 'L1P', 'L1R', 'L1S', 'L1T', 'L1V', 'L1W', 'L1X', 'L1Y', 'L1Z',
    
    # Halton Region (Oakville, Burlington, Milton)
    'L6H', 'L6J', 'L6K', 'L6L', 'L6M',  # Oakville
    'L7G', 'L7J', 'L7K', 'L7L', 'L7M', 'L7N', 'L7P', 'L7R', 'L7S', 'L7T',  # Burlington/Milton
    
    # Hamilton (wider GTA)
    'L8E', 'L8G', 'L8H', 'L8J', 'L8K', 'L8L', 'L8M', 'L8N', 'L8P', 'L8R', 'L8S', 'L8T', 'L8V', 'L8W',
    'L9A', 'L9B', 'L9C', 'L9G', 'L9H', 'L9K',
}

# GTA bounding box (approximate)
GTA_BOUNDING_BOX = {
    'min_lat': 43.40,   # Southern edge (Lake Ontario shore)
    'max_lat': 44.30,   # Northern edge (past Newmarket)
    'min_lon': -80.20,  # Western edge (Hamilton area)
    'max_lon': -78.80,  # Eastern edge (Oshawa area)
}

# Toronto core bounding box (tighter)
TORONTO_CORE_BOUNDING_BOX = {
    'min_lat': 43.58,   # Southern Toronto
    'max_lat': 43.86,   # Northern Toronto
    'min_lon': -79.64,  # Western (Etobicoke)
    'max_lon': -79.10,  # Eastern (Scarborough)
}

# GTA localities (cities, neighborhoods)
GTA_LOCALITIES = {
    # Toronto
    'toronto', 'north york', 'northyork', 'scarborough', 'etobicoke', 
    'east york', 'eastyork', 'york',
    'downtown toronto', 'midtown', 'the annex', 'kensington',
    'liberty village', 'queen west', 'king west', 'parkdale',
    'leslieville', 'riverdale', 'beaches', 'the beach',
    'yorkville', 'rosedale', 'forest hill', 'lawrence park',
    
    # Peel Region
    'mississauga', 'brampton', 'caledon', 'port credit', 'streetsville',
    'meadowvale', 'erin mills', 'square one',
    
    # York Region  
    'vaughan', 'richmond hill', 'markham', 'thornhill', 'woodbridge',
    'maple', 'concord', 'unionville', 'stouffville', 'newmarket',
    'aurora', 'king city', 'nobleton', 'kleinburg',
    
    # Durham Region
    'pickering', 'ajax', 'whitby', 'oshawa', 'clarington', 'bowmanville',
    'courtice', 'uxbridge', 'port perry',
    
    # Halton Region
    'oakville', 'burlington', 'milton', 'halton hills', 'georgetown',
    'acton',
    
    # Hamilton (wider GTA)
    'hamilton', 'dundas', 'ancaster', 'stoney creek', 'waterdown',
}

# Ontario region identifiers
ONTARIO_REGIONS = {'ontario', 'on', 'ont'}

# Canada identifiers
CANADA_IDENTIFIERS = {'canada', 'ca', 'can'}


@dataclass
class GeoMatchResult:
    """Result of geo-matching an event/location."""
    is_gta: bool = False
    confidence: float = 0.0
    match_reason: str = ""
    match_details: dict = None
    
    def __post_init__(self):
        if self.match_details is None:
            self.match_details = {}


class GeoFilter:
    """Multi-strategy geo-filter for Toronto/GTA events."""
    
    def __init__(self):
        self.stats = {
            'total_checked': 0,
            'postal_matches': 0,
            'coordinate_matches': 0,
            'locality_matches': 0,
            'region_matches': 0,
            'no_location': 0,
        }
    
    def check_postal_code(self, postal_code: str) -> Optional[GeoMatchResult]:
        """Check if postal code is in GTA."""
        if not postal_code:
            return None
        
        # Normalize postal code
        postal = postal_code.upper().replace(' ', '').replace('-', '')
        
        # Check against GTA prefixes
        for prefix in GTA_POSTAL_PREFIXES:
            if postal.startswith(prefix):
                # Determine sub-region
                if postal.startswith('M'):
                    region = "Toronto"
                elif postal.startswith('L4') or postal.startswith('L5') or postal.startswith('L6P') or postal.startswith('L6R'):
                    region = "Peel Region"
                elif postal.startswith('L1'):
                    region = "Durham Region"
                elif postal.startswith('L6H') or postal.startswith('L7'):
                    region = "Halton Region"
                elif postal.startswith('L8') or postal.startswith('L9'):
                    region = "Hamilton"
                else:
                    region = "York Region"
                
                return GeoMatchResult(
                    is_gta=True,
                    confidence=0.95,
                    match_reason=f"postal_code:{region}",
                    match_details={'postal_code': postal, 'region': region}
                )
        
        return None
    
    def check_coordinates(self, lat: float, lon: float) -> Optional[GeoMatchResult]:
        """Check if coordinates are within GTA bounding box."""
        if lat is None or lon is None:
            return None
        
        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            return None
        
        # Check Toronto core first
        core = TORONTO_CORE_BOUNDING_BOX
        if (core['min_lat'] <= lat <= core['max_lat'] and 
            core['min_lon'] <= lon <= core['max_lon']):
            return GeoMatchResult(
                is_gta=True,
                confidence=0.95,
                match_reason="coordinates:Toronto",
                match_details={'lat': lat, 'lon': lon, 'region': 'Toronto Core'}
            )
        
        # Check wider GTA
        gta = GTA_BOUNDING_BOX
        if (gta['min_lat'] <= lat <= gta['max_lat'] and 
            gta['min_lon'] <= lon <= gta['max_lon']):
            return GeoMatchResult(
                is_gta=True,
                confidence=0.85,
                match_reason="coordinates:GTA",
                match_details={'lat': lat, 'lon': lon, 'region': 'Greater Toronto Area'}
            )
        
        return None
    
    def check_locality(self, locality: str) -> Optional[GeoMatchResult]:
        """Check if locality name matches GTA cities/neighborhoods."""
        if not locality:
            return None
        
        locality_lower = locality.lower().strip()
        
        # Direct match
        if locality_lower in GTA_LOCALITIES:
            return GeoMatchResult(
                is_gta=True,
                confidence=0.90,
                match_reason=f"locality:{locality_lower}",
                match_details={'locality': locality}
            )
        
        # Partial match (locality contains GTA name)
        for gta_place in GTA_LOCALITIES:
            if gta_place in locality_lower:
                return GeoMatchResult(
                    is_gta=True,
                    confidence=0.75,
                    match_reason=f"locality_partial:{gta_place}",
                    match_details={'locality': locality, 'matched': gta_place}
                )
        
        return None
    
    def check_region(self, region: str, country: str = None) -> Optional[GeoMatchResult]:
        """Check if region is Ontario (weaker signal, needs other context)."""
        if not region:
            return None
        
        region_lower = region.lower().strip()
        
        if region_lower in ONTARIO_REGIONS:
            # This is a weaker signal - many Ontario events are not in GTA
            # Only count as match if combined with Canada
            if country:
                country_lower = country.lower().strip()
                if country_lower in CANADA_IDENTIFIERS:
                    return GeoMatchResult(
                        is_gta=False,  # Ontario but not confirmed GTA
                        confidence=0.30,
                        match_reason="region:Ontario",
                        match_details={'region': region, 'country': country}
                    )
        
        return None
    
    def filter_event(self, event: dict) -> GeoMatchResult:
        """
        Apply all geo-filters to an event.
        
        Args:
            event: Event dict with optional 'location' field
        
        Returns:
            GeoMatchResult with best match
        """
        self.stats['total_checked'] += 1
        
        location = event.get('location', {})
        if not location:
            self.stats['no_location'] += 1
            return GeoMatchResult(confidence=0.0, match_reason="no_location")
        
        # Try each strategy in order of confidence
        
        # 1. Postal code (highest confidence)
        postal_code = location.get('postal_code')
        result = self.check_postal_code(postal_code)
        if result and result.is_gta:
            self.stats['postal_matches'] += 1
            return result
        
        # 2. Coordinates
        lat = location.get('latitude')
        lon = location.get('longitude')
        result = self.check_coordinates(lat, lon)
        if result and result.is_gta:
            self.stats['coordinate_matches'] += 1
            return result
        
        # 3. Locality name
        locality = location.get('address_locality')
        result = self.check_locality(locality)
        if result and result.is_gta:
            self.stats['locality_matches'] += 1
            return result
        
        # Also check location name as fallback
        loc_name = location.get('name')
        result = self.check_locality(loc_name)
        if result and result.is_gta:
            self.stats['locality_matches'] += 1
            return result
        
        # 4. Region (weakest signal)
        region = location.get('address_region')
        country = location.get('country')
        result = self.check_region(region, country)
        if result:
            self.stats['region_matches'] += 1
            return result
        
        # No match
        return GeoMatchResult(confidence=0.0, match_reason="no_match")
    
    def get_stats(self) -> dict:
        """Get filtering statistics."""
        return self.stats.copy()


def main():
    parser = argparse.ArgumentParser(
        description="Filter events by GTA location"
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=DATA_INTERMEDIATE / "events" / "extracted_events.ndjson",
        help='Input file (ndjson)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=DATA_INTERMEDIATE / "events" / "gta_events.ndjson",
        help='Output file for GTA events'
    )
    parser.add_argument(
        '--min-confidence',
        type=float,
        default=0.5,
        help='Minimum confidence score to include'
    )
    
    args = parser.parse_args()
    
    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        return 1
    
    geo_filter = GeoFilter()
    gta_events = []
    
    logger.info(f"Processing {args.input}...")
    
    with open(args.input, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            
            event = json.loads(line)
            result = geo_filter.filter_event(event)
            
            if result.confidence >= args.min_confidence:
                event['geo_match'] = {
                    'is_gta': result.is_gta,
                    'confidence': result.confidence,
                    'match_reason': result.match_reason,
                }
                gta_events.append(event)
    
    # Save filtered events
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        for event in gta_events:
            f.write(json.dumps(event) + '\n')
    
    logger.info(f"Saved {len(gta_events)} GTA events to {args.output}")
    
    # Summary
    print("\n" + "=" * 60)
    print("GEO-FILTER SUMMARY")
    print("=" * 60)
    stats = geo_filter.get_stats()
    print(f"Total events checked: {stats['total_checked']}")
    print(f"Events with no location: {stats['no_location']}")
    print(f"\nGTA matches by strategy:")
    print(f"  Postal code: {stats['postal_matches']}")
    print(f"  Coordinates: {stats['coordinate_matches']}")
    print(f"  Locality name: {stats['locality_matches']}")
    print(f"  Region (Ontario): {stats['region_matches']}")
    print(f"\nFiltered events (conf >= {args.min_confidence}): {len(gta_events)}")
    
    return 0


if __name__ == '__main__':
    exit(main())
