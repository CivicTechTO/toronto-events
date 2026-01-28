#!/usr/bin/env python3
"""
N-Quads Parser

Streaming parser for N-Quads format files (gzipped or plain text).
Optimized for memory-efficient processing of large files.

N-Quads format:
    subject predicate object graph .
    
Example:
    _:node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Event> <https://example.com/> .
"""

import regex as re  # Faster than stdlib re
import gzip
import logging
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Iterator, Optional
from urllib.parse import urlparse

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

logger = logging.getLogger(__name__)

# Regex patterns for parsing N-Quads
# Based on W3C N-Quads spec: https://www.w3.org/TR/n-quads/

# IRI pattern: <http://...>
IRI_PATTERN = r'<([^>]+)>'

# Blank node pattern: _:label
BLANK_NODE_PATTERN = r'_:([^\s]+)'

# Literal pattern: "value"^^<datatype> or "value"@lang or just "value"
# Handle escaped quotes inside strings
LITERAL_PATTERN = r'"((?:[^"\\]|\\.)*)(?:"(?:\^\^<([^>]+)>|@(\w+))?)?'

# Subject: IRI or blank node
SUBJECT_PATTERN = f'(?:{IRI_PATTERN}|{BLANK_NODE_PATTERN})'

# Predicate: IRI
PREDICATE_PATTERN = IRI_PATTERN

# Object: IRI, blank node, or literal
OBJECT_PATTERN = f'(?:{IRI_PATTERN}|{BLANK_NODE_PATTERN}|{LITERAL_PATTERN})'

# Graph: IRI (optional in N-Triples, required in N-Quads)
GRAPH_PATTERN = IRI_PATTERN

# Full N-Quad line pattern
NQUAD_PATTERN = re.compile(
    rf'^\s*{SUBJECT_PATTERN}\s+{PREDICATE_PATTERN}\s+{OBJECT_PATTERN}\s+{GRAPH_PATTERN}\s*\.\s*$'
)

# Simpler approach: split by whitespace, handle each part
def parse_term(term: str) -> tuple[str, str, Optional[str]]:
    """
    Parse a single N-Quads term.
    
    Returns:
        (value, term_type, datatype_or_lang)
        term_type: 'iri', 'blank', 'literal'
    """
    term = term.strip()
    
    if term.startswith('<') and term.endswith('>'):
        # IRI
        return term[1:-1], 'iri', None
    
    elif term.startswith('_:'):
        # Blank node
        return term[2:], 'blank', None
    
    elif term.startswith('"'):
        # Literal - find the closing quote (handling escapes)
        # This is simplified - proper parsing would handle all escape sequences
        value_end = term.rfind('"')
        if value_end <= 0:
            value_end = len(term)
        
        value = term[1:value_end]
        # Unescape basic sequences
        value = value.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
        
        suffix = term[value_end+1:]
        
        if suffix.startswith('^^<'):
            # Typed literal
            datatype = suffix[3:-1]  # Remove ^^< and >
            return value, 'literal', datatype
        elif suffix.startswith('@'):
            # Language-tagged literal
            lang = suffix[1:]
            return value, 'literal', f'@{lang}'
        else:
            return value, 'literal', None
    
    else:
        # Unknown format
        return term, 'unknown', None


@dataclass
class Quad:
    """Represents a single N-Quad."""
    subject: str
    subject_type: str  # 'iri' or 'blank'
    predicate: str
    object_value: str
    object_type: str  # 'iri', 'blank', or 'literal'
    object_datatype: Optional[str]  # datatype IRI or @lang
    graph: str  # Source URL
    
    @property
    def predicate_local(self) -> str:
        """Get the local name of the predicate (after last # or /)."""
        if '#' in self.predicate:
            return self.predicate.split('#')[-1]
        elif '/' in self.predicate:
            return self.predicate.split('/')[-1]
        return self.predicate
    
    @property
    def domain(self) -> str:
        """Extract domain from graph URL."""
        try:
            parsed = urlparse(self.graph)
            return parsed.netloc.lower()
        except Exception:
            return ''


class NQuadsParser:
    """Streaming parser for N-Quads format files."""
    
    def __init__(self, allowed_domains: set[str] = None):
        """
        Initialize parser.
        
        Args:
            allowed_domains: If provided, only parse quads from these domains.
                             Use lowercase domain names.
        """
        self.allowed_domains = allowed_domains
        self.stats = {
            'lines_read': 0,
            'quads_parsed': 0,
            'parse_errors': 0,
            'domain_filtered': 0,
        }
    
    def parse_line(self, line: str) -> Optional[Quad]:
        """
        Parse a single N-Quads line.
        
        Returns:
            Quad object or None if line is empty/comment/invalid
        """
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#'):
            return None
        
        # Split into parts - tricky because literals can contain spaces
        # Strategy: Find the graph (last <url>) and work backwards
        
        try:
            # Find the trailing " ."
            if not line.endswith(' .'):
                line = line.rstrip('.')
                line = line.rstrip()
            else:
                line = line[:-2]  # Remove " ."
            
            # Find graph (last IRI)
            last_iri_start = line.rfind(' <')
            if last_iri_start == -1:
                self.stats['parse_errors'] += 1
                return None
            
            graph_part = line[last_iri_start+1:]
            rest = line[:last_iri_start].strip()
            
            # Find object (can be IRI, blank node, or literal)
            # Note: typed literals end with ^^<datatype> which ends with >, so check for that first
            if rest.endswith('>') and '^^<' in rest:
                # Typed literal - find the datatype start
                dt_start = rest.rfind('^^<')
                # Now find the opening quote before that
                quote_end = dt_start - 1  # Position of closing quote
                if quote_end >= 0 and rest[quote_end] == '"':
                    # Find opening quote
                    quote_start = -1
                    for i in range(quote_end - 1, -1, -1):
                        if rest[i] == '"' and (i == 0 or rest[i-1] != '\\'):
                            if i == 0 or rest[i-1] in ' \t':
                                quote_start = i
                                break
                    if quote_start != -1:
                        object_part = rest[quote_start:]
                        rest = rest[:quote_start].strip()
                    else:
                        self.stats['parse_errors'] += 1
                        return None
                else:
                    self.stats['parse_errors'] += 1
                    return None
            elif rest.endswith('>'):
                # Object is IRI - find its start
                obj_start = rest.rfind(' <')
                if obj_start == -1:
                    obj_start = rest.rfind('\t<')
                object_part = rest[obj_start+1:]
                rest = rest[:obj_start].strip()
            elif rest.endswith('"') or rest[-10:].startswith('@'):
                # Object is literal - find the opening quote
                # This is complex due to possible datatype/lang suffix
                # Find where the literal starts
                
                # Handle typed literals: "value"^^<datatype>
                if '^^<' in rest:
                    dt_start = rest.rfind('^^<')
                    rest_before = rest[:dt_start]
                else:
                    rest_before = rest
                
                # Handle language tag: "value"@lang
                if rest_before.endswith('"'):
                    quote_end = len(rest_before) - 1
                elif '@' in rest_before[-10:]:
                    at_pos = rest_before.rfind('@')
                    rest_before = rest_before[:at_pos]
                    quote_end = len(rest_before) - 1
                else:
                    quote_end = rest_before.rfind('"')
                
                # Find the opening quote (skip escaped quotes)
                quote_start = -1
                for i in range(quote_end - 1, -1, -1):
                    if rest[i] == '"' and (i == 0 or rest[i-1] != '\\'):
                        # Check if this is preceded by whitespace (start of literal)
                        if i == 0 or rest[i-1] in ' \t':
                            quote_start = i
                            break
                
                if quote_start == -1:
                    self.stats['parse_errors'] += 1
                    return None
                
                object_part = rest[quote_start:]
                rest = rest[:quote_start].strip()
            else:
                # Object is blank node
                parts = rest.rsplit(None, 1)
                if len(parts) != 2:
                    self.stats['parse_errors'] += 1
                    return None
                rest, object_part = parts
            
            # Now rest should be "subject predicate"
            parts = rest.split(None, 1)
            if len(parts) != 2:
                self.stats['parse_errors'] += 1
                return None
            
            subject_part, predicate_part = parts
            
            # Parse each part
            subject, subject_type, _ = parse_term(subject_part)
            predicate, _, _ = parse_term(predicate_part)
            object_value, object_type, object_datatype = parse_term(object_part)
            graph, _, _ = parse_term(graph_part)
            
            quad = Quad(
                subject=subject,
                subject_type=subject_type,
                predicate=predicate,
                object_value=object_value,
                object_type=object_type,
                object_datatype=object_datatype,
                graph=graph,
            )
            
            return quad
            
        except Exception as e:
            self.stats['parse_errors'] += 1
            logger.debug(f"Parse error: {e} on line: {line[:100]}...")
            return None
    
    def stream_file(self, path: Path, show_progress: bool = True) -> Iterator[Quad]:
        """
        Stream quads from a file (gzipped or plain text).
        
        Args:
            path: Path to the file
            show_progress: Whether to show progress bar
        
        Yields:
            Quad objects
        """
        path = Path(path)
        
        # Determine if gzipped
        if path.suffix == '.gz':
            opener = lambda: gzip.open(path, 'rt', encoding='utf-8', errors='replace')
        else:
            opener = lambda: open(path, 'r', encoding='utf-8', errors='replace')
        
        # Get file size for progress estimation
        file_size = path.stat().st_size
        # For gzipped files, estimate ~10x compression ratio for text
        estimated_uncompressed = file_size * 10 if path.suffix == '.gz' else file_size
        # Estimate ~100 bytes per line average
        estimated_lines = estimated_uncompressed // 100
        
        logger.info(f"Streaming {path.name} (~{estimated_lines:,} estimated lines)...")
        
        with opener() as f:
            # Wrap with progress bar if tqdm available and requested
            iterator = f
            if show_progress and tqdm:
                iterator = tqdm(f, total=estimated_lines, desc=f"Parsing {path.name}",
                               unit="lines", dynamic_ncols=True, miniters=10000)
            
            for line in iterator:
                self.stats['lines_read'] += 1
                
                quad = self.parse_line(line)
                if quad is None:
                    continue
                
                # Domain filtering
                if self.allowed_domains:
                    domain = quad.domain
                    # Check both with and without www.
                    if domain not in self.allowed_domains:
                        domain_no_www = domain.replace('www.', '')
                        if domain_no_www not in self.allowed_domains:
                            self.stats['domain_filtered'] += 1
                            continue
                
                self.stats['quads_parsed'] += 1
                yield quad
    
    def get_stats(self) -> dict:
        """Get parsing statistics."""
        return self.stats.copy()


def group_by_subject(quads: Iterator[Quad]) -> Iterator[list[Quad]]:
    """
    Group consecutive quads by subject.
    
    N-Quads files typically have quads for the same subject grouped together.
    This yields lists of quads that share the same subject.
    
    Note: This only groups *consecutive* quads. Quads for the same subject
    that appear in different parts of the file will be in different groups.
    """
    current_subject = None
    current_group = []
    
    for quad in quads:
        if quad.subject != current_subject:
            if current_group:
                yield current_group
            current_subject = quad.subject
            current_group = [quad]
        else:
            current_group.append(quad)
    
    # Yield final group
    if current_group:
        yield current_group


# Simple test
if __name__ == '__main__':
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample file
    sample_path = Path(__file__).parent.parent / "data" / "raw" / "Event_sample.txt"
    
    if not sample_path.exists():
        print(f"Sample file not found: {sample_path}")
        sys.exit(1)
    
    parser = NQuadsParser()
    
    print(f"Parsing {sample_path}...\n")
    
    event_count = 0
    for quad in parser.stream_file(sample_path):
        # Print first few Event type quads
        if quad.predicate_local == 'type' and 'Event' in quad.object_value:
            print(f"Event found: {quad.subject[:30]}... from {quad.domain}")
            event_count += 1
            if event_count >= 5:
                break
    
    print(f"\nStats: {parser.get_stats()}")
