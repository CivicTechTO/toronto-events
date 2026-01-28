"""
Core modules for the Toronto Events pipeline.

This package contains the fundamental building blocks:
- nquads_parser: Streaming N-Quads parser for RDF data
- geo_filter: Geographic filtering for Toronto/GTA locations
"""

from .nquads_parser import NQuadsParser, Quad
from .geo_filter import GeoFilter

__all__ = ['NQuadsParser', 'Quad', 'GeoFilter']
