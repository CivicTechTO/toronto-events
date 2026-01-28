#!/usr/bin/env python3
"""
Toronto Event Source Pipeline

Run the complete pipeline to identify Toronto/GTA event sources 
from Web Data Commons Event data.

Classification approach:
  - INCLUDE: Toronto domain signals, Toronto geo, or Toronto keywords
  - EXCLUDE: Foreign regional TLD or explicitly non-Toronto geo
  - UNKNOWN: No clear signals (generic TLD, no geo data)

Usage:
    # Process downloaded data
    uv run python scripts/run_pipeline.py
    
    # Process specific part files
    uv run python scripts/run_pipeline.py --parts part_101.gz part_14.gz
    
    # Quick test run
    uv run python scripts/run_pipeline.py --limit 100
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent


def run_script(script_name: str, args: list = None) -> bool:
    """Run a pipeline script."""
    script_path = SCRIPT_DIR / script_name
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    logger.info(f"Running: {script_name} {' '.join(args or [])}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    
    if result.returncode != 0:
        logger.error(f"Script {script_name} failed with code {result.returncode}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run the Toronto Event Source pipeline"
    )
    parser.add_argument(
        '--parts',
        nargs='+',
        help='Specific part files to process'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit events extracted (for testing)'
    )
    parser.add_argument(
        '--skip-phase1',
        action='store_true',
        help='Skip domain signal analysis (use existing signals)'
    )
    parser.add_argument(
        '--skip-phase2',
        action='store_true',
        help='Skip part file prioritization'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("TORONTO EVENT SOURCE PIPELINE")
    print("=" * 60)
    
    # Phase 1: Domain Signal Analysis
    if not args.skip_phase1:
        print("\n[Phase 1] Analyzing domain signals...")
        if not run_script("analyze_domains.py"):
            return 1
    else:
        print("\n[Phase 1] Skipped (using existing domain signals)")
    
    # Phase 2: Identify Relevant Parts
    if not args.skip_phase2:
        print("\n[Phase 2] Prioritizing part files...")
        if not run_script("identify_relevant_parts.py"):
            return 1
    else:
        print("\n[Phase 2] Skipped")
    
    # Phase 3: Extract Events
    print("\n[Phase 3] Extracting events from N-Quads data...")
    extract_args = []
    if args.parts:
        for part in args.parts:
            extract_args.extend(['--part', part])
    if args.limit:
        extract_args.extend(['--limit', str(args.limit)])
    
    if not run_script("extract_events.py", extract_args):
        return 1
    
    # Phase 4: Geo-filter (integrated into Phase 5)
    
    # Phase 5: Score Domains
    print("\n[Phase 5] Scoring domains...")
    if not run_script("score_domains.py"):
        return 1
    
    # Phase 6: Generate Outputs
    print("\n[Phase 6] Generating final outputs...")
    if not run_script("generate_outputs.py"):
        return 1
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\nOutputs in: {PROJECT_ROOT / 'data' / 'processed'}")
    print("  - toronto_event_sources.csv")
    print("  - toronto_event_samples.ndjson")
    print("  - manual_review_queue.csv")
    
    return 0


if __name__ == '__main__':
    exit(main())
