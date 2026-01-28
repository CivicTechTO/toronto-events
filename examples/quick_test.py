#!/usr/bin/env python3
"""
Example: Quick Pipeline Test

This example shows how to run a quick test of the pipeline
on a small sample of data.
"""

import sys
import subprocess
from pathlib import Path

def run_command(cmd, description):
    """Run a command and show the output."""
    print(f"\n{'='*60}")
    print(f"Step: {description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Show output
    if result.stdout:
        print(result.stdout)
    
    if result.returncode != 0:
        print(f"\n❌ Error: {description} failed")
        if result.stderr:
            print(f"Error output:\n{result.stderr}")
        return False
    
    print(f"\n✅ {description} completed successfully")
    return True

def main():
    """Run a quick pipeline test."""
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Toronto Events Pipeline - Quick Test            ║
    ╚══════════════════════════════════════════════════════════╝
    
    This will:
    1. Download metadata files (~90MB)
    2. Download one sample data file (~150MB)
    3. Run pipeline on 1,000 events
    4. Show you the results
    
    Total time: ~5-10 minutes
    """)
    
    # Check if user wants to continue
    response = input("Continue? [y/N]: ").strip().lower()
    if response not in ['y', 'yes']:
        print("Cancelled.")
        return 0
    
    # Step 1: Download metadata
    if not run_command(
        [sys.executable, 'scripts/download_wdc_events.py', '--metadata-only'],
        'Download metadata'
    ):
        return 1
    
    # Step 2: Download one part file
    if not run_command(
        [sys.executable, 'scripts/download_wdc_events.py', '--parts', 'part_101.gz'],
        'Download sample data (part_101.gz)'
    ):
        return 1
    
    # Step 3: Run pipeline
    if not run_command(
        [sys.executable, 'scripts/run_pipeline.py', '--parts', 'part_101.gz', '--limit', '1000'],
        'Run pipeline (limit: 1000 events)'
    ):
        return 1
    
    # Show results
    print(f"\n{'='*60}")
    print("🎉 Test Complete!")
    print(f"{'='*60}\n")
    
    output_dir = Path('data/processed')
    
    print("Output files created:")
    for file in output_dir.glob('*.csv'):
        size = file.stat().st_size
        print(f"  - {file.name} ({size:,} bytes)")
    
    for file in output_dir.glob('*.ndjson'):
        size = file.stat().st_size
        print(f"  - {file.name} ({size:,} bytes)")
    
    print("\nTo view Toronto event sources:")
    print(f"  head data/processed/toronto_event_sources.csv")
    
    print("\nNext steps:")
    print("  1. Run full pipeline: uv run python scripts/run_pipeline.py")
    print("  2. Review the documentation: cat README.md")
    print("  3. Try the validation UI: python -m http.server 8000")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
