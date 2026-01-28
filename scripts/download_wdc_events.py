#!/usr/bin/env python3
"""
Download Web Data Commons Event Dataset (2024-12)

This script downloads the complete Event schema.org dataset from the Web Data Commons.
Dataset info: https://webdatacommons.org/structureddata/

Files downloaded:
- Event_domain_stats.csv (~76MB) - Domain-level statistics
- Event_lookup.csv (~13MB) - Domain to file mapping
- Event_sample.txt (~164KB) - Sample data
- part_0.gz through part_132.gz (~20GB total) - N-Quads event data
"""

import os
import sys
import time
import signal
import hashlib
import argparse
import logging
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('download.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/Event/"
DEFAULT_OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "wdc_events"

# All files to download
METADATA_FILES = [
    "Event_domain_stats.csv",
    "Event_lookup.csv", 
    "Event_sample.txt",
]

# Part files: part_0.gz through part_132.gz
NUM_PARTS = 133  # 0 to 132 inclusive

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global shutdown_requested
    if shutdown_requested:
        logger.info("\nForce quit...")
        sys.exit(1)
    shutdown_requested = True
    logger.info("\nShutdown requested. Finishing current download...")


signal.signal(signal.SIGINT, signal_handler)


def get_part_files():
    """Generate list of all part file names."""
    return [f"part_{i}.gz" for i in range(NUM_PARTS)]


def get_all_files():
    """Get complete list of files to download."""
    return METADATA_FILES + get_part_files()


def download_file(url: str, dest_path: Path, chunk_size: int = 8192, 
                  max_retries: int = 5, timeout: int = 120) -> tuple[bool, str]:
    """
    Download a file with resume support and exponential backoff.
    
    Returns:
        tuple: (success: bool, message: str)
    """
    global shutdown_requested
    
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = dest_path.with_suffix(dest_path.suffix + '.partial')
    
    base_delay = 5  # Base delay for exponential backoff
    
    for attempt in range(max_retries):
        if shutdown_requested:
            return False, "Shutdown requested"
            
        try:
            # Check if we have a partial download to resume
            resume_pos = 0
            if temp_path.exists():
                resume_pos = temp_path.stat().st_size
                logger.info(f"Resuming {dest_path.name} from {resume_pos:,} bytes")
            
            # Create request with resume header if needed
            headers = {
                'User-Agent': 'WDC-Event-Downloader/1.0 (Research project; sequential download)',
                'Accept': '*/*',
            }
            if resume_pos > 0:
                headers['Range'] = f'bytes={resume_pos}-'
            
            request = Request(url, headers=headers)
            
            with urlopen(request, timeout=timeout) as response:
                # Check if server supports resume
                status_code = response.getcode()
                
                # Get total file size
                if status_code == 206:  # Partial content (resume)
                    content_range = response.headers.get('Content-Range', '')
                    if '/' in content_range:
                        total_size = int(content_range.split('/')[-1])
                    else:
                        total_size = resume_pos + int(response.headers.get('Content-Length', 0))
                else:
                    total_size = int(response.headers.get('Content-Length', 0))
                    resume_pos = 0  # Server doesn't support resume, start over
                
                # Open file in appropriate mode
                mode = 'ab' if resume_pos > 0 and status_code == 206 else 'wb'
                
                with open(temp_path, mode) as f:
                    downloaded = resume_pos
                    start_time = time.time()
                    last_log_time = start_time
                    
                    while True:
                        if shutdown_requested:
                            logger.info(f"Pausing {dest_path.name} at {downloaded:,} bytes")
                            return False, "Shutdown requested"
                        
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log progress every 10 seconds
                        current_time = time.time()
                        if current_time - last_log_time > 10:
                            elapsed = current_time - start_time
                            speed = (downloaded - resume_pos) / elapsed / 1024 / 1024  # MB/s
                            if total_size > 0:
                                pct = downloaded / total_size * 100
                                logger.info(f"  {dest_path.name}: {pct:.1f}% ({downloaded:,}/{total_size:,} bytes) @ {speed:.2f} MB/s")
                            else:
                                logger.info(f"  {dest_path.name}: {downloaded:,} bytes @ {speed:.2f} MB/s")
                            last_log_time = current_time
            
            # Download complete, rename temp file
            temp_path.rename(dest_path)
            
            elapsed = time.time() - start_time
            size_mb = dest_path.stat().st_size / 1024 / 1024
            logger.info(f"✓ Downloaded {dest_path.name}: {size_mb:.1f} MB in {elapsed:.1f}s")
            return True, f"Downloaded {dest_path.name}"
            
        except HTTPError as e:
            if e.code == 416:  # Range not satisfiable - file already complete
                if temp_path.exists():
                    temp_path.rename(dest_path)
                    logger.info(f"✓ {dest_path.name} already complete")
                    return True, f"Already complete: {dest_path.name}"
            
            elif e.code == 429:  # Rate limited - exponential backoff
                wait_time = base_delay * (2 ** attempt) + (attempt * 2)  # Exponential + linear component
                logger.warning(f"Rate limited (429) for {dest_path.name}. "
                             f"Attempt {attempt + 1}/{max_retries}. "
                             f"Waiting {wait_time}s...")
                
                # Wait with interruptibility
                for _ in range(wait_time):
                    if shutdown_requested:
                        return False, "Shutdown requested"
                    time.sleep(1)
                continue
            
            else:
                logger.warning(f"HTTP error {e.code} for {dest_path.name}: {e.reason}")
            
        except (URLError, TimeoutError, ConnectionError) as e:
            wait_time = base_delay * (2 ** attempt)
            logger.warning(f"Network error for {dest_path.name}: {e}. "
                         f"Attempt {attempt + 1}/{max_retries}. "
                         f"Waiting {wait_time}s...")
            
            for _ in range(wait_time):
                if shutdown_requested:
                    return False, "Shutdown requested"
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Unexpected error downloading {dest_path.name}: {e}")
            return False, f"Error: {e}"
    
    return False, f"Failed after {max_retries} attempts"


def check_existing_files(output_dir: Path, files: list[str]) -> dict:
    """Check which files already exist and their sizes."""
    status = {}
    for filename in files:
        filepath = output_dir / filename
        if filepath.exists():
            status[filename] = {'exists': True, 'size': filepath.stat().st_size}
        elif filepath.with_suffix(filepath.suffix + '.partial').exists():
            partial = filepath.with_suffix(filepath.suffix + '.partial')
            status[filename] = {'exists': False, 'partial': True, 'partial_size': partial.stat().st_size}
        else:
            status[filename] = {'exists': False}
    return status


def download_all(output_dir: Path, delay_between_files: float = 2.0,
                 skip_existing: bool = True, metadata_only: bool = False,
                 parts_only: bool = False):
    """
    Download all files sequentially with delays to avoid rate limiting.
    
    Args:
        output_dir: Directory to save files
        delay_between_files: Seconds to wait between file downloads
        skip_existing: Skip files that already exist
        metadata_only: Only download metadata files (no part files)
        parts_only: Only download part files (no metadata)
    """
    global shutdown_requested
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine which files to download
    if metadata_only:
        files = METADATA_FILES
    elif parts_only:
        files = get_part_files()
    else:
        files = get_all_files()
    
    # Check existing files
    existing = check_existing_files(output_dir, files)
    
    # Build download queue
    to_download = []
    skipped = []
    for filename in files:
        status = existing.get(filename, {})
        if status.get('exists') and skip_existing:
            skipped.append(filename)
        else:
            to_download.append(filename)
    
    if skipped:
        logger.info(f"Skipping {len(skipped)} existing files")
    
    if not to_download:
        logger.info("All files already downloaded!")
        return {'success': [], 'failed': [], 'skipped': skipped}
    
    logger.info(f"Downloading {len(to_download)} files to {output_dir}")
    logger.info(f"Using sequential downloads with {delay_between_files}s delay between files")
    logger.info("Press Ctrl+C to pause (can resume later)\n")
    
    # Download files sequentially
    results = {'success': [], 'failed': [], 'skipped': skipped}
    
    for i, filename in enumerate(to_download):
        if shutdown_requested:
            # Add remaining files to failed list
            for remaining in to_download[i:]:
                results['failed'].append((remaining, "Shutdown requested"))
            break
        
        logger.info(f"[{i+1}/{len(to_download)}] Downloading {filename}...")
        url = BASE_URL + filename
        dest = output_dir / filename
        
        success, message = download_file(url, dest)
        
        if success:
            results['success'].append(filename)
        else:
            results['failed'].append((filename, message))
        
        # Delay between files (unless it's the last one or shutdown requested)
        if i < len(to_download) - 1 and not shutdown_requested:
            logger.debug(f"Waiting {delay_between_files}s before next download...")
            for _ in range(int(delay_between_files)):
                if shutdown_requested:
                    break
                time.sleep(1)
            # Handle fractional seconds
            time.sleep(delay_between_files % 1)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✓ Successful: {len(results['success'])}")
    logger.info(f"⊘ Skipped:    {len(results['skipped'])}")
    logger.info(f"✗ Failed:     {len(results['failed'])}")
    
    if results['failed']:
        failed_non_shutdown = [(f, e) for f, e in results['failed'] if e != "Shutdown requested"]
        if failed_non_shutdown:
            logger.error("\nFailed downloads:")
            for filename, error in failed_non_shutdown:
                logger.error(f"  - {filename}: {error}")
        
        pending = [f for f, e in results['failed'] if e == "Shutdown requested"]
        if pending:
            logger.info(f"\nPending (can resume): {len(pending)} files")
        
        logger.info("\nRun the script again to retry/resume remaining downloads.")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Download Web Data Commons Event Dataset (2024-12)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download everything (default, ~20GB, takes several hours)
  python download_wdc_events.py
  
  # Download only metadata files (fast, ~90MB)
  python download_wdc_events.py --metadata-only
  
  # Download to a specific directory
  python download_wdc_events.py -o /path/to/output
  
  # Adjust delay between files (default 2s)
  python download_wdc_events.py --delay 5
  
  # Force re-download of existing files
  python download_wdc_events.py --force
  
  # Resume after Ctrl+C - just run again!
  python download_wdc_events.py
        """
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '-d', '--delay',
        type=float,
        default=2.0,
        help='Seconds to wait between file downloads (default: 2.0)'
    )
    
    parser.add_argument(
        '--metadata-only',
        action='store_true',
        help='Only download metadata files (Event_*.csv, Event_sample.txt)'
    )
    
    parser.add_argument(
        '--parts-only',
        action='store_true', 
        help='Only download part files (part_*.gz)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-download existing files'
    )
    
    parser.add_argument(
        '--list-files',
        action='store_true',
        help='List all files without downloading'
    )
    
    args = parser.parse_args()
    
    if args.list_files:
        print("\nMetadata files:")
        for f in METADATA_FILES:
            print(f"  {f}")
        print(f"\nPart files: part_0.gz through part_{NUM_PARTS-1}.gz ({NUM_PARTS} files)")
        print(f"\nTotal: {len(get_all_files())} files (~20GB)")
        return
    
    logger.info("=" * 60)
    logger.info("Web Data Commons Event Dataset Downloader")
    logger.info("=" * 60)
    logger.info(f"Dataset: 2024-12 Common Crawl Event Markup")
    logger.info(f"Source:  {BASE_URL}")
    logger.info(f"Output:  {args.output_dir}")
    logger.info("")
    
    download_all(
        output_dir=args.output_dir,
        delay_between_files=args.delay,
        skip_existing=not args.force,
        metadata_only=args.metadata_only,
        parts_only=args.parts_only
    )


if __name__ == '__main__':
    main()
