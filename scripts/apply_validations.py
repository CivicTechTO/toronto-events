#!/usr/bin/env python3
"""
Apply user validations to the domain scores dataset.

Reads the validations.json exported from the validation UI and updates
domain_scores.csv with the validated classifications.
"""

import csv
import json
import argparse
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_INTERMEDIATE = PROJECT_ROOT / "data" / "intermediate"


def load_validations(path: Path) -> dict:
    """Load validations from exported JSON."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data.get('validations', {})


def apply_validations(scores_path: Path, validations: dict, output_path: Path):
    """Apply validations to domain scores and write updated CSV."""
    rows = []
    fieldnames = None
    
    with open(scores_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        # Add new columns if not present
        if 'validated' not in fieldnames:
            fieldnames = list(fieldnames) + ['validated', 'validation_status', 'validated_at']
        
        for row in reader:
            domain = row.get('domain', '')
            if domain in validations:
                v = validations[domain]
                row['validated'] = 'true'
                row['validation_status'] = v.get('status', '')
                row['validated_at'] = v.get('timestamp', '')
                
                # Override classification for rejected domains
                if v.get('status') == 'reject':
                    row['classification'] = 'rejected'
            rows.append(row)
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_report(validations: dict) -> str:
    """Generate a summary report of validations."""
    by_status = {}
    for domain, v in validations.items():
        status = v.get('status', 'unknown')
        by_status[status] = by_status.get(status, 0) + 1
    
    lines = [
        "Validation Summary",
        "=" * 40,
        f"Total validated: {len(validations)}",
        ""
    ]
    
    for status in ['accept', 'reject', 'uncertain']:
        count = by_status.get(status, 0)
        lines.append(f"  {status.capitalize()}: {count}")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Apply validations to domain scores')
    parser.add_argument(
        'validations',
        type=Path,
        help='Path to validations.json exported from the UI'
    )
    parser.add_argument(
        '--scores', '-s',
        type=Path,
        default=DATA_INTERMEDIATE / 'domain_scores.csv',
        help='Path to domain_scores.csv'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=None,
        help='Output path (default: overwrite input)'
    )
    parser.add_argument(
        '--backup', '-b',
        action='store_true',
        help='Create backup before modifying'
    )
    args = parser.parse_args()

    if not args.validations.exists():
        print(f"Error: Validations file not found: {args.validations}")
        return 1

    if not args.scores.exists():
        print(f"Error: Scores file not found: {args.scores}")
        return 1

    # Load validations
    print(f"Loading validations from: {args.validations}")
    validations = load_validations(args.validations)
    
    if not validations:
        print("No validations found in file.")
        return 1

    print(generate_report(validations))
    print()

    # Backup if requested
    if args.backup:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = args.scores.with_suffix(f'.backup_{timestamp}.csv')
        import shutil
        shutil.copy(args.scores, backup_path)
        print(f"Backup created: {backup_path}")

    # Apply validations
    output_path = args.output or args.scores
    print(f"Applying validations to: {output_path}")
    apply_validations(args.scores, validations, output_path)
    
    print("Done!")
    return 0


if __name__ == '__main__':
    exit(main())
