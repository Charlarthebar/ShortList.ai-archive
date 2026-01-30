#!/usr/bin/env python3
"""
Fix salary data issues:
1. Extract compensation from job descriptions and sync with salary fields
2. Tighten salary ranges to max $20k spread where appropriate
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def parse_salary_from_text(text):
    """
    Extract salary min and max from text like:
    - "$140k - $180k"
    - "$140,000 - $180,000"
    - "$95,000"
    - "$70k"
    Returns (min_salary, max_salary) as integers, or (None, None) if not found.
    """
    if not text:
        return None, None

    # Pattern for k-format: $140k or $140K
    k_pattern = r'\$(\d+(?:\.\d+)?)\s*[kK]'
    # Pattern for full format: $140,000 or $140000
    full_pattern = r'\$(\d{1,3}(?:,\d{3})*|\d+)'

    # Find all k-format matches
    k_matches = re.findall(k_pattern, text)
    if k_matches:
        salaries = [int(float(m) * 1000) for m in k_matches]
        if len(salaries) >= 2:
            return min(salaries), max(salaries)
        elif len(salaries) == 1:
            return salaries[0], salaries[0]

    # Find all full-format matches
    full_matches = re.findall(full_pattern, text)
    if full_matches:
        salaries = [int(m.replace(',', '')) for m in full_matches]
        # Filter out unreasonably small numbers (likely not salaries)
        salaries = [s for s in salaries if s >= 20000]
        if len(salaries) >= 2:
            return min(salaries), max(salaries)
        elif len(salaries) == 1:
            return salaries[0], salaries[0]

    return None, None


def extract_compensation_from_description(description):
    """
    Extract compensation info from job description.
    Looks for patterns like "Compensation: $X - $Y"
    """
    if not description:
        return None, None

    # Look for Compensation: or Salary: line
    comp_match = re.search(r'(?:Compensation|Salary):\s*([^\n]+)', description, re.IGNORECASE)
    if comp_match:
        comp_text = comp_match.group(1)
        return parse_salary_from_text(comp_text)

    return None, None


def format_salary_range(min_sal, max_sal):
    """Format salary range as a string."""
    if min_sal == max_sal:
        return f"${min_sal:,}"
    return f"${min_sal:,} - ${max_sal:,}"


def tighten_range(min_sal, max_sal, max_spread=20000):
    """
    Tighten salary range to max $20k spread.
    Uses the midpoint and creates a symmetric range.
    """
    if min_sal is None or max_sal is None:
        return min_sal, max_sal

    spread = max_sal - min_sal
    if spread <= max_spread:
        return min_sal, max_sal

    # Calculate midpoint
    midpoint = (min_sal + max_sal) // 2

    # Create a range around the midpoint
    half_spread = max_spread // 2
    new_min = midpoint - half_spread
    new_max = midpoint + half_spread

    # Round to nearest 5000
    new_min = round(new_min / 5000) * 5000
    new_max = round(new_max / 5000) * 5000

    return new_min, new_max


def sync_salaries_with_description(dry_run=True):
    """
    Find jobs where description compensation doesn't match salary fields.
    Update to match the description (source of truth).
    """
    conn = get_db()
    fixed = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find jobs with compensation in description
        cur.execute("""
            SELECT id, title, company_name, salary_range, salary_min, salary_max, description
            FROM watchable_positions
            WHERE description ILIKE '%Compensation:%'
               OR description ILIKE '%Salary:%'
        """)
        jobs = cur.fetchall()

        print(f"Found {len(jobs)} jobs with compensation in description")

        mismatches = []
        for job in jobs:
            desc_min, desc_max = extract_compensation_from_description(job['description'])

            if desc_min is None:
                continue

            # Check for significant mismatch (more than 10% difference)
            if job['salary_min'] and desc_min:
                diff_min = abs(job['salary_min'] - desc_min) / max(job['salary_min'], desc_min)
                diff_max = abs((job['salary_max'] or job['salary_min']) - desc_max) / max(job['salary_max'] or job['salary_min'], desc_max) if desc_max else 0

                if diff_min > 0.1 or diff_max > 0.1:
                    mismatches.append({
                        'job': job,
                        'desc_min': desc_min,
                        'desc_max': desc_max
                    })

        print(f"\nFound {len(mismatches)} jobs with salary mismatches")

        for m in mismatches:
            job = m['job']
            desc_min = m['desc_min']
            desc_max = m['desc_max']
            new_range = format_salary_range(desc_min, desc_max)

            print(f"\n{job['title']} @ {job['company_name']}")
            print(f"  Old: {job['salary_range']} (${job['salary_min']:,} - ${job['salary_max']:,})")
            print(f"  New: {new_range} (${desc_min:,} - ${desc_max:,})")

            if not dry_run:
                cur.execute("""
                    UPDATE watchable_positions
                    SET salary_range = %s, salary_min = %s, salary_max = %s
                    WHERE id = %s
                """, (new_range, desc_min, desc_max, job['id']))

            fixed += 1

        if not dry_run:
            conn.commit()

    conn.close()
    return fixed


def tighten_all_salary_ranges(dry_run=True, max_spread=20000):
    """
    Tighten all salary ranges to max $20k spread.
    """
    conn = get_db()
    fixed = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find jobs with wide salary ranges
        cur.execute("""
            SELECT id, title, company_name, salary_range, salary_min, salary_max
            FROM watchable_positions
            WHERE salary_min IS NOT NULL
              AND salary_max IS NOT NULL
              AND (salary_max - salary_min) > %s
        """, (max_spread,))
        jobs = cur.fetchall()

        print(f"\nFound {len(jobs)} jobs with salary ranges > ${max_spread:,}")

        if dry_run:
            print("\nFirst 20 examples:")
            for job in jobs[:20]:
                new_min, new_max = tighten_range(job['salary_min'], job['salary_max'], max_spread)
                new_range = format_salary_range(new_min, new_max)
                old_spread = job['salary_max'] - job['salary_min']
                new_spread = new_max - new_min

                print(f"\n{job['title'][:50]}... @ {job['company_name'][:20]}")
                print(f"  Old: {job['salary_range']} (spread: ${old_spread:,})")
                print(f"  New: {new_range} (spread: ${new_spread:,})")

        if not dry_run:
            for job in jobs:
                new_min, new_max = tighten_range(job['salary_min'], job['salary_max'], max_spread)
                new_range = format_salary_range(new_min, new_max)

                cur.execute("""
                    UPDATE watchable_positions
                    SET salary_range = %s, salary_min = %s, salary_max = %s
                    WHERE id = %s
                """, (new_range, new_min, new_max, job['id']))

                fixed += 1

                if fixed % 100 == 0:
                    print(f"  Updated {fixed} jobs...")
                    conn.commit()

            conn.commit()
        else:
            fixed = len(jobs)

    conn.close()
    return fixed


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fix salary data issues')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Show changes without applying them')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the changes')
    parser.add_argument('--sync', action='store_true',
                        help='Sync salary fields with description compensation')
    parser.add_argument('--tighten', action='store_true',
                        help='Tighten salary ranges to max $20k spread')
    parser.add_argument('--max-spread', type=int, default=20000,
                        help='Maximum salary spread (default: 20000)')
    parser.add_argument('--all', action='store_true',
                        help='Run all fixes')

    args = parser.parse_args()
    dry_run = not args.apply

    if dry_run:
        print("DRY RUN - No changes will be made")
        print("Use --apply to actually make changes\n")

    if args.sync or args.all:
        print("=" * 60)
        print("SYNCING SALARIES WITH DESCRIPTIONS")
        print("=" * 60)
        fixed = sync_salaries_with_description(dry_run=dry_run)
        print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed} salary mismatches")

    if args.tighten or args.all:
        print("\n" + "=" * 60)
        print(f"TIGHTENING SALARY RANGES (max ${args.max_spread:,} spread)")
        print("=" * 60)
        fixed = tighten_all_salary_ranges(dry_run=dry_run, max_spread=args.max_spread)
        print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed} wide salary ranges")

    if not any([args.sync, args.tighten, args.all]):
        parser.print_help()


if __name__ == '__main__':
    main()
