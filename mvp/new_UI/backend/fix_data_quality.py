#!/usr/bin/env python3
"""
Fix data quality issues in job listings:
1. Remove ", Unknown" from locations
2. Limit locations to max 5, prioritizing Boston
3. Convert intern salaries to hourly rates displayed properly
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


def clean_location(location):
    """Clean up location string:
    - Remove ', Unknown' suffix
    - Limit to 5 locations, prioritizing Boston/Massachusetts
    """
    if not location:
        return location

    cleaned = location

    # Remove ", Unknown" suffix
    cleaned = re.sub(r',\s*Unknown\s*$', '', cleaned)
    cleaned = re.sub(r',\s*Unknown\s*;', ';', cleaned)

    # Check if there are multiple locations (separated by ; or ,)
    # Use semicolon as primary separator
    if ';' in cleaned:
        locations = [loc.strip() for loc in cleaned.split(';') if loc.strip()]
    elif cleaned.count(',') > 2:  # Multiple locations separated by commas
        # Be careful - some locations have commas (e.g., "Boston, MA")
        # Split on patterns like "City, ST; City, ST" or "City; City"
        locations = [loc.strip() for loc in re.split(r';\s*', cleaned) if loc.strip()]
        if len(locations) == 1:
            # Try splitting on comma-separated cities
            # Match pattern: word(s), ST, word(s), ST
            parts = cleaned.split(', ')
            if len(parts) > 4:
                # Reconstruct as "City, ST" pairs
                locations = []
                i = 0
                while i < len(parts):
                    if i + 1 < len(parts) and len(parts[i + 1]) <= 3:
                        # This looks like "City, ST"
                        locations.append(f"{parts[i]}, {parts[i + 1]}")
                        i += 2
                    else:
                        locations.append(parts[i])
                        i += 1
    else:
        locations = [cleaned]

    if len(locations) <= 5:
        return cleaned

    # More than 5 locations - need to prioritize
    boston_related = []
    remote_locations = []
    other_locations = []

    for loc in locations:
        loc_lower = loc.lower()
        if 'boston' in loc_lower or 'massachusetts' in loc_lower or ', ma' in loc_lower:
            boston_related.append(loc)
        elif 'remote' in loc_lower:
            remote_locations.append(loc)
        else:
            other_locations.append(loc)

    # Build final list: Boston first, then remote, then others
    final_locations = boston_related[:2] + remote_locations[:1] + other_locations

    # Take first 5
    final_locations = final_locations[:5]

    if not final_locations:
        final_locations = locations[:5]

    return '; '.join(final_locations)


def fix_intern_salary(title, salary_range, salary_min, salary_max):
    """
    Fix intern salaries - convert annual to hourly if needed.
    Interns typically make $20-50/hr for summer internships.
    """
    if not title:
        return salary_range, salary_min, salary_max

    title_lower = title.lower()

    # Check if this is an internship - must match "intern" as a word boundary
    # Exclude: internal, international, internet
    is_intern = bool(re.search(r'\bintern\b|\binternship\b', title_lower))

    if not is_intern:
        return salary_range, salary_min, salary_max

    # If salary looks like annual (>$60k), it's probably wrong
    # Summer interns working 12 weeks at 40 hrs/week = 480 hours
    # At $50/hr that's $24k for the summer

    if salary_min and salary_min > 60000:
        # Convert to estimated hourly rate
        # Assume 480 hours for summer internship
        hourly_min = round(salary_min / 2080)  # If they listed annual, convert
        hourly_max = round(salary_max / 2080) if salary_max else hourly_min

        # Cap at reasonable intern rates
        hourly_min = max(20, min(hourly_min, 80))
        hourly_max = max(hourly_min, min(hourly_max, 100))

        new_salary_range = f"${hourly_min}/hr - ${hourly_max}/hr"
        # Store as estimated summer earnings
        new_min = hourly_min * 480  # ~12 week internship
        new_max = hourly_max * 480

        return new_salary_range, new_min, new_max

    return salary_range, salary_min, salary_max


def fix_locations(dry_run=True):
    """Fix location data quality issues."""
    conn = get_db()
    fixed = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find jobs with location issues
        cur.execute("""
            SELECT id, title, location
            FROM watchable_positions
            WHERE location ILIKE '%unknown%'
               OR LENGTH(location) > 100
        """)
        jobs = cur.fetchall()

        print(f"Found {len(jobs)} jobs with location issues")

        for job in jobs:
            old_location = job['location']
            new_location = clean_location(old_location)

            if old_location != new_location:
                print(f"\n{job['title'][:50]}...")
                print(f"  Old: {old_location[:80]}...")
                print(f"  New: {new_location}")

                if not dry_run:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET location = %s
                        WHERE id = %s
                    """, (new_location, job['id']))

                fixed += 1

        if not dry_run:
            conn.commit()

    conn.close()
    return fixed


def fix_intern_salaries(dry_run=True):
    """Fix intern salary data."""
    conn = get_db()
    fixed = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find intern jobs with high salaries
        # Use word boundary matching to avoid "internal", "international", "internet"
        cur.execute("""
            SELECT id, title, salary_range, salary_min, salary_max
            FROM watchable_positions
            WHERE (title ~* '\\yintern\\y' OR title ~* '\\yinternship\\y')
              AND salary_min > 60000
        """)
        jobs = cur.fetchall()

        print(f"\nFound {len(jobs)} intern jobs with high salaries")

        for job in jobs:
            new_range, new_min, new_max = fix_intern_salary(
                job['title'],
                job['salary_range'],
                job['salary_min'],
                job['salary_max']
            )

            if new_range != job['salary_range']:
                print(f"\n{job['title']}")
                print(f"  Old: {job['salary_range']} (${job['salary_min']:,} - ${job['salary_max']:,})")
                print(f"  New: {new_range} (${new_min:,} - ${new_max:,} summer earnings)")

                if not dry_run:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET salary_range = %s, salary_min = %s, salary_max = %s
                        WHERE id = %s
                    """, (new_range, new_min, new_max, job['id']))

                fixed += 1

        if not dry_run:
            conn.commit()

    conn.close()
    return fixed


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fix data quality issues')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Show changes without applying them')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the changes')
    parser.add_argument('--locations', action='store_true',
                        help='Fix location issues')
    parser.add_argument('--salaries', action='store_true',
                        help='Fix intern salary issues')
    parser.add_argument('--all', action='store_true',
                        help='Fix all issues')

    args = parser.parse_args()
    dry_run = not args.apply

    if dry_run:
        print("DRY RUN - No changes will be made")
        print("Use --apply to actually make changes\n")

    if args.locations or args.all:
        print("=" * 60)
        print("FIXING LOCATIONS")
        print("=" * 60)
        fixed = fix_locations(dry_run=dry_run)
        print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed} location issues")

    if args.salaries or args.all:
        print("\n" + "=" * 60)
        print("FIXING INTERN SALARIES")
        print("=" * 60)
        fixed = fix_intern_salaries(dry_run=dry_run)
        print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed} salary issues")

    if not any([args.locations, args.salaries, args.all]):
        parser.print_help()


if __name__ == '__main__':
    main()
