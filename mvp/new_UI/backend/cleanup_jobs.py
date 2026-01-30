#!/usr/bin/env python3
"""
Cleanup script for job data:
1. Remove duplicate jobs (same title + company)
2. Fix salary display (exact salaries → single value, round to $1k)
3. Clean job titles (remove [REMOTE] prefixes, etc.)
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


def clean_title(title):
    """Clean job titles by removing noise like job codes, IDs, etc."""
    if not title:
        return title

    cleaned = title

    # Remove bracketed prefixes like [REMOTE], [HYBRID], [ON-SITE]
    cleaned = re.sub(r'^\s*\[(?:REMOTE|HYBRID|ON-?SITE|ONSITE)\]\s*', '', cleaned, flags=re.IGNORECASE)

    # Remove job codes in parentheses at the end like (JP13711), (SE3), (#2464)
    cleaned = re.sub(r'\s*\([A-Z]{2}\d+\)\s*$', '', cleaned)  # (JP13711)
    cleaned = re.sub(r'\s*\(#?\d+\)\s*$', '', cleaned)  # (#2464) or (12345)

    # Remove job codes with # in the middle like "Field Consultant #2464 (Boston, MA)"
    cleaned = re.sub(r'\s*#\d+\s*', ' ', cleaned)

    # Remove "Job ID: XXX" or "Req ID: XXX" patterns
    cleaned = re.sub(r'\s*(?:Job|Req|Requisition)\s*(?:ID|#)?:?\s*[A-Z0-9-]+\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove location in parentheses at end like "(Boston, MA)" or "(Remote)"
    cleaned = re.sub(r'\s*\([^)]*(?:MA|Remote|Hybrid|On-?site)[^)]*\)\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove trailing dashes or colons
    cleaned = re.sub(r'\s*[-:]\s*$', '', cleaned)

    # Normalize multiple spaces to single space
    cleaned = re.sub(r'\s+', ' ', cleaned)

    # Remove leading/trailing whitespace
    cleaned = cleaned.strip()

    return cleaned


def format_salary(salary_min, salary_max):
    """
    Format salary range for display.
    - If exact (min == max), show single value
    - Round to nearest $1,000
    - Return formatted string like "$70,000" or "$70,000 - $90,000"
    """
    if salary_min is None and salary_max is None:
        return None

    def round_to_k(val):
        """Round to nearest $1,000."""
        if val is None:
            return None
        return round(val / 1000) * 1000

    min_rounded = round_to_k(salary_min)
    max_rounded = round_to_k(salary_max)

    # If only one value, or they're equal after rounding
    if min_rounded == max_rounded and min_rounded is not None:
        return f"${min_rounded:,.0f}"

    if min_rounded is not None and max_rounded is not None:
        return f"${min_rounded:,.0f} - ${max_rounded:,.0f}"

    if min_rounded is not None:
        return f"${min_rounded:,.0f}+"

    if max_rounded is not None:
        return f"Up to ${max_rounded:,.0f}"

    return None


def remove_duplicates():
    """Remove duplicate jobs, keeping only one per title+company combination."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find duplicates (same title and company_name), excluding any with applications
        cur.execute("""
            SELECT title, company_name, COUNT(*) as cnt,
                   array_agg(id ORDER BY id) as ids
            FROM watchable_positions
            WHERE title IS NOT NULL AND company_name IS NOT NULL
            GROUP BY title, company_name
            HAVING COUNT(*) > 1
        """)

        duplicates = cur.fetchall()
        print(f"Found {len(duplicates)} groups of duplicate jobs")

        total_deleted = 0
        for dup in duplicates:
            ids = dup['ids']
            # Keep the first one (lowest id), delete the rest
            ids_to_delete = ids[1:]  # All but the first

            if ids_to_delete:
                # Skip any IDs that have applications
                cur.execute("""
                    SELECT DISTINCT position_id FROM shortlist_applications
                    WHERE position_id = ANY(%s)
                """, (ids_to_delete,))
                ids_with_apps = {row['position_id'] for row in cur.fetchall()}

                # Filter out IDs that have applications
                ids_to_delete = [id for id in ids_to_delete if id not in ids_with_apps]

                if ids_to_delete:
                    # First, delete related records in job_required_skills
                    cur.execute("""
                        DELETE FROM job_required_skills WHERE position_id = ANY(%s)
                    """, (ids_to_delete,))

                    # Then delete the duplicate positions
                    cur.execute("""
                        DELETE FROM watchable_positions WHERE id = ANY(%s)
                    """, (ids_to_delete,))

                    total_deleted += len(ids_to_delete)

        conn.commit()
        print(f"Deleted {total_deleted} duplicate jobs")

    conn.close()
    return total_deleted


def clean_titles():
    """Clean all job titles by removing noise like job codes, IDs, brackets, etc."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find titles that likely need cleaning (have brackets, job codes, etc.)
        cur.execute(r"""
            SELECT id, title FROM watchable_positions
            WHERE title ~ '^\s*\['
               OR title ~ '\([A-Z]{2}\d+\)'
               OR title ~ '#\d+'
               OR title ~ '\(.*(?:MA|Remote|Hybrid).*\)'
               OR title ~ '(?:Job|Req)\s*(?:ID|#)'
        """)

        jobs_to_clean = cur.fetchall()
        print(f"Found {len(jobs_to_clean)} job titles to clean")

        cleaned_count = 0
        for job in jobs_to_clean:
            old_title = job['title']
            new_title = clean_title(old_title)

            if new_title != old_title:
                cur.execute("""
                    UPDATE watchable_positions SET title = %s WHERE id = %s
                """, (new_title, job['id']))
                cleaned_count += 1

        conn.commit()
        print(f"Cleaned {cleaned_count} job titles")

    conn.close()
    return cleaned_count


def fix_salary_ranges():
    """Fix salary_range display format for all jobs."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get all jobs with salary data
        cur.execute("""
            SELECT id, salary_min, salary_max, salary_range
            FROM watchable_positions
            WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL
        """)

        jobs = cur.fetchall()
        print(f"Found {len(jobs)} jobs with salary data")

        updated_count = 0
        for job in jobs:
            new_range = format_salary(job['salary_min'], job['salary_max'])

            if new_range != job['salary_range']:
                cur.execute("""
                    UPDATE watchable_positions SET salary_range = %s WHERE id = %s
                """, (new_range, job['id']))
                updated_count += 1

        conn.commit()
        print(f"Updated {updated_count} salary ranges")

    conn.close()
    return updated_count


def main():
    print("=" * 50)
    print("Job Data Cleanup")
    print("=" * 50)

    print("\n1. Removing duplicate jobs...")
    remove_duplicates()

    print("\n2. Cleaning job titles...")
    clean_titles()

    print("\n3. Fixing salary ranges...")
    fix_salary_ranges()

    print("\n" + "=" * 50)
    print("Cleanup complete!")
    print("=" * 50)


if __name__ == '__main__':
    main()
