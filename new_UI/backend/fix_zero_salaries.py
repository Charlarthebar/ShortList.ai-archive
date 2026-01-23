#!/usr/bin/env python3
"""
Fix jobs with $0 salaries:
1. For jobs with $0 min but valid max, set min to reasonable estimate
2. For jobs with no valid salary, infer based on role type and title
3. Remove jobs that can't be reasonably estimated
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

# Salary estimates by role type (annual, in dollars)
ROLE_SALARY_RANGES = {
    'software_engineer': (90000, 160000),
    'data_scientist': (95000, 170000),
    'data_analyst': (65000, 110000),
    'product_manager': (100000, 170000),
    'engineering_manager': (140000, 220000),
    'sales': (55000, 120000),
    'marketing': (55000, 100000),
    'design': (70000, 130000),
    'finance': (65000, 120000),
    'hr': (55000, 95000),
    'support': (40000, 70000),
    'operations': (45000, 85000),
}

# Title-based salary adjustments
TITLE_SALARY_MODIFIERS = {
    # Senior/Lead positions get higher salaries
    'senior': 1.25,
    'lead': 1.3,
    'principal': 1.4,
    'staff': 1.35,
    'director': 1.5,
    'vp': 1.7,
    'chief': 2.0,
    'head of': 1.5,
    # Entry level gets lower
    'junior': 0.75,
    'entry': 0.7,
    'intern': 0.4,
    'associate': 0.85,
    # Specific roles
    'nurse': (60000, 95000),
    'physician': (180000, 350000),
    'doctor': (180000, 350000),
    'lawyer': (100000, 200000),
    'attorney': (100000, 200000),
    'accountant': (55000, 100000),
    'teacher': (45000, 75000),
    'professor': (70000, 150000),
    'cook': (30000, 50000),
    'bartender': (25000, 45000),
    'server': (25000, 45000),
    'cashier': (28000, 40000),
    'janitor': (28000, 45000),
    'custodian': (28000, 45000),
    'maintenance': (35000, 60000),
    'technician': (40000, 70000),
    'mechanic': (40000, 70000),
    'electrician': (50000, 85000),
    'plumber': (50000, 85000),
    'driver': (40000, 65000),
    'cdl': (50000, 80000),
    'warehouse': (35000, 55000),
    'forklift': (35000, 55000),
    'security': (35000, 55000),
    'guard': (32000, 50000),
    'receptionist': (32000, 48000),
    'administrative': (38000, 60000),
    'executive assistant': (50000, 80000),
    'analyst': (60000, 100000),
    'consultant': (70000, 140000),
    'manager': (70000, 120000),
    'engineer': (75000, 140000),
    'scientist': (70000, 130000),
    'researcher': (60000, 110000),
    'specialist': (50000, 85000),
    'coordinator': (45000, 70000),
    'representative': (40000, 70000),
    'agent': (40000, 70000),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def estimate_salary(title, role_type, experience_level=None):
    """Estimate a reasonable salary range based on title and role type."""
    title_lower = (title or '').lower()

    # Check for specific job titles that override role-based estimates
    specific_overrides = [
        ('nurse', (60000, 95000)),
        ('physician', (180000, 350000)),
        ('doctor', (180000, 350000)),
        ('lawyer', (100000, 200000)),
        ('attorney', (100000, 200000)),
        ('cook', (30000, 50000)),
        ('bartender', (25000, 45000)),
        ('server', (25000, 45000)),
        ('cashier', (28000, 40000)),
        ('janitor', (28000, 45000)),
        ('custodian', (28000, 45000)),
        ('cdl', (50000, 80000)),
        ('truck driver', (50000, 80000)),
    ]

    for keyword, salary_range in specific_overrides:
        if keyword in title_lower:
            return salary_range

    # Get base salary from role type
    base_range = ROLE_SALARY_RANGES.get(role_type, (50000, 90000))
    min_sal, max_sal = base_range

    # Apply modifiers based on seniority in title
    seniority_modifiers = [
        ('chief', 2.0),
        ('vp ', 1.7),
        ('vice president', 1.7),
        ('director', 1.5),
        ('head of', 1.5),
        ('principal', 1.4),
        ('staff', 1.35),
        ('lead', 1.3),
        ('senior', 1.25),
        ('sr ', 1.25),
        ('sr.', 1.25),
        ('associate', 0.85),
        ('junior', 0.75),
        ('jr ', 0.75),
        ('jr.', 0.75),
        ('entry', 0.7),
        ('intern', 0.4),
    ]

    modifier = 1.0
    for keyword, mod in seniority_modifiers:
        if keyword in title_lower:
            modifier = mod
            break  # Use first match (highest priority)

    # Apply experience level modifier if no title modifier found
    if modifier == 1.0 and experience_level:
        if experience_level == 'senior':
            modifier = 1.25
        elif experience_level == 'entry':
            modifier = 0.75
        elif experience_level == 'intern':
            modifier = 0.4

    min_sal = int(min_sal * modifier)
    max_sal = int(max_sal * modifier)

    # Round to nearest $5k
    min_sal = round(min_sal / 5000) * 5000
    max_sal = round(max_sal / 5000) * 5000

    return (min_sal, max_sal)


def format_salary(salary_min, salary_max):
    """Format salary for display."""
    if salary_min is None and salary_max is None:
        return None

    def round_to_k(val):
        if val is None:
            return None
        return round(val / 1000) * 1000

    min_rounded = round_to_k(salary_min)
    max_rounded = round_to_k(salary_max)

    if min_rounded == max_rounded and min_rounded is not None:
        return f"${min_rounded:,.0f}"

    if min_rounded is not None and max_rounded is not None:
        return f"${min_rounded:,.0f} - ${max_rounded:,.0f}"

    if min_rounded is not None:
        return f"${min_rounded:,.0f}+"

    if max_rounded is not None:
        return f"Up to ${max_rounded:,.0f}"

    return None


def fix_zero_salaries(dry_run=False):
    """Fix all jobs with $0 salaries."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get all jobs with zero or missing salary
        cur.execute("""
            SELECT id, title, company_name, role_type, experience_level,
                   salary_range, salary_min, salary_max
            FROM watchable_positions
            WHERE salary_range LIKE '%$0%'
               OR salary_min = 0
               OR salary_max = 0
               OR (salary_min IS NULL AND salary_max IS NULL)
        """)

        jobs = cur.fetchall()
        print(f"Found {len(jobs)} jobs with zero/missing salary")

        fixed = 0
        removed = 0

        for job in jobs:
            job_id = job['id']
            title = job['title']
            role_type = job['role_type']
            exp_level = job['experience_level']
            current_min = job['salary_min']
            current_max = job['salary_max']

            # Case 1: Has a max but min is 0 - set min to ~70% of max
            if current_max and current_max > 0 and (current_min is None or current_min == 0):
                new_min = int(current_max * 0.7)
                new_min = round(new_min / 5000) * 5000
                new_range = format_salary(new_min, current_max)

                if not dry_run:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET salary_min = %s, salary_range = %s
                        WHERE id = %s
                    """, (new_min, new_range, job_id))

                print(f"  Fixed min: {title[:50]} -> {new_range}")
                fixed += 1
                continue

            # Case 2: Has a min but max is 0 - set max to ~130% of min
            if current_min and current_min > 0 and (current_max is None or current_max == 0):
                new_max = int(current_min * 1.3)
                new_max = round(new_max / 5000) * 5000
                new_range = format_salary(current_min, new_max)

                if not dry_run:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET salary_max = %s, salary_range = %s
                        WHERE id = %s
                    """, (new_max, new_range, job_id))

                print(f"  Fixed max: {title[:50]} -> {new_range}")
                fixed += 1
                continue

            # Case 3: Both are zero/null - estimate based on title/role
            estimated = estimate_salary(title, role_type, exp_level)
            if estimated:
                new_min, new_max = estimated
                new_range = format_salary(new_min, new_max)

                if not dry_run:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET salary_min = %s, salary_max = %s, salary_range = %s
                        WHERE id = %s
                    """, (new_min, new_max, new_range, job_id))

                print(f"  Estimated: {title[:50]} -> {new_range}")
                fixed += 1
            else:
                # Can't estimate - remove the job
                if not dry_run:
                    # First remove related skills
                    cur.execute("DELETE FROM job_required_skills WHERE position_id = %s", (job_id,))
                    # Then remove the job
                    cur.execute("DELETE FROM watchable_positions WHERE id = %s", (job_id,))

                print(f"  Removed: {title[:50]} (can't estimate salary)")
                removed += 1

        if not dry_run:
            conn.commit()

        print(f"\n{'='*50}")
        print(f"Fixed: {fixed} jobs")
        print(f"Removed: {removed} jobs")
        if dry_run:
            print("(DRY RUN - no changes made)")

    conn.close()
    return fixed, removed


def show_sample_estimates():
    """Show sample salary estimates for verification."""
    test_cases = [
        ("Senior Software Engineer", "software_engineer", "senior"),
        ("Junior Data Analyst", "data_analyst", "entry"),
        ("Bartender", "operations", None),
        ("Cook", "operations", None),
        ("Registered Nurse", "operations", None),
        ("Product Manager", "product_manager", "mid"),
        ("CDL-A Driver", "operations", None),
        ("Inside Sales Representative", "sales", None),
        ("Customer Support Agent", "support", None),
        ("Director of Engineering", "engineering_manager", "senior"),
        ("Intern - Software Development", "software_engineer", "intern"),
        ("Maintenance Mechanic", "operations", None),
    ]

    print("Sample salary estimates:")
    print("-" * 60)
    for title, role_type, exp in test_cases:
        est = estimate_salary(title, role_type, exp)
        formatted = format_salary(est[0], est[1])
        print(f"  {title[:40]:40} -> {formatted}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Fix jobs with $0 salaries')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without making changes')
    parser.add_argument('--preview', action='store_true', help='Show sample salary estimates')

    args = parser.parse_args()

    if args.preview:
        show_sample_estimates()
    else:
        fix_zero_salaries(dry_run=args.dry_run)
