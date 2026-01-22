#!/usr/bin/env python3
"""
Import jobs from observed_jobs table into watchable_positions.
These have richer descriptions from direct ATS scraping.
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from html import unescape

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

ROLE_TYPE_KEYWORDS = {
    'software_engineer': ['software engineer', 'developer', 'swe', 'backend', 'frontend', 'full stack', 'fullstack', 'programmer'],
    'data_scientist': ['data scientist', 'machine learning', 'ml engineer', 'ai engineer', 'research scientist'],
    'data_analyst': ['data analyst', 'business analyst', 'analytics', 'bi analyst'],
    'product_manager': ['product manager', 'product owner', 'pm'],
    'engineering_manager': ['engineering manager', 'tech lead', 'director of engineering', 'vp engineering'],
    'sales': ['sales', 'account executive', 'business development', 'bdr', 'sdr', 'account manager'],
    'marketing': ['marketing', 'content', 'seo', 'growth', 'brand'],
    'design': ['designer', 'ux', 'ui', 'graphic', 'product design'],
    'finance': ['finance', 'accounting', 'cfo', 'controller', 'bookkeeper', 'financial'],
    'hr': ['hr', 'human resources', 'people', 'recruiter', 'talent'],
    'support': ['support', 'customer success', 'customer service'],
    'operations': ['operations', 'ops', 'logistics', 'supply chain']
}

EXP_LEVEL_KEYWORDS = {
    'intern': ['intern', 'internship'],
    'entry': ['entry', 'junior', 'associate', ' i ', ' 1 ', 'new grad'],
    'senior': ['senior', 'sr ', 'lead', 'principal', 'staff', 'director', 'vp ', 'chief'],
    'mid': []  # Default
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def clean_html(text):
    """Remove HTML tags and clean up text."""
    if not text:
        return text

    # Convert common HTML entities
    text = unescape(text)

    # Replace common block elements with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)

    # Add bullet points for list items
    text = re.sub(r'<li[^>]*>', '• ', text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    text = text.strip()

    return text


def infer_role_type(title):
    """Infer role type from job title."""
    title_lower = title.lower() if title else ''

    for role_type, keywords in ROLE_TYPE_KEYWORDS.items():
        for kw in keywords:
            if kw in title_lower:
                return role_type

    return 'operations'  # Default


def infer_experience_level(title, seniority=None):
    """Infer experience level from title or seniority field."""
    # Use seniority field if available
    if seniority:
        seniority_lower = seniority.lower()
        if 'entry' in seniority_lower or 'junior' in seniority_lower:
            return 'entry'
        elif 'senior' in seniority_lower or 'lead' in seniority_lower:
            return 'senior'
        elif 'mid' in seniority_lower:
            return 'mid'

    title_lower = (title or '').lower()

    for level, keywords in EXP_LEVEL_KEYWORDS.items():
        for kw in keywords:
            if kw in title_lower:
                return level

    return 'mid'  # Default


def format_salary(salary_min, salary_max):
    """Format salary for display."""
    if salary_min is None and salary_max is None:
        return None

    def round_to_k(val):
        if val is None:
            return None
        return round(float(val) / 1000) * 1000

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


def import_observed_jobs():
    """Import observed_jobs into watchable_positions."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get Boston-area jobs from observed_jobs with good descriptions
        cur.execute("""
            SELECT
                raw_title, raw_company, raw_location, description, requirements,
                salary_min, salary_max, seniority, employment_type
            FROM observed_jobs
            WHERE (raw_location ILIKE '%%boston%%'
                   OR raw_location ILIKE '%%cambridge%%'
                   OR raw_location ILIKE '%%massachusetts%%'
                   OR raw_location ILIKE '%%, MA%%')
              AND description IS NOT NULL
              AND LENGTH(description) > 200
        """)

        jobs = cur.fetchall()
        print(f"Found {len(jobs)} jobs to import from observed_jobs")

        imported = 0
        updated = 0
        skipped = 0

        for job in jobs:
            title = job['raw_title']
            company = job['raw_company']
            location = job['raw_location']

            # Clean description (remove HTML)
            description = clean_html(job['description'])
            if job['requirements']:
                requirements = clean_html(job['requirements'])
                if requirements:
                    description = f"{description}\n\nRequirements:\n{requirements}"

            # Check if job already exists
            cur.execute("""
                SELECT id, description FROM watchable_positions
                WHERE LOWER(title) = LOWER(%s) AND LOWER(company_name) = LOWER(%s)
            """, (title, company))

            existing = cur.fetchone()

            if existing:
                # Update if new description is longer/better
                if len(description) > len(existing['description'] or ''):
                    cur.execute("""
                        UPDATE watchable_positions
                        SET description = %s
                        WHERE id = %s
                    """, (description, existing['id']))
                    updated += 1
                else:
                    skipped += 1
            else:
                # Insert new job
                role_type = infer_role_type(title)
                exp_level = infer_experience_level(title, job['seniority'])
                salary_range = format_salary(job['salary_min'], job['salary_max'])

                # Infer work arrangement from location/description
                work_arrangement = 'onsite'
                if 'remote' in (location or '').lower() or 'remote' in description.lower()[:500]:
                    work_arrangement = 'remote'
                elif 'hybrid' in description.lower()[:500]:
                    work_arrangement = 'hybrid'

                cur.execute("""
                    INSERT INTO watchable_positions
                    (title, company_name, location, description, salary_range,
                     salary_min, salary_max, role_type, experience_level,
                     work_arrangement, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'open')
                """, (
                    title, company, location, description, salary_range,
                    int(job['salary_min']) if job['salary_min'] else None,
                    int(job['salary_max']) if job['salary_max'] else None,
                    role_type, exp_level, work_arrangement
                ))
                imported += 1

        conn.commit()
        print(f"Imported: {imported}, Updated descriptions: {updated}, Skipped: {skipped}")

    conn.close()
    return imported, updated


def update_short_descriptions():
    """
    Find Cambridge jobs with short descriptions and see if we can
    enhance them from observed_jobs by matching company.
    """
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find watchable_positions with short descriptions
        cur.execute("""
            SELECT wp.id, wp.title, wp.company_name, wp.description,
                   LENGTH(wp.description) as desc_len
            FROM watchable_positions wp
            WHERE LENGTH(wp.description) < 300
              AND wp.company_name IS NOT NULL
            ORDER BY desc_len
            LIMIT 100
        """)

        short_desc_jobs = cur.fetchall()
        print(f"Found {len(short_desc_jobs)} jobs with short descriptions")

        enhanced = 0
        for job in short_desc_jobs:
            # Try to find a better description from observed_jobs for same company
            cur.execute("""
                SELECT description FROM observed_jobs
                WHERE LOWER(raw_company) = LOWER(%s)
                  AND description IS NOT NULL
                  AND LENGTH(description) > 500
                LIMIT 1
            """, (job['company_name'],))

            better = cur.fetchone()
            if better:
                clean_desc = clean_html(better['description'])
                if len(clean_desc) > len(job['description'] or ''):
                    # We found a better description from the same company
                    # Note: This is a fallback - ideally we'd match by similar title too
                    pass  # Skip for now as it might not be the same role

        print(f"Enhanced {enhanced} descriptions")

    conn.close()


if __name__ == '__main__':
    print("=" * 50)
    print("Importing observed_jobs into watchable_positions")
    print("=" * 50)

    import_observed_jobs()

    print("\nDone!")
