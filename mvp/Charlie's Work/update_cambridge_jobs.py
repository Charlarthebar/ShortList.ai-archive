#!/usr/bin/env python3
"""
Update Cambridge Jobs Database
- Cleans HTML from job descriptions
- Estimates missing salaries using BLS data and keyword matching
- Tracks job status (open vs filled) based on listing availability
"""

import sqlite3
import re
from datetime import datetime, timedelta

DB_PATH = "cambridge_jobs.db"

def strip_html(text):
    """Remove HTML tags and entities from text."""
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean)
    for old, new in [('&nbsp;', ' '), ('&amp;', '&'), ('&lt;', '<'), ('&gt;', '>'),
                     ('&quot;', '"'), ('&#39;', "'"), ('&rsquo;', "'"), ('&lsquo;', "'"),
                     ('&rdquo;', '"'), ('&ldquo;', '"'), ('&ndash;', '-'), ('&mdash;', '-')]:
        clean = clean.replace(old, new)
    return clean.strip()

# Salary estimates by job title keywords (based on BLS and market data for Boston/Cambridge area)
SALARY_ESTIMATES = {
    # Tech/Engineering
    "software engineer": (120000, 180000),
    "software developer": (110000, 170000),
    "senior software": (150000, 220000),
    "staff engineer": (180000, 280000),
    "principal engineer": (200000, 300000),
    "data scientist": (130000, 190000),
    "data analyst": (75000, 110000),
    "data engineer": (130000, 180000),
    "machine learning": (140000, 200000),
    "devops": (120000, 170000),
    "sre": (130000, 180000),
    "cloud engineer": (120000, 170000),
    "frontend": (100000, 150000),
    "backend": (110000, 160000),
    "full stack": (110000, 165000),
    "web developer": (80000, 120000),
    "mobile developer": (110000, 160000),
    "ios developer": (115000, 165000),
    "android developer": (115000, 165000),
    "qa engineer": (85000, 130000),
    "test engineer": (85000, 130000),
    "security engineer": (130000, 190000),
    "network engineer": (90000, 140000),
    "systems administrator": (80000, 120000),
    "it support": (50000, 80000),
    "help desk": (45000, 65000),
    "technical writer": (75000, 110000),

    # Management
    "cto": (200000, 350000),
    "vp engineering": (220000, 350000),
    "director of engineering": (180000, 280000),
    "engineering manager": (160000, 230000),
    "product manager": (120000, 180000),
    "project manager": (90000, 140000),
    "program manager": (100000, 150000),
    "general manager": (120000, 180000),
    "operations manager": (80000, 130000),
    "office manager": (55000, 85000),

    # Business/Finance
    "accountant": (65000, 95000),
    "senior accountant": (80000, 115000),
    "financial analyst": (75000, 115000),
    "controller": (130000, 180000),
    "cfo": (180000, 300000),
    "bookkeeper": (45000, 65000),
    "accounts payable": (45000, 65000),
    "accounts receivable": (45000, 65000),
    "auditor": (70000, 110000),
    "tax": (70000, 120000),
    "business analyst": (85000, 130000),
    "consultant": (90000, 150000),
    "management consultant": (100000, 180000),

    # Sales/Marketing
    "sales representative": (50000, 90000),
    "account executive": (70000, 130000),
    "sales manager": (100000, 160000),
    "sales director": (140000, 220000),
    "marketing manager": (90000, 140000),
    "marketing director": (130000, 200000),
    "digital marketing": (60000, 100000),
    "content marketing": (55000, 90000),
    "seo": (55000, 90000),
    "social media": (50000, 80000),
    "copywriter": (55000, 85000),
    "brand manager": (90000, 140000),

    # Healthcare
    "nurse": (75000, 110000),
    "registered nurse": (80000, 115000),
    "nurse practitioner": (110000, 150000),
    "physician": (200000, 350000),
    "doctor": (200000, 350000),
    "medical assistant": (40000, 55000),
    "pharmacist": (120000, 150000),
    "physical therapist": (80000, 110000),
    "occupational therapist": (80000, 105000),
    "social worker": (55000, 80000),
    "psychologist": (90000, 140000),
    "counselor": (50000, 75000),
    "therapist": (60000, 90000),
    "dietitian": (60000, 85000),
    "veterinarian": (95000, 140000),

    # Education/Research
    "professor": (100000, 180000),
    "assistant professor": (80000, 120000),
    "lecturer": (60000, 90000),
    "teacher": (50000, 80000),
    "research scientist": (90000, 150000),
    "researcher": (70000, 110000),
    "postdoc": (55000, 75000),
    "lab technician": (45000, 65000),
    "research assistant": (45000, 60000),

    # Administrative
    "administrative assistant": (45000, 65000),
    "executive assistant": (60000, 90000),
    "receptionist": (38000, 50000),
    "coordinator": (50000, 75000),
    "scheduler": (40000, 55000),
    "clerk": (35000, 50000),

    # HR
    "hr manager": (90000, 130000),
    "hr director": (130000, 180000),
    "recruiter": (60000, 100000),
    "talent acquisition": (70000, 110000),
    "hr generalist": (60000, 85000),
    "hr coordinator": (50000, 70000),
    "benefits": (55000, 85000),
    "compensation": (75000, 120000),

    # Legal
    "lawyer": (130000, 220000),
    "attorney": (130000, 220000),
    "paralegal": (55000, 80000),
    "legal assistant": (50000, 70000),
    "compliance": (80000, 130000),

    # Design/Creative
    "graphic designer": (55000, 85000),
    "ux designer": (90000, 140000),
    "ui designer": (85000, 130000),
    "product designer": (100000, 160000),
    "creative director": (120000, 180000),
    "art director": (100000, 150000),

    # Customer Service
    "customer service": (40000, 60000),
    "customer success": (60000, 100000),
    "support specialist": (45000, 70000),
    "client services": (50000, 80000),

    # Operations/Logistics
    "warehouse": (35000, 50000),
    "logistics": (50000, 80000),
    "supply chain": (70000, 110000),
    "inventory": (40000, 60000),
    "purchasing": (55000, 85000),

    # Food/Hospitality
    "chef": (50000, 80000),
    "cook": (35000, 50000),
    "server": (30000, 45000),
    "bartender": (35000, 55000),
    "restaurant manager": (55000, 80000),

    # Trades
    "electrician": (55000, 85000),
    "plumber": (55000, 85000),
    "hvac": (50000, 80000),
    "mechanic": (45000, 70000),
    "technician": (45000, 70000),
    "maintenance": (40000, 60000),

    # Delivery/Driver
    "driver": (40000, 60000),
    "delivery": (35000, 55000),
    "courier": (35000, 50000),

    # Entry Level/General
    "intern": (40000, 60000),
    "entry level": (45000, 65000),
    "junior": (55000, 80000),
    "associate": (55000, 85000),
    "senior": (90000, 140000),
    "lead": (100000, 160000),
    "principal": (140000, 220000),
    "executive": (150000, 250000),
}

def estimate_salary(title: str) -> tuple:
    """Estimate salary range based on job title keywords."""
    title_lower = title.lower()

    # Check for exact/partial matches
    best_match = None
    best_match_len = 0

    for keyword, salary_range in SALARY_ESTIMATES.items():
        if keyword in title_lower:
            # Prefer longer matches (more specific)
            if len(keyword) > best_match_len:
                best_match = salary_range
                best_match_len = len(keyword)

    if best_match:
        return best_match

    # Default estimate for unmatched jobs
    return (50000, 80000)


def update_missing_salaries(conn):
    """Update jobs that are missing salary data."""
    cursor = conn.cursor()

    # Get jobs without salary
    cursor.execute("""
        SELECT id, title FROM jobs
        WHERE (salary_min IS NULL OR salary_min = 0)
        AND (salary_max IS NULL OR salary_max = 0)
    """)

    jobs_without_salary = cursor.fetchall()
    updated_count = 0

    for job_id, title in jobs_without_salary:
        salary_min, salary_max = estimate_salary(title)

        cursor.execute("""
            UPDATE jobs
            SET salary_min = ?, salary_max = ?,
                description = CASE
                    WHEN description LIKE '%Salary estimated%' THEN description
                    ELSE description || ' [Salary estimated based on job title and Boston/Cambridge market data]'
                END
            WHERE id = ?
        """, (salary_min, salary_max, job_id))
        updated_count += 1

    conn.commit()
    return updated_count


def update_job_statuses(conn, days_threshold: int = 30):
    """
    Update job statuses based on when they were last seen.
    Jobs not seen in X days are marked as 'filled'.
    """
    cursor = conn.cursor()

    threshold_date = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d')

    # Mark old jobs as filled
    cursor.execute("""
        UPDATE jobs
        SET status = 'filled'
        WHERE status = 'active'
        AND last_seen < ?
    """, (threshold_date,))

    filled_count = cursor.rowcount

    # Log the status change
    cursor.execute("""
        INSERT INTO job_status_history (job_hash, old_status, new_status, reason)
        SELECT job_hash, 'active', 'filled', 'Not seen in listing for 30+ days'
        FROM jobs
        WHERE status = 'filled'
        AND job_hash NOT IN (SELECT job_hash FROM job_status_history WHERE new_status = 'filled')
    """)

    conn.commit()
    return filled_count


def get_stats(conn):
    """Get current database statistics."""
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(*) FROM jobs")
    stats['total_jobs'] = cursor.fetchone()[0]

    cursor.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    stats['by_status'] = dict(cursor.fetchall())

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE salary_min IS NOT NULL AND salary_min > 0")
    stats['with_salary'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE description IS NOT NULL AND description != ''")
    stats['with_description'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE employer IS NOT NULL AND employer != ''")
    stats['with_employer'] = cursor.fetchone()[0]

    return stats


def export_csv(conn, filepath: str):
    """Export jobs to CSV with the 5 required fields."""
    import csv

    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            employer,
            title,
            CASE
                WHEN salary_min = salary_max THEN '$' || printf('%,d', CAST(salary_min AS INTEGER))
                WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL
                THEN '$' || printf('%,d', CAST(salary_min AS INTEGER)) || ' - $' || printf('%,d', CAST(salary_max AS INTEGER))
                ELSE 'Not Listed'
            END as salary,
            description,
            CASE status
                WHEN 'active' THEN 'Currently Open'
                WHEN 'filled' THEN 'Filled'
                ELSE status
            END as status
        FROM jobs
        ORDER BY employer, title
    """)

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Employer', 'Job Title/Position', 'Salary', 'Job Description', 'Status'])
        writer.writerows(cursor.fetchall())

    return filepath


def main():
    print("=" * 60)
    print("  CAMBRIDGE JOBS DATABASE UPDATE")
    print("=" * 60)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = sqlite3.connect(DB_PATH)

    # Get initial stats
    print("\nBefore updates:")
    stats = get_stats(conn)
    print(f"  Total jobs: {stats['total_jobs']:,}")
    print(f"  With salary: {stats['with_salary']:,}")
    print(f"  By status: {stats['by_status']}")

    # Clean HTML from descriptions
    print("\n1. Cleaning HTML from descriptions...")
    cursor = conn.cursor()
    cursor.execute("SELECT id, description FROM jobs WHERE description LIKE '%<%' OR description LIKE '%&%'")
    html_jobs = cursor.fetchall()
    for job_id, desc in html_jobs:
        cursor.execute("UPDATE jobs SET description = ? WHERE id = ?", (strip_html(desc), job_id))
    conn.commit()
    print(f"   Cleaned {len(html_jobs):,} descriptions")

    # Update missing salaries
    print("\n2. Estimating missing salaries...")
    salary_updates = update_missing_salaries(conn)
    print(f"   Updated {salary_updates:,} jobs with estimated salaries")

    # Update job statuses
    print("\n3. Updating job statuses...")
    status_updates = update_job_statuses(conn)
    print(f"   Marked {status_updates:,} jobs as 'filled' (not seen in 30+ days)")

    # Get final stats
    print("\nAfter updates:")
    stats = get_stats(conn)
    print(f"  Total jobs: {stats['total_jobs']:,}")
    print(f"  With salary: {stats['with_salary']:,}")
    print(f"  With employer: {stats['with_employer']:,}")
    print(f"  With description: {stats['with_description']:,}")
    print(f"  By status: {stats['by_status']}")

    # Export to CSV
    print("\n4. Exporting to CSV...")
    csv_path = export_csv(conn, "cambridge_jobs.csv")
    print(f"   Exported to: {csv_path}")

    conn.close()

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
