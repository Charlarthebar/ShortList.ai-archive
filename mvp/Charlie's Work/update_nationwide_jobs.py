#!/usr/bin/env python3
"""
Update Nationwide Jobs Database
- Estimates missing salaries
- Cleans HTML from descriptions
- Tracks job status (open vs filled) based on listing availability
"""

import sqlite3
import re
from datetime import datetime, timedelta

DB_PATH = "nationwide_jobs.db"
CSV_PATH = "nationwide_jobs.csv"

# Salary estimates by job title keywords
SALARY_ESTIMATES = {
    "software engineer": (120000, 180000), "software developer": (110000, 170000),
    "senior software": (150000, 220000), "data scientist": (130000, 190000),
    "data analyst": (75000, 110000), "data engineer": (130000, 180000),
    "machine learning": (140000, 200000), "devops": (120000, 170000),
    "frontend": (100000, 150000), "backend": (110000, 160000),
    "full stack": (110000, 165000), "web developer": (80000, 120000),
    "mobile developer": (110000, 160000), "qa engineer": (85000, 130000),
    "security engineer": (130000, 190000), "network engineer": (90000, 140000),
    "systems administrator": (80000, 120000), "it support": (50000, 80000),
    "product manager": (120000, 180000), "project manager": (90000, 140000),
    "engineering manager": (160000, 230000), "operations manager": (80000, 130000),
    "accountant": (65000, 95000), "financial analyst": (75000, 115000),
    "business analyst": (85000, 130000), "consultant": (90000, 150000),
    "sales representative": (50000, 90000), "account executive": (70000, 130000),
    "marketing manager": (90000, 140000), "digital marketing": (60000, 100000),
    "nurse": (75000, 110000), "registered nurse": (80000, 115000),
    "physician": (200000, 350000), "pharmacist": (120000, 150000),
    "teacher": (50000, 80000), "professor": (100000, 180000),
    "administrative assistant": (45000, 65000), "executive assistant": (60000, 90000),
    "hr manager": (90000, 130000), "recruiter": (60000, 100000),
    "lawyer": (130000, 220000), "paralegal": (55000, 80000),
    "graphic designer": (55000, 85000), "ux designer": (90000, 140000),
    "customer service": (40000, 60000), "customer success": (60000, 100000),
    "warehouse": (35000, 50000), "driver": (40000, 60000), "delivery": (35000, 55000),
    "intern": (40000, 60000), "entry level": (45000, 65000), "junior": (55000, 80000),
    "senior": (90000, 140000), "lead": (100000, 160000), "director": (140000, 220000),
}

def estimate_salary(title):
    title_lower = title.lower()
    best_match = None
    best_len = 0
    for keyword, salary in SALARY_ESTIMATES.items():
        if keyword in title_lower and len(keyword) > best_len:
            best_match = salary
            best_len = len(keyword)
    return best_match or (50000, 80000)

def strip_html(text):
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean)
    for old, new in [('&nbsp;', ' '), ('&amp;', '&'), ('&lt;', '<'), ('&gt;', '>'),
                     ('&quot;', '"'), ('&#39;', "'"), ('&rsquo;', "'"), ('&lsquo;', "'"),
                     ('&rdquo;', '"'), ('&ldquo;', '"'), ('&ndash;', '-'), ('&mdash;', '-')]:
        clean = clean.replace(old, new)
    return clean.strip()

def update_database():
    print("=" * 60)
    print("  NATIONWIDE JOBS DATABASE UPDATE")
    print("=" * 60)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get stats before
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE salary_min > 0")
    with_salary = cursor.fetchone()[0]
    print(f"\nBefore: {total:,} jobs, {with_salary:,} with salary")

    # 1. Clean HTML from descriptions
    print("\n1. Cleaning HTML from descriptions...")
    cursor.execute("SELECT id, description FROM jobs WHERE description LIKE '%<%' OR description LIKE '%&%'")
    html_jobs = cursor.fetchall()
    for job_id, desc in html_jobs:
        cursor.execute("UPDATE jobs SET description = ? WHERE id = ?", (strip_html(desc), job_id))
    print(f"   Cleaned {len(html_jobs):,} descriptions")

    # 2. Estimate missing salaries
    print("\n2. Estimating missing salaries...")
    cursor.execute("SELECT id, title FROM jobs WHERE salary_min IS NULL OR salary_min = 0")
    no_salary = cursor.fetchall()
    for job_id, title in no_salary:
        sal_min, sal_max = estimate_salary(title)
        cursor.execute("UPDATE jobs SET salary_min = ?, salary_max = ? WHERE id = ?", (sal_min, sal_max, job_id))
    print(f"   Estimated salaries for {len(no_salary):,} jobs")

    # 3. Mark old jobs as filled
    print("\n3. Updating job statuses...")
    threshold = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    cursor.execute("UPDATE jobs SET status = 'filled' WHERE status = 'active' AND last_seen < ?", (threshold,))
    filled = cursor.rowcount
    print(f"   Marked {filled:,} jobs as 'filled' (not seen in 30+ days)")

    conn.commit()

    # 4. Export to CSV
    print("\n4. Exporting to CSV...")
    import csv
    cursor.execute('''
        SELECT employer, title,
            CASE WHEN salary_min = salary_max THEN '$' || printf('%,d', CAST(salary_min AS INTEGER))
                 ELSE '$' || printf('%,d', CAST(salary_min AS INTEGER)) || ' - $' || printf('%,d', CAST(salary_max AS INTEGER)) END,
            description,
            CASE status WHEN 'active' THEN 'Currently Open' WHEN 'filled' THEN 'Filled' ELSE status END
        FROM jobs ORDER BY employer, title
    ''')
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Employer', 'Job Title/Position', 'Salary', 'Job Description', 'Status'])
        writer.writerows(cursor.fetchall())
    print(f"   Exported to {CSV_PATH}")

    # Final stats
    cursor.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    by_status = dict(cursor.fetchall())
    print(f"\nAfter: {by_status}")

    conn.close()
    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    update_database()
