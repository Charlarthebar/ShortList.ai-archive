#!/usr/bin/env python3
"""
Harvard University Careers Scraper
===================================
Fetches job listings from Harvard's SmartRecruiters API.
All jobs are located in Cambridge, MA.

Source: https://api.smartrecruiters.com/v1/companies/HarvardUniversity/postings
"""

import requests
import sqlite3
import hashlib
import re
import html
import csv
from datetime import datetime
from typing import List, Dict

DB_PATH = "cambridge_jobs.db"
CSV_PATH = "cambridge_jobs.csv"

# Harvard SmartRecruiters API
API_URL = "https://api.smartrecruiters.com/v1/companies/HarvardUniversity/postings"


def strip_html(text):
    """Remove HTML tags from text."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<br\s*/?>|</?p>|</?div>|</?li>|</?h[1-6]>|</?ul>|</?ol>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]*>', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_harvard_jobs() -> List[Dict]:
    """Fetch all jobs from Harvard's SmartRecruiters API."""
    jobs = []
    offset = 0
    limit = 100

    print("Fetching Harvard University jobs...")

    while True:
        try:
            response = requests.get(
                API_URL,
                params={"offset": offset, "limit": limit},
                headers={"Accept": "application/json"},
                timeout=30
            )

            if response.status_code != 200:
                print(f"  Error: HTTP {response.status_code}")
                break

            data = response.json()
            postings = data.get("content", [])

            if not postings:
                break

            for posting in postings:
                location = posting.get("location", {})
                city = location.get("city", "Cambridge")
                state = location.get("region", "MA")

                # Get job description (need separate API call for full details)
                job_id = posting.get("id")
                description = ""

                # Try to get full job details
                try:
                    detail_url = f"https://api.smartrecruiters.com/v1/companies/HarvardUniversity/postings/{job_id}"
                    detail_resp = requests.get(detail_url, headers={"Accept": "application/json"}, timeout=30)
                    if detail_resp.status_code == 200:
                        detail_data = detail_resp.json()
                        job_ad = detail_data.get("jobAd", {})
                        sections = job_ad.get("sections", {})
                        description_section = sections.get("jobDescription", {})
                        description = strip_html(description_section.get("text", ""))

                        # Also get qualifications if available
                        qualifications = sections.get("qualifications", {})
                        if qualifications.get("text"):
                            description += "\n\nQualifications:\n" + strip_html(qualifications.get("text", ""))
                except:
                    pass

                job = {
                    "title": posting.get("name", ""),
                    "employer": "Harvard University",
                    "location": f"{city}, {state}",
                    "city": city,
                    "state": state,
                    "latitude": None,
                    "longitude": None,
                    "salary_min": None,
                    "salary_max": None,
                    "description": description,
                    "url": f"https://careers.harvard.edu/jobs/{posting.get('uuid', '')}",
                    "posted_date": posting.get("releasedDate", ""),
                    "source": "harvard_careers",
                    "source_id": str(job_id),
                    "is_remote": 1 if location.get("remote") else 0,
                    "department": posting.get("department", {}).get("label", ""),
                    "job_function": posting.get("function", {}).get("label", ""),
                }
                jobs.append(job)

            print(f"  Fetched {len(jobs)} jobs so far...")

            # Check if there are more results
            total = data.get("totalFound", 0)
            if offset + limit >= total:
                break

            offset += limit

        except Exception as e:
            print(f"  Error: {e}")
            break

    print(f"Total Harvard jobs fetched: {len(jobs)}")
    return jobs


def hash_job(job):
    """Create unique hash for a job."""
    unique_str = f"{job['title']}|{job['employer']}|{job['location']}|{job['source']}"
    return hashlib.md5(unique_str.encode()).hexdigest()


def save_to_database(jobs: List[Dict], db_path: str):
    """Save jobs to database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted, updated = 0, 0

    for job in jobs:
        job_hash = hash_job(job)

        cursor.execute("SELECT id FROM jobs WHERE job_hash = ?", (job_hash,))
        existing = cursor.fetchone()

        if existing:
            cursor.execute(
                "UPDATE jobs SET last_seen = CURRENT_TIMESTAMP, status = 'active', description = ? WHERE job_hash = ?",
                (job.get('description', ''), job_hash)
            )
            updated += 1
        else:
            is_remote = job.get('is_remote', 0)
            cursor.execute("""
                INSERT INTO jobs (job_hash, title, employer, location, city, state,
                                  latitude, longitude, salary_min, salary_max, description,
                                  source, source_id, url, posted_date, is_remote)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_hash, job['title'], job['employer'], job['location'],
                job.get('city', 'Cambridge'), job.get('state', 'MA'),
                job.get('latitude'), job.get('longitude'),
                job.get('salary_min'), job.get('salary_max'),
                job.get('description', ''), job['source'], job.get('source_id', ''),
                job.get('url', ''), job.get('posted_date', ''), is_remote
            ))
            inserted += 1

    conn.commit()
    conn.close()
    return inserted, updated


def export_csv(db_path, csv_path):
    """Export to CSV with clean formatting."""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("""
        SELECT title, employer, location, city, state,
               salary_min, salary_max, source, url, posted_date,
               is_remote, status, first_seen, description, skills
        FROM jobs
        ORDER BY source, employer, title
    """)

    rows = cursor.fetchall()

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'employer', 'location', 'city', 'state',
                       'salary_min', 'salary_max', 'source', 'url', 'posted_date',
                       'is_remote', 'status', 'first_seen', 'description', 'skills'])

        for row in rows:
            row_list = list(row)
            if row_list[13]:
                row_list[13] = row_list[13].replace('\n', ' ').replace('\r', ' ')
                row_list[13] = re.sub(r'\s+', ' ', row_list[13]).strip()
            writer.writerow(row_list)

    conn.close()
    return len(rows)


def main():
    print("=" * 70)
    print("HARVARD UNIVERSITY CAREERS SCRAPER")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source: SmartRecruiters API")
    print()

    # Fetch jobs
    jobs = fetch_harvard_jobs()

    if not jobs:
        print("No jobs fetched!")
        return

    # Show sample
    print()
    print("Sample jobs:")
    for job in jobs[:5]:
        print(f"  - {job['title']} ({job['department']})")

    # Save to database
    print()
    print("Saving to database...")
    inserted, updated = save_to_database(jobs, DB_PATH)
    print(f"  Inserted: {inserted} new jobs")
    print(f"  Updated: {updated} existing jobs")

    # Final count
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE source = 'harvard_careers' AND status = 'active'")
    harvard_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'active'")
    total_count = cursor.fetchone()[0]
    conn.close()

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Harvard jobs in database: {harvard_count}")
    print(f"Total active jobs: {total_count}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
