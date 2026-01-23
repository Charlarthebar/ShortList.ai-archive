#!/usr/bin/env python3
"""
Enhanced Adzuna Collector - Query by Category
==============================================
Bypasses the 5,000 job limit by querying each category separately.
30 categories × 5,000 jobs = up to 150,000 jobs potential.

This significantly increases Cambridge job coverage.
"""

import os
import sys
import requests
import time
import math
import sqlite3
import hashlib
import re
import html
import csv
from datetime import datetime
from typing import List, Dict, Set

# Load environment
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass

# Configuration - hardcoded for reliability
ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID') or "516145e7"
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY') or "34e6f3959d58461538354d674d48b881"
DB_PATH = "cambridge_jobs.db"
CSV_PATH = "cambridge_jobs.csv"

# Cambridge area settings
CENTER_LAT = 42.3736
CENTER_LON = -71.1097
RADIUS_MILES = 10.0

# All Adzuna categories
CATEGORIES = [
    "accounting-finance-jobs",
    "it-jobs",
    "sales-jobs",
    "customer-services-jobs",
    "engineering-jobs",
    "hr-jobs",
    "healthcare-nursing-jobs",
    "hospitality-catering-jobs",
    "pr-advertising-marketing-jobs",
    "logistics-warehouse-jobs",
    "teaching-jobs",
    "trade-construction-jobs",
    "admin-jobs",
    "legal-jobs",
    "creative-design-jobs",
    "graduate-jobs",
    "retail-jobs",
    "consultancy-jobs",
    "manufacturing-jobs",
    "scientific-qa-jobs",
    "social-work-jobs",
    "travel-jobs",
    "energy-oil-gas-jobs",
    "property-jobs",
    "charity-voluntary-jobs",
    "domestic-help-cleaning-jobs",
    "maintenance-jobs",
    "part-time-jobs",
    "other-general-jobs",
]

# Cambridge area cities for filtering
CAMBRIDGE_AREA_CITIES = {
    'cambridge', 'boston', 'somerville', 'brookline', 'watertown',
    'belmont', 'arlington', 'medford', 'malden', 'everett', 'chelsea',
    'newton', 'waltham', 'allston', 'brighton', 'charlestown',
    'dorchester', 'roxbury', 'jamaica plain', 'roslindale', 'west roxbury',
    'south boston', 'east boston', 'revere', 'winthrop', 'quincy',
    'lexington', 'woburn', 'winchester', 'stoneham', 'melrose',
    'reading', 'wakefield', 'saugus', 'lynn', 'dedham', 'needham',
    'wellesley', 'natick', 'framingham', 'burlington', 'bedford', 'concord'
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in miles."""
    R = 3959
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


def is_in_cambridge_area(job):
    """Check if job is in Cambridge area."""
    # Check coordinates
    job_lat = job.get('latitude')
    job_lon = job.get('longitude')
    if job_lat and job_lon:
        try:
            distance = haversine_distance(CENTER_LAT, CENTER_LON, float(job_lat), float(job_lon))
            if distance <= RADIUS_MILES:
                return True
        except:
            pass

    # Check city name
    city = (job.get('city') or '').lower().strip()
    if city in CAMBRIDGE_AREA_CITIES:
        return True

    # Check location string
    location = (job.get('location') or '').lower()
    for area_city in CAMBRIDGE_AREA_CITIES:
        if area_city in location:
            return True

    return False


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


def fetch_jobs_by_category(category, max_per_category=2000, delay=0.5):
    """Fetch jobs for a specific category."""
    jobs = []
    page = 1
    results_per_page = 50

    while len(jobs) < max_per_category:
        try:
            params = {
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "results_per_page": results_per_page,
                "where": "massachusetts",
                "category": category,
                "sort_by": "date",
            }

            response = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/us/search/{page}",
                params=params,
                timeout=30
            )

            if response.status_code == 429:
                print(f"      Rate limited, waiting 30s...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                break

            data = response.json()
            results = data.get("results", [])

            if not results:
                break

            for job in results:
                location_area = job.get("location", {}).get("area", [])
                city = location_area[0] if len(location_area) >= 1 else ""
                state = location_area[2] if len(location_area) >= 3 else ""

                jobs.append({
                    "title": job.get("title", ""),
                    "employer": job.get("company", {}).get("display_name", "Unknown"),
                    "location": job.get("location", {}).get("display_name", ""),
                    "city": city,
                    "state": state,
                    "latitude": job.get("latitude"),
                    "longitude": job.get("longitude"),
                    "salary_min": job.get("salary_min"),
                    "salary_max": job.get("salary_max"),
                    "description": strip_html(job.get("description", "")),
                    "url": job.get("redirect_url", ""),
                    "posted_date": job.get("created", ""),
                    "source": "adzuna",
                    "source_id": str(job.get("id", "")),
                    "category": category,
                })

            page += 1
            time.sleep(delay)

            if page > 40:  # Max 40 pages = 2000 jobs per category
                break

        except Exception as e:
            print(f"      Error: {e}")
            break

    return jobs


def hash_job(job):
    """Create unique hash for a job."""
    unique_str = f"{job['title']}|{job['employer']}|{job['location']}|{job['source']}"
    return hashlib.md5(unique_str.encode()).hexdigest()


def save_to_database(jobs, db_path):
    """Save jobs to database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted = 0
    updated = 0

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
            cursor.execute("""
                INSERT INTO jobs (job_hash, title, employer, location, city, state,
                                  latitude, longitude, salary_min, salary_max, description,
                                  source, source_id, url, posted_date, is_remote)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_hash, job['title'], job['employer'], job['location'],
                job.get('city', ''), job.get('state', ''),
                job.get('latitude'), job.get('longitude'),
                job.get('salary_min'), job.get('salary_max'),
                job.get('description', ''), job['source'], job.get('source_id', ''),
                job.get('url', ''), job.get('posted_date', ''), 0
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
            # Clean description
            if row_list[13]:
                row_list[13] = row_list[13].replace('\n', ' ').replace('\r', ' ')
                row_list[13] = re.sub(r'\s+', ' ', row_list[13]).strip()
            writer.writerow(row_list)

    conn.close()
    return len(rows)


def main():
    print("=" * 70)
    print("ENHANCED ADZUNA COLLECTOR - QUERY BY CATEGORY")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Categories to query: {len(CATEGORIES)}")
    print(f"Max jobs per category: 2,000")
    print(f"Potential total: {len(CATEGORIES) * 2000:,} jobs")
    print()

    all_jobs = []
    seen_ids = set()

    for i, category in enumerate(CATEGORIES, 1):
        print(f"[{i}/{len(CATEGORIES)}] Fetching: {category}...")

        category_jobs = fetch_jobs_by_category(category, max_per_category=2000, delay=0.3)

        # Deduplicate
        new_jobs = []
        for job in category_jobs:
            job_id = job.get('source_id')
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                new_jobs.append(job)

        all_jobs.extend(new_jobs)
        print(f"    Got {len(category_jobs)} jobs, {len(new_jobs)} new (total: {len(all_jobs):,})")

    print(f"\nTotal MA jobs fetched: {len(all_jobs):,}")

    # Filter to Cambridge area
    print("Filtering to Cambridge area...")
    cambridge_jobs = [j for j in all_jobs if is_in_cambridge_area(j)]
    print(f"Cambridge area jobs: {len(cambridge_jobs):,}")

    # Count Cambridge city specifically
    cambridge_city_jobs = [j for j in cambridge_jobs if 'cambridge' in (j.get('city') or '').lower()]
    print(f"Cambridge city jobs: {len(cambridge_city_jobs):,}")

    # Save to database
    print("\nSaving to database...")
    inserted, updated = save_to_database(cambridge_jobs, DB_PATH)
    print(f"  Inserted: {inserted:,} new jobs")
    print(f"  Updated: {updated:,} existing jobs")

    # Export CSV
    print("\nExporting CSV...")
    total_rows = export_csv(DB_PATH, CSV_PATH)
    print(f"  Exported {total_rows:,} jobs to {CSV_PATH}")

    # Final stats
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='active' AND city LIKE '%Cambridge%'")
    final_cambridge = cursor.fetchone()[0]
    conn.close()

    print(f"\n{'=' * 70}")
    print("RESULTS")
    print("=" * 70)
    print(f"Total Cambridge city jobs in database: {final_cambridge:,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
