#!/usr/bin/env python3
"""
MAXIMIZED Adzuna Collector
===========================
Maximizes job coverage by querying across multiple dimensions:
1. Categories (29 job types)
2. Locations (Cambridge + surrounding cities)
3. Keywords (industry-specific search terms)

This creates hundreds of unique queries to bypass the 5,000 job limit.
"""

import os
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

# Configuration
ADZUNA_APP_ID = "516145e7"
ADZUNA_APP_KEY = "34e6f3959d58461538354d674d48b881"
DB_PATH = "cambridge_jobs.db"
CSV_PATH = "cambridge_jobs.csv"

# Cambridge area settings
CENTER_LAT = 42.3736
CENTER_LON = -71.1097
RADIUS_MILES = 10.0

# =============================================================================
# QUERY DIMENSIONS - Each combination creates a unique query
# =============================================================================

# Dimension 1: Job Categories (29)
CATEGORIES = [
    "accounting-finance-jobs", "it-jobs", "sales-jobs", "customer-services-jobs",
    "engineering-jobs", "hr-jobs", "healthcare-nursing-jobs", "hospitality-catering-jobs",
    "pr-advertising-marketing-jobs", "logistics-warehouse-jobs", "teaching-jobs",
    "trade-construction-jobs", "admin-jobs", "legal-jobs", "creative-design-jobs",
    "graduate-jobs", "retail-jobs", "consultancy-jobs", "manufacturing-jobs",
    "scientific-qa-jobs", "social-work-jobs", "travel-jobs", "energy-oil-gas-jobs",
    "property-jobs", "charity-voluntary-jobs", "domestic-help-cleaning-jobs",
    "maintenance-jobs", "part-time-jobs", "other-general-jobs",
]

# Dimension 2: Locations to search (focus on Cambridge area)
LOCATIONS = [
    "cambridge, ma",
    "boston, ma",
    "somerville, ma",
    "brookline, ma",
    "watertown, ma",
    "waltham, ma",
    "medford, ma",
    "arlington, ma",
    "belmont, ma",
    "newton, ma",
    "02139",  # Cambridge ZIP
    "02138",  # Cambridge ZIP
    "02140",  # Cambridge ZIP
    "02141",  # East Cambridge ZIP
    "02142",  # Kendall Square ZIP
]

# Dimension 3: Industry keywords (for Cambridge's key industries)
KEYWORDS = [
    "",  # No keyword (general search)
    "biotech",
    "pharmaceutical",
    "research",
    "university",
    "software",
    "data scientist",
    "machine learning",
    "clinical",
    "laboratory",
    "professor",
    "postdoc",
    "startup",
    "consulting",
    "finance",
]

# Cambridge area cities for filtering results
CAMBRIDGE_AREA_CITIES = {
    'cambridge', 'boston', 'somerville', 'brookline', 'watertown',
    'belmont', 'arlington', 'medford', 'malden', 'everett', 'chelsea',
    'newton', 'waltham', 'allston', 'brighton', 'charlestown',
    'dorchester', 'roxbury', 'jamaica plain', 'roslindale', 'west roxbury',
    'south boston', 'east boston', 'revere', 'winthrop', 'quincy',
    'lexington', 'woburn', 'winchester', 'stoneham', 'melrose',
    'reading', 'wakefield', 'saugus', 'lynn', 'dedham', 'needham',
    'wellesley', 'natick', 'framingham', 'burlington', 'bedford', 'concord',
    'kendall square', 'harvard square', 'central square', 'porter square',
    'cambridgeport', 'east cambridge', 'north cambridge', 'mid-cambridge'
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in miles."""
    R = 3959
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


def is_in_cambridge_area(job):
    """Check if job is in Cambridge area."""
    # Check coordinates
    job_lat, job_lon = job.get('latitude'), job.get('longitude')
    if job_lat and job_lon:
        try:
            if haversine_distance(CENTER_LAT, CENTER_LON, float(job_lat), float(job_lon)) <= RADIUS_MILES:
                return True
        except:
            pass

    # Check city/location strings
    city = (job.get('city') or '').lower().strip()
    location = (job.get('location') or '').lower()

    if city in CAMBRIDGE_AREA_CITIES:
        return True

    for area_city in CAMBRIDGE_AREA_CITIES:
        if area_city in location or area_city in city:
            return True

    # Check for Middlesex County (Cambridge is in Middlesex)
    if 'middlesex' in location:
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


def fetch_jobs(where=None, category=None, what=None, max_jobs=1000, delay=0.2):
    """Fetch jobs with given parameters."""
    jobs = []
    page = 1
    results_per_page = 50

    while len(jobs) < max_jobs:
        try:
            params = {
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "results_per_page": results_per_page,
                "sort_by": "date",
            }

            if where:
                params["where"] = where
            if category:
                params["category"] = category
            if what:
                params["what"] = what

            response = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/us/search/{page}",
                params=params,
                timeout=30
            )

            if response.status_code == 429:
                print("      Rate limited, waiting 60s...")
                time.sleep(60)
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
                })

            page += 1
            time.sleep(delay)

            if page > 20:  # Max 20 pages per query = 1000 jobs
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
            if row_list[13]:
                row_list[13] = row_list[13].replace('\n', ' ').replace('\r', ' ')
                row_list[13] = re.sub(r'\s+', ' ', row_list[13]).strip()
            writer.writerow(row_list)

    conn.close()
    return len(rows)


def main():
    print("=" * 70)
    print("MAXIMIZED ADZUNA COLLECTOR")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Query Dimensions:")
    print(f"  - Locations: {len(LOCATIONS)}")
    print(f"  - Categories: {len(CATEGORIES)}")
    print(f"  - Keywords: {len(KEYWORDS)}")
    print(f"  - Max queries: {len(LOCATIONS) * len(KEYWORDS)} location+keyword combos")
    print()

    all_jobs = []
    seen_ids: Set[str] = set()
    query_count = 0

    # Strategy 1: Query by Location + Keyword (most targeted for Cambridge)
    print("=" * 70)
    print("PHASE 1: Location + Keyword Queries (Cambridge-focused)")
    print("=" * 70)

    for loc in LOCATIONS:
        for keyword in KEYWORDS:
            query_count += 1
            query_desc = f"'{keyword}' in {loc}" if keyword else f"all jobs in {loc}"
            print(f"[{query_count}] {query_desc}...")

            jobs = fetch_jobs(where=loc, what=keyword if keyword else None, max_jobs=500, delay=0.2)

            # Deduplicate
            new_jobs = []
            for job in jobs:
                job_id = job.get('source_id')
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    new_jobs.append(job)

            if new_jobs:
                all_jobs.extend(new_jobs)
                print(f"    +{len(new_jobs)} new jobs (total: {len(all_jobs):,})")
            else:
                print(f"    No new jobs")

    # Strategy 2: Query top categories for Massachusetts (broader coverage)
    print()
    print("=" * 70)
    print("PHASE 2: Category Queries (Massachusetts-wide)")
    print("=" * 70)

    top_categories = [
        "it-jobs", "healthcare-nursing-jobs", "engineering-jobs",
        "scientific-qa-jobs", "teaching-jobs", "accounting-finance-jobs"
    ]

    for category in top_categories:
        query_count += 1
        print(f"[{query_count}] Category: {category}...")

        jobs = fetch_jobs(where="massachusetts", category=category, max_jobs=2000, delay=0.2)

        new_jobs = []
        for job in jobs:
            job_id = job.get('source_id')
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                new_jobs.append(job)

        if new_jobs:
            all_jobs.extend(new_jobs)
            print(f"    +{len(new_jobs)} new jobs (total: {len(all_jobs):,})")
        else:
            print(f"    No new jobs")

    print()
    print(f"Total jobs fetched: {len(all_jobs):,}")
    print(f"Total queries made: {query_count}")

    # Filter to Cambridge area
    print()
    print("Filtering to Cambridge area...")
    cambridge_jobs = [j for j in all_jobs if is_in_cambridge_area(j)]
    print(f"Cambridge area jobs: {len(cambridge_jobs):,}")

    # Count Cambridge city specifically
    cambridge_city = [j for j in cambridge_jobs
                      if 'cambridge' in (j.get('location') or '').lower()
                      and 'middlesex' in (j.get('location') or '').lower()]
    print(f"Cambridge, MA specific: {len(cambridge_city):,}")

    # Save to database
    print()
    print("Saving to database...")
    inserted, updated = save_to_database(cambridge_jobs, DB_PATH)
    print(f"  Inserted: {inserted:,} new jobs")
    print(f"  Updated: {updated:,} existing jobs")

    # Export CSV
    print()
    print("Exporting CSV...")
    total_rows = export_csv(DB_PATH, CSV_PATH)
    print(f"  Exported {total_rows:,} jobs to {CSV_PATH}")

    # Final stats
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='active'")
    total_active = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='active' AND location LIKE '%Cambridge%Middlesex%'")
    cambridge_count = cursor.fetchone()[0]

    conn.close()

    print()
    print("=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)
    print(f"Total active jobs in database: {total_active:,}")
    print(f"Cambridge, MA jobs: {cambridge_count:,}")
    print(f"Queries executed: {query_count}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
