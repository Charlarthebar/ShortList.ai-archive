#!/usr/bin/env python3
"""
Test Job Collection for Cambridge, MA (02139)
=============================================
Quick test script to collect jobs using Adzuna and USAJOBS APIs
with SQLite storage (no PostgreSQL setup needed).

Usage:
    1. Copy .env.template to .env and fill in your API keys
    2. Run: python test_cambridge.py
"""

import os
import sys
import json
import sqlite3
import hashlib
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, try manual loading
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

import requests
import pgeocode
import csv
import math

# =========================================================
# GEO UTILITIES
# =========================================================

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles using Haversine formula."""
    R = 3959  # Earth's radius in miles

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def get_zip_coordinates(zip_code: str) -> tuple:
    """Get lat/long for a ZIP code."""
    nomi = pgeocode.Nominatim('us')
    result = nomi.query_postal_code(zip_code)
    if result is not None and not math.isnan(result.latitude):
        return (result.latitude, result.longitude)
    return (None, None)


def filter_jobs_by_distance(jobs: List[Dict], center_lat: float, center_lon: float,
                            radius_miles: float) -> List[Dict]:
    """Filter jobs to only those within radius of center point."""
    filtered = []
    for job in jobs:
        job_lat = job.get('latitude')
        job_lon = job.get('longitude')

        if job_lat is None or job_lon is None:
            # No coordinates - skip this job
            continue

        distance = haversine_distance(center_lat, center_lon, job_lat, job_lon)
        if distance <= radius_miles:
            job['distance_miles'] = round(distance, 1)
            filtered.append(job)

    return filtered


def export_to_csv(db, filepath: str) -> int:
    """Export all jobs to CSV file."""
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT
            title, employer, location, city, state,
            salary_min, salary_max, job_type, source,
            url, posted_date, status, first_seen
        FROM jobs
        ORDER BY first_seen DESC
    """)

    rows = cursor.fetchall()
    columns = ['title', 'employer', 'location', 'city', 'state',
               'salary_min', 'salary_max', 'job_type', 'source',
               'url', 'posted_date', 'status', 'first_seen']

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return len(rows)


# =========================================================
# CONFIGURATION
# =========================================================

@dataclass
class Config:
    """Simple configuration."""
    # API Keys (from environment)
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")
    usajobs_api_key: str = os.getenv("USAJOBS_API_KEY", "")
    usajobs_email: str = os.getenv("USAJOBS_EMAIL", "")

    # Target location
    zip_code: str = "02139"  # Cambridge, MA
    city: str = "Cambridge"
    state: str = "MA"
    radius_miles: int = 10

    # Database
    db_path: str = "cambridge_jobs.db"

    # Request settings
    request_delay: float = 1.0
    max_results_per_source: int = 100


# =========================================================
# DATABASE (SQLite)
# =========================================================

class JobDatabase:
    """Simple SQLite job database."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect and create schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        print(f"Connected to database: {self.db_path}")

    def _create_schema(self):
        """Create tables."""
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_hash TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                employer TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                latitude REAL,
                longitude REAL,
                salary_min REAL,
                salary_max REAL,
                description TEXT,
                job_type TEXT,
                source TEXT NOT NULL,
                source_id TEXT,
                url TEXT,
                posted_date TEXT,
                status TEXT DEFAULT 'active',
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(city, state)")

        self.conn.commit()

    def insert_jobs(self, jobs: List[Dict]) -> tuple:
        """Insert jobs, return (inserted, updated, skipped)."""
        if not jobs:
            return (0, 0, 0)

        cursor = self.conn.cursor()
        inserted, updated, skipped = 0, 0, 0

        for job in jobs:
            try:
                job_hash = self._hash_job(job)

                # Check if exists
                cursor.execute("SELECT id FROM jobs WHERE job_hash = ?", (job_hash,))
                existing = cursor.fetchone()

                if existing:
                    # Update last_seen
                    cursor.execute(
                        "UPDATE jobs SET last_seen = CURRENT_TIMESTAMP WHERE job_hash = ?",
                        (job_hash,)
                    )
                    updated += 1
                else:
                    # Insert new
                    cursor.execute("""
                        INSERT INTO jobs (
                            job_hash, title, employer, location, city, state, zip_code,
                            latitude, longitude, salary_min, salary_max, description,
                            job_type, source, source_id, url, posted_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        job_hash, job.get('title'), job.get('employer'),
                        job.get('location'), job.get('city'), job.get('state'),
                        job.get('zip_code'), job.get('latitude'), job.get('longitude'),
                        job.get('salary_min'), job.get('salary_max'),
                        job.get('description'), job.get('job_type'),
                        job.get('source'), job.get('source_id'),
                        job.get('url'), job.get('posted_date')
                    ))
                    inserted += 1

            except Exception as e:
                print(f"  Error inserting job: {e}")
                skipped += 1

        self.conn.commit()
        return (inserted, updated, skipped)

    def _hash_job(self, job: Dict) -> str:
        """Generate unique hash for job."""
        key = f"{job.get('title', '')}|{job.get('employer', '')}|{job.get('source', '')}|{job.get('source_id', '')}"
        return hashlib.sha256(key.encode()).hexdigest()

    def get_stats(self) -> Dict:
        """Get database statistics."""
        cursor = self.conn.cursor()

        stats = {}
        cursor.execute("SELECT COUNT(*) FROM jobs")
        stats['total_jobs'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'active'")
        stats['active_jobs'] = cursor.fetchone()[0]

        cursor.execute("SELECT source, COUNT(*) FROM jobs GROUP BY source")
        stats['by_source'] = dict(cursor.fetchall())

        cursor.execute("SELECT COUNT(DISTINCT employer) FROM jobs WHERE employer IS NOT NULL")
        stats['unique_employers'] = cursor.fetchone()[0]

        return stats

    def get_sample_jobs(self, limit: int = 10) -> List[Dict]:
        """Get sample jobs."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT title, employer, location, source, salary_min, salary_max, url
            FROM jobs ORDER BY created_at DESC LIMIT ?
        """, (limit,))

        columns = ['title', 'employer', 'location', 'source', 'salary_min', 'salary_max', 'url']
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()


# =========================================================
# JOB SOURCES
# =========================================================

class AdzunaSource:
    """Adzuna job board API."""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search"

    def __init__(self, app_id: str, app_key: str):
        self.app_id = app_id
        self.app_key = app_key
        self.session = requests.Session()

    def is_configured(self) -> bool:
        return bool(self.app_id and self.app_key)

    def search(self, state: str, city: str = None, max_results: int = 100) -> List[Dict]:
        """Search for jobs in a state/city."""
        if not self.is_configured():
            print("  Adzuna: Not configured (missing API keys)")
            return []

        jobs = []
        page = 1
        results_per_page = 50

        # Adzuna works best with state names
        location = state.lower()

        while len(jobs) < max_results:
            try:
                params = {
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "where": location,
                    "results_per_page": min(results_per_page, max_results - len(jobs)),
                }

                response = self.session.get(f"{self.BASE_URL}/{page}", params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                results = data.get('results', [])
                if not results:
                    break

                for item in results:
                    job = self._normalize(item)
                    if job:
                        jobs.append(job)

                if len(results) < results_per_page:
                    break

                page += 1
                time.sleep(0.5)

            except Exception as e:
                print(f"  Adzuna error: {e}")
                break

        return jobs

    def _normalize(self, item: Dict) -> Optional[Dict]:
        """Normalize Adzuna job to common format."""
        try:
            location = item.get('location', {})
            area = location.get('area', [])

            # Extract city/state from area
            city = area[-1] if len(area) > 0 else None
            state = area[-2] if len(area) > 1 else None

            return {
                'title': item.get('title', '').strip(),
                'employer': item.get('company', {}).get('display_name', '').strip(),
                'location': item.get('location', {}).get('display_name', ''),
                'city': city,
                'state': state,
                'latitude': item.get('latitude'),
                'longitude': item.get('longitude'),
                'salary_min': item.get('salary_min'),
                'salary_max': item.get('salary_max'),
                'description': item.get('description', ''),
                'job_type': item.get('contract_type'),
                'source': 'adzuna',
                'source_id': item.get('id'),
                'url': item.get('redirect_url'),
                'posted_date': item.get('created')
            }
        except Exception:
            return None


class USAJobsSource:
    """USAJOBS federal government jobs API."""

    BASE_URL = "https://data.usajobs.gov/api/search"

    def __init__(self, api_key: str, email: str):
        self.api_key = api_key
        self.email = email
        self.session = requests.Session()

    def is_configured(self) -> bool:
        return bool(self.api_key and self.email)

    def search(self, zip_code: str, radius_miles: int = 25, max_results: int = 50) -> List[Dict]:
        """Search for federal jobs near ZIP code."""
        if not self.is_configured():
            print("  USAJOBS: Not configured (missing API key or email)")
            return []

        jobs = []
        page = 1

        headers = {
            "Authorization-Key": self.api_key,
            "User-Agent": self.email,
            "Host": "data.usajobs.gov"
        }

        try:
            params = {
                "LocationName": zip_code,
                "Radius": radius_miles,
                "ResultsPerPage": min(max_results, 500),
                "Page": page
            }

            response = self.session.get(self.BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            results = data.get('SearchResult', {}).get('SearchResultItems', [])

            for item in results:
                job = self._normalize(item)
                if job:
                    jobs.append(job)

        except Exception as e:
            print(f"  USAJOBS error: {e}")

        return jobs

    def _normalize(self, item: Dict) -> Optional[Dict]:
        """Normalize USAJOBS job to common format."""
        try:
            matched = item.get('MatchedObjectDescriptor', {})
            position = matched.get('PositionLocationDisplay', '')

            # Parse salary
            salary = matched.get('PositionRemuneration', [{}])[0] if matched.get('PositionRemuneration') else {}
            salary_min = salary.get('MinimumRange')
            salary_max = salary.get('MaximumRange')

            # Try to convert salary to float
            try:
                salary_min = float(salary_min) if salary_min else None
                salary_max = float(salary_max) if salary_max else None
            except (ValueError, TypeError):
                salary_min, salary_max = None, None

            return {
                'title': matched.get('PositionTitle', '').strip(),
                'employer': matched.get('OrganizationName', '').strip(),
                'location': position,
                'city': None,  # Would need parsing
                'state': None,
                'latitude': None,
                'longitude': None,
                'salary_min': salary_min,
                'salary_max': salary_max,
                'description': matched.get('UserArea', {}).get('Details', {}).get('JobSummary', ''),
                'job_type': matched.get('PositionSchedule', [{}])[0].get('Name') if matched.get('PositionSchedule') else None,
                'source': 'usajobs',
                'source_id': matched.get('PositionID'),
                'url': matched.get('PositionURI'),
                'posted_date': matched.get('PublicationStartDate')
            }
        except Exception:
            return None


# =========================================================
# MAIN
# =========================================================

def print_banner():
    """Print welcome banner."""
    print("""
============================================================
        Job Database Test - Cambridge, MA (02139)
============================================================
    """)


def check_api_keys(config: Config) -> Dict[str, bool]:
    """Check which APIs are configured."""
    return {
        'Adzuna': bool(config.adzuna_app_id and config.adzuna_app_key),
        'USAJOBS': bool(config.usajobs_api_key and config.usajobs_email)
    }


def main():
    print_banner()

    # Load configuration
    config = Config()

    print(f"Target: {config.city}, {config.state} ({config.zip_code})")
    print(f"Radius: {config.radius_miles} miles")
    print()

    # Get coordinates for target location
    print("Getting coordinates for target location...")
    center_lat, center_lon = get_zip_coordinates(config.zip_code)
    if center_lat is None:
        print(f"ERROR: Could not get coordinates for ZIP {config.zip_code}")
        return
    print(f"  Center: {center_lat:.4f}, {center_lon:.4f}")
    print()

    # Check API keys
    print("API Key Status:")
    api_status = check_api_keys(config)
    for api, configured in api_status.items():
        status = "Configured" if configured else "NOT CONFIGURED"
        print(f"  {api}: {status}")
    print()

    if not any(api_status.values()):
        print("ERROR: No API keys configured!")
        print()
        print("Please set environment variables:")
        print("  export ADZUNA_APP_ID='your_app_id'")
        print("  export ADZUNA_APP_KEY='your_app_key'")
        print("  export USAJOBS_API_KEY='your_api_key'")
        print("  export USAJOBS_EMAIL='your_email'")
        print()
        print("Get API keys from:")
        print("  Adzuna: https://developer.adzuna.com")
        print("  USAJOBS: https://developer.usajobs.gov")
        return

    # Initialize database
    print("Initializing database...")
    db = JobDatabase(config.db_path)
    db.connect()
    print()

    # Collect jobs from each source
    total_jobs = []

    # Adzuna - fetch more since we'll filter by distance
    if api_status['Adzuna']:
        print("Fetching from Adzuna...")
        adzuna = AdzunaSource(config.adzuna_app_id, config.adzuna_app_key)
        # Fetch extra jobs since we'll filter by location
        adzuna_jobs = adzuna.search(state="massachusetts", city=config.city, max_results=500)
        print(f"  Fetched {len(adzuna_jobs)} jobs from Adzuna")

        # Filter to only jobs within radius
        adzuna_jobs = filter_jobs_by_distance(adzuna_jobs, center_lat, center_lon, config.radius_miles)
        print(f"  After filtering to {config.radius_miles} mile radius: {len(adzuna_jobs)} jobs")
        total_jobs.extend(adzuna_jobs)

    # USAJOBS - already filters by location
    if api_status['USAJOBS']:
        print("Fetching from USAJOBS...")
        usajobs = USAJobsSource(config.usajobs_api_key, config.usajobs_email)
        usajobs_jobs = usajobs.search(config.zip_code, radius_miles=config.radius_miles, max_results=config.max_results_per_source)
        print(f"  Found {len(usajobs_jobs)} jobs from USAJOBS")
        total_jobs.extend(usajobs_jobs)

    print()

    # Insert into database
    if total_jobs:
        print(f"Inserting {len(total_jobs)} jobs into database...")
        inserted, updated, skipped = db.insert_jobs(total_jobs)
        print(f"  Inserted: {inserted}")
        print(f"  Updated: {updated}")
        print(f"  Skipped: {skipped}")
    else:
        print("No jobs found!")

    print()

    # Show statistics
    print("Database Statistics:")
    stats = db.get_stats()
    print(f"  Total Jobs: {stats['total_jobs']}")
    print(f"  Active Jobs: {stats['active_jobs']}")
    print(f"  Unique Employers: {stats['unique_employers']}")
    print(f"  By Source: {stats['by_source']}")

    print()

    # Show sample jobs
    print("Sample Jobs (most recent):")
    print("-" * 60)
    samples = db.get_sample_jobs(10)
    for job in samples:
        title = (job['title'] or 'Unknown')[:45]
        employer = (job['employer'] or 'Unknown')[:25]
        source = job['source']
        salary = ""
        if job['salary_min'] and job['salary_max']:
            salary = f" | ${job['salary_min']:,.0f}-${job['salary_max']:,.0f}"
        print(f"  {title}")
        print(f"    {employer} ({source}){salary}")

    print()
    print("-" * 60)

    # Auto-export to CSV
    csv_path = config.db_path.replace('.db', '.csv')
    exported = export_to_csv(db, csv_path)
    print(f"Exported {exported} jobs to: {csv_path}")

    print(f"Database saved to: {config.db_path}")
    print()
    print("Next steps:")
    print("  1. Run again to update/refresh jobs")
    print("  2. Open the CSV in Excel/Google Sheets")
    print("  3. Expand to more ZIP codes")

    db.close()


if __name__ == "__main__":
    main()
