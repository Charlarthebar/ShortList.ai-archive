#!/usr/bin/env python3
"""
Cambridge Area Jobs Collector
=============================
Collects job listings relevant to Cambridge, MA residents.

Sources:
- Adzuna API (filtered to Cambridge area, 10-mile radius)
- USAJOBS API (filtered by ZIP code radius)
- The Muse API (filtered to Boston/Cambridge area)
- Coresignal API (filtered to Cambridge area)
- RemoteOK (all remote jobs - Cambridge residents can work them)
- Remotive (all remote jobs - Cambridge residents can work them)

Usage:
    python cambridge_jobs_collector.py

Schedule:
    Set up with: python schedule_scraping.py --setup-launchd
"""

import os
import sys
import json
import sqlite3
import hashlib
import time
import math
import csv
import re
import html
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import requests


def strip_html(text: str) -> str:
    """Remove HTML tags and clean up whitespace from text."""
    if not text:
        return ""
    # Decode HTML entities
    text = html.unescape(text)
    # Replace block elements with newlines
    text = re.sub(r'<br\s*/?>|</?p>|</?div>|</?li>|</?h[1-6]>|</?ul>|</?ol>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]*>', '', text)
    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

# Load .env file (check both current and parent directory)
def load_env_file():
    """Load environment variables from .env file."""
    env_paths = [
        os.path.join(os.path.dirname(__file__), '.env'),
        os.path.join(os.path.dirname(__file__), '..', '.env'),
    ]
    for env_path in env_paths:
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ.setdefault(key.strip(), value.strip())

try:
    from dotenv import load_dotenv
    # Try current dir first, then parent
    load_dotenv()
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    load_env_file()

# =========================================================
# CONFIGURATION
# =========================================================

@dataclass
class Config:
    """Collection configuration for Cambridge area."""
    db_path: str = "cambridge_jobs.db"
    csv_path: str = "cambridge_jobs.csv"
    # Cambridge, MA location settings
    center_zip: str = "02139"
    center_lat: float = 42.3736
    center_lon: float = -71.1097
    radius_miles: float = 10.0
    # Adzuna settings - search MA then filter
    adzuna_max: int = 5000  # Fetch more since we'll filter
    adzuna_delay: float = 0.5
    # USAJOBS settings - uses built-in radius
    usajobs_max: int = 500
    usajobs_delay: float = 0.5
    # The Muse settings - fetch and filter
    themuse_max: int = 10000  # Fetch more since we filter
    themuse_delay: float = 0.2
    # Coresignal settings
    coresignal_max: int = 5000
    coresignal_delay: float = 0.3
    # RemoteOK/Remotive (include all - remote jobs)
    remote_delay: float = 1.0
    # Progress save interval
    progress_save_interval: int = 500

config = Config()

# Cities within ~10 miles of Cambridge for fallback filtering
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


def is_in_cambridge_area(job: Dict, center_lat: float = None, center_lon: float = None,
                         radius_miles: float = None) -> bool:
    """
    Check if a job is in the Cambridge area.

    Uses multiple strategies:
    1. If job has lat/lon coordinates, check distance
    2. If job has city name, check against known Cambridge area cities
    3. If job location contains Cambridge area keywords
    """
    center_lat = center_lat or config.center_lat
    center_lon = center_lon or config.center_lon
    radius_miles = radius_miles or config.radius_miles

    # Strategy 1: Check coordinates if available
    job_lat = job.get('latitude')
    job_lon = job.get('longitude')
    if job_lat is not None and job_lon is not None:
        try:
            distance = haversine_distance(center_lat, center_lon, float(job_lat), float(job_lon))
            return distance <= radius_miles
        except (TypeError, ValueError):
            pass  # Fall through to city check

    # Strategy 2: Check city name
    city = (job.get('city') or '').lower().strip()
    if city and city in CAMBRIDGE_AREA_CITIES:
        return True

    # Strategy 3: Check location string for Cambridge area keywords
    location = (job.get('location') or '').lower()
    for area_city in CAMBRIDGE_AREA_CITIES:
        if area_city in location:
            return True

    # Strategy 4: Check for Massachusetts + major keywords
    state = (job.get('state') or '').upper()
    if state in ('MA', 'MASSACHUSETTS'):
        # If it's in MA, give it a chance if location has Boston metro keywords
        boston_metro_keywords = ['greater boston', 'boston metro', 'boston area', 'metro boston']
        if any(kw in location for kw in boston_metro_keywords):
            return True

    return False


def filter_jobs_to_cambridge_area(jobs: List[Dict]) -> List[Dict]:
    """Filter a list of jobs to only those in the Cambridge area."""
    filtered = []
    for job in jobs:
        if is_in_cambridge_area(job):
            filtered.append(job)
    return filtered


# =========================================================
# API CLIENTS
# =========================================================

class AdzunaClient:
    """Client for Adzuna job search API - filtered to Cambridge area."""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search"

    def __init__(self):
        self.app_id = os.environ.get('ADZUNA_APP_ID')
        self.app_key = os.environ.get('ADZUNA_APP_KEY')

    def is_configured(self) -> bool:
        return bool(self.app_id and self.app_key)

    def search_cambridge_area(self, max_results: int = 5000, delay: float = 0.5) -> List[Dict]:
        """
        Search for jobs in Massachusetts, then filter to Cambridge area.

        Adzuna provides lat/lon coordinates for jobs, so we filter by distance.
        """
        if not self.is_configured():
            print("  Adzuna: Not configured (missing API keys)")
            return []

        jobs = []
        page = 1
        results_per_page = 50
        total_available = None
        consecutive_errors = 0
        max_consecutive_errors = 5

        print(f"  Fetching up to {max_results:,} jobs from Adzuna (MA)...")

        while len(jobs) < max_results:
            try:
                params = {
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "results_per_page": results_per_page,
                    "where": "massachusetts",  # Search entire state
                    "sort_by": "date",
                    # No max_days_old limit - fetch all available jobs
                }

                response = requests.get(f"{self.BASE_URL}/{page}", params=params, timeout=30)

                if response.status_code == 429:
                    print(f"    Rate limited on page {page}, waiting 30 seconds...")
                    time.sleep(30)
                    continue

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"    Too many errors, stopping at page {page}")
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                data = response.json()

                if total_available is None:
                    total_available = data.get("count", 0)
                    print(f"    Total available in MA: {total_available:,} jobs")

                results = data.get("results", [])
                if not results:
                    break

                for job in results:
                    location_area = job.get("location", {}).get("area", [])
                    state = ""
                    city = ""
                    if location_area:
                        if len(location_area) >= 1:
                            city = location_area[0]
                        if len(location_area) >= 3:
                            state = location_area[2]

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
                        "description": job.get("description", ""),
                        "url": job.get("redirect_url", ""),
                        "posted_date": job.get("created", ""),
                        "source": "adzuna",
                        "source_id": str(job.get("id", "")),
                    })

                if page % 50 == 0:
                    print(f"    Page {page}: {len(jobs):,} jobs fetched...")

                page += 1
                time.sleep(delay)

                if page > 1000:  # Adzuna limit
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"    Error on page {page}: {e}")
                if consecutive_errors >= max_consecutive_errors:
                    break
                time.sleep(5)

        # Filter to Cambridge area
        print(f"    Filtering {len(jobs):,} MA jobs to Cambridge area...")
        cambridge_jobs = filter_jobs_to_cambridge_area(jobs)
        print(f"  Adzuna: {len(cambridge_jobs):,} jobs in Cambridge area (from {len(jobs):,} MA jobs)")

        return cambridge_jobs


class USAJobsClient:
    """Client for USAJOBS API - uses built-in ZIP radius filter."""

    BASE_URL = "https://data.usajobs.gov/api/search"

    def __init__(self):
        self.api_key = os.environ.get('USAJOBS_API_KEY')
        self.email = os.environ.get('USAJOBS_EMAIL')

    def is_configured(self) -> bool:
        return bool(self.api_key and self.email)

    def search_cambridge_area(self, zip_code: str = "02139", radius_miles: int = 10,
                               max_results: int = 500, delay: float = 0.5) -> List[Dict]:
        """
        Search for federal jobs near Cambridge ZIP code.

        USAJOBS supports built-in ZIP radius filtering.
        """
        if not self.is_configured():
            print("  USAJOBS: Not configured (missing API key or email)")
            return []

        jobs = []
        page = 1
        total_available = None
        consecutive_errors = 0

        headers = {
            "Authorization-Key": self.api_key,
            "User-Agent": self.email,
            "Host": "data.usajobs.gov"
        }

        print(f"  Fetching federal jobs within {radius_miles} miles of {zip_code}...")

        while len(jobs) < max_results:
            try:
                params = {
                    "LocationName": zip_code,
                    "Radius": radius_miles,
                    "Page": page,
                    "ResultsPerPage": min(500, max_results - len(jobs)),
                    # No DatePosted limit - fetch all available jobs
                    "SortField": "DatePosted",
                    "SortDirection": "Desc",
                }

                response = requests.get(self.BASE_URL, headers=headers, params=params, timeout=30)

                if response.status_code == 429:
                    print(f"    Rate limited, waiting 30 seconds...")
                    time.sleep(30)
                    continue

                response.raise_for_status()
                data = response.json()

                if total_available is None:
                    total_available = int(data.get("SearchResult", {}).get("SearchResultCountAll", 0))
                    print(f"    Total available: {total_available:,} federal jobs")

                results = data.get("SearchResult", {}).get("SearchResultItems", [])
                if not results:
                    break

                for item in results:
                    job = item.get("MatchedObjectDescriptor", {})
                    position = job.get("PositionLocation", [{}])[0] if job.get("PositionLocation") else {}
                    salary = job.get("PositionRemuneration", [{}])[0] if job.get("PositionRemuneration") else {}

                    jobs.append({
                        "title": job.get("PositionTitle", ""),
                        "employer": job.get("OrganizationName", ""),
                        "location": position.get("LocationName", ""),
                        "city": position.get("CityName", ""),
                        "state": position.get("CountrySubDivisionCode", ""),
                        "latitude": position.get("Latitude"),
                        "longitude": position.get("Longitude"),
                        "salary_min": float(salary.get("MinimumRange", 0) or 0),
                        "salary_max": float(salary.get("MaximumRange", 0) or 0),
                        "description": job.get("UserArea", {}).get("Details", {}).get("MajorDuties", [""])[0] if job.get("UserArea") else "",
                        "url": job.get("PositionURI", ""),
                        "posted_date": job.get("PublicationStartDate", ""),
                        "source": "usajobs",
                        "source_id": job.get("PositionID", ""),
                    })

                page += 1
                time.sleep(delay)

                if len(jobs) >= total_available:
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"    USAJOBS error on page {page}: {e}")
                if consecutive_errors >= 5:
                    break
                time.sleep(5)

        print(f"  USAJOBS: {len(jobs):,} federal jobs in Cambridge area")
        return jobs


class TheMuseClient:
    """Client for The Muse job API - filtered to Cambridge area."""

    BASE_URL = "https://www.themuse.com/api/public/jobs"

    def search_cambridge_area(self, max_results: int = 10000, delay: float = 0.2) -> List[Dict]:
        """Fetch jobs from The Muse and filter to Cambridge area."""
        jobs = []
        page = 0
        total_pages = None
        consecutive_errors = 0

        print(f"  Fetching jobs from The Muse (will filter to Cambridge area)...")

        while len(jobs) < max_results:
            try:
                # The Muse supports location filter but it's not precise
                # Fetch Boston/Cambridge area and then filter
                params = {
                    "page": page,
                    "location": "Boston, MA"  # Pre-filter to Boston area
                }
                response = requests.get(self.BASE_URL, params=params, timeout=30)

                if response.status_code == 429:
                    print(f"    Rate limited, waiting 60 seconds...")
                    time.sleep(60)
                    continue

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                data = response.json()

                if total_pages is None:
                    total_pages = data.get("page_count", 0)
                    total_jobs = data.get("total", 0)
                    print(f"    Total in Boston area: {total_jobs:,} jobs across {total_pages:,} pages")

                results = data.get("results", [])
                if not results:
                    break

                for job in results:
                    locations = job.get("locations", [])
                    location_name = locations[0].get("name", "") if locations else ""

                    city, state = "", ""
                    if ", " in location_name:
                        parts = location_name.rsplit(", ", 1)
                        city = parts[0]
                        state = parts[1] if len(parts) > 1 else ""

                    company = job.get("company", {})

                    jobs.append({
                        "title": job.get("name", ""),
                        "employer": company.get("name", "Unknown"),
                        "location": location_name,
                        "city": city,
                        "state": state,
                        "latitude": None,
                        "longitude": None,
                        "salary_min": None,
                        "salary_max": None,
                        "description": job.get("contents", "") if job.get("contents") else "",
                        "url": job.get("refs", {}).get("landing_page", ""),
                        "posted_date": job.get("publication_date", ""),
                        "source": "themuse",
                        "source_id": str(job.get("id", "")),
                    })

                if page % 50 == 0 and page > 0:
                    print(f"    Page {page:,}/{total_pages:,}: {len(jobs):,} jobs fetched...")

                page += 1
                time.sleep(delay)

                if page >= total_pages:
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"    Error on page {page}: {e}")
                if consecutive_errors >= 5:
                    break
                time.sleep(5)

        # Filter to Cambridge area
        print(f"    Filtering {len(jobs):,} jobs to Cambridge area...")
        cambridge_jobs = filter_jobs_to_cambridge_area(jobs)
        print(f"  The Muse: {len(cambridge_jobs):,} jobs in Cambridge area")
        return cambridge_jobs


class CoresignalClient:
    """Client for Coresignal Job Posting API - filtered to Cambridge area."""

    SEARCH_URL = "https://api.coresignal.com/cdapi/v2/job_base/search/es_dsl"
    COLLECT_URL = "https://api.coresignal.com/cdapi/v2/job_base/collect"

    def __init__(self):
        self.api_key = os.environ.get('CORESIGNAL_API_KEY') or os.environ.get('INDEED_API_KEY')

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def search_cambridge_area(self, max_jobs: int = 5000, delay: float = 0.3) -> List[Dict]:
        """
        Search and collect jobs from Coresignal, filtering to Cambridge/Boston area.
        """
        if not self.is_configured():
            print("  Coresignal: Not configured (missing API key)")
            return []

        headers = {
            "accept": "application/json",
            "apikey": self.api_key,
            "Content-Type": "application/json"
        }

        all_job_ids = []
        last_id = 0
        consecutive_errors = 0

        print(f"  Fetching job IDs from Coresignal (Boston/Cambridge area)...")

        # Step 1: Collect job IDs for Boston/Cambridge area
        while len(all_job_ids) < max_jobs:
            try:
                # Search for Boston/Cambridge area jobs
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"country": {"query": "United States", "operator": "and"}}},
                                {"bool": {
                                    "should": [
                                        {"match": {"location": "Boston"}},
                                        {"match": {"location": "Cambridge"}},
                                        {"match": {"location": "Massachusetts"}}
                                    ],
                                    "minimum_should_match": 1
                                }}
                            ],
                            "filter": [{"range": {"id": {"gt": last_id}}}] if last_id > 0 else []
                        }
                    },
                    "sort": ["id"]
                }

                response = requests.post(self.SEARCH_URL, headers=headers, json=query, timeout=30)

                if response.status_code == 429:
                    print(f"    Rate limited, waiting 60 seconds...")
                    time.sleep(60)
                    continue

                if response.status_code == 402:
                    print(f"    API quota exceeded (trial limit reached)")
                    break

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        print(f"    Too many errors, stopping search")
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                job_ids = response.json()

                if not job_ids or not isinstance(job_ids, list):
                    break

                all_job_ids.extend(job_ids)
                last_id = job_ids[-1]

                print(f"    Retrieved {len(all_job_ids):,} job IDs...")
                time.sleep(delay)

            except Exception as e:
                print(f"    Search error: {e}")
                break

        all_job_ids = all_job_ids[:max_jobs]
        print(f"  Found {len(all_job_ids):,} job IDs, now collecting details...")

        # Step 2: Collect full job details
        jobs = []
        for i, job_id in enumerate(all_job_ids):
            try:
                response = requests.get(
                    f"{self.COLLECT_URL}/{job_id}",
                    headers=headers,
                    timeout=30
                )

                if response.status_code == 429:
                    print(f"    Rate limited at job {i}, waiting...")
                    time.sleep(60)
                    continue

                if response.status_code == 402:
                    print(f"    API quota exceeded at job {i}")
                    break

                if response.status_code == 200:
                    job = response.json()

                    location = job.get("location", "")
                    city, state = "", ""
                    if ", " in location:
                        parts = location.rsplit(", ", 1)
                        city = parts[0]
                        state = parts[1] if len(parts) > 1 else ""

                    jobs.append({
                        "title": job.get("title", ""),
                        "employer": job.get("company_name", "Unknown"),
                        "location": location,
                        "city": city,
                        "state": state,
                        "latitude": None,
                        "longitude": None,
                        "salary_min": None,
                        "salary_max": None,
                        "description": job.get("description") or "",
                        "url": job.get("url", ""),
                        "posted_date": job.get("created", ""),
                        "source": "coresignal",
                        "source_id": str(job_id),
                    })

                if (i + 1) % 500 == 0:
                    print(f"    Collected {len(jobs):,} jobs...")

                time.sleep(delay / 2)

            except Exception as e:
                if "402" in str(e) or "quota" in str(e).lower():
                    print(f"    API quota exceeded")
                    break
                continue

        # Filter to Cambridge area
        print(f"    Filtering {len(jobs):,} jobs to Cambridge area...")
        cambridge_jobs = filter_jobs_to_cambridge_area(jobs)
        print(f"  Coresignal: {len(cambridge_jobs):,} jobs in Cambridge area")
        return cambridge_jobs


class RemoteOKClient:
    """Client for RemoteOK API - all remote jobs (Cambridge residents can work them)."""

    BASE_URL = "https://remoteok.com/api"

    def search(self) -> List[Dict]:
        """Fetch all jobs from RemoteOK (all are remote, so all relevant to Cambridge residents)."""
        print(f"  Fetching remote jobs from RemoteOK...")

        try:
            headers = {"User-Agent": "ShortList Job Collector/1.0"}
            response = requests.get(self.BASE_URL, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data[1:] if len(data) > 1 else []  # First item is metadata

            jobs = []
            for job in results:
                jobs.append({
                    "title": job.get("position", ""),
                    "employer": job.get("company", "Unknown"),
                    "location": "Remote",
                    "city": "",
                    "state": "",
                    "latitude": None,
                    "longitude": None,
                    "salary_min": self._parse_salary(job.get("salary_min")),
                    "salary_max": self._parse_salary(job.get("salary_max")),
                    "description": job.get("description", "") if job.get("description") else "",
                    "url": job.get("url", ""),
                    "posted_date": job.get("date", ""),
                    "source": "remoteok",
                    "source_id": str(job.get("id", "")),
                    "is_remote": True,
                })

            print(f"  RemoteOK: {len(jobs)} remote jobs (all available to Cambridge residents)")
            return jobs

        except Exception as e:
            print(f"  RemoteOK error: {e}")
            return []

    def _parse_salary(self, val):
        if val is None:
            return None
        try:
            return float(val)
        except:
            return None


class RemotiveClient:
    """Client for Remotive API - all remote jobs (Cambridge residents can work them)."""

    BASE_URL = "https://remotive.com/api/remote-jobs"

    def search(self) -> List[Dict]:
        """Fetch all jobs from Remotive (all are remote, so all relevant to Cambridge residents)."""
        print(f"  Fetching remote jobs from Remotive...")

        try:
            response = requests.get(self.BASE_URL, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data.get("jobs", [])

            jobs = []
            for job in results:
                jobs.append({
                    "title": job.get("title", ""),
                    "employer": job.get("company_name", "Unknown"),
                    "location": job.get("candidate_required_location", "Remote"),
                    "city": "",
                    "state": "",
                    "latitude": None,
                    "longitude": None,
                    "salary_min": None,
                    "salary_max": None,
                    "description": job.get("description", "") if job.get("description") else "",
                    "url": job.get("url", ""),
                    "posted_date": job.get("publication_date", ""),
                    "source": "remotive",
                    "source_id": str(job.get("id", "")),
                    "is_remote": True,
                })

            print(f"  Remotive: {len(jobs)} remote jobs (all available to Cambridge residents)")
            return jobs

        except Exception as e:
            print(f"  Remotive error: {e}")
            return []


class ArbeitnowClient:
    """Client for Arbeitnow API - remote/tech jobs (free, no key needed)."""

    BASE_URL = "https://www.arbeitnow.com/api/job-board-api"

    def search(self) -> List[Dict]:
        """Fetch jobs from Arbeitnow (remote-friendly tech jobs)."""
        print(f"  Fetching jobs from Arbeitnow...")

        jobs = []
        page = 1

        try:
            while True:
                response = requests.get(f"{self.BASE_URL}?page={page}", timeout=30)
                if response.status_code != 200:
                    break

                data = response.json()
                results = data.get("data", [])

                if not results:
                    break

                for job in results:
                    # Check if remote or US-based
                    location = job.get("location", "")
                    is_remote = job.get("remote", False)

                    jobs.append({
                        "title": job.get("title", ""),
                        "employer": job.get("company_name", "Unknown"),
                        "location": location if not is_remote else "Remote",
                        "city": "",
                        "state": "",
                        "latitude": None,
                        "longitude": None,
                        "salary_min": None,
                        "salary_max": None,
                        "description": job.get("description", "") if job.get("description") else "",
                        "url": job.get("url", ""),
                        "posted_date": job.get("created_at", ""),
                        "source": "arbeitnow",
                        "source_id": str(job.get("slug", "")),
                        "is_remote": is_remote,
                    })

                # Check for more pages
                if not data.get("links", {}).get("next"):
                    break

                page += 1
                time.sleep(0.5)

                if page > 50:  # Safety limit
                    break

            # Filter to remote jobs only (since Arbeitnow is global)
            remote_jobs = [j for j in jobs if j.get("is_remote")]
            print(f"  Arbeitnow: {len(remote_jobs)} remote jobs (from {len(jobs)} total)")
            return remote_jobs

        except Exception as e:
            print(f"  Arbeitnow error: {e}")
            return []


class JobicyClient:
    """Client for Jobicy API - remote jobs (free, no key needed)."""

    BASE_URL = "https://jobicy.com/api/v2/remote-jobs"

    def search(self) -> List[Dict]:
        """Fetch remote jobs from Jobicy."""
        print(f"  Fetching jobs from Jobicy...")

        try:
            params = {"count": 50, "geo": "usa"}  # Get US remote jobs
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data.get("jobs", [])

            jobs = []
            for job in results:
                jobs.append({
                    "title": job.get("jobTitle", ""),
                    "employer": job.get("companyName", "Unknown"),
                    "location": "Remote",
                    "city": "",
                    "state": "",
                    "latitude": None,
                    "longitude": None,
                    "salary_min": None,
                    "salary_max": None,
                    "description": job.get("jobDescription", "") if job.get("jobDescription") else "",
                    "url": job.get("url", ""),
                    "posted_date": job.get("pubDate", ""),
                    "source": "jobicy",
                    "source_id": str(job.get("id", "")),
                    "is_remote": True,
                })

            print(f"  Jobicy: {len(jobs)} remote jobs")
            return jobs

        except Exception as e:
            print(f"  Jobicy error: {e}")
            return []


class WeWorkRemotelyClient:
    """Client for We Work Remotely RSS feed - remote jobs."""

    # WWR has RSS feeds for different categories
    RSS_URLS = [
        "https://weworkremotely.com/categories/remote-programming-jobs.rss",
        "https://weworkremotely.com/categories/remote-design-jobs.rss",
        "https://weworkremotely.com/categories/remote-sales-and-marketing-jobs.rss",
        "https://weworkremotely.com/categories/remote-product-jobs.rss",
        "https://weworkremotely.com/categories/remote-customer-support-jobs.rss",
        "https://weworkremotely.com/categories/remote-finance-and-legal-jobs.rss",
        "https://weworkremotely.com/categories/remote-devops-and-sysadmin-jobs.rss",
    ]

    def search(self) -> List[Dict]:
        """Fetch remote jobs from We Work Remotely RSS feeds."""
        print(f"  Fetching jobs from We Work Remotely...")

        jobs = []
        seen_urls = set()

        try:
            import xml.etree.ElementTree as ET

            for rss_url in self.RSS_URLS:
                try:
                    response = requests.get(rss_url, timeout=30)
                    if response.status_code != 200:
                        continue

                    root = ET.fromstring(response.content)

                    for item in root.findall(".//item"):
                        url = item.findtext("link", "")
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                        title = item.findtext("title", "")
                        # WWR titles are often "Company: Job Title"
                        company = "Unknown"
                        job_title = title
                        if ": " in title:
                            parts = title.split(": ", 1)
                            company = parts[0]
                            job_title = parts[1] if len(parts) > 1 else title

                        jobs.append({
                            "title": job_title,
                            "employer": company,
                            "location": "Remote",
                            "city": "",
                            "state": "",
                            "latitude": None,
                            "longitude": None,
                            "salary_min": None,
                            "salary_max": None,
                            "description": item.findtext("description", ""),
                            "url": url,
                            "posted_date": item.findtext("pubDate", ""),
                            "source": "weworkremotely",
                            "source_id": url.split("/")[-1] if url else "",
                            "is_remote": True,
                        })

                    time.sleep(0.3)

                except Exception as e:
                    print(f"    Error fetching {rss_url}: {e}")
                    continue

            print(f"  We Work Remotely: {len(jobs)} remote jobs")
            return jobs

        except Exception as e:
            print(f"  We Work Remotely error: {e}")
            return []


# =========================================================
# DATABASE
# =========================================================

class JobDatabase:
    """SQLite database for storing Cambridge area jobs."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Create tables if they don't exist, and migrate existing schema."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_hash TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                employer TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                latitude REAL,
                longitude REAL,
                salary_min REAL,
                salary_max REAL,
                description TEXT,
                source TEXT NOT NULL,
                source_id TEXT,
                url TEXT,
                posted_date TEXT,
                is_remote INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
            CREATE INDEX IF NOT EXISTS idx_jobs_city ON jobs(city);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        """)
        self.conn.commit()

        # Add is_remote column if it doesn't exist (migration for existing databases)
        try:
            self.conn.execute("ALTER TABLE jobs ADD COLUMN is_remote INTEGER DEFAULT 0")
            self.conn.commit()
            print("  Migrated database: added is_remote column")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add latitude/longitude columns if they don't exist
        for col in ['latitude', 'longitude']:
            try:
                self.conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} REAL")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Create index on is_remote if it doesn't exist
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_is_remote ON jobs(is_remote)")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

    def _hash_job(self, job: Dict) -> str:
        """Create unique hash for a job."""
        unique_str = f"{job['title']}|{job['employer']}|{job['location']}|{job['source']}"
        return hashlib.md5(unique_str.encode()).hexdigest()

    def upsert_job(self, job: Dict) -> str:
        """Insert or update a job. Returns 'inserted', 'updated', or 'skipped'."""
        job_hash = self._hash_job(job)
        clean_description = strip_html(job.get('description', ''))

        cursor = self.conn.execute(
            "SELECT id, status FROM jobs WHERE job_hash = ?", (job_hash,)
        )
        existing = cursor.fetchone()

        if existing:
            self.conn.execute(
                "UPDATE jobs SET last_seen = CURRENT_TIMESTAMP, status = 'active', description = ? WHERE job_hash = ?",
                (clean_description, job_hash)
            )
            return "updated"
        else:
            is_remote = 1 if job.get('is_remote') or job.get('location', '').lower() == 'remote' else 0
            self.conn.execute("""
                INSERT INTO jobs (job_hash, title, employer, location, city, state,
                                  latitude, longitude, salary_min, salary_max, description,
                                  source, source_id, url, posted_date, is_remote)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_hash, job['title'], job['employer'], job['location'],
                job.get('city', ''), job.get('state', ''),
                job.get('latitude'), job.get('longitude'),
                job.get('salary_min'), job.get('salary_max'),
                clean_description, job['source'], job.get('source_id', ''),
                job.get('url', ''), job.get('posted_date', ''), is_remote
            ))
            return "inserted"

    def commit(self):
        self.conn.commit()

    def get_stats(self) -> Dict:
        """Get database statistics."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM jobs")
        total = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'active'")
        active = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE is_remote = 1")
        remote = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT COUNT(DISTINCT employer) FROM jobs")
        employers = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT source, COUNT(*) FROM jobs GROUP BY source")
        by_source = dict(cursor.fetchall())

        cursor = self.conn.execute("SELECT city, COUNT(*) FROM jobs WHERE city != '' GROUP BY city ORDER BY COUNT(*) DESC LIMIT 10")
        top_cities = dict(cursor.fetchall())

        return {
            "total": total,
            "active": active,
            "remote": remote,
            "employers": employers,
            "by_source": by_source,
            "top_cities": top_cities,
        }

    def export_csv(self, filepath: str):
        """Export jobs to CSV with full descriptions and skills."""
        cursor = self.conn.execute("""
            SELECT title, employer, location, city, state,
                   salary_min, salary_max, source, url, posted_date,
                   is_remote, status, first_seen, description, skills
            FROM jobs
            ORDER BY source, employer, title
        """)

        rows = cursor.fetchall()

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(['title', 'employer', 'location', 'city', 'state',
                           'salary_min', 'salary_max', 'source', 'url', 'posted_date',
                           'is_remote', 'status', 'first_seen', 'description', 'skills'])

            for row in rows:
                # Convert to list to clean description
                row_list = list(row)
                # Clean description - replace newlines with spaces for CSV
                if row_list[13]:
                    row_list[13] = re.sub(r'[\r\n]+', ' ', row_list[13])
                    row_list[13] = re.sub(r'\s+', ' ', row_list[13]).strip()
                writer.writerow(row_list)

        print(f"Exported {len(rows):,} jobs to {filepath}")


# =========================================================
# MAIN COLLECTION
# =========================================================

def save_jobs_to_db(db: JobDatabase, jobs: List[Dict], source_name: str) -> tuple:
    """Save jobs to database and return (inserted, updated) counts."""
    inserted = 0
    updated = 0
    for i, job in enumerate(jobs):
        result = db.upsert_job(job)
        if result == "inserted":
            inserted += 1
        elif result == "updated":
            updated += 1
        if (i + 1) % 500 == 0:
            db.commit()
    db.commit()
    return inserted, updated


def extract_skills_for_all_jobs(db_path: str):
    """Extract skills from job descriptions using onet_skills.csv taxonomy."""
    import os

    onet_path = os.path.join(os.path.dirname(__file__), "onet_skills.csv")

    # Build skills taxonomy
    core_skills = set()
    all_skills = set()

    if os.path.exists(onet_path):
        with open(onet_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                skill = row.get('skill_name', '').strip().lower()
                skill_type = row.get('skill_type', '').strip()
                if skill:
                    all_skills.add(skill)
                    if skill_type == 'Core Skill':
                        core_skills.add(skill)

    # Add common skills not in O*NET
    common_skills = {
        "python", "java", "javascript", "typescript", "c++", "c#", "ruby", "go",
        "rust", "scala", "kotlin", "swift", "php", "sql", "react", "angular", "vue",
        "node.js", "django", "flask", "aws", "azure", "gcp", "docker", "kubernetes",
        "machine learning", "data science", "data analysis", "tensorflow", "pytorch",
        "project management", "agile", "scrum", "leadership", "communication",
        "clinical research", "regulatory affairs", "biotechnology", "healthcare",
        "marketing", "sales", "customer service", "financial analysis", "accounting",
    }
    all_skills.update(common_skills)

    print(f"\nExtracting skills for all jobs...")
    print(f"  Skills taxonomy: {len(all_skills)} skills ({len(core_skills)} core)")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add skills column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE jobs ADD COLUMN skills TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Get all jobs
    cursor.execute("SELECT id, description FROM jobs")
    jobs = cursor.fetchall()

    updated = 0
    with_skills = 0

    for job_id, description in jobs:
        if not description:
            continue

        desc_lower = description.lower()
        found_skills = []

        for skill in all_skills:
            # Use word boundary matching for short skills
            if len(skill) <= 3:
                pattern = r'\b' + re.escape(skill) + r'\b'
                if re.search(pattern, desc_lower):
                    # Skip single letter 'r' unless clearly programming context
                    if skill == "r" and not re.search(r'\br\s+(programming|language|studio)', desc_lower):
                        continue
                    found_skills.append(skill)
            else:
                if skill in desc_lower:
                    found_skills.append(skill)

        # Deduplicate and format
        found_skills = list(set(found_skills))
        # Prioritize core skills
        core_found = [s for s in found_skills if s in core_skills]
        other_found = [s for s in found_skills if s not in core_skills]
        found_skills = (core_found + other_found)[:25]

        # Format properly (title case, uppercase for short acronyms)
        formatted = []
        for s in found_skills:
            if len(s) <= 3:
                formatted.append(s.upper())
            else:
                formatted.append(s.title())

        skills_str = "; ".join(formatted)

        if skills_str:
            with_skills += 1

        cursor.execute("UPDATE jobs SET skills = ? WHERE id = ?", (skills_str, job_id))
        updated += 1

        if updated % 5000 == 0:
            conn.commit()
            print(f"    Processed {updated:,} jobs...")

    conn.commit()
    conn.close()

    print(f"  Skills extracted: {with_skills:,} of {updated:,} jobs have skills")
    return with_skills


def collect_cambridge():
    """Main collection function - fetches from ALL available sources, filtered to Cambridge area."""
    print("=" * 70)
    print("     CAMBRIDGE AREA JOBS COLLECTOR (MULTI-SOURCE)")
    print("=" * 70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nTarget: Cambridge, MA ({config.center_zip}) - {config.radius_miles} mile radius")
    print(f"Sources: Adzuna, USAJOBS, The Muse, Coresignal, RemoteOK, Remotive, Arbeitnow, Jobicy, WWR")
    print(f"Note: Remote jobs included (Cambridge residents can work them)")

    # Initialize database and clients
    db = JobDatabase(config.db_path)
    adzuna = AdzunaClient()
    usajobs = USAJobsClient()
    themuse = TheMuseClient()
    coresignal = CoresignalClient()
    remoteok = RemoteOKClient()
    remotive = RemotiveClient()

    print(f"\nAPI Status:")
    print(f"  Adzuna: {'Configured' if adzuna.is_configured() else 'NOT CONFIGURED'}")
    print(f"  USAJOBS: {'Configured' if usajobs.is_configured() else 'NOT CONFIGURED'}")
    print(f"  Coresignal: {'Configured' if coresignal.is_configured() else 'NOT CONFIGURED'}")
    print(f"  The Muse: Available (no key needed)")
    print(f"  RemoteOK: Available (no key needed)")
    print(f"  Remotive: Available (no key needed)")
    print(f"  Arbeitnow: Available (no key needed)")
    print(f"  Jobicy: Available (no key needed)")
    print(f"  We Work Remotely: Available (no key needed)")

    results = {
        'adzuna': {'inserted': 0, 'updated': 0},
        'usajobs': {'inserted': 0, 'updated': 0},
        'themuse': {'inserted': 0, 'updated': 0},
        'coresignal': {'inserted': 0, 'updated': 0},
        'remoteok': {'inserted': 0, 'updated': 0},
        'remotive': {'inserted': 0, 'updated': 0},
        'arbeitnow': {'inserted': 0, 'updated': 0},
        'jobicy': {'inserted': 0, 'updated': 0},
        'weworkremotely': {'inserted': 0, 'updated': 0},
    }

    # =========================================================
    # 1. ADZUNA (Massachusetts -> Cambridge area filter)
    # =========================================================
    print(f"\n{'='*70}")
    print("1. ADZUNA - Job Listings (filtered to Cambridge area)")
    print("=" * 70)

    if adzuna.is_configured():
        adzuna_jobs = adzuna.search_cambridge_area(max_results=config.adzuna_max, delay=config.adzuna_delay)
        ins, upd = save_jobs_to_db(db, adzuna_jobs, "adzuna")
        results['adzuna'] = {'inserted': ins, 'updated': upd}
        print(f"  Saved: +{ins:,} new, {upd:,} updated")
    else:
        print("  SKIPPED (not configured)")

    # =========================================================
    # 2. USAJOBS (Federal jobs with ZIP radius)
    # =========================================================
    print(f"\n{'='*70}")
    print("2. USAJOBS - Federal Government Jobs")
    print("=" * 70)

    if usajobs.is_configured():
        usajobs_jobs = usajobs.search_cambridge_area(
            zip_code=config.center_zip,
            radius_miles=int(config.radius_miles),
            max_results=config.usajobs_max,
            delay=config.usajobs_delay
        )
        ins, upd = save_jobs_to_db(db, usajobs_jobs, "usajobs")
        results['usajobs'] = {'inserted': ins, 'updated': upd}
        print(f"  Saved: +{ins:,} new, {upd:,} updated")
    else:
        print("  SKIPPED (not configured)")

    # =========================================================
    # 3. THE MUSE (Boston/Cambridge filter)
    # =========================================================
    print(f"\n{'='*70}")
    print("3. THE MUSE - Career Platform Jobs")
    print("=" * 70)

    themuse_jobs = themuse.search_cambridge_area(max_results=config.themuse_max, delay=config.themuse_delay)
    ins, upd = save_jobs_to_db(db, themuse_jobs, "themuse")
    results['themuse'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 4. CORESIGNAL (Boston/Cambridge filter)
    # =========================================================
    print(f"\n{'='*70}")
    print("4. CORESIGNAL - LinkedIn & Other Sources")
    print("=" * 70)

    if coresignal.is_configured():
        coresignal_jobs = coresignal.search_cambridge_area(max_jobs=config.coresignal_max, delay=config.coresignal_delay)
        ins, upd = save_jobs_to_db(db, coresignal_jobs, "coresignal")
        results['coresignal'] = {'inserted': ins, 'updated': upd}
        print(f"  Saved: +{ins:,} new, {upd:,} updated")
    else:
        print("  SKIPPED (not configured)")

    # =========================================================
    # 5. REMOTEOK (All remote jobs - Cambridge residents can work them)
    # =========================================================
    print(f"\n{'='*70}")
    print("5. REMOTEOK - Remote Jobs (all included)")
    print("=" * 70)

    remoteok_jobs = remoteok.search()
    ins, upd = save_jobs_to_db(db, remoteok_jobs, "remoteok")
    results['remoteok'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 6. REMOTIVE (All remote jobs - Cambridge residents can work them)
    # =========================================================
    print(f"\n{'='*70}")
    print("6. REMOTIVE - Remote Jobs (all included)")
    print("=" * 70)

    remotive_jobs = remotive.search()
    ins, upd = save_jobs_to_db(db, remotive_jobs, "remotive")
    results['remotive'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 7. ARBEITNOW (Remote tech jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("7. ARBEITNOW - Remote Tech Jobs (all included)")
    print("=" * 70)

    arbeitnow = ArbeitnowClient()
    arbeitnow_jobs = arbeitnow.search()
    ins, upd = save_jobs_to_db(db, arbeitnow_jobs, "arbeitnow")
    results['arbeitnow'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 8. JOBICY (Remote jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("8. JOBICY - Remote Jobs (all included)")
    print("=" * 70)

    jobicy = JobicyClient()
    jobicy_jobs = jobicy.search()
    ins, upd = save_jobs_to_db(db, jobicy_jobs, "jobicy")
    results['jobicy'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 9. WE WORK REMOTELY (Remote jobs via RSS)
    # =========================================================
    print(f"\n{'='*70}")
    print("9. WE WORK REMOTELY - Remote Jobs (all included)")
    print("=" * 70)

    wwr = WeWorkRemotelyClient()
    wwr_jobs = wwr.search()
    ins, upd = save_jobs_to_db(db, wwr_jobs, "weworkremotely")
    results['weworkremotely'] = {'inserted': ins, 'updated': upd}
    print(f"  Saved: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # EXTRACT SKILLS FOR ALL JOBS
    # =========================================================
    print(f"\n{'='*70}")
    print("EXTRACTING SKILLS FROM JOB DESCRIPTIONS")
    print("=" * 70)

    jobs_with_skills = extract_skills_for_all_jobs(config.db_path)

    # =========================================================
    # EXPORT AND SUMMARY
    # =========================================================
    db.export_csv(config.csv_path)

    stats = db.get_stats()

    total_inserted = sum(r['inserted'] for r in results.values())
    total_updated = sum(r['updated'] for r in results.values())

    print(f"\n{'='*70}")
    print("COLLECTION COMPLETE")
    print("=" * 70)

    print(f"\nThis run by source:")
    for source, counts in results.items():
        print(f"  {source}: +{counts['inserted']:,} new, {counts['updated']:,} updated")

    print(f"\nTotal this run:")
    print(f"  New jobs inserted: {total_inserted:,}")
    print(f"  Existing jobs updated: {total_updated:,}")

    print(f"\nDatabase totals:")
    print(f"  Total jobs: {stats['total']:,}")
    print(f"  Active jobs: {stats['active']:,}")
    print(f"  Remote jobs: {stats['remote']:,}")
    print(f"  Unique employers: {stats['employers']:,}")

    print(f"\nJobs by source:")
    for source, count in sorted(stats['by_source'].items(), key=lambda x: -x[1]):
        print(f"  {source}: {count:,}")

    print(f"\nTop cities:")
    for city, count in stats['top_cities'].items():
        if city:
            print(f"  {city}: {count:,}")

    print(f"\nOutput files:")
    print(f"  Database: {config.db_path}")
    print(f"  CSV: {config.csv_path}")

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return stats


if __name__ == "__main__":
    collect_cambridge()
