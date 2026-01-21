#!/usr/bin/env python3
"""
Nationwide Open Jobs Collector
==============================
Collects open job listings from across the entire United States.

Sources:
- Adzuna API (aggregates jobs from company websites)
- USAJOBS API (official US government job portal)

Usage:
    python nationwide_jobs_collector.py

Schedule:
    Set up with: python schedule_scraping.py --setup-launchd
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
import requests
import csv

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
    """Collection configuration."""
    db_path: str = "nationwide_jobs.db"
    # Adzuna settings
    adzuna_max_per_category: int = 0  # 0 = fetch all (up to 50K per category)
    adzuna_delay: float = 1.0  # Slower to avoid rate limits
    # USAJOBS settings
    usajobs_max: int = 0  # 0 = fetch all
    usajobs_delay: float = 0.5
    # The Muse settings
    themuse_max: int = 0  # 0 = fetch all (~480K available)
    themuse_delay: float = 0.2
    # Coresignal settings (trial may have limited calls)
    coresignal_max: int = 10000  # Start conservative for trial
    coresignal_delay: float = 0.3
    # RemoteOK/Remotive (small, fetch all)
    remote_delay: float = 1.0
    # Progress save interval
    progress_save_interval: int = 1000

config = Config()

# Adzuna job categories
ADZUNA_CATEGORIES = [
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

# =========================================================
# API CLIENTS
# =========================================================

class AdzunaClient:
    """Client for Adzuna job search API."""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search"

    def __init__(self):
        self.app_id = os.environ.get('ADZUNA_APP_ID')
        self.app_key = os.environ.get('ADZUNA_APP_KEY')

    def is_configured(self) -> bool:
        return bool(self.app_id and self.app_key)

    def search_nationwide(self, max_results: int = 0, delay: float = 0.25,
                          on_batch: callable = None, start_page: int = 1) -> List[Dict]:
        """
        Search for jobs nationwide (all US jobs, no location filter).

        Args:
            max_results: Maximum jobs to fetch (0 = unlimited/all available)
            delay: Seconds between API requests
            on_batch: Callback function(jobs_batch, total_collected) called after each page
            start_page: Page to start from (for resuming interrupted collection)
        """
        if not self.is_configured():
            return []

        jobs = []
        page = start_page
        results_per_page = 50
        total_available = None
        consecutive_errors = 0
        max_consecutive_errors = 5

        limit_str = f"{max_results:,}" if max_results > 0 else "ALL"
        print(f"  Fetching {limit_str} jobs from Adzuna (starting page {page})...")

        while True:
            # Check if we've reached the limit
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "results_per_page": results_per_page,
                    "sort_by": "date",
                    "max_days_old": 30,  # Only jobs from last 30 days
                }

                response = requests.get(f"{self.BASE_URL}/{page}", params=params, timeout=30)

                if response.status_code == 429:  # Rate limited
                    print(f"    Rate limited on page {page}, waiting 30 seconds...")
                    time.sleep(30)
                    continue

                if response.status_code != 200:
                    consecutive_errors += 1
                    print(f"    Error {response.status_code} on page {page} (attempt {consecutive_errors})")
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"    Too many errors, stopping at page {page}")
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0  # Reset on success
                data = response.json()

                # Get total available on first request
                if total_available is None:
                    total_available = data.get("count", 0)
                    print(f"    Total available: {total_available:,} jobs")

                results = data.get("results", [])
                if not results:
                    print(f"    No more results at page {page}")
                    break

                batch = []
                for job in results:
                    # Extract state from location area array
                    location_area = job.get("location", {}).get("area", [])
                    state = ""
                    city = ""
                    if location_area:
                        if len(location_area) >= 1:
                            city = location_area[0]
                        if len(location_area) >= 3:
                            state = location_area[2]

                    batch.append({
                        "title": job.get("title", ""),
                        "employer": job.get("company", {}).get("display_name", "Unknown"),
                        "location": job.get("location", {}).get("display_name", ""),
                        "city": city,
                        "state": state,
                        "salary_min": job.get("salary_min"),
                        "salary_max": job.get("salary_max"),
                        "description": job.get("description", "")[:1000],
                        "url": job.get("redirect_url", ""),
                        "posted_date": job.get("created", ""),
                        "source": "adzuna",
                        "source_id": job.get("id", ""),
                    })

                jobs.extend(batch)

                # Call batch callback if provided (for incremental DB saves)
                if on_batch:
                    on_batch(batch, len(jobs))

                # Progress reporting
                if page % 100 == 0:
                    pct = (len(jobs) / total_available * 100) if total_available else 0
                    print(f"    Page {page:,}: {len(jobs):,} jobs ({pct:.1f}%)")

                page += 1
                time.sleep(delay)

                # Safety check - Adzuna limits to 1000 pages
                if page > 1000:
                    print(f"    Reached Adzuna's 1000 page limit")
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"    Error on page {page}: {e}")
                if consecutive_errors >= max_consecutive_errors:
                    print(f"    Too many consecutive errors, stopping")
                    break
                time.sleep(5)

        final_count = len(jobs) if max_results == 0 else min(len(jobs), max_results)
        print(f"  Collected {final_count:,} jobs from Adzuna")
        return jobs[:max_results] if max_results > 0 else jobs

    def search_by_category(self, category: str, max_results: int = 0, delay: float = 0.25) -> List[Dict]:
        """
        Search for jobs in a specific category.

        Args:
            category: Adzuna category tag (e.g., 'it-jobs', 'healthcare-nursing-jobs')
            max_results: Maximum jobs to fetch (0 = unlimited, up to 50K API limit)
            delay: Seconds between requests
        """
        if not self.is_configured():
            return []

        jobs = []
        page = 1
        results_per_page = 50
        total_available = None
        consecutive_errors = 0

        while True:
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "results_per_page": results_per_page,
                    "category": category,
                    "sort_by": "date",
                    "max_days_old": 30,
                }

                response = requests.get(f"{self.BASE_URL}/{page}", params=params, timeout=30)

                if response.status_code == 429:
                    consecutive_errors += 1
                    if consecutive_errors >= 10:
                        print(f"      Too many rate limits, skipping category")
                        break
                    print(f"      Rate limited ({consecutive_errors}/10), waiting 30s...")
                    time.sleep(30)
                    continue

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        print(f"      Too many errors, stopping")
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                data = response.json()

                if total_available is None:
                    total_available = data.get("count", 0)
                    print(f"      Total available: {total_available:,} jobs")

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
                        "salary_min": job.get("salary_min"),
                        "salary_max": job.get("salary_max"),
                        "description": job.get("description", "")[:1000],
                        "url": job.get("redirect_url", ""),
                        "posted_date": job.get("created", ""),
                        "source": "adzuna",
                        "source_id": job.get("id", ""),
                        "category": category,
                    })

                # Progress every 100 pages
                if page % 100 == 0:
                    pct = (len(jobs) / total_available * 100) if total_available else 0
                    print(f"      Page {page}: {len(jobs):,} jobs ({pct:.1f}%)")

                page += 1
                time.sleep(delay)

                if page > 1000:  # Adzuna limit
                    print(f"      Reached 1000 page limit")
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"      Error: {e}")
                if consecutive_errors >= 5:
                    break
                time.sleep(5)

        return jobs[:max_results] if max_results > 0 else jobs

    def search(self, state: str, max_results: int = 100) -> List[Dict]:
        """Search for jobs in a state (deprecated - use search_nationwide instead)."""
        return []


class TheMuseClient:
    """Client for The Muse job API (free, ~480K jobs)."""

    BASE_URL = "https://www.themuse.com/api/public/jobs"

    def search(self, max_results: int = 0, delay: float = 0.2) -> List[Dict]:
        """Fetch jobs from The Muse."""
        jobs = []
        page = 0
        total_pages = None
        consecutive_errors = 0

        limit_str = f"{max_results:,}" if max_results > 0 else "ALL"
        print(f"  Fetching {limit_str} jobs from The Muse...")

        while True:
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {"page": page}
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
                    print(f"    Total available: {total_jobs:,} jobs across {total_pages:,} pages")

                results = data.get("results", [])
                if not results:
                    break

                for job in results:
                    locations = job.get("locations", [])
                    location_name = locations[0].get("name", "") if locations else ""

                    # Parse city, state from location name (format: "City, State")
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
                        "salary_min": None,
                        "salary_max": None,
                        "description": job.get("contents", "")[:1000] if job.get("contents") else "",
                        "url": job.get("refs", {}).get("landing_page", ""),
                        "posted_date": job.get("publication_date", ""),
                        "source": "themuse",
                        "source_id": str(job.get("id", "")),
                        "category": ",".join([c.get("name", "") for c in job.get("categories", [])]),
                    })

                if page % 100 == 0 and page > 0:
                    pct = (page / total_pages * 100) if total_pages else 0
                    print(f"    Page {page:,}/{total_pages:,}: {len(jobs):,} jobs ({pct:.1f}%)")

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

        print(f"  Collected {len(jobs):,} jobs from The Muse")
        return jobs[:max_results] if max_results > 0 else jobs


class RemoteOKClient:
    """Client for RemoteOK API (free, remote jobs only)."""

    BASE_URL = "https://remoteok.com/api"

    def search(self) -> List[Dict]:
        """Fetch all jobs from RemoteOK."""
        print(f"  Fetching jobs from RemoteOK...")

        try:
            headers = {"User-Agent": "ShortList Job Collector/1.0"}
            response = requests.get(self.BASE_URL, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            # First item is metadata, skip it
            results = data[1:] if len(data) > 1 else []

            jobs = []
            for job in results:
                jobs.append({
                    "title": job.get("position", ""),
                    "employer": job.get("company", "Unknown"),
                    "location": job.get("location", "Remote"),
                    "city": "",
                    "state": "",
                    "salary_min": self._parse_salary(job.get("salary_min")),
                    "salary_max": self._parse_salary(job.get("salary_max")),
                    "description": job.get("description", "")[:1000] if job.get("description") else "",
                    "url": job.get("url", ""),
                    "posted_date": job.get("date", ""),
                    "source": "remoteok",
                    "source_id": str(job.get("id", "")),
                    "category": ",".join(job.get("tags", [])) if job.get("tags") else "",
                })

            print(f"  Collected {len(jobs)} jobs from RemoteOK")
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
    """Client for Remotive API (free, remote jobs only)."""

    BASE_URL = "https://remotive.com/api/remote-jobs"

    def search(self) -> List[Dict]:
        """Fetch all jobs from Remotive."""
        print(f"  Fetching jobs from Remotive...")

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
                    "salary_min": None,
                    "salary_max": None,
                    "description": job.get("description", "")[:1000] if job.get("description") else "",
                    "url": job.get("url", ""),
                    "posted_date": job.get("publication_date", ""),
                    "source": "remotive",
                    "source_id": str(job.get("id", "")),
                    "category": job.get("category", ""),
                })

            print(f"  Collected {len(jobs)} jobs from Remotive")
            return jobs

        except Exception as e:
            print(f"  Remotive error: {e}")
            return []


class ArbeitnowClient:
    """Client for Arbeitnow API (free, tech/startup jobs)."""

    BASE_URL = "https://www.arbeitnow.com/api/job-board-api"

    def search(self, max_results: int = 0, delay: float = 1.0) -> List[Dict]:
        """Fetch jobs from Arbeitnow (mostly Europe but some US remote)."""
        print(f"  Fetching jobs from Arbeitnow...")

        jobs = []
        page = 1
        consecutive_errors = 0

        while True:
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {"page": page}
                response = requests.get(self.BASE_URL, params=params, timeout=30)

                if response.status_code == 429:
                    print(f"    Rate limited, waiting 60s...")
                    time.sleep(60)
                    continue

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                data = response.json()
                results = data.get("data", [])

                if not results:
                    break

                for job in results:
                    # Filter for US-related jobs (remote-friendly or US location)
                    location = job.get("location", "").lower()
                    remote = job.get("remote", False)

                    if remote or "us" in location or "united states" in location or "america" in location:
                        jobs.append({
                            "title": job.get("title", ""),
                            "employer": job.get("company_name", "Unknown"),
                            "location": job.get("location", "Remote") if not remote else "Remote",
                            "city": "",
                            "state": "",
                            "salary_min": None,
                            "salary_max": None,
                            "description": job.get("description", "")[:1000] if job.get("description") else "",
                            "url": job.get("url", ""),
                            "posted_date": job.get("created_at", ""),
                            "source": "arbeitnow",
                            "source_id": str(job.get("slug", "")),
                            "category": ",".join(job.get("tags", [])) if job.get("tags") else "",
                        })

                # Check for more pages
                links = data.get("links", {})
                if not links.get("next"):
                    break

                page += 1
                time.sleep(delay)

                if page % 10 == 0:
                    print(f"    Page {page}: {len(jobs)} jobs")

            except Exception as e:
                consecutive_errors += 1
                print(f"    Arbeitnow error: {e}")
                if consecutive_errors >= 3:
                    break
                time.sleep(5)

        print(f"  Collected {len(jobs)} jobs from Arbeitnow")
        return jobs[:max_results] if max_results > 0 else jobs


class HimalayasClient:
    """Client for Himalayas.app API (free, remote jobs)."""

    BASE_URL = "https://himalayas.app/jobs/api"

    def search(self, max_results: int = 0) -> List[Dict]:
        """Fetch remote jobs from Himalayas."""
        print(f"  Fetching jobs from Himalayas...")

        jobs = []
        offset = 0
        limit = 50
        consecutive_errors = 0

        while True:
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {"offset": offset, "limit": limit}
                response = requests.get(self.BASE_URL, params=params, timeout=30)

                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        break
                    time.sleep(5)
                    continue

                consecutive_errors = 0
                data = response.json()
                results = data.get("jobs", [])

                if not results:
                    break

                for job in results:
                    jobs.append({
                        "title": job.get("title", ""),
                        "employer": job.get("companyName", "Unknown"),
                        "location": "Remote",
                        "city": "",
                        "state": "",
                        "salary_min": job.get("minSalary"),
                        "salary_max": job.get("maxSalary"),
                        "description": job.get("description", "")[:1000] if job.get("description") else "",
                        "url": job.get("applicationLink", "") or f"https://himalayas.app/jobs/{job.get('slug', '')}",
                        "posted_date": job.get("pubDate", ""),
                        "source": "himalayas",
                        "source_id": str(job.get("id", "")),
                        "category": job.get("categories", [""])[0] if job.get("categories") else "",
                    })

                offset += limit

                if len(results) < limit:
                    break

                time.sleep(0.5)

            except Exception as e:
                print(f"    Himalayas error: {e}")
                break

        print(f"  Collected {len(jobs)} jobs from Himalayas")
        return jobs[:max_results] if max_results > 0 else jobs


class JobicyClient:
    """Client for Jobicy API (free, remote jobs)."""

    BASE_URL = "https://jobicy.com/api/v2/remote-jobs"

    def search(self, max_results: int = 0) -> List[Dict]:
        """Fetch remote jobs from Jobicy."""
        print(f"  Fetching jobs from Jobicy...")

        jobs = []

        # Jobicy has different industry categories
        industries = ["", "marketing", "design", "dev", "copywriting", "customer-support",
                     "finance", "hr", "data-science", "devops", "product", "management", "sales"]

        for industry in industries:
            try:
                params = {"count": 50, "geo": "usa"}
                if industry:
                    params["industry"] = industry

                response = requests.get(self.BASE_URL, params=params, timeout=30)

                if response.status_code != 200:
                    continue

                data = response.json()
                results = data.get("jobs", [])

                for job in results:
                    jobs.append({
                        "title": job.get("jobTitle", ""),
                        "employer": job.get("companyName", "Unknown"),
                        "location": job.get("jobGeo", "Remote"),
                        "city": "",
                        "state": "",
                        "salary_min": None,
                        "salary_max": None,
                        "description": job.get("jobExcerpt", "")[:1000] if job.get("jobExcerpt") else "",
                        "url": job.get("url", ""),
                        "posted_date": job.get("pubDate", ""),
                        "source": "jobicy",
                        "source_id": str(job.get("id", "")),
                        "category": job.get("jobIndustry", [""])[0] if job.get("jobIndustry") else "",
                    })

                time.sleep(0.5)

                if max_results > 0 and len(jobs) >= max_results:
                    break

            except Exception as e:
                print(f"    Jobicy error for {industry}: {e}")
                continue

        # Dedupe by source_id
        seen = set()
        unique_jobs = []
        for job in jobs:
            if job["source_id"] not in seen:
                seen.add(job["source_id"])
                unique_jobs.append(job)

        print(f"  Collected {len(unique_jobs)} jobs from Jobicy")
        return unique_jobs[:max_results] if max_results > 0 else unique_jobs


class JSearchClient:
    """
    Client for JSearch API via RapidAPI.

    This aggregates jobs from LinkedIn, Indeed, Glassdoor, ZipRecruiter, and more.
    Can access millions of jobs but requires RapidAPI subscription.
    """

    BASE_URL = "https://jsearch.p.rapidapi.com/search"

    def __init__(self):
        self.api_key = os.environ.get('RAPIDAPI_KEY') or os.environ.get('JSEARCH_API_KEY')

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def search(self, max_results: int = 10000, delay: float = 1.0) -> List[Dict]:
        """
        Search for jobs via JSearch (aggregates LinkedIn, Indeed, etc).

        Note: RapidAPI has rate limits based on subscription tier.
        """
        if not self.is_configured():
            return []

        jobs = []
        page = 1
        consecutive_errors = 0

        # Search multiple queries to maximize coverage
        queries = [
            "software engineer USA",
            "data analyst USA",
            "marketing USA",
            "sales USA",
            "healthcare USA",
            "accounting USA",
            "engineering USA",
            "customer service USA",
            "operations USA",
            "human resources USA",
        ]

        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }

        print(f"  Fetching jobs from JSearch (RapidAPI)...")

        for query in queries:
            if max_results > 0 and len(jobs) >= max_results:
                break

            page = 1
            print(f"    Query: {query}")

            while True:
                if max_results > 0 and len(jobs) >= max_results:
                    break

                try:
                    params = {
                        "query": query,
                        "page": str(page),
                        "num_pages": "1",
                        "date_posted": "month"  # Last 30 days
                    }

                    response = requests.get(self.BASE_URL, headers=headers, params=params, timeout=30)

                    if response.status_code == 429:
                        print(f"      Rate limited, waiting 60s...")
                        time.sleep(60)
                        continue

                    if response.status_code == 403:
                        print(f"      API quota exceeded")
                        break

                    if response.status_code != 200:
                        consecutive_errors += 1
                        if consecutive_errors >= 3:
                            break
                        time.sleep(5)
                        continue

                    consecutive_errors = 0
                    data = response.json()
                    results = data.get("data", [])

                    if not results:
                        break

                    for job in results:
                        city = job.get("job_city", "")
                        state = job.get("job_state", "")

                        jobs.append({
                            "title": job.get("job_title", ""),
                            "employer": job.get("employer_name", "Unknown"),
                            "location": f"{city}, {state}" if city and state else city or state or "USA",
                            "city": city,
                            "state": state,
                            "salary_min": job.get("job_min_salary"),
                            "salary_max": job.get("job_max_salary"),
                            "description": job.get("job_description", "")[:1000] if job.get("job_description") else "",
                            "url": job.get("job_apply_link", ""),
                            "posted_date": job.get("job_posted_at_datetime_utc", ""),
                            "source": "jsearch",
                            "source_id": job.get("job_id", ""),
                            "category": job.get("job_employment_type", ""),
                        })

                    page += 1
                    time.sleep(delay)

                    # JSearch limits pages per query
                    if page > 10:
                        break

                except Exception as e:
                    print(f"      JSearch error: {e}")
                    break

        print(f"  Collected {len(jobs)} jobs from JSearch")
        return jobs[:max_results] if max_results > 0 else jobs


class LinkedInJobsClient:
    """
    Client for LinkedIn Jobs (unofficial).

    Uses the public job search which doesn't require auth for basic searches.
    Limited but can get some jobs without API access.
    """

    BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def search(self, max_results: int = 1000, delay: float = 2.0) -> List[Dict]:
        """Fetch public job listings from LinkedIn."""
        print(f"  Fetching jobs from LinkedIn (public)...")

        jobs = []
        start = 0
        consecutive_errors = 0

        # Multiple search terms to maximize coverage
        keywords_list = [
            "software", "engineer", "developer", "analyst", "manager",
            "sales", "marketing", "finance", "healthcare", "data"
        ]

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }

        for keyword in keywords_list:
            if max_results > 0 and len(jobs) >= max_results:
                break

            start = 0
            print(f"    Searching: {keyword}")

            while True:
                if max_results > 0 and len(jobs) >= max_results:
                    break

                try:
                    params = {
                        "keywords": keyword,
                        "location": "United States",
                        "start": start,
                        "f_TPR": "r2592000",  # Last 30 days
                    }

                    response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=30)

                    if response.status_code == 429:
                        print(f"      Rate limited, waiting 60s...")
                        time.sleep(60)
                        continue

                    if response.status_code != 200:
                        consecutive_errors += 1
                        if consecutive_errors >= 5:
                            break
                        time.sleep(10)
                        continue

                    consecutive_errors = 0

                    # Parse HTML response (basic extraction)
                    html = response.text
                    if not html or "No matching jobs found" in html:
                        break

                    # Extract job cards using basic parsing
                    import re

                    # Find all job card sections
                    job_cards = re.findall(r'<div class="base-card[^"]*job-search-card[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>', html, re.DOTALL)

                    if not job_cards:
                        # Alternative pattern
                        job_cards = re.findall(r'data-entity-urn="urn:li:jobPosting:(\d+)"', html)

                    if not job_cards:
                        break

                    # Extract basic info from each card
                    titles = re.findall(r'<span class="sr-only">([^<]+)</span>', html)
                    companies = re.findall(r'<h4 class="base-search-card__subtitle"[^>]*>\s*<a[^>]*>([^<]+)</a>', html)
                    locations = re.findall(r'<span class="job-search-card__location">([^<]+)</span>', html)
                    urls = re.findall(r'<a[^>]*class="base-card__full-link[^"]*"[^>]*href="([^"]+)"', html)

                    # Zip together what we found
                    batch_size = min(len(titles), len(companies), len(locations), len(urls))

                    if batch_size == 0:
                        break

                    for i in range(batch_size):
                        loc = locations[i].strip() if i < len(locations) else ""
                        city, state = "", ""
                        if ", " in loc:
                            parts = loc.rsplit(", ", 1)
                            city = parts[0]
                            state = parts[1]

                        jobs.append({
                            "title": titles[i].strip() if i < len(titles) else "",
                            "employer": companies[i].strip() if i < len(companies) else "Unknown",
                            "location": loc,
                            "city": city,
                            "state": state,
                            "salary_min": None,
                            "salary_max": None,
                            "description": "",
                            "url": urls[i] if i < len(urls) else "",
                            "posted_date": "",
                            "source": "linkedin",
                            "source_id": re.search(r'jobPosting:(\d+)', urls[i]).group(1) if i < len(urls) and re.search(r'jobPosting:(\d+)', urls[i]) else "",
                            "category": keyword,
                        })

                    start += 25
                    time.sleep(delay)

                    # LinkedIn limits pagination
                    if start >= 200:
                        break

                except Exception as e:
                    print(f"      LinkedIn error: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        break
                    time.sleep(10)

        # Dedupe by source_id
        seen = set()
        unique_jobs = []
        for job in jobs:
            if job["source_id"] and job["source_id"] not in seen:
                seen.add(job["source_id"])
                unique_jobs.append(job)

        print(f"  Collected {len(unique_jobs)} jobs from LinkedIn")
        return unique_jobs[:max_results] if max_results > 0 else unique_jobs


class CoresignalClient:
    """
    Client for Coresignal Job Posting API.

    Coresignal aggregates jobs from LinkedIn and other sources.
    Two-step process: search returns IDs, then collect each job.
    """

    SEARCH_URL = "https://api.coresignal.com/cdapi/v2/job_base/search/es_dsl"
    COLLECT_URL = "https://api.coresignal.com/cdapi/v2/job_base/collect"

    def __init__(self):
        self.api_key = os.environ.get('CORESIGNAL_API_KEY') or os.environ.get('INDEED_API_KEY')

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def search(self, max_jobs: int = 10000, delay: float = 0.5) -> List[Dict]:
        """
        Search and collect US jobs from Coresignal.

        Note: Trial accounts may have limited API calls.
        """
        if not self.is_configured():
            return []

        headers = {
            "accept": "application/json",
            "apikey": self.api_key,
            "Content-Type": "application/json"
        }

        all_job_ids = []
        last_id = 0
        consecutive_errors = 0

        print(f"  Fetching job IDs from Coresignal (max {max_jobs:,})...")

        # Step 1: Collect job IDs via search
        while len(all_job_ids) < max_jobs:
            try:
                query = {
                    "query": {
                        "bool": {
                            "must": [{"match": {"country": {"query": "United States", "operator": "and"}}}],
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

        # Limit to max_jobs
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

                    # Parse location
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
                        "salary_min": None,
                        "salary_max": None,
                        "description": (job.get("description") or "")[:1000],
                        "url": job.get("url", ""),
                        "posted_date": job.get("created", ""),
                        "source": "coresignal",
                        "source_id": str(job_id),
                        "category": job.get("employment_type", ""),
                    })

                if (i + 1) % 500 == 0:
                    print(f"    Collected {len(jobs):,} jobs...")

                time.sleep(delay / 2)  # Faster for collect

            except Exception as e:
                if "402" in str(e) or "quota" in str(e).lower():
                    print(f"    API quota exceeded")
                    break
                continue

        print(f"  Collected {len(jobs):,} jobs from Coresignal")
        return jobs


class USAJobsClient:
    """Client for USAJOBS API."""

    BASE_URL = "https://data.usajobs.gov/api/search"

    def __init__(self):
        self.api_key = os.environ.get('USAJOBS_API_KEY')
        self.email = os.environ.get('USAJOBS_EMAIL')

    def is_configured(self) -> bool:
        return bool(self.api_key and self.email)

    def search(self, max_results: int = 0, delay: float = 0.5) -> List[Dict]:
        """
        Search for all federal jobs nationwide.

        Args:
            max_results: Maximum jobs to fetch (0 = unlimited/all available)
            delay: Seconds between API requests
        """
        if not self.is_configured():
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

        limit_str = f"{max_results:,}" if max_results > 0 else "ALL"
        print(f"  Fetching {limit_str} jobs from USAJOBS...")

        while True:
            if max_results > 0 and len(jobs) >= max_results:
                break

            try:
                params = {
                    "Page": page,
                    "ResultsPerPage": 500,  # Max allowed by USAJOBS
                    "DatePosted": 30,  # Last 30 days
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

                # Get total on first request
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
                        "salary_min": float(salary.get("MinimumRange", 0) or 0),
                        "salary_max": float(salary.get("MaximumRange", 0) or 0),
                        "description": job.get("UserArea", {}).get("Details", {}).get("MajorDuties", [""])[0][:1000] if job.get("UserArea") else "",
                        "url": job.get("PositionURI", ""),
                        "posted_date": job.get("PublicationStartDate", ""),
                        "source": "usajobs",
                        "source_id": job.get("PositionID", ""),
                    })

                if page % 10 == 0:
                    print(f"    Page {page}: {len(jobs):,} jobs")

                page += 1
                time.sleep(delay)

                # Check if we've fetched all
                if len(jobs) >= total_available:
                    break

            except Exception as e:
                consecutive_errors += 1
                print(f"    USAJOBS error on page {page}: {e}")
                if consecutive_errors >= 5:
                    break
                time.sleep(5)

        print(f"  Collected {len(jobs):,} jobs from USAJOBS")
        return jobs[:max_results] if max_results > 0 else jobs


# =========================================================
# DATABASE
# =========================================================

class JobDatabase:
    """SQLite database for storing jobs."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Create tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_hash TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                employer TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                salary_min REAL,
                salary_max REAL,
                description TEXT,
                source TEXT NOT NULL,
                source_id TEXT,
                url TEXT,
                posted_date TEXT,
                status TEXT DEFAULT 'active',
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
            CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        """)
        self.conn.commit()

    def _hash_job(self, job: Dict) -> str:
        """Create unique hash for a job."""
        unique_str = f"{job['title']}|{job['employer']}|{job['location']}|{job['source']}"
        return hashlib.md5(unique_str.encode()).hexdigest()

    def upsert_job(self, job: Dict) -> str:
        """Insert or update a job. Returns 'inserted', 'updated', or 'skipped'."""
        job_hash = self._hash_job(job)

        cursor = self.conn.execute(
            "SELECT id, status FROM jobs WHERE job_hash = ?", (job_hash,)
        )
        existing = cursor.fetchone()

        if existing:
            self.conn.execute(
                "UPDATE jobs SET last_seen = CURRENT_TIMESTAMP, status = 'active' WHERE job_hash = ?",
                (job_hash,)
            )
            return "updated"
        else:
            self.conn.execute("""
                INSERT INTO jobs (job_hash, title, employer, location, city, state,
                                  salary_min, salary_max, description, source, source_id,
                                  url, posted_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_hash, job['title'], job['employer'], job['location'],
                job['city'], job['state'], job.get('salary_min'), job.get('salary_max'),
                job.get('description', ''), job['source'], job.get('source_id', ''),
                job.get('url', ''), job.get('posted_date', '')
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

        cursor = self.conn.execute("SELECT COUNT(DISTINCT employer) FROM jobs")
        employers = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT source, COUNT(*) FROM jobs GROUP BY source")
        by_source = dict(cursor.fetchall())

        cursor = self.conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state ORDER BY COUNT(*) DESC LIMIT 10")
        top_states = dict(cursor.fetchall())

        return {
            "total": total,
            "active": active,
            "employers": employers,
            "by_source": by_source,
            "top_states": top_states,
        }

    def export_csv(self, filepath: str):
        """Export jobs to CSV."""
        cursor = self.conn.execute("""
            SELECT title, employer, location, city, state,
                   salary_min, salary_max, source, url, posted_date, status
            FROM jobs
            ORDER BY state, city, employer
        """)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['title', 'employer', 'location', 'city', 'state',
                           'salary_min', 'salary_max', 'source', 'url', 'posted_date', 'status'])
            writer.writerows(cursor.fetchall())

        print(f"Exported to {filepath}")


# =========================================================
# MAIN COLLECTION
# =========================================================

def save_jobs_to_db(db, jobs: List[Dict], source_name: str) -> tuple:
    """Save jobs to database and return (inserted, updated) counts."""
    inserted = 0
    updated = 0
    for i, job in enumerate(jobs):
        result = db.upsert_job(job)
        if result == "inserted":
            inserted += 1
        elif result == "updated":
            updated += 1
        # Commit every 1000 jobs
        if (i + 1) % 1000 == 0:
            db.commit()
    db.commit()
    return inserted, updated


def collect_nationwide():
    """Main collection function - fetches from ALL available sources."""
    print("=" * 70)
    print("     NATIONWIDE OPEN JOBS COLLECTOR (MULTI-SOURCE)")
    print("=" * 70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nSources: USAJOBS, Adzuna, LinkedIn (public), JSearch, Coresignal,")
    print(f"         The Muse, RemoteOK, Remotive, Arbeitnow, Himalayas, Jobicy")

    # Initialize database and clients
    db = JobDatabase(config.db_path)
    adzuna = AdzunaClient()
    usajobs = USAJobsClient()
    coresignal = CoresignalClient()
    jsearch = JSearchClient()
    themuse = TheMuseClient()
    remoteok = RemoteOKClient()
    remotive = RemotiveClient()
    arbeitnow = ArbeitnowClient()
    himalayas = HimalayasClient()
    jobicy = JobicyClient()
    linkedin = LinkedInJobsClient()

    print(f"\nAPI Status:")
    print(f"  Adzuna: {'Configured' if adzuna.is_configured() else 'NOT CONFIGURED'}")
    print(f"  USAJOBS: {'Configured' if usajobs.is_configured() else 'NOT CONFIGURED'}")
    print(f"  Coresignal: {'Configured' if coresignal.is_configured() else 'NOT CONFIGURED'}")
    print(f"  JSearch (RapidAPI): {'Configured' if jsearch.is_configured() else 'NOT CONFIGURED - add RAPIDAPI_KEY for millions more jobs'}")
    print(f"  LinkedIn: Available (public scraping)")
    print(f"  The Muse: Available (no key needed)")
    print(f"  RemoteOK: Available (no key needed)")
    print(f"  Remotive: Available (no key needed)")
    print(f"  Arbeitnow: Available (no key needed)")
    print(f"  Himalayas: Available (no key needed)")
    print(f"  Jobicy: Available (no key needed)")

    results = {
        'usajobs': {'inserted': 0, 'updated': 0},
        'adzuna': {'inserted': 0, 'updated': 0},
        'linkedin': {'inserted': 0, 'updated': 0},
        'jsearch': {'inserted': 0, 'updated': 0},
        'coresignal': {'inserted': 0, 'updated': 0},
        'themuse': {'inserted': 0, 'updated': 0},
        'remoteok': {'inserted': 0, 'updated': 0},
        'remotive': {'inserted': 0, 'updated': 0},
        'arbeitnow': {'inserted': 0, 'updated': 0},
        'himalayas': {'inserted': 0, 'updated': 0},
        'jobicy': {'inserted': 0, 'updated': 0},
    }

    # =========================================================
    # 1. USAJOBS (Federal jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("1. USAJOBS - Federal Government Jobs")
    print("=" * 70)

    usajobs_jobs = usajobs.search(max_results=config.usajobs_max, delay=config.usajobs_delay)
    ins, upd = save_jobs_to_db(db, usajobs_jobs, "usajobs")
    results['usajobs'] = {'inserted': ins, 'updated': upd}
    print(f"  USAJOBS: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 2. ADZUNA (By Category - up to 50K per category)
    # =========================================================
    print(f"\n{'='*70}")
    print("2. ADZUNA - Jobs by Category (29 categories)")
    print("=" * 70)
    print("  Each category can return up to 50K jobs (API limit)")

    adzuna_total_inserted = 0
    adzuna_total_updated = 0

    for i, category in enumerate(ADZUNA_CATEGORIES):
        print(f"\n  [{i+1}/{len(ADZUNA_CATEGORIES)}] Category: {category}")

        jobs = adzuna.search_by_category(
            category=category,
            max_results=config.adzuna_max_per_category,
            delay=config.adzuna_delay
        )

        if jobs:
            ins, upd = save_jobs_to_db(db, jobs, f"adzuna:{category}")
            adzuna_total_inserted += ins
            adzuna_total_updated += upd
            print(f"      Collected {len(jobs):,} jobs, +{ins:,} new")

    results['adzuna'] = {'inserted': adzuna_total_inserted, 'updated': adzuna_total_updated}
    print(f"\n  Adzuna total: +{adzuna_total_inserted:,} new, {adzuna_total_updated:,} updated")

    # =========================================================
    # 3. CORESIGNAL (LinkedIn jobs + more)
    # =========================================================
    print(f"\n{'='*70}")
    print("3. CORESIGNAL - LinkedIn & Other Sources")
    print("=" * 70)
    print(f"  Note: Trial accounts may have limited API calls")

    if coresignal.is_configured():
        coresignal_jobs = coresignal.search(max_jobs=config.coresignal_max, delay=config.coresignal_delay)
        ins, upd = save_jobs_to_db(db, coresignal_jobs, "coresignal")
        results['coresignal'] = {'inserted': ins, 'updated': upd}
        print(f"  Coresignal: +{ins:,} new, {upd:,} updated")
    else:
        print("  Coresignal: SKIPPED (not configured)")

    # =========================================================
    # 4. THE MUSE (~480K jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("4. THE MUSE - Career Platform Jobs")
    print("=" * 70)

    themuse_jobs = themuse.search(max_results=config.themuse_max, delay=config.themuse_delay)
    ins, upd = save_jobs_to_db(db, themuse_jobs, "themuse")
    results['themuse'] = {'inserted': ins, 'updated': upd}
    print(f"  The Muse: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 5. REMOTEOK (Remote jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("5. REMOTEOK - Remote Jobs")
    print("=" * 70)

    remoteok_jobs = remoteok.search()
    ins, upd = save_jobs_to_db(db, remoteok_jobs, "remoteok")
    results['remoteok'] = {'inserted': ins, 'updated': upd}
    print(f"  RemoteOK: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 6. REMOTIVE (Remote jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("6. REMOTIVE - Remote Jobs")
    print("=" * 70)

    remotive_jobs = remotive.search()
    ins, upd = save_jobs_to_db(db, remotive_jobs, "remotive")
    results['remotive'] = {'inserted': ins, 'updated': upd}
    print(f"  Remotive: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 7. LINKEDIN (Public job listings)
    # =========================================================
    print(f"\n{'='*70}")
    print("7. LINKEDIN - Public Job Listings")
    print("=" * 70)
    print("  Note: Scraping public listings (rate limited)")

    linkedin_jobs = linkedin.search(max_results=2000, delay=2.0)
    ins, upd = save_jobs_to_db(db, linkedin_jobs, "linkedin")
    results['linkedin'] = {'inserted': ins, 'updated': upd}
    print(f"  LinkedIn: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 8. JSEARCH (RapidAPI - LinkedIn, Indeed, Glassdoor aggregator)
    # =========================================================
    print(f"\n{'='*70}")
    print("8. JSEARCH - Job Aggregator (RapidAPI)")
    print("=" * 70)

    if jsearch.is_configured():
        jsearch_jobs = jsearch.search(max_results=50000, delay=1.0)
        ins, upd = save_jobs_to_db(db, jsearch_jobs, "jsearch")
        results['jsearch'] = {'inserted': ins, 'updated': upd}
        print(f"  JSearch: +{ins:,} new, {upd:,} updated")
    else:
        print("  JSearch: SKIPPED (add RAPIDAPI_KEY to .env for millions more jobs)")

    # =========================================================
    # 9. ARBEITNOW (Tech/startup jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("9. ARBEITNOW - Tech & Startup Jobs")
    print("=" * 70)

    arbeitnow_jobs = arbeitnow.search(max_results=0, delay=1.0)
    ins, upd = save_jobs_to_db(db, arbeitnow_jobs, "arbeitnow")
    results['arbeitnow'] = {'inserted': ins, 'updated': upd}
    print(f"  Arbeitnow: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 10. HIMALAYAS (Remote jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("10. HIMALAYAS - Remote Jobs")
    print("=" * 70)

    himalayas_jobs = himalayas.search(max_results=0)
    ins, upd = save_jobs_to_db(db, himalayas_jobs, "himalayas")
    results['himalayas'] = {'inserted': ins, 'updated': upd}
    print(f"  Himalayas: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # 11. JOBICY (Remote jobs)
    # =========================================================
    print(f"\n{'='*70}")
    print("11. JOBICY - Remote Jobs (USA)")
    print("=" * 70)

    jobicy_jobs = jobicy.search(max_results=0)
    ins, upd = save_jobs_to_db(db, jobicy_jobs, "jobicy")
    results['jobicy'] = {'inserted': ins, 'updated': upd}
    print(f"  Jobicy: +{ins:,} new, {upd:,} updated")

    # =========================================================
    # EXPORT AND SUMMARY
    # =========================================================
    csv_path = config.db_path.replace('.db', '.csv')
    db.export_csv(csv_path)

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
    print(f"  Unique employers: {stats['employers']:,}")

    print(f"\nJobs by source:")
    for source, count in sorted(stats['by_source'].items(), key=lambda x: -x[1]):
        print(f"  {source}: {count:,}")

    print(f"\nTop 10 locations:")
    for loc, count in stats['top_states'].items():
        print(f"  {loc}: {count:,}")

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return stats


if __name__ == "__main__":
    collect_nationwide()
