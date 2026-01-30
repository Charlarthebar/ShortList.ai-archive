#!/usr/bin/env python3
"""
Efficient Job Scraper for Active Jobs DB API

This script is optimized to minimize API requests while maximizing job retrieval.
Instead of running hundreds of keyword queries, it does a single location-based
search and paginates through ALL results.

Usage:
    python3 scrape_jobs_api.py --state "North Carolina" --limit 5000
    python3 scrape_jobs_api.py --state "NC" --limit 1000 --endpoint 7d
    python3 scrape_jobs_api.py --help
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import requests

# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ActiveJobsAPI:
    """Efficient client for Active Jobs DB API."""

    BASE_URL = "https://active-jobs-db.p.rapidapi.com"

    # Endpoints (in order of data volume)
    ENDPOINTS = {
        "6m": "/active-ats-6m",    # 6 months backfill (Ultra/Mega only) - MOST DATA
        "7d": "/active-ats-7d",    # Last 7 days
        "24h": "/active-ats-24h",  # Last 24 hours
        "1h": "/active-ats-1h",    # Last hour (Ultra/Mega only)
    }

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("RAPIDAPI_KEY", "")
        if not self.api_key:
            raise ValueError(
                "RapidAPI key required. Set RAPIDAPI_KEY in .env or pass to constructor.\n"
                "Get your key at: https://rapidapi.com/fantastic-jobs-fantastic-jobs-default/api/active-jobs-db"
            )

        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "active-jobs-db.p.rapidapi.com",
            "Accept": "application/json",
        })

        # Stats
        self.requests_made = 0
        self.jobs_retrieved = 0

    def search(
        self,
        location: str = None,
        title_filter: str = None,
        limit: int = 10000,
        endpoint: str = "6m",
        rate_limit: float = 0.2
    ) -> list[dict]:
        """
        Search for jobs with minimal API usage.

        Args:
            location: State, city, or country (e.g., "North Carolina", "Charlotte, NC")
            title_filter: Optional title filter with Boolean support ("engineer OR developer")
            limit: Maximum jobs to retrieve
            endpoint: "6m" (6 months), "7d" (7 days), "24h", or "1h"
            rate_limit: Seconds between requests (0.2 = 5 req/sec)

        Returns:
            List of job dictionaries
        """
        endpoint_path = self.ENDPOINTS.get(endpoint, self.ENDPOINTS["7d"])
        jobs = []
        offset = 0
        page_size = 100  # API maximum

        logger.info(f"Starting search: location='{location}', endpoint={endpoint}, limit={limit}")

        while len(jobs) < limit:
            params = {
                "limit": page_size,
                "offset": offset,
            }
            if location:
                params["location_filter"] = location
            if title_filter:
                params["title_filter"] = title_filter

            url = f"{self.BASE_URL}{endpoint_path}?{urlencode(params)}"

            try:
                response = self.session.get(url, timeout=30)
                self.requests_made += 1

                if response.status_code == 403:
                    if endpoint_path == self.ENDPOINTS["6m"]:
                        logger.warning("6-month backfill requires Ultra/Mega plan. Falling back to 7-day.")
                        endpoint_path = self.ENDPOINTS["7d"]
                        continue
                    else:
                        logger.error("API access forbidden. Check your subscription.")
                        break

                if response.status_code == 429:
                    logger.warning("Rate limited. Waiting 5 seconds...")
                    time.sleep(5)
                    continue

                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code}: {response.text[:200]}")
                    break

                data = response.json()
                items = data if isinstance(data, list) else data.get("jobs", data.get("data", []))

                if not items:
                    logger.info(f"No more jobs found. Total: {len(jobs)}")
                    break

                jobs.extend(items)
                self.jobs_retrieved = len(jobs)

                logger.info(f"Page {self.requests_made}: +{len(items)} jobs (total: {len(jobs)}, requests: {self.requests_made})")

                # Stop if we got fewer than a full page (no more results)
                if len(items) < page_size:
                    break

                offset += page_size
                time.sleep(rate_limit)

            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                break

        logger.info(f"Search complete: {len(jobs)} jobs in {self.requests_made} requests")
        return jobs[:limit]  # Trim to exact limit

    def get_stats(self) -> dict:
        """Get usage statistics."""
        return {
            "requests_made": self.requests_made,
            "jobs_retrieved": self.jobs_retrieved,
            "efficiency": self.jobs_retrieved / max(self.requests_made, 1)
        }


def parse_job(item: dict) -> dict:
    """Parse API response into clean job record."""
    # Location parsing - API returns locations_derived as string array
    location = "Unknown"
    locations_derived = item.get("locations_derived", [])
    if locations_derived and isinstance(locations_derived, list):
        location = locations_derived[0]  # e.g., "Concord, North Carolina, United States"
    else:
        # Fallback to cities_derived + regions_derived
        city = (item.get("cities_derived", []) or [""])[0]
        region = (item.get("regions_derived", []) or [""])[0]
        if city and region:
            location = f"{city}, {region}"
        elif city or region:
            location = city or region

    # Salary parsing - API uses salary_raw with nested structure
    salary_min, salary_max, salary_type = None, None, None
    salary_raw = item.get("salary_raw")
    if salary_raw and isinstance(salary_raw, dict):
        value = salary_raw.get("value", {})
        if isinstance(value, dict):
            salary_min = value.get("minValue")
            salary_max = value.get("maxValue")
            unit = value.get("unitText", "").upper()
            if unit == "HOUR":
                salary_type = "hourly"
            elif unit == "YEAR":
                salary_type = "yearly"
            elif unit == "MONTH":
                salary_type = "monthly"

    # Remote detection - API uses remote_derived
    remote = item.get("remote_derived", False)
    if not remote and "remote" in location.lower():
        remote = True

    # Employment type (can be string or list)
    emp_type = item.get("employment_type", "")
    if isinstance(emp_type, list):
        emp_type = emp_type[0] if emp_type else ""
    job_type = None
    if emp_type and isinstance(emp_type, str):
        emp_lower = emp_type.lower()
        if "full" in emp_lower:
            job_type = "full-time"
        elif "part" in emp_lower:
            job_type = "part-time"
        elif "contract" in emp_lower:
            job_type = "contract"
        elif "intern" in emp_lower:
            job_type = "internship"

    # Source info
    source = item.get("source", "")
    source_type = item.get("source_type", "")

    return {
        "title": item.get("title", ""),
        "company": item.get("organization", "Unknown"),
        "location": location,
        "city": (item.get("cities_derived", []) or [""])[0],
        "state": (item.get("regions_derived", []) or [""])[0],
        "county": (item.get("counties_derived", []) or [""])[0],
        "url": item.get("url", ""),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_type": salary_type,
        "job_type": job_type,
        "remote": remote,
        "posted_date": item.get("date_posted", ""),
        "source": source,
        "source_type": source_type,
    }


def save_results(jobs: list[dict], output_dir: Path, prefix: str):
    """Save jobs to JSON and CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Parse all jobs
    parsed_jobs = [parse_job(j) for j in jobs]
    parsed_jobs = [j for j in parsed_jobs if j["title"] and j["url"]]  # Filter invalid

    # Save JSON
    json_file = output_dir / f"{prefix}_jobs_{timestamp}.json"
    with open(json_file, 'w') as f:
        json.dump({
            "scraped_at": datetime.now().isoformat(),
            "total_jobs": len(parsed_jobs),
            "jobs": parsed_jobs
        }, f, indent=2)
    logger.info(f"Saved JSON: {json_file}")

    # Save CSV
    csv_file = output_dir / f"{prefix}_jobs_{timestamp}.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        if parsed_jobs:
            writer = csv.DictWriter(f, fieldnames=parsed_jobs[0].keys())
            writer.writeheader()
            writer.writerows(parsed_jobs)
    logger.info(f"Saved CSV: {csv_file}")

    return json_file, csv_file, len(parsed_jobs)


def main():
    parser = argparse.ArgumentParser(
        description="Efficient job scraper using Active Jobs DB API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --state "North Carolina" --limit 5000
  %(prog)s --state "NC" --endpoint 7d --limit 1000
  %(prog)s --state "California" --title "software engineer" --limit 2000

Endpoints:
  6m  - 6 months backfill (most data, requires Ultra/Mega plan)
  7d  - Last 7 days (default fallback)
  24h - Last 24 hours
  1h  - Last hour (requires Ultra/Mega plan)

API Credits:
  Each request returns up to 100 jobs and costs 1 request + jobs returned.
  Example: 5000 jobs = ~50 requests + 5000 job credits
        """
    )
    parser.add_argument(
        "--state", "-s",
        required=True,
        help="State to search (e.g., 'North Carolina' or 'NC')"
    )
    parser.add_argument(
        "--title", "-t",
        help="Optional title filter (supports Boolean: 'engineer OR developer')"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=5000,
        help="Maximum jobs to retrieve (default: 5000)"
    )
    parser.add_argument(
        "--endpoint", "-e",
        choices=["6m", "7d", "24h", "1h"],
        default="6m",
        help="API endpoint: 6m (6 months), 7d (7 days), 24h, 1h (default: 6m)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path(__file__).parent / "output",
        help="Output directory"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making API calls"
    )

    args = parser.parse_args()

    # Dry run info
    if args.dry_run:
        estimated_requests = (args.limit // 100) + 1
        print(f"\n=== DRY RUN ===")
        print(f"State: {args.state}")
        print(f"Title filter: {args.title or '(none)'}")
        print(f"Limit: {args.limit} jobs")
        print(f"Endpoint: {args.endpoint}")
        print(f"Estimated API requests: ~{estimated_requests}")
        print(f"Estimated job credits used: ~{args.limit}")
        print(f"\nRemove --dry-run to execute")
        return 0

    # Run search
    try:
        api = ActiveJobsAPI()
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    print(f"\n{'='*50}")
    print(f"Active Jobs DB Search")
    print(f"{'='*50}")
    print(f"State: {args.state}")
    print(f"Title filter: {args.title or '(none)'}")
    print(f"Limit: {args.limit}")
    print(f"Endpoint: {args.endpoint}")
    print(f"{'='*50}\n")

    start_time = time.time()
    jobs = api.search(
        location=args.state,
        title_filter=args.title,
        limit=args.limit,
        endpoint=args.endpoint
    )
    duration = time.time() - start_time

    if not jobs:
        print("No jobs found.")
        return 1

    # Save results
    prefix = args.state.lower().replace(" ", "_")
    json_file, csv_file, valid_jobs = save_results(jobs, args.output, prefix)

    # Print summary
    stats = api.get_stats()
    print(f"\n{'='*50}")
    print(f"RESULTS")
    print(f"{'='*50}")
    print(f"Jobs retrieved: {valid_jobs:,}")
    print(f"API requests made: {stats['requests_made']}")
    print(f"Efficiency: {stats['efficiency']:.1f} jobs/request")
    print(f"Duration: {duration:.1f}s")
    print(f"Output: {json_file}")
    print(f"{'='*50}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
