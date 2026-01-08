#!/usr/bin/env python3
"""
job_scraper.py

This script builds a SQLite database of businesses and job postings for a set of ZIP codes.

It uses two web APIs:

1. **Yelp Fusion API** – to find businesses in a given ZIP code.  The search endpoint
   accepts parameters like `location` (a city, address or postal code) and `radius`
   (in meters).  The API returns up to 240 businesses with basic information,
   including the business name, unique Yelp ID, address, and category data【745187283821994†L289-L339】.

2. **Adzuna Jobs API** – to look up job advertisements.  The search endpoint
   `/v1/api/jobs/<country>/search/<page>` accepts `what` (search terms) and
   `where` (location) parameters and returns job adverts with fields such as
   salary range, job title, location, description and company display name【913526798484586†L19-L64】.
   A more complex query can include additional filters such as salary and job type【913526798484586†L75-L129】.

Because many job boards block simple scraping attempts, this script relies on
official APIs rather than scraping HTML pages, which often results in 403
responses and anti‑scraping challenges【927601102983610†L359-L466】.

Before running this script you need valid credentials for both APIs.  You can
obtain a Yelp API key by creating a developer account at
https://www.yelp.com/developers and generating an API key.  For Adzuna you must
register at https://developer.adzuna.com/ to get an `app_id` and `app_key`.

Usage:

    python3 job_scraper.py --zip 02139 02138 02140 --radius 40000 \
        --yelp_api_key YOUR_YELP_KEY --adzuna_app_id YOUR_ADZUNA_ID \
        --adzuna_app_key YOUR_ADZUNA_KEY --db jobs.db

You may also store credentials in environment variables `YELP_API_KEY`,
`ADZUNA_APP_ID` and `ADZUNA_APP_KEY` instead of passing them via the command
line.

The resulting SQLite database will contain two tables:

* **businesses** – metadata about each business discovered via Yelp.
* **jobs** – job ads returned by Adzuna, linked to their originating business.

Author: OpenAI ChatGPT
"""

import argparse
import os
import sqlite3
import sys
import time
from typing import Dict, Generator, Iterable, List, Optional, Tuple

import requests

###############################################################################
# Configuration and helpers
###############################################################################

DEFAULT_YELP_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
DEFAULT_ADZUNA_ENDPOINT = "https://api.adzuna.com/v1/api/jobs/us/search/"


def get_env_or_default(name: str, default: Optional[str]) -> Optional[str]:
    """Return an environment variable if set, else the provided default."""
    return os.environ.get(name, default)


###############################################################################
# Database functions
###############################################################################

def create_tables(conn: sqlite3.Connection) -> None:
    """Create businesses and jobs tables if they do not already exist."""
    cur = conn.cursor()
    # Businesses table holds me
    # tadata about each business discovered via Yelp.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS businesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            yelp_id TEXT UNIQUE,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            phone TEXT,
            categories TEXT,
            rating REAL,
            review_count INTEGER
        )
        """
    )
    # Jobs table holds job adverts returned from Adzuna, linked to the business
    # through the Yelp ID and business name.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            business_name TEXT,
            yelp_id TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            description TEXT,
            salary_min REAL,
            salary_max REAL,
            created DATE,
            url TEXT,
            UNIQUE(job_id, yelp_id)
        )
        """
    )
    conn.commit()


def insert_business(conn: sqlite3.Connection, business: Dict) -> None:
    """Insert a single business record into the businesses table."""
    conn.execute(
        """
        INSERT OR IGNORE INTO businesses (
            yelp_id, name, address, city, state, zip_code, phone,
            categories, rating, review_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            business.get("id"),
            business.get("name"),
            business.get("address"),
            business.get("city"),
            business.get("state"),
            business.get("zip_code"),
            business.get("phone"),
            business.get("categories"),
            business.get("rating"),
            business.get("review_count"),
        ),
    )
    conn.commit()


def insert_job(conn: sqlite3.Connection, job: Dict) -> None:
    """Insert a single job record into the jobs table."""
    conn.execute(
        """
        INSERT OR IGNORE INTO jobs (
            job_id, business_name, yelp_id, title, company, location, description,
            salary_min, salary_max, created, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job.get("id"),
            job.get("business_name"),
            job.get("yelp_id"),
            job.get("title"),
            job.get("company"),
            job.get("location"),
            job.get("description"),
            job.get("salary_min"),
            job.get("salary_max"),
            job.get("created"),
            job.get("url"),
        ),
    )
    conn.commit()


###############################################################################
# Yelp API search
###############################################################################

def search_yelp_businesses(
    api_key: str,
    zip_code: str,
    radius: int = 40000,
    limit: int = 50,
    max_pages: int = 5,
) -> Generator[Dict, None, None]:
    """Yield businesses from Yelp for a given zip code.

    Args:
        api_key: Yelp API key.
        zip_code: Postal code to search.
        radius: Search radius in meters (max 40000).  A large radius will
            encompass the entire zip code.  Note that the API may return
            businesses just outside the specified radius because the radius is
            treated as a suggestion【745187283821994†L330-L339】.
        limit: Number of businesses per request (max 50).
        max_pages: Maximum number of pages to retrieve (each page yields up to
            `limit` businesses).  The Yelp API returns up to 240 results.

    Yields:
        Dictionaries containing business metadata.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    offset = 0
    page = 0
    while page < max_pages:
        params = {
            "location": zip_code,
            "radius": radius,
            "limit": limit,
            "offset": offset,
            # Sort by best match (default).  Alternatively, sort by rating or review_count.
        }
        response = requests.get(DEFAULT_YELP_ENDPOINT, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Yelp API request failed for zip {zip_code}, page {page}: {response.status_code}", file=sys.stderr)
            break
        data = response.json()
        businesses = data.get("businesses", [])
        if not businesses:
            break
        for b in businesses:
            # Flatten address fields
            location = b.get("location", {}) or {}
            categories = ", ".join([c.get("title", "") for c in b.get("categories", [])])
            yield {
                "id": b.get("id"),
                "name": b.get("name"),
                "address": location.get("address1", ""),
                "city": location.get("city", ""),
                "state": location.get("state", ""),
                "zip_code": location.get("zip_code", zip_code),
                "phone": b.get("phone"),
                "categories": categories,
                "rating": b.get("rating"),
                "review_count": b.get("review_count"),
            }
        # Pagination
        offset += limit
        page += 1
        total = data.get("total", 0)
        # Stop if we've retrieved all available results
        if offset >= total:
            break
        # Respect API rate limits
        time.sleep(0.5)


###############################################################################
# Adzuna API job search
###############################################################################

def search_adzuna_jobs(
    app_id: str,
    app_key: str,
    business_name: str,
    zip_code: str,
    results_per_page: int = 50,
    max_pages: int = 5,
) -> Generator[Dict, None, None]:
    """Yield job adverts from Adzuna for a given business and zip code.

    Args:
        app_id: Adzuna application ID.
        app_key: Adzuna application key.
        business_name: Name of the business to search for.
        zip_code: Postal code where the job is located.
        results_per_page: Number of results per page (max 50 recommended).
        max_pages: Maximum number of pages to retrieve.

    Yields:
        Dictionaries containing job advert data.
    """
    base_url = DEFAULT_ADZUNA_ENDPOINT
    page = 1
    while page <= max_pages:
        url = f"{base_url}{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "what": business_name,
            "where": zip_code,
            "results_per_page": results_per_page,
            "content-type": "application/json",
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(
                f"Adzuna API request failed for business '{business_name}' in {zip_code}, page {page}: {response.status_code}",
                file=sys.stderr,
            )
            break
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            yield {
                "id": r.get("id"),
                "title": r.get("title"),
                "company": (r.get("company") or {}).get("display_name"),
                "location": (r.get("location") or {}).get("display_name"),
                "description": r.get("description"),
                "salary_min": r.get("salary_min"),
                "salary_max": r.get("salary_max"),
                "created": r.get("created"),
                "url": r.get("redirect_url"),
            }
        # Pagination: check if there are more results
        if page * results_per_page >= data.get("count", 0):
            break
        page += 1
        time.sleep(0.5)


###############################################################################
# Main workflow
###############################################################################

def process_zip_code(
    conn: sqlite3.Connection,
    yelp_api_key: str,
    adzuna_app_id: str,
    adzuna_app_key: str,
    zip_code: str,
    radius: int,
    yelp_pages: int,
    adzuna_pages: int,
) -> None:
    """Retrieve businesses in a zip code and their jobs, inserting into DB."""
    for biz in search_yelp_businesses(yelp_api_key, zip_code, radius=radius, max_pages=yelp_pages):
        # Insert business data
        insert_business(conn, biz)
        # For each business, search jobs on Adzuna
        for job in search_adzuna_jobs(
            adzuna_app_id,
            adzuna_app_key,
            biz["name"],
            zip_code,
            max_pages=adzuna_pages,
        ):
            job_record = job.copy()
            job_record["business_name"] = biz["name"]
            job_record["yelp_id"] = biz["id"]
            insert_job(conn, job_record)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command‑line arguments."""
    parser = argparse.ArgumentParser(description="Build a database of jobs for businesses in given ZIP codes.")
    parser.add_argument(
        "--zip",
        nargs="+",
        required=True,
        help="One or more ZIP codes to process (space‑separated).",
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=40000,
        help="Search radius in meters for Yelp API (max 40000).",
    )
    parser.add_argument(
        "--yelp_pages",
        type=int,
        default=5,
        help="Maximum number of pages of Yelp results to fetch per ZIP code (each page returns up to 50 businesses).",
    )
    parser.add_argument(
        "--adzuna_pages",
        type=int,
        default=5,
        help="Maximum number of pages of Adzuna results to fetch per business.  Each page returns up to 50 jobs.",
    )
    parser.add_argument(
        "--db",
        default="jobs.db",
        help="SQLite database file name.",
    )
    parser.add_argument("--yelp_api_key", help="Yelp API key.  If omitted, uses environment variable YELP_API_KEY.")
    parser.add_argument("--adzuna_app_id", help="Adzuna app ID.  If omitted, uses environment variable ADZUNA_APP_ID.")
    parser.add_argument("--adzuna_app_key", help="Adzuna app key.  If omitted, uses environment variable ADZUNA_APP_KEY.")
    return parser.parse_args(args)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    # Use provided credentials or fallback to environment variables
    yelp_api_key = args.yelp_api_key or get_env_or_default("YELP_API_KEY", None)
    adzuna_app_id = args.adzuna_app_id or get_env_or_default("ADZUNA_APP_ID", None)
    adzuna_app_key = args.adzuna_app_key or get_env_or_default("ADZUNA_APP_KEY", None)
    if not (yelp_api_key and adzuna_app_id and adzuna_app_key):
        print(
            "Error: Missing API credentials. Please provide Yelp API key and Adzuna app ID/key via command line or environment variables.",
            file=sys.stderr,
        )
        sys.exit(1)
    # Connect to SQLite database
    conn = sqlite3.connect(args.db)
    create_tables(conn)
    # Process each ZIP code
    for zip_code in args.zip:
        print(f"Processing ZIP code {zip_code}...")
        process_zip_code(
            conn,
            yelp_api_key,
            adzuna_app_id,
            adzuna_app_key,
            zip_code,
            radius=args.radius,
            yelp_pages=args.yelp_pages,
            adzuna_pages=args.adzuna_pages,
        )
        print(f"Finished ZIP code {zip_code}.")
    conn.close()
    print(f"Data collection complete. Results saved to {args.db}.")


if __name__ == "__main__":
    main()
