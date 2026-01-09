#!/usr/bin/env python3
"""
job_database_builder.py

This module expands upon the earlier job_scraper by incorporating additional
data sources to build a more complete representation of jobs across the
United States.  It now integrates the USAJOBS API, which publishes job
opportunity announcements for U.S. federal government positions【831584421036712†L82-L89】, while
retaining support for the Yelp + Adzuna pipeline described previously.

Overview of supported data sources:

* **Yelp + Adzuna** – Identifies businesses in a geographic area using the
  Yelp Fusion API and then searches Adzuna for jobs mentioning those
  businesses.  See job_scraper.py for details and citations.

* **USAJOBS** – The official U.S. federal jobs API.  It returns job
  announcements with metadata such as position title, organization, location
  and salary information.  The search API returns results in pages of
  250 job opportunity announcements (JOAs) by default, and this can be
  increased to 500 by using the `ResultsPerPage` parameter【831584421036712†L82-L89】.  Pagination is
  controlled by the `Page` parameter【831584421036712†L82-L89】.  Location-based searches can be
  performed using the `LocationName` parameter, which accepts a city or
  state and automatically matches the appropriate location codes【515286973750120†L194-L200】.

* **Arbeitnow (optional)** – A job board API that aggregates postings from
  applicant tracking systems such as Greenhouse, SmartRecruiters, Join,
  Team Tailor, Recruitee and Comeet.  The free API requires no API key and
  returns jobs in a consistent format, including fields like `remote` and
  a `visa_sponsorship` filter【161774275562007†L68-L94】.  Due to server restrictions, this
  implementation does not pull jobs from Arbeitnow automatically, but the
  API is documented as an additional source the user may integrate manually.

Usage Example:

    python3 job_database_builder.py \
        --zip 02139 02138 \
        --states "Massachusetts" "California" \
        --yelp_api_key <YELP_KEY> \
        --adzuna_app_id <ADZUNA_APP_ID> \
        --adzuna_app_key <ADZUNA_APP_KEY> \
        --usajobs_api_key <USAJOBS_API_KEY> \
        --usajobs_user_agent <YOUR_EMAIL> \
        --db us_jobs.db

This will fetch local businesses via Yelp and jobs via Adzuna for the
specified ZIP codes, then query USAJOBS for federal positions in the
specified states.  Results are written into separate tables within the
SQLite database.
"""

import argparse
import os
import sqlite3
import sys
import time
from typing import Dict, Generator, Iterable, List, Optional

import requests

# Import functions from job_scraper if available; if not, define local stubs.
try:
    from job_scraper import (
        create_tables as create_base_tables,
        insert_business,
        insert_job as insert_adzuna_job,
        search_yelp_businesses,
        search_adzuna_jobs,
    )
except ImportError:
    # Fallback: define minimal stubs to avoid NameError.  These stubs
    # raise exceptions if called, because the full functionality requires
    # job_scraper.py to be in the same directory.
    def create_base_tables(conn: sqlite3.Connection) -> None:
        raise NotImplementedError("job_scraper.py not found; please ensure it is present.")

    def insert_business(conn: sqlite3.Connection, business: Dict) -> None:
        raise NotImplementedError

    def insert_adzuna_job(conn: sqlite3.Connection, job: Dict) -> None:
        raise NotImplementedError

    def search_yelp_businesses(*args, **kwargs):
        raise NotImplementedError

    def search_adzuna_jobs(*args, **kwargs):
        raise NotImplementedError


###############################################################################
# Additional database tables
###############################################################################

def create_extra_tables(conn: sqlite3.Connection) -> None:
    """Create tables specific to USAJOBS and other sources."""
    cur = conn.cursor()
    # Table for USAJOBS job opportunity announcements
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs_usajobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id TEXT UNIQUE,
            position_title TEXT,
            organization_name TEXT,
            department_name TEXT,
            location_name TEXT,
            salary_min REAL,
            salary_max REAL,
            rate_interval_code TEXT,
            description TEXT,
            start_date TEXT,
            end_date TEXT,
            publication_date TEXT,
            close_date TEXT,
            url TEXT
        )
        """
    )
    # Table for Arbeitnow jobs (optional integration)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs_arbeitnow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE,
            title TEXT,
            company_name TEXT,
            location TEXT,
            remote INTEGER,
            visa_sponsorship INTEGER,
            tags TEXT,
            job_types TEXT,
            created_at INTEGER,
            url TEXT,
            description TEXT
        )
        """
    )
    conn.commit()


def insert_usajobs_job(conn: sqlite3.Connection, job: Dict) -> None:
    """Insert a single USAJOBS job record into the jobs_usajobs table."""
    conn.execute(
        """
        INSERT OR IGNORE INTO jobs_usajobs (
            position_id, position_title, organization_name, department_name,
            location_name, salary_min, salary_max, rate_interval_code,
            description, start_date, end_date, publication_date, close_date, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job.get("position_id"),
            job.get("position_title"),
            job.get("organization_name"),
            job.get("department_name"),
            job.get("location_name"),
            job.get("salary_min"),
            job.get("salary_max"),
            job.get("rate_interval_code"),
            job.get("description"),
            job.get("start_date"),
            job.get("end_date"),
            job.get("publication_date"),
            job.get("close_date"),
            job.get("url"),
        ),
    )
    conn.commit()


def insert_arbeitnow_job(conn: sqlite3.Connection, job: Dict) -> None:
    """Insert a single Arbeitnow job record into the jobs_arbeitnow table."""
    conn.execute(
        """
        INSERT OR IGNORE INTO jobs_arbeitnow (
            slug, title, company_name, location, remote, visa_sponsorship,
            tags, job_types, created_at, url, description
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job.get("slug"),
            job.get("title"),
            job.get("company_name"),
            job.get("location"),
            int(job.get("remote", False)),
            int(job.get("visa_sponsorship", False)),
            job.get("tags"),
            job.get("job_types"),
            job.get("created_at"),
            job.get("url"),
            job.get("description"),
        ),
    )
    conn.commit()


###############################################################################
# USAJOBS API integration
###############################################################################

def search_usajobs_jobs(
    api_key: str,
    user_agent: str,
    keyword: str = "",
    location_name: str = "",
    results_per_page: int = 500,
    max_pages: int = 2,
) -> Generator[Dict, None, None]:
    """Yield job opportunity announcements from USAJOBS.

    Args:
        api_key: USAJOBS API key (Authorization-Key).
        user_agent: Contact email or name used for the User-Agent header.
        keyword: Optional search keyword to filter jobs.
        location_name: Optional city or state to limit jobs【515286973750120†L194-L200】.
        results_per_page: Number of results per page (max 500).  Default is 500 as
            allowed by the API【831584421036712†L82-L89】.
        max_pages: Maximum number of pages to retrieve.  Because the total
            number of federal jobs may be large, limiting pages helps control
            runtime.

    Yields:
        Dictionaries with key job metadata.
    """
    base_url = "https://data.usajobs.gov/api/search"
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": user_agent,
        "Authorization-Key": api_key,
    }
    for page in range(1, max_pages + 1):
        params = {
            "Keyword": keyword or None,
            "LocationName": location_name or None,
            "ResultsPerPage": results_per_page,
            "Page": page,
            "Fields": "full",  # Request full fields to include details【515286973750120†L350-L358】
        }
        # Remove None values to avoid API confusion
        params = {k: v for k, v in params.items() if v is not None and v != ""}
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code != 200:
            print(
                f"USAJOBS API error for page {page}, location '{location_name}': {response.status_code}",
                file=sys.stderr,
            )
            break
        data = response.json()
        search_result = (data.get("SearchResult") or {})
        items = search_result.get("SearchResultItems", [])
        if not items:
            break
        for item in items:
            descriptor = item.get("MatchedObjectDescriptor", {})
            # Flatten salary info: there may be multiple ranges; take first
            salary_info = (descriptor.get("PositionRemuneration") or [{}])[0]
            job_record = {
                "position_id": descriptor.get("PositionID"),
                "position_title": descriptor.get("PositionTitle"),
                "organization_name": descriptor.get("OrganizationName"),
                "department_name": descriptor.get("DepartmentName"),
                "location_name": descriptor.get("PositionLocationDisplay"),
                "salary_min": salary_info.get("MinimumRange"),
                "salary_max": salary_info.get("MaximumRange"),
                "rate_interval_code": salary_info.get("RateIntervalCode"),
                "description": descriptor.get("QualificationSummary"),
                "start_date": descriptor.get("PositionStartDate"),
                "end_date": descriptor.get("PositionEndDate"),
                "publication_date": descriptor.get("PublicationStartDate"),
                "close_date": descriptor.get("ApplicationCloseDate"),
                "url": descriptor.get("PositionURI"),
            }
            yield job_record
        # Determine if there are more pages
        user_area = search_result.get("UserArea", {})
        number_of_pages = int(user_area.get("NumberOfPages", 0))
        if page >= number_of_pages:
            break
        # Pause to respect rate limits
        time.sleep(0.5)


###############################################################################
# Arbeitnow API integration (optional demonstration only)
###############################################################################

def search_arbeitnow_jobs(
    page: int = 1,
    visa_sponsorship: Optional[bool] = None,
) -> Generator[Dict, None, None]:
    """Yield job postings from the Arbeitnow free job board API.

    This function illustrates how to pull data from the Arbeitnow API, which
    aggregates jobs from multiple applicant tracking systems.  According to the
    API's documentation, the free endpoint requires no API key and includes
    fields like `remote` and a `visa_sponsorship` filter【161774275562007†L68-L94】.  However,
    note that some hosting providers block automated requests; if a 403
    response is encountered, the caller should fetch the data manually using
    the browser tool or adjust request headers.

    Args:
        page: Page number to retrieve.
        visa_sponsorship: If set to True or False, filters jobs accordingly.

    Yields:
        Dictionaries representing jobs.
    """
    base_url = "https://www.arbeitnow.com/api/job-board-api"
    params = {"page": page}
    if visa_sponsorship is not None:
        params["visa_sponsorship"] = str(visa_sponsorship).lower()
    try:
        resp = requests.get(base_url, params=params, timeout=30)
    except Exception as exc:
        print(f"Error calling Arbeitnow API: {exc}", file=sys.stderr)
        return
    if resp.status_code != 200:
        print(
            f"Arbeitnow API error on page {page}: {resp.status_code}. Some servers block automated requests; consider manual retrieval.",
            file=sys.stderr,
        )
        return
    data = resp.json()
    jobs = data.get("data", [])
    for job in jobs:
        # Flatten tags and job_types lists into comma-separated strings
        tags = ", ".join(job.get("tags", [])) if isinstance(job.get("tags"), list) else job.get("tags")
        job_types = ", ".join(job.get("job_types", [])) if isinstance(job.get("job_types"), list) else job.get("job_types")
        yield {
            "slug": job.get("slug"),
            "title": job.get("title"),
            "company_name": job.get("company_name"),
            "location": job.get("location"),
            "remote": job.get("remote", False),
            "visa_sponsorship": visa_sponsorship if visa_sponsorship is not None else job.get("visa_sponsorship", False),
            "tags": tags,
            "job_types": job_types,
            "created_at": job.get("created_at"),
            "url": job.get("url"),
            "description": job.get("description"),
        }


###############################################################################
# Main workflow combining sources
###############################################################################

def process_usajobs_states(
    conn: sqlite3.Connection,
    api_key: str,
    user_agent: str,
    states: Iterable[str],
    usajobs_pages: int,
    usajobs_results_per_page: int,
) -> None:
    """Fetch USAJOBS announcements for each state and insert into DB."""
    for state in states:
        print(f"Fetching USAJOBS jobs for {state}...")
        for job in search_usajobs_jobs(
            api_key,
            user_agent,
            location_name=state,
            results_per_page=usajobs_results_per_page,
            max_pages=usajobs_pages,
        ):
            insert_usajobs_job(conn, job)
        print(f"Completed USAJOBS jobs for {state}.")


def process_arbeitnow_pages(
    conn: sqlite3.Connection,
    pages: int = 1,
    visa_sponsorship: Optional[bool] = None,
) -> None:
    """Fetch Arbeitnow jobs for the given number of pages and insert into DB."""
    for page in range(1, pages + 1):
        for job in search_arbeitnow_jobs(page=page, visa_sponsorship=visa_sponsorship):
            insert_arbeitnow_job(conn, job)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a comprehensive jobs database by aggregating multiple job sources."
    )
    # Args for Yelp/Adzuna pipeline
    parser.add_argument("--zip", nargs="*", default=[], help="ZIP codes to search for local businesses.")
    parser.add_argument("--radius", type=int, default=40000, help="Search radius (m) for Yelp.")
    parser.add_argument("--yelp_pages", type=int, default=5, help="Pages of Yelp results to fetch per ZIP.")
    parser.add_argument("--adzuna_pages", type=int, default=5, help="Pages of Adzuna results to fetch per business.")
    parser.add_argument("--yelp_api_key", help="Yelp API key.")
    parser.add_argument("--adzuna_app_id", help="Adzuna app ID.")
    parser.add_argument("--adzuna_app_key", help="Adzuna app key.")
    # Args for USAJOBS
    parser.add_argument("--states", nargs="*", default=[], help="List of U.S. states to fetch federal jobs for (e.g. 'Massachusetts'). If omitted, no USAJOBS jobs will be fetched.")
    parser.add_argument("--usajobs_api_key", help="USAJOBS API key (Authorization-Key).")
    parser.add_argument("--usajobs_user_agent", help="USAJOBS User-Agent header (usually your email).")
    parser.add_argument("--usajobs_pages", type=int, default=2, help="Pages of USAJOBS results to fetch per state.")
    parser.add_argument(
        "--usajobs_results_per_page",
        type=int,
        default=500,
        help="Number of USAJOBS results per page (max 500).",
    )
    # Args for Arbeitnow
    parser.add_argument("--arbeitnow_pages", type=int, default=0, help="Pages of Arbeitnow jobs to fetch (0 to skip).")
    parser.add_argument(
        "--arbeitnow_visa_sponsorship",
        type=str,
        choices=["true", "false"],
        help="Filter Arbeitnow jobs by visa sponsorship (true/false).",
    )
    # General
    parser.add_argument("--db", default="jobs_aggregated.db", help="SQLite database file.")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    # Connect to DB and create tables
    conn = sqlite3.connect(args.db)
    # Create base tables from job_scraper if available
    try:
        create_base_tables(conn)
    except Exception as e:
        print(f"Warning: Could not create base tables: {e}")
    create_extra_tables(conn)
    # Process Yelp + Adzuna pipeline
    if args.zip and args.yelp_api_key and args.adzuna_app_id and args.adzuna_app_key:
        for zip_code in args.zip:
            print(f"Processing ZIP code {zip_code}...")
            for biz in search_yelp_businesses(
                args.yelp_api_key,
                zip_code,
                radius=args.radius,
                max_pages=args.yelp_pages,
            ):
                insert_business(conn, biz)
                for job in search_adzuna_jobs(
                    args.adzuna_app_id,
                    args.adzuna_app_key,
                    biz["name"],
                    zip_code,
                    max_pages=args.adzuna_pages,
                ):
                    job_record = job.copy()
                    job_record["business_name"] = biz["name"]
                    job_record["yelp_id"] = biz["id"]
                    insert_adzuna_job(conn, job_record)
            print(f"Finished ZIP code {zip_code}.")
    elif args.zip:
        print(
            "Yelp/Adzuna parameters incomplete: skipping business-specific jobs. Provide --yelp_api_key, --adzuna_app_id and --adzuna_app_key to enable this pipeline.",
            file=sys.stderr,
        )
    # Process USAJOBS for specified states
    if args.states and args.usajobs_api_key and args.usajobs_user_agent:
        process_usajobs_states(
            conn,
            args.usajobs_api_key,
            args.usajobs_user_agent,
            args.states,
            usajobs_pages=args.usajobs_pages,
            usajobs_results_per_page=args.usajobs_results_per_page,
        )
    elif args.states:
        print(
            "USAJOBS parameters incomplete: provide --usajobs_api_key and --usajobs_user_agent to enable federal job retrieval.",
            file=sys.stderr,
        )
    # Process Arbeitnow if requested
    visa_filter = None
    if args.arbeitnow_visa_sponsorship:
        visa_filter = args.arbeitnow_visa_sponsorship.lower() == "true"
    if args.arbeitnow_pages and args.arbeitnow_pages > 0:
        process_arbeitnow_pages(conn, pages=args.arbeitnow_pages, visa_sponsorship=visa_filter)
    conn.close()
    print(f"Job aggregation complete. Data saved to {args.db}.")


if __name__ == "__main__":
    main()
