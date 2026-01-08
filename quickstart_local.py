#!/usr/bin/env python3
"""
Quick Start Script for Local Testing
=====================================

This script helps you get started with the comprehensive job database
by focusing on a small, confined area first.

Usage:
    # Test with Boston/Cambridge area
    python quickstart_local.py

    # Test with a specific city
    python quickstart_local.py --city "San Francisco" --state "CA"

    # Test with specific ZIP codes
    python quickstart_local.py --zips 02139 02138 02140
"""

import os
import sys
import json
import argparse
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comprehensive_job_database import (
    ComprehensiveJobDatabase, Config, DatabaseManager,
    GeoHelper, JobStatus, COMMON_SKILLS
)


# =========================================================
# SAMPLE DATA FOR TESTING
# =========================================================

# Major cities and their representative ZIP codes
SAMPLE_LOCATIONS = {
    "Boston Metro": {
        "city": "Boston",
        "state": "MA",
        "zip_codes": ["02139", "02138", "02115", "02116", "02109", "02111"]
    },
    "San Francisco Bay Area": {
        "city": "San Francisco",
        "state": "CA",
        "zip_codes": ["94102", "94103", "94105", "94107", "94110", "94114"]
    },
    "New York City": {
        "city": "New York",
        "state": "NY",
        "zip_codes": ["10001", "10002", "10003", "10011", "10012", "10013"]
    },
    "Seattle Metro": {
        "city": "Seattle",
        "state": "WA",
        "zip_codes": ["98101", "98102", "98103", "98104", "98105", "98109"]
    },
    "Austin Metro": {
        "city": "Austin",
        "state": "TX",
        "zip_codes": ["78701", "78702", "78703", "78704", "78705", "78751"]
    },
    "Chicago Metro": {
        "city": "Chicago",
        "state": "IL",
        "zip_codes": ["60601", "60602", "60603", "60604", "60605", "60606"]
    }
}


def check_database_connection(config: Config) -> bool:
    """Check if we can connect to the database."""
    try:
        db = DatabaseManager(config)
        db.connect()
        db.close()
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def check_api_keys(config: Config) -> dict:
    """Check which API keys are configured."""
    return {
        "Adzuna": bool(config.adzuna_app_id and config.adzuna_app_key),
        "USAJOBS": bool(config.usajobs_api_key and config.usajobs_email),
        "Google Places": bool(config.google_places_api_key),
        "Yelp": bool(config.yelp_api_key)
    }


def load_sample_companies(filepath: str = "sample_companies.json") -> list:
    """Load sample companies from JSON file."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get('companies', [])
    return []


def print_banner():
    """Print welcome banner."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║        Comprehensive Job Database - Quick Start               ║
║                                                               ║
║  Build a database of ALL jobs, including unlisted ones!       ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def print_status(label: str, status: bool):
    """Print a status line."""
    icon = "✓" if status else "✗"
    color = "\033[92m" if status else "\033[91m"
    reset = "\033[0m"
    print(f"  {color}{icon}{reset} {label}")


def run_local_test(city: str = "Boston", state: str = "MA",
                   zip_codes: list = None, skip_scraping: bool = False):
    """Run a local test with a confined area."""
    print_banner()

    # Load or create config
    config = Config()
    config.target_city = city
    config.target_state = state
    config.target_zip_codes = zip_codes or SAMPLE_LOCATIONS.get(
        f"{city} Metro", {}
    ).get("zip_codes", ["02139"])

    print(f"\n📍 Target Location: {city}, {state}")
    print(f"   ZIP Codes: {', '.join(config.target_zip_codes)}")

    # Check prerequisites
    print("\n🔍 Checking prerequisites...")

    # Database
    db_ok = check_database_connection(config)
    print_status("PostgreSQL Database", db_ok)

    if not db_ok:
        print("""
    ⚠️  Database connection failed. You can:
    1. Set up PostgreSQL and create a database called 'jobs_db'
    2. Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    3. Use SQLite for testing (see run_with_sqlite.py)
        """)

    # API Keys
    print("\n🔑 API Keys Status:")
    api_status = check_api_keys(config)
    for api, configured in api_status.items():
        print_status(api, configured)

    if not any(api_status.values()):
        print("""
    ⚠️  No API keys configured. The system will use web scraping only.
    For better results, configure API keys:
    - ADZUNA_APP_ID / ADZUNA_APP_KEY: Get from https://developer.adzuna.com
    - USAJOBS_API_KEY / USAJOBS_EMAIL: Get from https://developer.usajobs.gov
        """)

    if not db_ok:
        print("\n❌ Cannot proceed without database connection.")
        return

    # Initialize database
    print("\n📦 Initializing database...")
    job_db = ComprehensiveJobDatabase(config)
    job_db.initialize()

    try:
        # Load sample companies
        print("\n🏢 Loading sample companies...")
        companies = load_sample_companies()
        if companies:
            # Filter to target location
            local_companies = [
                c for c in companies
                if c.get('headquarters_state') == state
            ]
            if local_companies:
                added = job_db.add_companies_bulk(local_companies)
                print(f"   Added {added} local companies for career page scraping")
            else:
                print("   No companies found for target location")
        else:
            print("   No sample_companies.json found - skipping")

        if not skip_scraping:
            # Process job boards
            print(f"\n🔎 Scraping job boards for {city}, {state}...")
            total_stats = {'found': 0, 'inserted': 0, 'updated': 0}

            for zip_code in config.target_zip_codes[:3]:  # Limit to first 3 for quick test
                print(f"   Processing ZIP: {zip_code}")
                stats = job_db.process_location(zip_code)
                for key in total_stats:
                    total_stats[key] += stats.get(key, 0)

            print(f"\n   📊 Job Board Results:")
            print(f"      Found: {total_stats['found']}")
            print(f"      New: {total_stats['inserted']}")
            print(f"      Updated: {total_stats['updated']}")

            # Process career pages
            if config.enable_career_pages:
                print("\n🏢 Scraping company career pages (unlisted jobs)...")
                career_stats = job_db.process_career_pages(limit=10)
                print(f"   Checked {career_stats['companies_checked']} companies")
                print(f"   Found {career_stats['found']} jobs from career pages")
                print(f"   New jobs: {career_stats['inserted']}")

        # Show statistics
        print("\n📈 Database Statistics:")
        stats = job_db.get_statistics(state, city)
        print(f"   Total Jobs: {stats.get('total_jobs', 0)}")
        print(f"   Active Jobs: {stats.get('active_jobs', 0)}")
        print(f"   Closed Jobs: {stats.get('closed_jobs', 0)}")
        print(f"   From Career Pages: {stats.get('from_career_pages', 0)}")
        print(f"   From Job Boards: {stats.get('from_job_boards', 0)}")
        print(f"   Unique Employers: {stats.get('unique_employers', 0)}")
        if stats.get('avg_salary_min'):
            print(f"   Avg Salary Range: ${stats['avg_salary_min']:,.0f} - ${stats.get('avg_salary_max', 0):,.0f}")

        # Sample search
        print("\n🔍 Sample Search Results:")
        jobs = job_db.search_jobs("", {"state": state, "city": city}, limit=5)
        if jobs:
            for job in jobs:
                title = job.get('title', 'Unknown')[:50]
                employer = job.get('employer', 'Unknown')[:30]
                source = job.get('source_type', job.get('source', 'unknown'))
                print(f"   • {title}")
                print(f"     {employer} | Source: {source}")
        else:
            print("   No jobs found yet. Run scraping to populate the database.")

        print("\n✅ Quick start complete!")
        print("""
Next Steps:
1. Add more companies to sample_companies.json for career page scraping
2. Configure API keys for better coverage
3. Run full update: python comprehensive_job_database.py --full
4. Search jobs: python comprehensive_job_database.py --search "engineer" --state MA
        """)

    finally:
        job_db.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Quick start script for local job database testing"
    )
    parser.add_argument("--city", default="Boston", help="Target city")
    parser.add_argument("--state", default="MA", help="Target state (2-letter)")
    parser.add_argument("--zips", nargs="+", help="Specific ZIP codes")
    parser.add_argument("--skip-scraping", action="store_true",
                       help="Skip scraping, just show stats")
    parser.add_argument("--list-locations", action="store_true",
                       help="List available sample locations")

    args = parser.parse_args()

    if args.list_locations:
        print("\nAvailable Sample Locations:")
        for name, data in SAMPLE_LOCATIONS.items():
            print(f"  • {name}: {data['city']}, {data['state']}")
            print(f"    ZIP codes: {', '.join(data['zip_codes'])}")
        return

    run_local_test(
        city=args.city,
        state=args.state,
        zip_codes=args.zips,
        skip_scraping=args.skip_scraping
    )


if __name__ == "__main__":
    main()
