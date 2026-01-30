#!/usr/bin/env python3
"""
Apollo.io Profile Collector
============================

Collects professional profiles from Apollo.io API for Cambridge/Boston area.
Provides LinkedIn-quality employment data with verified companies.

Usage:
    python apollo_collector.py              # Collect profiles
    python apollo_collector.py --export     # Export to CSV
    python apollo_collector.py --sample     # Show sample profiles

Setup:
    1. Sign up at https://www.apollo.io/ (free tier: 50 credits/month)
    2. Get API key from Settings → Integrations → API
    3. Add to .env: APOLLO_API_KEY=your_key_here
"""

import os
import sys
import sqlite3
import logging
import argparse
import requests
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent.absolute()
DB_PATH = SCRIPT_DIR / "cambridge_jobs.db"


class ApolloCollector:
    """Collect professional profiles from Apollo.io API."""

    # Locations to search
    SEARCH_LOCATIONS = [
        "Cambridge, Massachusetts, United States",
        "Boston, Massachusetts, United States",
        "Somerville, Massachusetts, United States",
        "Brookline, Massachusetts, United States",
    ]

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self.api_key = os.getenv('APOLLO_API_KEY')
        self.base_url = "https://api.apollo.io/v1"

        if not self.api_key:
            logger.error("APOLLO_API_KEY not found in environment. Add it to .env file.")
            print("\n" + "="*60)
            print("SETUP REQUIRED")
            print("="*60)
            print("1. Sign up at https://www.apollo.io/ (free)")
            print("2. Go to Settings → Integrations → API")
            print("3. Copy your API key")
            print("4. Add to .env file: APOLLO_API_KEY=your_key_here")
            print("="*60 + "\n")
            sys.exit(1)

        self.headers = {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
        }
        self.conn = None
        self.credits_used = 0

    def connect_db(self):
        """Connect to database and create schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS apollo_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                apollo_id TEXT UNIQUE,
                first_name TEXT,
                last_name TEXT,
                full_name TEXT,
                title TEXT,
                company TEXT,
                company_domain TEXT,
                email TEXT,
                linkedin_url TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                industry TEXT,
                seniority TEXT,
                departments TEXT,
                employment_history TEXT,
                collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
                job_status TEXT DEFAULT 'filled',
                confidence_score REAL DEFAULT 0.95,
                source TEXT DEFAULT 'apollo.io'
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_apollo_company ON apollo_profiles(company)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_apollo_city ON apollo_profiles(city)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_apollo_title ON apollo_profiles(title)")
        self.conn.commit()
        logger.info(f"Connected to database: {self.db_path}")

    def search_people(self, location: str, page: int = 1, per_page: int = 25) -> Dict:
        """Search for people by location using Apollo.io API."""
        try:
            url = f"{self.base_url}/mixed_people/search"

            payload = {
                "api_key": self.api_key,
                "q_organization_domains": "",
                "page": page,
                "per_page": per_page,
                "person_locations": [location],
                "person_seniorities": [
                    "senior", "manager", "director", "vp", "c_suite", "owner", "partner"
                ],
            }

            response = requests.post(url, json=payload, headers=self.headers, timeout=30)

            if response.status_code == 401:
                logger.error("Invalid API key. Check your APOLLO_API_KEY.")
                return {"people": [], "pagination": {}}

            if response.status_code == 429:
                logger.warning("Rate limited. Waiting 60s...")
                time.sleep(60)
                return self.search_people(location, page, per_page)

            if response.status_code != 200:
                logger.error(f"API error {response.status_code}: {response.text[:200]}")
                return {"people": [], "pagination": {}}

            data = response.json()
            self.credits_used += 1
            return data

        except Exception as e:
            logger.error(f"Search error: {e}")
            return {"people": [], "pagination": {}}

    def parse_profile(self, person: Dict) -> Dict:
        """Parse Apollo person data into our schema."""

        # Get employment history as JSON string
        employment = person.get('employment_history', [])
        employment_str = None
        if employment:
            jobs = []
            for job in employment[:5]:  # Last 5 jobs
                jobs.append(f"{job.get('title', 'Unknown')} @ {job.get('organization_name', 'Unknown')}")
            employment_str = " | ".join(jobs)

        # Get departments
        departments = person.get('departments', [])
        dept_str = ", ".join(departments) if departments else None

        return {
            'apollo_id': person.get('id'),
            'first_name': person.get('first_name'),
            'last_name': person.get('last_name'),
            'full_name': person.get('name'),
            'title': person.get('title'),
            'company': person.get('organization', {}).get('name') if person.get('organization') else None,
            'company_domain': person.get('organization', {}).get('primary_domain') if person.get('organization') else None,
            'email': person.get('email'),
            'linkedin_url': person.get('linkedin_url'),
            'location': person.get('city') + ", " + person.get('state') if person.get('city') and person.get('state') else None,
            'city': person.get('city'),
            'state': person.get('state'),
            'country': person.get('country'),
            'industry': person.get('organization', {}).get('industry') if person.get('organization') else None,
            'seniority': person.get('seniority'),
            'departments': dept_str,
            'employment_history': employment_str,
        }

    def save_profiles(self, profiles: List[Dict]) -> int:
        """Save profiles to database."""
        cursor = self.conn.cursor()
        inserted = 0

        for p in profiles:
            if not p.get('apollo_id'):
                continue
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO apollo_profiles
                    (apollo_id, first_name, last_name, full_name, title, company,
                     company_domain, email, linkedin_url, location, city, state,
                     country, industry, seniority, departments, employment_history)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p['apollo_id'], p['first_name'], p['last_name'], p['full_name'],
                    p['title'], p['company'], p['company_domain'], p['email'],
                    p['linkedin_url'], p['location'], p['city'], p['state'],
                    p['country'], p['industry'], p['seniority'], p['departments'],
                    p['employment_history']
                ))
                if cursor.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.error(f"Error saving {p.get('full_name')}: {e}")

        self.conn.commit()
        return inserted

    def collect(self, max_per_location: int = 100, max_credits: int = 25) -> Dict:
        """Collect profiles from Apollo.io."""
        self.connect_db()

        all_profiles = []
        seen_ids = set()

        print("\n" + "="*60)
        print("APOLLO.IO PROFILE COLLECTOR")
        print("="*60)
        print(f"API Key: {'*' * 20}{self.api_key[-6:]}")
        print(f"Locations: {len(self.SEARCH_LOCATIONS)}")
        print(f"Max credits to use: {max_credits}")
        print("="*60 + "\n")

        for location in self.SEARCH_LOCATIONS:
            if self.credits_used >= max_credits:
                logger.warning(f"Reached credit limit ({max_credits}). Stopping.")
                break

            logger.info(f"Searching: {location}")
            page = 1
            location_count = 0

            while location_count < max_per_location and self.credits_used < max_credits:
                result = self.search_people(location, page=page, per_page=25)
                people = result.get('people', [])

                if not people:
                    break

                for person in people:
                    person_id = person.get('id')
                    if person_id in seen_ids:
                        continue
                    seen_ids.add(person_id)

                    parsed = self.parse_profile(person)
                    all_profiles.append(parsed)
                    location_count += 1

                    if len(all_profiles) % 25 == 0:
                        logger.info(f"Collected {len(all_profiles)} profiles (credits used: {self.credits_used})")

                pagination = result.get('pagination', {})
                if page >= pagination.get('total_pages', 1):
                    break
                page += 1
                time.sleep(1)  # Rate limiting

        # Save to database
        inserted = self.save_profiles(all_profiles)

        # Get stats
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM apollo_profiles")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM apollo_profiles WHERE company IS NOT NULL AND company != ''")
        with_company = cursor.fetchone()[0]

        print("\n" + "="*60)
        print("COLLECTION COMPLETE")
        print("="*60)
        print(f"Credits used: {self.credits_used}")
        print(f"Profiles collected this run: {len(all_profiles)}")
        print(f"New profiles saved: {inserted}")
        print(f"Total in database: {total}")
        print(f"With company info: {with_company}")
        print("="*60)

        return {
            'credits_used': self.credits_used,
            'collected': len(all_profiles),
            'inserted': inserted,
            'total': total,
            'with_company': with_company
        }

    def export_csv(self, output_path: str = "apollo_profiles.csv"):
        """Export profiles to CSV."""
        import csv

        self.connect_db()
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT full_name, title, company, industry, location, city, state,
                   linkedin_url, seniority, departments, employment_history, job_status
            FROM apollo_profiles
            ORDER BY company, full_name
        """)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['full_name', 'title', 'company', 'industry', 'location',
                           'city', 'state', 'linkedin_url', 'seniority', 'departments',
                           'employment_history', 'job_status'])
            for row in cursor.fetchall():
                writer.writerow(list(row))

        logger.info(f"Exported to {output_path}")

    def show_sample(self, limit: int = 15):
        """Show sample profiles."""
        self.connect_db()
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT full_name, title, company, industry, location, seniority
            FROM apollo_profiles
            WHERE company IS NOT NULL AND company != ''
            ORDER BY company
            LIMIT ?
        """, (limit,))

        print("\n" + "="*60)
        print("SAMPLE APOLLO PROFILES")
        print("="*60)
        for row in cursor.fetchall():
            print(f"\n{row[0]}")
            print(f"  Title: {row[1]}")
            print(f"  Company: {row[2]}")
            if row[3]:
                print(f"  Industry: {row[3]}")
            print(f"  Location: {row[4]}")
            if row[5]:
                print(f"  Seniority: {row[5]}")

    def show_stats(self):
        """Show database statistics."""
        self.connect_db()
        cursor = self.conn.cursor()

        print("\n" + "="*60)
        print("APOLLO DATABASE STATISTICS")
        print("="*60)

        cursor.execute("SELECT COUNT(*) FROM apollo_profiles")
        print(f"Total profiles: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM apollo_profiles WHERE company IS NOT NULL")
        print(f"With company: {cursor.fetchone()[0]}")

        cursor.execute("""
            SELECT company, COUNT(*) as cnt
            FROM apollo_profiles
            WHERE company IS NOT NULL
            GROUP BY company
            ORDER BY cnt DESC
            LIMIT 10
        """)
        print("\nTop Companies:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} employees")

        cursor.execute("""
            SELECT city, COUNT(*) as cnt
            FROM apollo_profiles
            WHERE city IS NOT NULL
            GROUP BY city
            ORDER BY cnt DESC
            LIMIT 5
        """)
        print("\nTop Cities:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} profiles")


def main():
    parser = argparse.ArgumentParser(description="Collect professional profiles from Apollo.io")
    parser.add_argument("--export", action="store_true", help="Export to CSV")
    parser.add_argument("--sample", action="store_true", help="Show sample profiles")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--max-credits", type=int, default=25, help="Max API credits to use (default: 25)")
    parser.add_argument("--max-per-location", type=int, default=100, help="Max profiles per location")

    args = parser.parse_args()

    collector = ApolloCollector()

    if args.sample:
        collector.show_sample()
    elif args.stats:
        collector.show_stats()
    elif args.export:
        collector.export_csv()
    else:
        collector.collect(max_per_location=args.max_per_location, max_credits=args.max_credits)


if __name__ == "__main__":
    main()
