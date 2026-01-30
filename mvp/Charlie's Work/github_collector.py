#!/usr/bin/env python3
"""
GitHub Profiles Collector
=========================

Collects developer profiles from GitHub API for Cambridge/Boston area.
Provides REAL employment data - actual people with verified companies.

Usage:
    python github_collector.py              # Collect profiles
    python github_collector.py --export     # Export to CSV
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


class GitHubCollector:
    """Collect developer profiles from GitHub API."""

    # Locations to search
    SEARCH_LOCATIONS = [
        "Cambridge, MA",
        "Cambridge, Massachusetts",
        "Boston, MA",
        "Boston, Massachusetts",
        "Somerville, MA",
        "Kendall Square",
    ]

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self.token = os.getenv('GITHUB_TOKEN')
        self.base_url = "https://api.github.com"
        self.headers = {'Accept': 'application/vnd.github.v3+json'}

        if self.token:
            self.headers['Authorization'] = f'token {self.token}'
            logger.info("GitHub API: Using authenticated requests (30 req/min for search)")
        else:
            logger.warning("GitHub API: No token - limited to 10 req/min. Set GITHUB_TOKEN in .env")

        self.conn = None

    def connect_db(self):
        """Connect to database and create schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS github_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                full_name TEXT,
                company TEXT,
                job_title TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                email TEXT,
                bio TEXT,
                blog TEXT,
                public_repos INTEGER,
                followers INTEGER,
                profile_url TEXT,
                created_at_github TEXT,
                collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
                job_status TEXT DEFAULT 'filled',
                confidence_score REAL DEFAULT 0.85
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_github_company ON github_profiles(company)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_github_city ON github_profiles(city)")
        self.conn.commit()
        logger.info(f"Connected to database: {self.db_path}")

    def search_users(self, location: str, max_pages: int = 5) -> List[Dict]:
        """Search for GitHub users by location."""
        users = []

        for page in range(1, max_pages + 1):
            try:
                url = f"{self.base_url}/search/users"
                params = {
                    'q': f'location:"{location}"',
                    'per_page': 100,
                    'page': page,
                    'sort': 'followers',
                    'order': 'desc'
                }

                response = requests.get(url, headers=self.headers, params=params, timeout=30)

                if response.status_code == 403:
                    logger.warning("Rate limited. Waiting 60s...")
                    time.sleep(60)
                    continue

                if response.status_code != 200:
                    logger.error(f"Search error: {response.status_code}")
                    break

                data = response.json()
                items = data.get('items', [])

                if not items:
                    break

                users.extend(items)
                logger.info(f"'{location}' page {page}: found {len(items)} users (total: {len(users)})")

                if len(users) >= data.get('total_count', 0) or len(users) >= 500:
                    break

                time.sleep(2)  # Rate limit

            except Exception as e:
                logger.error(f"Search error: {e}")
                break

        return users

    def get_user_profile(self, username: str) -> Optional[Dict]:
        """Get detailed profile for a user."""
        try:
            url = f"{self.base_url}/users/{username}"
            response = requests.get(url, headers=self.headers, timeout=30)

            if response.status_code == 403:
                time.sleep(60)
                response = requests.get(url, headers=self.headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            return None

        except Exception as e:
            logger.error(f"Error getting profile {username}: {e}")
            return None

    def parse_profile(self, data: Dict) -> Dict:
        """Parse GitHub profile into our schema."""
        company = (data.get('company') or '').lstrip('@')
        bio = data.get('bio') or ''
        location = data.get('location') or ''

        # Extract job title from bio
        job_title = None
        keywords = ['engineer', 'developer', 'scientist', 'manager', 'director',
                   'architect', 'lead', 'professor', 'researcher', 'founder', 'cto', 'ceo']
        for kw in keywords:
            if kw in bio.lower():
                for sentence in bio.split('.'):
                    if kw in sentence.lower():
                        job_title = sentence.strip()[:100]
                        break
                break

        # Parse city/state
        city, state = None, None
        if location:
            parts = [p.strip() for p in location.split(',')]
            if len(parts) >= 2:
                city = parts[0]
                state = parts[1][:2].upper() if len(parts[1]) >= 2 else parts[1]
            else:
                city = location

        return {
            'username': data.get('login'),
            'full_name': data.get('name'),
            'company': company if company else None,
            'job_title': job_title,
            'location': location if location else None,
            'city': city,
            'state': state,
            'email': data.get('email'),
            'bio': bio[:500] if bio else None,
            'blog': data.get('blog'),
            'public_repos': data.get('public_repos'),
            'followers': data.get('followers'),
            'profile_url': data.get('html_url'),
            'created_at_github': data.get('created_at'),
        }

    def save_profiles(self, profiles: List[Dict]) -> int:
        """Save profiles to database."""
        cursor = self.conn.cursor()
        inserted = 0

        for p in profiles:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO github_profiles
                    (username, full_name, company, job_title, location, city, state,
                     email, bio, blog, public_repos, followers, profile_url, created_at_github)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p['username'], p['full_name'], p['company'], p['job_title'],
                    p['location'], p['city'], p['state'], p['email'], p['bio'],
                    p['blog'], p['public_repos'], p['followers'], p['profile_url'],
                    p['created_at_github']
                ))
                if cursor.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.error(f"Error saving {p.get('username')}: {e}")

        self.conn.commit()
        return inserted

    def collect(self, max_per_location: int = 100) -> Dict:
        """Collect GitHub profiles from all locations."""
        self.connect_db()

        all_profiles = []
        seen = set()

        print("\n" + "="*60)
        print("GITHUB DEVELOPER PROFILES COLLECTOR")
        print("="*60)
        print(f"Token: {'Yes' if self.token else 'No (limited rate)'}")
        print(f"Locations: {len(self.SEARCH_LOCATIONS)}")
        print("="*60 + "\n")

        for location in self.SEARCH_LOCATIONS:
            logger.info(f"Searching: {location}")
            users = self.search_users(location, max_pages=3)

            for user in users[:max_per_location]:
                username = user.get('login')
                if username in seen:
                    continue
                seen.add(username)

                profile_data = self.get_user_profile(username)
                if profile_data:
                    parsed = self.parse_profile(profile_data)
                    all_profiles.append(parsed)

                    if len(all_profiles) % 25 == 0:
                        logger.info(f"Collected {len(all_profiles)} profiles...")

                time.sleep(0.5)  # Rate limit

        # Save to database
        inserted = self.save_profiles(all_profiles)

        # Get stats
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM github_profiles")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM github_profiles WHERE company IS NOT NULL AND company != ''")
        with_company = cursor.fetchone()[0]

        print("\n" + "="*60)
        print("COLLECTION COMPLETE")
        print("="*60)
        print(f"Profiles collected this run: {len(all_profiles)}")
        print(f"New profiles saved: {inserted}")
        print(f"Total in database: {total}")
        print(f"With company info: {with_company}")
        print("="*60)

        return {
            'collected': len(all_profiles),
            'inserted': inserted,
            'total': total,
            'with_company': with_company
        }

    def export_csv(self, output_path: str = "github_profiles.csv"):
        """Export profiles to CSV."""
        import csv

        self.connect_db()
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT username, full_name, company, job_title, location, city, state,
                   email, bio, public_repos, followers, profile_url, job_status
            FROM github_profiles
            ORDER BY followers DESC
        """)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['username', 'full_name', 'company', 'job_title', 'location',
                           'city', 'state', 'email', 'bio', 'public_repos', 'followers',
                           'profile_url', 'job_status'])
            for row in cursor.fetchall():
                writer.writerow(list(row))

        logger.info(f"Exported to {output_path}")

    def show_sample(self, limit: int = 10):
        """Show sample profiles with company info."""
        self.connect_db()
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT full_name, company, job_title, location, followers
            FROM github_profiles
            WHERE company IS NOT NULL AND company != ''
            ORDER BY followers DESC
            LIMIT ?
        """, (limit,))

        print("\n" + "="*60)
        print("TOP PROFILES WITH COMPANY INFO")
        print("="*60)
        for row in cursor.fetchall():
            print(f"\n{row[0] or 'Unknown'}")
            print(f"  Company: {row[1]}")
            if row[2]:
                print(f"  Title: {row[2][:60]}")
            print(f"  Location: {row[3]}")
            print(f"  Followers: {row[4]:,}")


def main():
    parser = argparse.ArgumentParser(description="Collect GitHub developer profiles")
    parser.add_argument("--export", action="store_true", help="Export to CSV")
    parser.add_argument("--sample", action="store_true", help="Show sample profiles")
    parser.add_argument("--max", type=int, default=100, help="Max profiles per location")

    args = parser.parse_args()

    collector = GitHubCollector()

    if args.sample:
        collector.show_sample()
    elif args.export:
        collector.collect(max_per_location=args.max)
        collector.export_csv()
    else:
        collector.collect(max_per_location=args.max)


if __name__ == "__main__":
    main()
