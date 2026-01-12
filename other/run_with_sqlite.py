#!/usr/bin/env python3
"""
Quick runner that uses SQLite instead of PostgreSQL
No database setup needed!
"""

import sys
import os
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# Import the main scraper
import importlib.util
spec = importlib.util.spec_from_file_location("scraper", "data.scraper.josh.py")
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)

# Override DatabaseManager to use SQLite
class SQLiteDatabaseManager(scraper.DatabaseManager):
    """SQLite version of DatabaseManager - no setup needed!"""

    def __init__(self, config):
        self.config = config
        self.conn = None
        self.db_path = "jobs.db"

    def connect(self):
        """Connect to SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            print(f"✓ Connected to SQLite database: {self.db_path}")
        except Exception as e:
            print(f"✗ Failed to connect to SQLite: {e}")
            raise

    def create_schema(self):
        """Create SQLite schema."""
        cursor = self.conn.cursor()

        # Jobs table
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
                remote INTEGER DEFAULT 0,
                salary_min REAL,
                salary_max REAL,
                salary_currency TEXT DEFAULT 'USD',
                description TEXT,
                requirements TEXT,
                job_type TEXT,
                sector TEXT,
                source TEXT NOT NULL,
                source_id TEXT,
                url TEXT,
                posted_date TEXT,
                expiration_date TEXT,
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                confidence_score REAL DEFAULT 0.5,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source, source_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(state, city, zip_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active, last_updated)")

        # Scraping status table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                location TEXT NOT NULL,
                location_type TEXT NOT NULL,
                last_scraped TEXT,
                jobs_found INTEGER DEFAULT 0,
                jobs_new INTEGER DEFAULT 0,
                jobs_updated INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, location, location_type)
            )
        """)

        self.conn.commit()
        cursor.close()
        print("✓ Database schema created")

    def insert_jobs(self, jobs: List[Dict[str, Any]]) -> tuple:
        """Insert jobs into SQLite."""
        if not jobs:
            return (0, 0, 0)

        cursor = self.conn.cursor()
        inserted = 0
        updated = 0
        skipped = 0

        for job in jobs:
            try:
                job_hash = self._generate_job_hash(job)
                job['job_hash'] = job_hash

                # Check if exists
                cursor.execute("SELECT id FROM jobs WHERE job_hash = ?", (job_hash,))
                existing = cursor.fetchone()

                # Prepare metadata
                metadata = json.dumps(job.get('metadata', {}))

                if existing:
                    # Update
                    cursor.execute("""
                        UPDATE jobs SET
                            title = ?, employer = ?, location = ?, city = ?, state = ?,
                            zip_code = ?, latitude = ?, longitude = ?, remote = ?,
                            salary_min = ?, salary_max = ?, salary_currency = ?,
                            description = ?, requirements = ?, job_type = ?, sector = ?,
                            source = ?, source_id = ?, url = ?, posted_date = ?,
                            expiration_date = ?, last_updated = CURRENT_TIMESTAMP,
                            is_active = 1, confidence_score = ?, metadata = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE job_hash = ?
                    """, (
                        job.get('title'), job.get('employer'), job.get('location'),
                        job.get('city'), job.get('state'), job.get('zip_code'),
                        job.get('latitude'), job.get('longitude'), job.get('remote', False),
                        job.get('salary_min'), job.get('salary_max'), job.get('salary_currency', 'USD'),
                        job.get('description'), job.get('requirements'), job.get('job_type'),
                        job.get('sector'), job.get('source'), job.get('source_id'),
                        job.get('url'), job.get('posted_date'), job.get('expiration_date'),
                        job.get('confidence_score', 0.5), metadata, job_hash
                    ))
                    updated += 1
                else:
                    # Insert
                    cursor.execute("""
                        INSERT INTO jobs (
                            job_hash, title, employer, location, city, state, zip_code,
                            latitude, longitude, remote, salary_min, salary_max,
                            salary_currency, description, requirements, job_type, sector,
                            source, source_id, url, posted_date, expiration_date,
                            confidence_score, metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        job_hash, job.get('title'), job.get('employer'), job.get('location'),
                        job.get('city'), job.get('state'), job.get('zip_code'),
                        job.get('latitude'), job.get('longitude'), job.get('remote', False),
                        job.get('salary_min'), job.get('salary_max'), job.get('salary_currency', 'USD'),
                        job.get('description'), job.get('requirements'), job.get('job_type'),
                        job.get('sector'), job.get('source'), job.get('source_id'),
                        job.get('url'), job.get('posted_date'), job.get('expiration_date'),
                        job.get('confidence_score', 0.5), metadata
                    ))
                    inserted += 1
            except Exception as e:
                print(f"Error inserting job: {e}")
                skipped += 1
                continue

        self.conn.commit()
        cursor.close()
        return (inserted, updated, skipped)

    def _generate_job_hash(self, job: Dict[str, Any]) -> str:
        """Generate hash for job."""
        import hashlib
        key_fields = (
            job.get('title', ''),
            job.get('employer', ''),
            job.get('location', ''),
            job.get('source', ''),
            job.get('source_id', '')
        )
        hash_string = '|'.join(str(f) for f in key_fields)
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def update_scraping_status(self, source: str, location: str, location_type: str,
                              jobs_found: int, jobs_new: int, jobs_updated: int,
                              status: str = 'completed', error_message: Optional[str] = None):
        """Update scraping status."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO scraping_status (
                source, location, location_type, last_scraped, jobs_found,
                jobs_new, jobs_updated, status, error_message, updated_at
            ) VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (source, location, location_type, jobs_found, jobs_new, jobs_updated, status, error_message))
        self.conn.commit()
        cursor.close()


def main():
    """Run with SQLite."""
    print("=" * 60)
    print("US Job Database Builder - SQLite Version")
    print("=" * 60)
    print("Using SQLite - no database setup needed!")
    print("")

    # Create config
    config = scraper.Config()

    # Override database manager
    builder = scraper.JobDatabaseBuilder(config)
    builder.db = SQLiteDatabaseManager(config)

    # Get locations from command line
    locations = sys.argv[1:] if len(sys.argv) > 1 else ["10001", "02139", "90210"]

    print(f"Processing locations: {', '.join(locations)}")
    print("")

    # Run
    builder.run(locations)


if __name__ == "__main__":
    main()
