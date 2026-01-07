#!/usr/bin/env python3
"""
US Job Database Builder
========================

A comprehensive system to build and maintain an active database of all jobs
in the United States. This system integrates multiple APIs and web scraping
sources to create a complete job database.

Features:
- Multi-source job aggregation (APIs + web scraping)
- Distributed processing across all US locations
- PostgreSQL database with proper indexing
- Automatic deduplication and normalization
- Scheduled updates to keep database current
- Rate limiting and respectful scraping
- Error handling and retry logic
- Monitoring and logging

Data Sources:
- Adzuna API
- USAJOBS API
- Indeed (scraping)
- LinkedIn (scraping)
- Glassdoor (scraping)
- ZipRecruiter (scraping)
- Monster (scraping)
- CareerBuilder (scraping)
- State/local government job boards
"""

import os
import sys
import json
import logging
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Generator
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_batch, Json
import pgeocode
from geopy.distance import geodesic
from bs4 import BeautifulSoup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# =========================================================
# CONFIGURATION
# =========================================================

@dataclass
class Config:
    """Configuration for the job database builder."""
    # Database
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "jobs_db")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")

    # API Keys
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")
    usajobs_api_key: str = os.getenv("USAJOBS_API_KEY", "")
    usajobs_email: str = os.getenv("USAJOBS_EMAIL", "")

    # Scraping settings
    max_workers: int = int(os.getenv("MAX_WORKERS", "10"))
    request_delay: float = float(os.getenv("REQUEST_DELAY", "1.0"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    timeout: int = int(os.getenv("TIMEOUT", "30"))

    # Processing settings
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    deduplication_window_days: int = int(os.getenv("DEDUP_WINDOW", "30"))

    # Location settings
    radius_miles: int = int(os.getenv("RADIUS_MILES", "50"))
    results_per_page: int = int(os.getenv("RESULTS_PER_PAGE", "50"))

    # Enable/disable sources
    enable_adzuna: bool = True
    enable_usajobs: bool = True
    enable_indeed: bool = True
    enable_linkedin: bool = False  # Disabled by default due to ToS
    enable_glassdoor: bool = True
    enable_ziprecruiter: bool = True
    enable_monster: bool = True
    enable_careerbuilder: bool = True


# =========================================================
# DATABASE SCHEMA AND OPERATIONS
# =========================================================

class DatabaseManager:
    """Manages database connections and operations."""

    def __init__(self, config: Config):
        self.config = config
        self.conn = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            self.conn.autocommit = False
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def create_schema(self):
        """Create database schema if it doesn't exist."""
        cursor = self.conn.cursor()

        # Main jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id BIGSERIAL PRIMARY KEY,
                job_hash VARCHAR(64) UNIQUE NOT NULL,
                title TEXT NOT NULL,
                employer TEXT,
                location TEXT,
                city TEXT,
                state VARCHAR(2),
                zip_code VARCHAR(10),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                remote BOOLEAN DEFAULT FALSE,
                salary_min DECIMAL(12, 2),
                salary_max DECIMAL(12, 2),
                salary_currency VARCHAR(3) DEFAULT 'USD',
                description TEXT,
                requirements TEXT,
                job_type VARCHAR(50),
                sector VARCHAR(50),
                source VARCHAR(50) NOT NULL,
                source_id VARCHAR(255),
                url TEXT,
                posted_date TIMESTAMP,
                expiration_date TIMESTAMP,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                confidence_score DECIMAL(3, 2) DEFAULT 0.5,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source, source_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(state, city, zip_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active, last_updated)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_date)")
        # Geographic index (requires PostGIS extension)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_geo ON jobs USING GIST(ll_to_earth(latitude, longitude))")
        except:
            # Fallback to regular index if PostGIS not available
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_geo ON jobs(latitude, longitude)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_metadata ON jobs USING GIN(metadata)")

        # Scraping status table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_status (
                id SERIAL PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                location VARCHAR(100) NOT NULL,
                location_type VARCHAR(20) NOT NULL,
                last_scraped TIMESTAMP,
                jobs_found INTEGER DEFAULT 0,
                jobs_new INTEGER DEFAULT 0,
                jobs_updated INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, location, location_type)
            )
        """)

        # Enable PostGIS extension for geographic queries (if available)
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        except:
            logger.warning("PostGIS extension not available, skipping geographic extensions")

        self.conn.commit()
        cursor.close()
        logger.info("Database schema created/verified")

    def insert_jobs(self, jobs: List[Dict[str, Any]]) -> tuple:
        """Insert or update jobs in batch. Returns (inserted, updated, skipped)."""
        if not jobs:
            return (0, 0, 0)

        cursor = self.conn.cursor()
        inserted = 0
        updated = 0
        skipped = 0

        for job in jobs:
            try:
                # Generate hash for deduplication
                job_hash = self._generate_job_hash(job)
                job['job_hash'] = job_hash

                # Check if job exists
                cursor.execute(
                    "SELECT id, last_updated FROM jobs WHERE job_hash = %s",
                    (job_hash,)
                )
                existing = cursor.fetchone()

                if existing:
                    # Update existing job
                    cursor.execute("""
                        UPDATE jobs SET
                            title = %s,
                            employer = %s,
                            location = %s,
                            city = %s,
                            state = %s,
                            zip_code = %s,
                            latitude = %s,
                            longitude = %s,
                            remote = %s,
                            salary_min = %s,
                            salary_max = %s,
                            salary_currency = %s,
                            description = %s,
                            requirements = %s,
                            job_type = %s,
                            sector = %s,
                            source = %s,
                            source_id = %s,
                            url = %s,
                            posted_date = %s,
                            expiration_date = %s,
                            last_updated = CURRENT_TIMESTAMP,
                            is_active = TRUE,
                            confidence_score = %s,
                            metadata = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE job_hash = %s
                    """, (
                        job.get('title'),
                        job.get('employer'),
                        job.get('location'),
                        job.get('city'),
                        job.get('state'),
                        job.get('zip_code'),
                        job.get('latitude'),
                        job.get('longitude'),
                        job.get('remote', False),
                        job.get('salary_min'),
                        job.get('salary_max'),
                        job.get('salary_currency', 'USD'),
                        job.get('description'),
                        job.get('requirements'),
                        job.get('job_type'),
                        job.get('sector'),
                        job.get('source'),
                        job.get('source_id'),
                        job.get('url'),
                        job.get('posted_date'),
                        job.get('expiration_date'),
                        job.get('confidence_score', 0.5),
                        Json(job.get('metadata', {})),
                        job_hash
                    ))
                    updated += 1
                else:
                    # Insert new job
                    cursor.execute("""
                        INSERT INTO jobs (
                            job_hash, title, employer, location, city, state, zip_code,
                            latitude, longitude, remote, salary_min, salary_max,
                            salary_currency, description, requirements, job_type, sector,
                            source, source_id, url, posted_date, expiration_date,
                            confidence_score, metadata
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        job_hash,
                        job.get('title'),
                        job.get('employer'),
                        job.get('location'),
                        job.get('city'),
                        job.get('state'),
                        job.get('zip_code'),
                        job.get('latitude'),
                        job.get('longitude'),
                        job.get('remote', False),
                        job.get('salary_min'),
                        job.get('salary_max'),
                        job.get('salary_currency', 'USD'),
                        job.get('description'),
                        job.get('requirements'),
                        job.get('job_type'),
                        job.get('sector'),
                        job.get('source'),
                        job.get('source_id'),
                        job.get('url'),
                        job.get('posted_date'),
                        job.get('expiration_date'),
                        job.get('confidence_score', 0.5),
                        Json(job.get('metadata', {}))
                    ))
                    inserted += 1
            except Exception as e:
                logger.error(f"Error inserting job {job.get('title', 'unknown')}: {e}")
                skipped += 1
                continue

        self.conn.commit()
        cursor.close()
        return (inserted, updated, skipped)

    def _generate_job_hash(self, job: Dict[str, Any]) -> str:
        """Generate a unique hash for a job based on key fields."""
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
        """Update scraping status for a source/location."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO scraping_status (
                source, location, location_type, last_scraped, jobs_found,
                jobs_new, jobs_updated, status, error_message
            ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s)
            ON CONFLICT (source, location, location_type) DO UPDATE SET
                last_scraped = CURRENT_TIMESTAMP,
                jobs_found = EXCLUDED.jobs_found,
                jobs_new = EXCLUDED.jobs_new,
                jobs_updated = EXCLUDED.jobs_updated,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
        """, (source, location, location_type, jobs_found, jobs_new, jobs_updated, status, error_message))
        self.conn.commit()
        cursor.close()

    def mark_jobs_inactive(self, source: str, days_threshold: int = 30):
        """Mark jobs as inactive if they haven't been updated in X days."""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE jobs
            SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE source = %s
            AND last_updated < CURRENT_TIMESTAMP - INTERVAL '%s days'
            AND is_active = TRUE
        """, (source, days_threshold))
        count = cursor.rowcount
        self.conn.commit()
        cursor.close()
        logger.info(f"Marked {count} jobs as inactive for source {source}")
        return count


# =========================================================
# HTTP CLIENT WITH RETRY LOGIC
# =========================================================

class HTTPClient:
    """HTTP client with retry logic and rate limiting."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with rate limiting."""
        time.sleep(self.config.request_delay)
        return self.session.get(url, timeout=self.config.timeout, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with rate limiting."""
        time.sleep(self.config.request_delay)
        return self.session.post(url, timeout=self.config.timeout, **kwargs)


# =========================================================
# GEO HELPERS
# =========================================================

class GeoHelper:
    """Geographic utilities."""

    def __init__(self):
        self.geo = pgeocode.Nominatim("us")

    def zip_to_latlon(self, zip_code: str) -> Optional[tuple]:
        """Convert ZIP code to (lat, lon)."""
        rec = self.geo.query_postal_code(zip_code)
        if pd.isna(rec.latitude) or pd.isna(rec.longitude):
            return None
        return (float(rec.latitude), float(rec.longitude))

    def within_radius(self, center: tuple, point: tuple, miles: float) -> bool:
        """Check if point is within radius of center."""
        if not center or not point:
            return False
        return geodesic(center, point).miles <= miles

    def parse_location(self, location_str: str) -> Dict[str, Optional[str]]:
        """Parse location string into components."""
        parts = location_str.split(',')
        city = parts[0].strip() if len(parts) > 0 else None
        state = parts[1].strip() if len(parts) > 1 else None
        zip_code = None

        if state:
            # Try to extract ZIP from state string
            zip_match = [p for p in parts if p.strip().isdigit() and len(p.strip()) == 5]
            if zip_match:
                zip_code = zip_match[0].strip()

        return {
            'city': city,
            'state': state[:2] if state and len(state) >= 2 else state,
            'zip_code': zip_code
        }


# =========================================================
# JOB SOURCE INTERFACES
# =========================================================

class JobSource:
    """Base class for job sources."""

    def __init__(self, config: Config, http_client: HTTPClient, geo_helper: GeoHelper):
        self.config = config
        self.http_client = http_client
        self.geo_helper = geo_helper

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs for a location. Must be implemented by subclasses."""
        raise NotImplementedError

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw job to common schema."""
        raise NotImplementedError


# =========================================================
# ADZUNA SOURCE
# =========================================================

class AdzunaSource(JobSource):
    """Adzuna API job source."""

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs from Adzuna API."""
        if not self.config.adzuna_app_id or not self.config.adzuna_app_key:
            logger.warning("Adzuna credentials missing")
            return

        max_pages = 10  # Adjust based on API limits
        for page in range(1, max_pages + 1):
            try:
                url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
                params = {
                    "app_id": self.config.adzuna_app_id,
                    "app_key": self.config.adzuna_app_key,
                    "where": location,
                    "distance": self.config.radius_miles,
                    "results_per_page": self.config.results_per_page,
                    "content-type": "application/json",
                }

                response = self.http_client.get(url, params=params)
                if response.status_code != 200:
                    logger.error(f"Adzuna API error: {response.status_code}")
                    break

                data = response.json()
                results = data.get("results", [])
                if not results:
                    break

                for entry in results:
                    yield self.normalize_job(entry)

            except Exception as e:
                logger.error(f"Error fetching Adzuna jobs page {page}: {e}")
                break

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Adzuna job to common schema."""
        location_str = raw_job.get("location", {}).get("display_name", "")
        location_parts = self.geo_helper.parse_location(location_str)

        return {
            "title": raw_job.get("title", ""),
            "employer": raw_job.get("company", {}).get("display_name"),
            "location": location_str,
            "city": location_parts.get("city"),
            "state": location_parts.get("state"),
            "zip_code": location_parts.get("zip_code"),
            "latitude": raw_job.get("latitude"),
            "longitude": raw_job.get("longitude"),
            "remote": "remote" in (raw_job.get("title", "") + " " + location_str).lower(),
            "salary_min": raw_job.get("salary_min"),
            "salary_max": raw_job.get("salary_max"),
            "description": raw_job.get("description", ""),
            "source": "adzuna",
            "source_id": str(raw_job.get("id", "")),
            "url": raw_job.get("redirect_url"),
            "posted_date": self._parse_date(raw_job.get("created")),
            "sector": "private",
            "confidence_score": 0.8,
            "metadata": {
                "category": raw_job.get("category", {}).get("label"),
                "contract_type": raw_job.get("contract_type"),
            }
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None


# =========================================================
# USAJOBS SOURCE
# =========================================================

class USAJobsSource(JobSource):
    """USAJOBS API job source."""

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs from USAJOBS API."""
        if not self.config.usajobs_api_key or not self.config.usajobs_email:
            logger.warning("USAJOBS credentials missing")
            return

        headers = {
            "Authorization-Key": self.config.usajobs_api_key,
            "User-Agent": self.config.usajobs_email,
        }

        max_pages = 10
        for page in range(1, max_pages + 1):
            try:
                params = {
                    "LocationName": location,
                    "Radius": self.config.radius_miles,
                    "ResultsPerPage": min(self.config.results_per_page, 500),
                    "Page": page,
                }

                response = self.http_client.get(
                    "https://data.usajobs.gov/api/search",
                    headers=headers,
                    params=params
                )

                if response.status_code != 200:
                    logger.error(f"USAJOBS API error: {response.status_code}")
                    break

                data = response.json()
                items = data.get("SearchResult", {}).get("SearchResultItems", [])
                if not items:
                    break

                for item in items:
                    yield self.normalize_job(item)

            except Exception as e:
                logger.error(f"Error fetching USAJOBS page {page}: {e}")
                break

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize USAJOBS job to common schema."""
        descriptor = raw_job.get("MatchedObjectDescriptor", {})
        locations = descriptor.get("PositionLocation", [])

        # Use first location with coordinates
        location_data = None
        for loc in locations:
            if loc.get("Latitude") and loc.get("Longitude"):
                location_data = loc
                break

        if not location_data and locations:
            location_data = locations[0]

        location_str = location_data.get("LocationName", "") if location_data else ""
        location_parts = self.geo_helper.parse_location(location_str)

        salary_info = (descriptor.get("PositionRemuneration") or [{}])[0]

        return {
            "title": descriptor.get("PositionTitle", ""),
            "employer": descriptor.get("OrganizationName"),
            "location": location_str,
            "city": location_parts.get("city"),
            "state": location_parts.get("state"),
            "zip_code": location_parts.get("zip_code"),
            "latitude": float(location_data.get("Latitude")) if location_data and location_data.get("Latitude") else None,
            "longitude": float(location_data.get("Longitude")) if location_data and location_data.get("Longitude") else None,
            "remote": descriptor.get("RemoteIndicator", False),
            "salary_min": salary_info.get("MinimumRange"),
            "salary_max": salary_info.get("MaximumRange"),
            "description": descriptor.get("QualificationSummary", ""),
            "requirements": descriptor.get("UserArea", {}).get("Details", {}).get("MajorDuties"),
            "source": "usajobs",
            "source_id": descriptor.get("PositionID", ""),
            "url": descriptor.get("PositionURI"),
            "posted_date": self._parse_date(descriptor.get("PublicationStartDate")),
            "expiration_date": self._parse_date(descriptor.get("ApplicationCloseDate")),
            "sector": "federal",
            "job_type": descriptor.get("PositionSchedule", [{}])[0].get("Name"),
            "confidence_score": 0.9,
            "metadata": {
                "department": descriptor.get("DepartmentName"),
                "job_category": descriptor.get("JobCategory", [{}])[0].get("Name") if descriptor.get("JobCategory") else None,
            }
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None


# =========================================================
# INDEED SOURCE (Web Scraping)
# =========================================================

class IndeedSource(JobSource):
    """Indeed.com web scraping source."""

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Scrape jobs from Indeed.com."""
        max_pages = 5  # Limit pages to avoid being blocked

        for page in range(max_pages):
            try:
                start = page * 10
                url = f"https://www.indeed.com/jobs"
                params = {
                    "q": "",  # Can add keyword search
                    "l": location,
                    "start": start,
                    "radius": self.config.radius_miles,
                }

                response = self.http_client.get(url, params=params)
                if response.status_code != 200:
                    logger.warning(f"Indeed returned status {response.status_code}")
                    break

                soup = BeautifulSoup(response.content, 'html.parser')
                job_cards = soup.find_all('div', class_='job_seen_beacon')

                if not job_cards:
                    break

                for card in job_cards:
                    job = self._parse_indeed_job(card, location)
                    if job:
                        yield job

            except Exception as e:
                logger.error(f"Error scraping Indeed page {page}: {e}")
                break

    def _parse_indeed_job(self, card, location: str) -> Optional[Dict[str, Any]]:
        """Parse a single Indeed job card."""
        try:
            title_elem = card.find('h2', class_='jobTitle')
            title = title_elem.get_text(strip=True) if title_elem else ""

            company_elem = card.find('span', class_='companyName')
            company = company_elem.get_text(strip=True) if company_elem else ""

            location_elem = card.find('div', class_='companyLocation')
            location_str = location_elem.get_text(strip=True) if location_elem else location

            link_elem = card.find('a', href=True)
            url = f"https://www.indeed.com{link_elem['href']}" if link_elem else None

            salary_elem = card.find('span', class_='salary-snippet')
            salary_text = salary_elem.get_text(strip=True) if salary_elem else ""
            salary_min, salary_max = self._parse_salary(salary_text)

            location_parts = self.geo_helper.parse_location(location_str)

            return {
                "title": title,
                "employer": company,
                "location": location_str,
                "city": location_parts.get("city"),
                "state": location_parts.get("state"),
                "zip_code": location_parts.get("zip_code"),
                "remote": "remote" in location_str.lower(),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "source": "indeed",
                "source_id": link_elem['href'].split('/')[-1] if link_elem and link_elem.get('href') else None,
                "url": url,
                "sector": "private",
                "confidence_score": 0.7,
                "metadata": {
                    "salary_text": salary_text,
                }
            }
        except Exception as e:
            logger.error(f"Error parsing Indeed job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple:
        """Parse salary text to min/max values."""
        if not salary_text:
            return (None, None)

        # Simple parsing - can be improved
        import re
        numbers = re.findall(r'\$?([\d,]+)', salary_text.replace(',', ''))
        if len(numbers) >= 2:
            return (int(numbers[0]), int(numbers[1]))
        elif len(numbers) == 1:
            return (int(numbers[0]), int(numbers[0]))
        return (None, None)

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Indeed job (already normalized in _parse_indeed_job)."""
        return raw_job


# =========================================================
# MAIN ORCHESTRATOR
# =========================================================

class JobDatabaseBuilder:
    """Main orchestrator for building the job database."""

    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config)
        self.http_client = HTTPClient(config)
        self.geo_helper = GeoHelper()

        # Initialize sources
        self.sources = []
        if config.enable_adzuna:
            self.sources.append(AdzunaSource(config, self.http_client, self.geo_helper))
        if config.enable_usajobs:
            self.sources.append(USAJobsSource(config, self.http_client, self.geo_helper))
        if config.enable_indeed:
            self.sources.append(IndeedSource(config, self.http_client, self.geo_helper))
        # Add more sources as needed

    def process_location(self, location: str, location_type: str = "zip") -> Dict[str, int]:
        """Process all sources for a single location."""
        total_found = 0
        total_new = 0
        total_updated = 0

        for source in self.sources:
            source_name = source.__class__.__name__.replace("Source", "").lower()
            logger.info(f"Processing {source_name} for {location}")

            try:
                jobs = list(source.fetch_jobs(location, location_type))
                total_found += len(jobs)

                if jobs:
                    # Normalize jobs
                    normalized = [source.normalize_job(j) if hasattr(source, 'normalize_job') else j for j in jobs]

                    # Insert into database
                    inserted, updated, skipped = self.db.insert_jobs(normalized)
                    total_new += inserted
                    total_updated += updated

                    logger.info(f"{source_name}: Found {len(jobs)}, New: {inserted}, Updated: {updated}, Skipped: {skipped}")

                # Update scraping status
                self.db.update_scraping_status(
                    source_name, location, location_type,
                    len(jobs), inserted, updated, 'completed'
                )

            except Exception as e:
                logger.error(f"Error processing {source_name} for {location}: {e}")
                self.db.update_scraping_status(
                    source_name, location, location_type,
                    0, 0, 0, 'error', str(e)
                )

        return {
            'found': total_found,
            'new': total_new,
            'updated': total_updated
        }

    def process_all_us_locations(self):
        """Process all US ZIP codes (or major cities)."""
        # Load US ZIP codes or cities
        # For now, using a sample - in production, load from a comprehensive list
        us_zip_codes = self._load_us_locations()

        logger.info(f"Processing {len(us_zip_codes)} locations")

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self.process_location, loc, "zip"): loc
                for loc in us_zip_codes
            }

            for future in as_completed(futures):
                location = futures[future]
                try:
                    result = future.result()
                    logger.info(f"Completed {location}: {result}")
                except Exception as e:
                    logger.error(f"Error processing {location}: {e}")

    def _load_us_locations(self) -> List[str]:
        """Load list of US locations to process."""
        try:
            # Try to load from JSON file
            import json
            if os.path.exists("us_locations.json"):
                with open("us_locations.json", "r") as f:
                    data = json.load(f)
                    return data.get("zip_codes", [])
        except Exception as e:
            logger.warning(f"Could not load locations from JSON: {e}")

        # Fallback to sample locations
        try:
            from load_us_locations import load_us_zip_codes_from_csv, get_sample_zip_codes
            zip_codes = load_us_zip_codes_from_csv()
            if zip_codes:
                return zip_codes
            return get_sample_zip_codes()
        except ImportError:
            # Final fallback
            return [
                "10001", "10002", "02139", "02138", "90210", "94102",
                "60601", "77001", "30301", "33101", "98101", "80201"
            ]

    def run(self, locations: Optional[List[str]] = None):
        """Run the job database builder."""
        self.db.connect()
        self.db.create_schema()

        try:
            if locations:
                for location in locations:
                    self.process_location(location)
            else:
                self.process_all_us_locations()
        finally:
            self.db.close()


# =========================================================
# COMMAND LINE INTERFACE
# =========================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Build comprehensive US job database")
    parser.add_argument("--locations", nargs="+", help="Specific locations (ZIP codes) to process")
    parser.add_argument("--all", action="store_true", help="Process all US locations")
    parser.add_argument("--config", help="Path to config file (JSON)")

    args = parser.parse_args()

    # Load config
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config_dict = json.load(f)
        config = Config(**config_dict)
    else:
        config = Config()

    # Build database
    builder = JobDatabaseBuilder(config)

    if args.all:
        builder.run()
    elif args.locations:
        builder.run(args.locations)
    else:
        # Default: process a few sample locations
        builder.run(["10001", "02139", "90210"])


if __name__ == "__main__":
    main()
