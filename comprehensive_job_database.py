#!/usr/bin/env python3
"""
Comprehensive US Job Database
==============================

A complete system to build and maintain a database of ALL jobs in the United States,
including:
- Jobs from major job boards (LinkedIn, Indeed, etc.)
- Jobs NOT listed on major sites (company career pages, local businesses)
- Historical job data (open and closed positions)
- Skills and requirements tracking

Key Features:
- Multi-source job aggregation (APIs, web scraping, company career pages)
- Company database for tracking businesses and their career pages
- Job history tracking (when jobs open/close)
- Skills extraction and normalization
- Location-based queries with geographic boundaries
- Support for starting small (one city/region) and scaling up
"""

import os
import sys
import json
import logging
import time
import hashlib
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Generator, Tuple, Set
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
from enum import Enum
import argparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_batch, Json, RealDictCursor
import pgeocode
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from bs4 import BeautifulSoup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_job_db.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# =========================================================
# ENUMS AND CONSTANTS
# =========================================================

class JobStatus(Enum):
    """Job posting status."""
    ACTIVE = "active"           # Currently open and accepting applications
    CLOSED = "closed"           # No longer accepting applications
    FILLED = "filled"           # Position was filled
    EXPIRED = "expired"         # Listing expired without being filled
    UNKNOWN = "unknown"         # Status cannot be determined


class CompanySize(Enum):
    """Company size categories."""
    MICRO = "micro"             # 1-9 employees
    SMALL = "small"             # 10-49 employees
    MEDIUM = "medium"           # 50-249 employees
    LARGE = "large"             # 250-999 employees
    ENTERPRISE = "enterprise"   # 1000+ employees
    UNKNOWN = "unknown"


class JobSourceType(Enum):
    """Type of job source."""
    JOB_BOARD = "job_board"             # Major job boards (Indeed, LinkedIn, etc.)
    COMPANY_CAREER_PAGE = "career_page" # Direct from company website
    GOVERNMENT = "government"           # Government job sites
    STAFFING_AGENCY = "staffing"        # Staffing/recruiting agencies
    LOCAL_LISTING = "local"             # Local business directories
    MANUAL_ENTRY = "manual"             # Manually added jobs
    REFERRAL = "referral"               # From employee referrals


# Common skills keywords for extraction
COMMON_SKILLS = {
    # Programming Languages
    "python", "java", "javascript", "typescript", "c++", "c#", "ruby", "go", "rust", "php",
    "swift", "kotlin", "scala", "r", "matlab", "perl", "sql", "bash", "shell",
    # Frameworks & Libraries
    "react", "angular", "vue", "node.js", "django", "flask", "spring", "rails", ".net",
    "tensorflow", "pytorch", "pandas", "numpy", "express", "fastapi", "nextjs",
    # Databases
    "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "cassandra", "oracle",
    "sql server", "dynamodb", "firebase", "sqlite", "neo4j",
    # Cloud & DevOps
    "aws", "azure", "gcp", "docker", "kubernetes", "terraform", "jenkins", "gitlab",
    "github actions", "ansible", "puppet", "chef", "circleci", "travis",
    # Data & AI
    "machine learning", "deep learning", "nlp", "computer vision", "data science",
    "data analysis", "data engineering", "etl", "tableau", "power bi", "spark", "hadoop",
    # Other Technical
    "api", "rest", "graphql", "microservices", "agile", "scrum", "ci/cd", "git",
    "linux", "unix", "networking", "security", "cryptography",
    # Soft Skills
    "communication", "leadership", "teamwork", "problem solving", "project management",
    "customer service", "time management", "analytical", "critical thinking",
    # Business Skills
    "sales", "marketing", "accounting", "finance", "excel", "powerpoint", "crm",
    "salesforce", "hubspot", "sap", "erp",
    # Industry Specific
    "healthcare", "hipaa", "nursing", "medical", "legal", "compliance", "regulatory",
    "manufacturing", "cad", "autocad", "solidworks", "engineering"
}


# =========================================================
# CONFIGURATION
# =========================================================

@dataclass
class Config:
    """Configuration for the comprehensive job database."""
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
    google_places_api_key: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
    yelp_api_key: str = os.getenv("YELP_API_KEY", "")

    # Scraping settings
    max_workers: int = int(os.getenv("MAX_WORKERS", "10"))
    request_delay: float = float(os.getenv("REQUEST_DELAY", "1.0"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    timeout: int = int(os.getenv("TIMEOUT", "30"))

    # Processing settings
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    deduplication_window_days: int = int(os.getenv("DEDUP_WINDOW", "30"))
    job_expiry_days: int = int(os.getenv("JOB_EXPIRY_DAYS", "60"))

    # Location settings
    radius_miles: int = int(os.getenv("RADIUS_MILES", "25"))
    results_per_page: int = int(os.getenv("RESULTS_PER_PAGE", "50"))

    # Target location (for starting small)
    target_city: str = os.getenv("TARGET_CITY", "")
    target_state: str = os.getenv("TARGET_STATE", "")
    target_zip_codes: List[str] = field(default_factory=list)

    # Enable/disable sources
    enable_adzuna: bool = True
    enable_usajobs: bool = True
    enable_indeed: bool = True
    enable_linkedin: bool = False  # Disabled due to ToS
    enable_career_pages: bool = True
    enable_local_businesses: bool = True
    enable_google_places: bool = False  # Requires API key
    enable_yelp: bool = False  # Requires API key


# =========================================================
# ENHANCED DATABASE SCHEMA
# =========================================================

SCHEMA_SQL = """
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For fuzzy text search
CREATE EXTENSION IF NOT EXISTS btree_gist;  -- For range queries

-- =========================================================
-- COMPANIES TABLE
-- Track all companies/employers for finding unlisted jobs
-- =========================================================
CREATE TABLE IF NOT EXISTS companies (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,  -- Lowercase, cleaned for matching
    website TEXT,
    career_page_url TEXT,  -- Direct link to careers/jobs page
    industry TEXT,
    company_size VARCHAR(20),
    employee_count_min INTEGER,
    employee_count_max INTEGER,
    headquarters_city TEXT,
    headquarters_state VARCHAR(2),
    headquarters_zip VARCHAR(10),
    headquarters_country VARCHAR(2) DEFAULT 'US',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    description TEXT,
    founded_year INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    has_career_page BOOLEAN DEFAULT FALSE,
    career_page_last_checked TIMESTAMP,
    career_page_job_count INTEGER DEFAULT 0,
    source VARCHAR(50),  -- Where we found this company
    source_id VARCHAR(255),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(normalized_name, headquarters_state)
);

CREATE INDEX IF NOT EXISTS idx_companies_name ON companies USING GIN(name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_normalized ON companies(normalized_name);
CREATE INDEX IF NOT EXISTS idx_companies_location ON companies(headquarters_state, headquarters_city);
CREATE INDEX IF NOT EXISTS idx_companies_career_page ON companies(has_career_page, career_page_last_checked);
CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(industry);

-- =========================================================
-- SKILLS TABLE
-- Master list of all skills
-- =========================================================
CREATE TABLE IF NOT EXISTS skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    normalized_name VARCHAR(100) NOT NULL,  -- Lowercase, cleaned
    category VARCHAR(50),  -- e.g., "programming", "soft_skill", "tool"
    parent_skill_id INTEGER REFERENCES skills(id),  -- For skill hierarchies
    aliases TEXT[],  -- Alternative names for this skill
    is_verified BOOLEAN DEFAULT FALSE,
    usage_count INTEGER DEFAULT 0,  -- How many jobs mention this skill
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_skills_normalized ON skills(normalized_name);
CREATE INDEX IF NOT EXISTS idx_skills_category ON skills(category);
CREATE INDEX IF NOT EXISTS idx_skills_aliases ON skills USING GIN(aliases);

-- =========================================================
-- JOBS TABLE (Enhanced)
-- Main jobs table with status tracking
-- =========================================================
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    job_hash VARCHAR(64) UNIQUE NOT NULL,

    -- Basic Info
    title TEXT NOT NULL,
    normalized_title TEXT,  -- Cleaned for matching
    employer TEXT,
    employer_id BIGINT REFERENCES companies(id),

    -- Location
    location TEXT,
    city TEXT,
    state VARCHAR(2),
    zip_code VARCHAR(10),
    country VARCHAR(2) DEFAULT 'US',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    is_remote BOOLEAN DEFAULT FALSE,
    remote_type VARCHAR(20),  -- 'full', 'hybrid', 'occasional'

    -- Compensation
    salary_min DECIMAL(12, 2),
    salary_max DECIMAL(12, 2),
    salary_currency VARCHAR(3) DEFAULT 'USD',
    salary_period VARCHAR(20) DEFAULT 'yearly',  -- yearly, hourly, monthly
    has_benefits BOOLEAN,
    benefits_description TEXT,

    -- Job Details
    description TEXT,
    requirements TEXT,
    responsibilities TEXT,
    qualifications TEXT,
    experience_min_years INTEGER,
    experience_max_years INTEGER,
    education_level VARCHAR(50),
    job_type VARCHAR(50),  -- full-time, part-time, contract, internship
    seniority_level VARCHAR(50),  -- entry, mid, senior, executive
    department TEXT,
    reports_to TEXT,

    -- Industry/Category
    industry TEXT,
    sector VARCHAR(50),  -- private, public, nonprofit, government
    job_function TEXT,

    -- Source tracking
    source VARCHAR(50) NOT NULL,
    source_type VARCHAR(30) DEFAULT 'job_board',  -- job_board, career_page, local, etc.
    source_id VARCHAR(255),
    url TEXT,
    application_url TEXT,

    -- Status and dates
    status VARCHAR(20) DEFAULT 'active',  -- active, closed, filled, expired
    posted_date TIMESTAMP,
    expiration_date TIMESTAMP,
    closed_date TIMESTAMP,
    application_deadline TIMESTAMP,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Quality metrics
    confidence_score DECIMAL(3, 2) DEFAULT 0.5,
    data_quality_score DECIMAL(3, 2),  -- How complete is the data
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_of_id BIGINT REFERENCES jobs(id),

    -- Flexible metadata
    metadata JSONB DEFAULT '{}',

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for jobs table
CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs USING GIN(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_jobs_employer ON jobs(employer_id);
CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(state, city, zip_code);
CREATE INDEX IF NOT EXISTS idx_jobs_geo ON jobs(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, last_seen);
CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_date DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source, source_type);
CREATE INDEX IF NOT EXISTS idx_jobs_industry ON jobs(industry);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(job_type, seniority_level);
CREATE INDEX IF NOT EXISTS idx_jobs_salary ON jobs(salary_min, salary_max) WHERE salary_min IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_metadata ON jobs USING GIN(metadata);

-- =========================================================
-- JOB_SKILLS TABLE
-- Junction table linking jobs to skills
-- =========================================================
CREATE TABLE IF NOT EXISTS job_skills (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill_id INTEGER NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    is_required BOOLEAN DEFAULT TRUE,  -- Required vs nice-to-have
    proficiency_level VARCHAR(20),  -- beginner, intermediate, expert
    years_required INTEGER,
    extracted_from VARCHAR(20),  -- 'title', 'description', 'requirements'
    confidence DECIMAL(3, 2) DEFAULT 0.8,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, skill_id)
);

CREATE INDEX IF NOT EXISTS idx_job_skills_job ON job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill ON job_skills(skill_id);

-- =========================================================
-- JOB_HISTORY TABLE
-- Track changes to job status over time
-- =========================================================
CREATE TABLE IF NOT EXISTS job_history (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type VARCHAR(30) NOT NULL,  -- 'created', 'updated', 'status_change', 'closed', 'reposted'
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    old_data JSONB,  -- Previous values for changed fields
    new_data JSONB,  -- New values for changed fields
    change_reason TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_history_job ON job_history(job_id);
CREATE INDEX IF NOT EXISTS idx_job_history_event ON job_history(event_type, detected_at);

-- =========================================================
-- SCRAPING_STATUS TABLE (Enhanced)
-- Track scraping operations
-- =========================================================
CREATE TABLE IF NOT EXISTS scraping_status (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_type VARCHAR(30) DEFAULT 'job_board',
    location VARCHAR(100),
    location_type VARCHAR(20),
    target_url TEXT,  -- For career page scraping
    last_scraped TIMESTAMP,
    next_scheduled TIMESTAMP,
    jobs_found INTEGER DEFAULT 0,
    jobs_new INTEGER DEFAULT 0,
    jobs_updated INTEGER DEFAULT 0,
    jobs_closed INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, location, location_type, target_url)
);

CREATE INDEX IF NOT EXISTS idx_scraping_status_source ON scraping_status(source, status);
CREATE INDEX IF NOT EXISTS idx_scraping_status_next ON scraping_status(next_scheduled);

-- =========================================================
-- LOCATIONS TABLE
-- Reference table for geographic locations
-- =========================================================
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    zip_code VARCHAR(10) UNIQUE,
    city TEXT NOT NULL,
    state VARCHAR(2) NOT NULL,
    county TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    population INTEGER,
    timezone VARCHAR(50),
    is_metro_area BOOLEAN DEFAULT FALSE,
    metro_area_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_locations_zip ON locations(zip_code);
CREATE INDEX IF NOT EXISTS idx_locations_city_state ON locations(state, city);
CREATE INDEX IF NOT EXISTS idx_locations_geo ON locations(latitude, longitude);

-- =========================================================
-- HELPER FUNCTIONS
-- =========================================================

-- Function to normalize company names for matching
CREATE OR REPLACE FUNCTION normalize_company_name(name TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN lower(regexp_replace(
        regexp_replace(name, '(,?\s*(Inc\.?|LLC|Corp\.?|Corporation|Ltd\.?|Co\.?|Company|L\.?P\.?))$', '', 'i'),
        '[^a-z0-9]', '', 'g'
    ));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate job data quality score
CREATE OR REPLACE FUNCTION calculate_job_quality_score(j jobs) RETURNS DECIMAL AS $$
DECLARE
    score DECIMAL := 0;
    max_score DECIMAL := 10;
BEGIN
    -- Points for having various fields filled
    IF j.title IS NOT NULL AND length(j.title) > 3 THEN score := score + 1; END IF;
    IF j.employer IS NOT NULL THEN score := score + 1; END IF;
    IF j.description IS NOT NULL AND length(j.description) > 50 THEN score := score + 1.5; END IF;
    IF j.city IS NOT NULL AND j.state IS NOT NULL THEN score := score + 1; END IF;
    IF j.salary_min IS NOT NULL OR j.salary_max IS NOT NULL THEN score := score + 1.5; END IF;
    IF j.requirements IS NOT NULL THEN score := score + 1; END IF;
    IF j.job_type IS NOT NULL THEN score := score + 0.5; END IF;
    IF j.experience_min_years IS NOT NULL THEN score := score + 0.5; END IF;
    IF j.url IS NOT NULL THEN score := score + 0.5; END IF;
    IF j.posted_date IS NOT NULL THEN score := score + 0.5; END IF;

    RETURN score / max_score;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Trigger to update timestamps
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp triggers
DROP TRIGGER IF EXISTS jobs_update_timestamp ON jobs;
CREATE TRIGGER jobs_update_timestamp BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS companies_update_timestamp ON companies;
CREATE TRIGGER companies_update_timestamp BEFORE UPDATE ON companies
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- =========================================================
-- VIEWS FOR COMMON QUERIES
-- =========================================================

-- Active jobs with skills
CREATE OR REPLACE VIEW v_active_jobs_with_skills AS
SELECT
    j.*,
    array_agg(DISTINCT s.name) FILTER (WHERE s.name IS NOT NULL) as skills,
    c.name as company_name,
    c.website as company_website,
    c.industry as company_industry
FROM jobs j
LEFT JOIN job_skills js ON j.id = js.job_id
LEFT JOIN skills s ON js.skill_id = s.id
LEFT JOIN companies c ON j.employer_id = c.id
WHERE j.status = 'active'
GROUP BY j.id, c.name, c.website, c.industry;

-- Job statistics by location
CREATE OR REPLACE VIEW v_job_stats_by_location AS
SELECT
    state,
    city,
    COUNT(*) as total_jobs,
    COUNT(*) FILTER (WHERE status = 'active') as active_jobs,
    COUNT(*) FILTER (WHERE status = 'closed') as closed_jobs,
    COUNT(*) FILTER (WHERE posted_date > NOW() - INTERVAL '7 days') as new_this_week,
    AVG(salary_min) FILTER (WHERE salary_min IS NOT NULL) as avg_salary_min,
    AVG(salary_max) FILTER (WHERE salary_max IS NOT NULL) as avg_salary_max
FROM jobs
WHERE state IS NOT NULL
GROUP BY state, city;

-- Companies with career pages to scrape
CREATE OR REPLACE VIEW v_companies_to_scrape AS
SELECT *
FROM companies
WHERE has_career_page = TRUE
AND is_active = TRUE
AND (career_page_last_checked IS NULL
     OR career_page_last_checked < NOW() - INTERVAL '24 hours')
ORDER BY career_page_last_checked NULLS FIRST;
"""


class DatabaseManager:
    """Enhanced database manager with full schema support."""

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
        """Create the comprehensive database schema."""
        cursor = self.conn.cursor()
        try:
            # Execute schema SQL in parts to handle dependencies
            statements = [s.strip() for s in SCHEMA_SQL.split(';') if s.strip()]
            for stmt in statements:
                try:
                    cursor.execute(stmt + ';')
                except psycopg2.Error as e:
                    # Log but continue for non-critical errors (like "already exists")
                    if 'already exists' not in str(e):
                        logger.warning(f"Schema statement warning: {e}")
            self.conn.commit()
            logger.info("Database schema created/verified")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating schema: {e}")
            raise
        finally:
            cursor.close()

    def seed_skills(self):
        """Seed the skills table with common skills."""
        cursor = self.conn.cursor()
        try:
            for skill in COMMON_SKILLS:
                cursor.execute("""
                    INSERT INTO skills (name, normalized_name, category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                """, (skill, skill.lower(), self._categorize_skill(skill)))
            self.conn.commit()
            logger.info(f"Seeded {len(COMMON_SKILLS)} skills")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error seeding skills: {e}")
        finally:
            cursor.close()

    def _categorize_skill(self, skill: str) -> str:
        """Categorize a skill."""
        programming = {"python", "java", "javascript", "typescript", "c++", "c#", "ruby", "go",
                      "rust", "php", "swift", "kotlin", "scala", "r", "sql", "bash", "shell"}
        frameworks = {"react", "angular", "vue", "node.js", "django", "flask", "spring", "rails",
                     ".net", "tensorflow", "pytorch", "pandas", "numpy", "express", "fastapi"}
        databases = {"postgresql", "mysql", "mongodb", "redis", "elasticsearch", "cassandra",
                    "oracle", "sql server", "dynamodb", "firebase", "sqlite", "neo4j"}
        cloud = {"aws", "azure", "gcp", "docker", "kubernetes", "terraform", "jenkins"}

        skill_lower = skill.lower()
        if skill_lower in programming:
            return "programming"
        elif skill_lower in frameworks:
            return "framework"
        elif skill_lower in databases:
            return "database"
        elif skill_lower in cloud:
            return "cloud_devops"
        else:
            return "general"

    # =========================================================
    # COMPANY OPERATIONS
    # =========================================================

    def upsert_company(self, company: Dict[str, Any]) -> Optional[int]:
        """Insert or update a company, return company ID."""
        cursor = self.conn.cursor()
        try:
            normalized = self._normalize_company_name(company.get('name', ''))

            # Check if exists
            cursor.execute("""
                SELECT id FROM companies
                WHERE normalized_name = %s AND headquarters_state = %s
            """, (normalized, company.get('headquarters_state')))

            existing = cursor.fetchone()

            if existing:
                # Update
                cursor.execute("""
                    UPDATE companies SET
                        website = COALESCE(%s, website),
                        career_page_url = COALESCE(%s, career_page_url),
                        industry = COALESCE(%s, industry),
                        company_size = COALESCE(%s, company_size),
                        description = COALESCE(%s, description),
                        has_career_page = COALESCE(%s, has_career_page),
                        metadata = metadata || %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id
                """, (
                    company.get('website'),
                    company.get('career_page_url'),
                    company.get('industry'),
                    company.get('company_size'),
                    company.get('description'),
                    company.get('has_career_page'),
                    Json(company.get('metadata', {})),
                    existing[0]
                ))
            else:
                # Insert
                cursor.execute("""
                    INSERT INTO companies (
                        name, normalized_name, website, career_page_url, industry,
                        company_size, headquarters_city, headquarters_state,
                        headquarters_zip, latitude, longitude, description,
                        has_career_page, source, source_id, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    company.get('name'),
                    normalized,
                    company.get('website'),
                    company.get('career_page_url'),
                    company.get('industry'),
                    company.get('company_size'),
                    company.get('headquarters_city'),
                    company.get('headquarters_state'),
                    company.get('headquarters_zip'),
                    company.get('latitude'),
                    company.get('longitude'),
                    company.get('description'),
                    company.get('has_career_page', False),
                    company.get('source'),
                    company.get('source_id'),
                    Json(company.get('metadata', {}))
                ))

            result = cursor.fetchone()
            self.conn.commit()
            return result[0] if result else None

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error upserting company: {e}")
            return None
        finally:
            cursor.close()

    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for matching."""
        # Remove common suffixes
        name = re.sub(r',?\s*(Inc\.?|LLC|Corp\.?|Corporation|Ltd\.?|Co\.?|Company|L\.?P\.?)$', '', name, flags=re.I)
        # Remove non-alphanumeric and lowercase
        return re.sub(r'[^a-z0-9]', '', name.lower())

    def get_companies_for_scraping(self, limit: int = 100) -> List[Dict]:
        """Get companies with career pages that need scraping."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT * FROM companies
                WHERE has_career_page = TRUE
                AND is_active = TRUE
                AND (career_page_last_checked IS NULL
                     OR career_page_last_checked < NOW() - INTERVAL '24 hours')
                ORDER BY career_page_last_checked NULLS FIRST
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
        finally:
            cursor.close()

    def update_company_career_check(self, company_id: int, job_count: int):
        """Update company after checking their career page."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE companies SET
                    career_page_last_checked = CURRENT_TIMESTAMP,
                    career_page_job_count = %s
                WHERE id = %s
            """, (job_count, company_id))
            self.conn.commit()
        finally:
            cursor.close()

    # =========================================================
    # JOB OPERATIONS
    # =========================================================

    def insert_job(self, job: Dict[str, Any]) -> Tuple[Optional[int], str]:
        """
        Insert or update a job.
        Returns (job_id, action) where action is 'inserted', 'updated', or 'skipped'.
        """
        cursor = self.conn.cursor()
        try:
            job_hash = self._generate_job_hash(job)

            # Check if exists
            cursor.execute("""
                SELECT id, status, last_seen FROM jobs WHERE job_hash = %s
            """, (job_hash,))
            existing = cursor.fetchone()

            if existing:
                job_id, old_status, last_seen = existing
                new_status = job.get('status', 'active')

                # Update existing job
                cursor.execute("""
                    UPDATE jobs SET
                        title = COALESCE(%s, title),
                        employer = COALESCE(%s, employer),
                        employer_id = COALESCE(%s, employer_id),
                        location = COALESCE(%s, location),
                        city = COALESCE(%s, city),
                        state = COALESCE(%s, state),
                        zip_code = COALESCE(%s, zip_code),
                        latitude = COALESCE(%s, latitude),
                        longitude = COALESCE(%s, longitude),
                        is_remote = COALESCE(%s, is_remote),
                        salary_min = COALESCE(%s, salary_min),
                        salary_max = COALESCE(%s, salary_max),
                        description = COALESCE(%s, description),
                        requirements = COALESCE(%s, requirements),
                        job_type = COALESCE(%s, job_type),
                        status = %s,
                        last_seen = CURRENT_TIMESTAMP,
                        last_checked = CURRENT_TIMESTAMP,
                        confidence_score = COALESCE(%s, confidence_score),
                        metadata = metadata || %s
                    WHERE id = %s
                """, (
                    job.get('title'),
                    job.get('employer'),
                    job.get('employer_id'),
                    job.get('location'),
                    job.get('city'),
                    job.get('state'),
                    job.get('zip_code'),
                    job.get('latitude'),
                    job.get('longitude'),
                    job.get('is_remote'),
                    job.get('salary_min'),
                    job.get('salary_max'),
                    job.get('description'),
                    job.get('requirements'),
                    job.get('job_type'),
                    new_status,
                    job.get('confidence_score'),
                    Json(job.get('metadata', {})),
                    job_id
                ))

                # Record status change in history
                if old_status != new_status:
                    self._record_job_history(cursor, job_id, 'status_change',
                                            {'status': old_status}, {'status': new_status})

                self.conn.commit()
                return (job_id, 'updated')
            else:
                # Insert new job
                cursor.execute("""
                    INSERT INTO jobs (
                        job_hash, title, normalized_title, employer, employer_id,
                        location, city, state, zip_code, latitude, longitude,
                        is_remote, remote_type, salary_min, salary_max, salary_currency,
                        salary_period, description, requirements, responsibilities,
                        experience_min_years, education_level, job_type, seniority_level,
                        industry, sector, source, source_type, source_id, url,
                        application_url, status, posted_date, expiration_date,
                        confidence_score, metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    RETURNING id
                """, (
                    job_hash,
                    job.get('title'),
                    self._normalize_title(job.get('title', '')),
                    job.get('employer'),
                    job.get('employer_id'),
                    job.get('location'),
                    job.get('city'),
                    job.get('state'),
                    job.get('zip_code'),
                    job.get('latitude'),
                    job.get('longitude'),
                    job.get('is_remote', False),
                    job.get('remote_type'),
                    job.get('salary_min'),
                    job.get('salary_max'),
                    job.get('salary_currency', 'USD'),
                    job.get('salary_period', 'yearly'),
                    job.get('description'),
                    job.get('requirements'),
                    job.get('responsibilities'),
                    job.get('experience_min_years'),
                    job.get('education_level'),
                    job.get('job_type'),
                    job.get('seniority_level'),
                    job.get('industry'),
                    job.get('sector', 'private'),
                    job.get('source'),
                    job.get('source_type', 'job_board'),
                    job.get('source_id'),
                    job.get('url'),
                    job.get('application_url'),
                    job.get('status', 'active'),
                    job.get('posted_date'),
                    job.get('expiration_date'),
                    job.get('confidence_score', 0.5),
                    Json(job.get('metadata', {}))
                ))

                result = cursor.fetchone()
                job_id = result[0]

                # Record creation in history
                self._record_job_history(cursor, job_id, 'created', None,
                                        {'title': job.get('title'), 'status': 'active'})

                self.conn.commit()
                return (job_id, 'inserted')

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting job {job.get('title', 'unknown')}: {e}")
            return (None, 'skipped')
        finally:
            cursor.close()

    def _generate_job_hash(self, job: Dict[str, Any]) -> str:
        """Generate unique hash for a job."""
        key_fields = (
            job.get('title', ''),
            job.get('employer', ''),
            job.get('location', ''),
            job.get('source', ''),
            job.get('source_id', '')
        )
        hash_string = '|'.join(str(f).lower() for f in key_fields)
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def _normalize_title(self, title: str) -> str:
        """Normalize job title for matching."""
        return re.sub(r'[^a-z0-9\s]', '', title.lower()).strip()

    def _record_job_history(self, cursor, job_id: int, event_type: str,
                           old_data: Optional[Dict], new_data: Optional[Dict]):
        """Record a job history event."""
        cursor.execute("""
            INSERT INTO job_history (job_id, event_type, old_status, new_status, old_data, new_data)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            job_id,
            event_type,
            old_data.get('status') if old_data else None,
            new_data.get('status') if new_data else None,
            Json(old_data) if old_data else None,
            Json(new_data) if new_data else None
        ))

    def insert_job_skills(self, job_id: int, skills: List[str]):
        """Link skills to a job."""
        cursor = self.conn.cursor()
        try:
            for skill in skills:
                # Get or create skill
                cursor.execute("""
                    INSERT INTO skills (name, normalized_name)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO UPDATE SET usage_count = skills.usage_count + 1
                    RETURNING id
                """, (skill, skill.lower()))
                skill_id = cursor.fetchone()[0]

                # Link to job
                cursor.execute("""
                    INSERT INTO job_skills (job_id, skill_id)
                    VALUES (%s, %s)
                    ON CONFLICT (job_id, skill_id) DO NOTHING
                """, (job_id, skill_id))

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting job skills: {e}")
        finally:
            cursor.close()

    def mark_jobs_closed(self, source: str, location: str, current_job_hashes: Set[str]):
        """
        Mark jobs as closed if they're no longer in the current scrape.
        This helps track when jobs are removed/filled.
        """
        cursor = self.conn.cursor()
        try:
            # Get jobs from this source/location that we last saw recently
            cursor.execute("""
                SELECT id, job_hash FROM jobs
                WHERE source = %s
                AND (city = %s OR state = %s OR zip_code = %s)
                AND status = 'active'
                AND last_seen > NOW() - INTERVAL '%s days'
            """, (source, location, location, location, self.config.deduplication_window_days))

            existing_jobs = cursor.fetchall()
            closed_count = 0

            for job_id, job_hash in existing_jobs:
                if job_hash not in current_job_hashes:
                    cursor.execute("""
                        UPDATE jobs SET
                            status = 'closed',
                            closed_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (job_id,))

                    self._record_job_history(cursor, job_id, 'closed',
                                            {'status': 'active'}, {'status': 'closed'})
                    closed_count += 1

            self.conn.commit()
            if closed_count > 0:
                logger.info(f"Marked {closed_count} jobs as closed for {source}/{location}")
            return closed_count

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error marking jobs closed: {e}")
            return 0
        finally:
            cursor.close()

    def expire_old_jobs(self, days: int = 60):
        """Mark jobs as expired if not seen in X days."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE jobs SET
                    status = 'expired',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'active'
                AND last_seen < NOW() - INTERVAL '%s days'
            """, (days,))

            count = cursor.rowcount
            self.conn.commit()
            logger.info(f"Marked {count} jobs as expired (not seen in {days} days)")
            return count
        finally:
            cursor.close()

    # =========================================================
    # QUERY OPERATIONS
    # =========================================================

    def get_jobs_by_location(self, city: str = None, state: str = None,
                            zip_code: str = None, radius_miles: int = None,
                            status: str = 'active', limit: int = 100) -> List[Dict]:
        """Get jobs by location."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            conditions = ["status = %s"]
            params = [status]

            if city:
                conditions.append("city ILIKE %s")
                params.append(f"%{city}%")
            if state:
                conditions.append("state = %s")
                params.append(state)
            if zip_code:
                conditions.append("zip_code = %s")
                params.append(zip_code)

            query = f"""
                SELECT * FROM jobs
                WHERE {' AND '.join(conditions)}
                ORDER BY posted_date DESC NULLS LAST
                LIMIT %s
            """
            params.append(limit)

            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_job_statistics(self, state: str = None, city: str = None) -> Dict:
        """Get job statistics for a location."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            conditions = []
            params = []

            if state:
                conditions.append("state = %s")
                params.append(state)
            if city:
                conditions.append("city ILIKE %s")
                params.append(f"%{city}%")

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_jobs,
                    COUNT(*) FILTER (WHERE status = 'active') as active_jobs,
                    COUNT(*) FILTER (WHERE status = 'closed') as closed_jobs,
                    COUNT(*) FILTER (WHERE status = 'filled') as filled_jobs,
                    COUNT(*) FILTER (WHERE status = 'expired') as expired_jobs,
                    COUNT(*) FILTER (WHERE posted_date > NOW() - INTERVAL '7 days') as new_this_week,
                    COUNT(*) FILTER (WHERE source_type = 'career_page') as from_career_pages,
                    COUNT(*) FILTER (WHERE source_type = 'job_board') as from_job_boards,
                    COUNT(DISTINCT employer) as unique_employers,
                    AVG(salary_min) FILTER (WHERE salary_min IS NOT NULL) as avg_salary_min,
                    AVG(salary_max) FILTER (WHERE salary_max IS NOT NULL) as avg_salary_max
                FROM jobs
                {where_clause}
            """, params)

            return dict(cursor.fetchone())
        finally:
            cursor.close()

    def search_jobs(self, query: str, filters: Dict = None, limit: int = 50) -> List[Dict]:
        """Full-text search for jobs."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            conditions = ["status = 'active'"]
            params = []

            if query:
                conditions.append("(title ILIKE %s OR description ILIKE %s OR employer ILIKE %s)")
                pattern = f"%{query}%"
                params.extend([pattern, pattern, pattern])

            if filters:
                if filters.get('state'):
                    conditions.append("state = %s")
                    params.append(filters['state'])
                if filters.get('city'):
                    conditions.append("city ILIKE %s")
                    params.append(f"%{filters['city']}%")
                if filters.get('job_type'):
                    conditions.append("job_type = %s")
                    params.append(filters['job_type'])
                if filters.get('is_remote'):
                    conditions.append("is_remote = TRUE")
                if filters.get('min_salary'):
                    conditions.append("salary_min >= %s")
                    params.append(filters['min_salary'])

            cursor.execute(f"""
                SELECT * FROM jobs
                WHERE {' AND '.join(conditions)}
                ORDER BY posted_date DESC NULLS LAST
                LIMIT %s
            """, params + [limit])

            return cursor.fetchall()
        finally:
            cursor.close()


# =========================================================
# SKILLS EXTRACTOR
# =========================================================

class SkillsExtractor:
    """Extract skills from job descriptions."""

    def __init__(self):
        self.skills_pattern = re.compile(
            r'\b(' + '|'.join(re.escape(s) for s in COMMON_SKILLS) + r')\b',
            re.IGNORECASE
        )

    def extract_skills(self, text: str) -> List[str]:
        """Extract skills from text."""
        if not text:
            return []

        # Find all matches
        matches = self.skills_pattern.findall(text.lower())

        # Deduplicate while preserving order
        seen = set()
        skills = []
        for match in matches:
            if match not in seen:
                seen.add(match)
                skills.append(match)

        return skills

    def extract_from_job(self, job: Dict[str, Any]) -> List[str]:
        """Extract skills from all relevant job fields."""
        text_parts = [
            job.get('title', ''),
            job.get('description', ''),
            job.get('requirements', ''),
            job.get('responsibilities', ''),
            job.get('qualifications', '')
        ]
        combined_text = ' '.join(filter(None, text_parts))
        return self.extract_skills(combined_text)


# =========================================================
# HTTP CLIENT
# =========================================================

class HTTPClient:
    """HTTP client with retry logic and rate limiting."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()

        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
# GEO HELPER
# =========================================================

class GeoHelper:
    """Geographic utilities."""

    def __init__(self):
        self.geo = pgeocode.Nominatim("us")
        self._geocoder = None

    @property
    def geocoder(self):
        if self._geocoder is None:
            self._geocoder = Nominatim(user_agent="shortlist_job_db")
        return self._geocoder

    def zip_to_location(self, zip_code: str) -> Optional[Dict]:
        """Convert ZIP code to location info."""
        rec = self.geo.query_postal_code(zip_code)
        if pd.isna(rec.latitude) or pd.isna(rec.longitude):
            return None
        return {
            'latitude': float(rec.latitude),
            'longitude': float(rec.longitude),
            'city': rec.place_name,
            'state': rec.state_code,
            'county': rec.county_name
        }

    def get_nearby_zips(self, zip_code: str, radius_miles: int = 25) -> List[str]:
        """Get ZIP codes within radius of a given ZIP."""
        center = self.zip_to_location(zip_code)
        if not center:
            return [zip_code]

        # This is a simplified approach - in production, use a proper ZIP code database
        # For now, return just the input ZIP
        return [zip_code]

    def parse_location(self, location_str: str) -> Dict[str, Optional[str]]:
        """Parse location string into components."""
        if not location_str:
            return {'city': None, 'state': None, 'zip_code': None}

        parts = [p.strip() for p in location_str.split(',')]
        city = parts[0] if parts else None
        state = None
        zip_code = None

        if len(parts) > 1:
            # Check for state abbreviation
            state_part = parts[-1].strip()
            state_match = re.search(r'\b([A-Z]{2})\b', state_part)
            if state_match:
                state = state_match.group(1)

            # Check for ZIP code
            zip_match = re.search(r'\b(\d{5})\b', state_part)
            if zip_match:
                zip_code = zip_match.group(1)

        return {'city': city, 'state': state, 'zip_code': zip_code}


# =========================================================
# JOB SOURCES
# =========================================================

class JobSource:
    """Base class for job sources."""

    def __init__(self, config: Config, http_client: HTTPClient,
                 geo_helper: GeoHelper, skills_extractor: SkillsExtractor):
        self.config = config
        self.http_client = http_client
        self.geo_helper = geo_helper
        self.skills_extractor = skills_extractor

    @property
    def source_name(self) -> str:
        return self.__class__.__name__.replace('Source', '').lower()

    @property
    def source_type(self) -> str:
        return JobSourceType.JOB_BOARD.value

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs for a location. Must be implemented by subclasses."""
        raise NotImplementedError

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw job to common schema."""
        raise NotImplementedError


class AdzunaSource(JobSource):
    """Adzuna API job source."""

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        if not self.config.adzuna_app_id or not self.config.adzuna_app_key:
            logger.warning("Adzuna credentials missing")
            return

        max_pages = 10
        for page in range(1, max_pages + 1):
            try:
                url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
                params = {
                    "app_id": self.config.adzuna_app_id,
                    "app_key": self.config.adzuna_app_key,
                    "where": location,
                    "distance": self.config.radius_miles,
                    "results_per_page": self.config.results_per_page,
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
        location_str = raw_job.get("location", {}).get("display_name", "")
        location_parts = self.geo_helper.parse_location(location_str)
        description = raw_job.get("description", "")

        return {
            "title": raw_job.get("title", ""),
            "employer": raw_job.get("company", {}).get("display_name"),
            "location": location_str,
            "city": location_parts.get("city"),
            "state": location_parts.get("state"),
            "zip_code": location_parts.get("zip_code"),
            "latitude": raw_job.get("latitude"),
            "longitude": raw_job.get("longitude"),
            "is_remote": "remote" in (raw_job.get("title", "") + " " + location_str).lower(),
            "salary_min": raw_job.get("salary_min"),
            "salary_max": raw_job.get("salary_max"),
            "description": description,
            "source": "adzuna",
            "source_type": "job_board",
            "source_id": str(raw_job.get("id", "")),
            "url": raw_job.get("redirect_url"),
            "posted_date": self._parse_date(raw_job.get("created")),
            "sector": "private",
            "confidence_score": 0.8,
            "skills": self.skills_extractor.extract_skills(description),
            "metadata": {
                "category": raw_job.get("category", {}).get("label"),
                "contract_type": raw_job.get("contract_type"),
            }
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None


class USAJobsSource(JobSource):
    """USAJOBS API job source."""

    @property
    def source_type(self) -> str:
        return JobSourceType.GOVERNMENT.value

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
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
        descriptor = raw_job.get("MatchedObjectDescriptor", {})
        locations = descriptor.get("PositionLocation", [])

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
        description = descriptor.get("QualificationSummary", "")

        return {
            "title": descriptor.get("PositionTitle", ""),
            "employer": descriptor.get("OrganizationName"),
            "location": location_str,
            "city": location_parts.get("city"),
            "state": location_parts.get("state"),
            "zip_code": location_parts.get("zip_code"),
            "latitude": float(location_data.get("Latitude")) if location_data and location_data.get("Latitude") else None,
            "longitude": float(location_data.get("Longitude")) if location_data and location_data.get("Longitude") else None,
            "is_remote": descriptor.get("RemoteIndicator", False),
            "salary_min": salary_info.get("MinimumRange"),
            "salary_max": salary_info.get("MaximumRange"),
            "description": description,
            "requirements": descriptor.get("UserArea", {}).get("Details", {}).get("MajorDuties"),
            "source": "usajobs",
            "source_type": "government",
            "source_id": descriptor.get("PositionID", ""),
            "url": descriptor.get("PositionURI"),
            "posted_date": self._parse_date(descriptor.get("PublicationStartDate")),
            "expiration_date": self._parse_date(descriptor.get("ApplicationCloseDate")),
            "sector": "federal",
            "job_type": descriptor.get("PositionSchedule", [{}])[0].get("Name") if descriptor.get("PositionSchedule") else None,
            "confidence_score": 0.9,
            "skills": self.skills_extractor.extract_skills(description),
            "metadata": {
                "department": descriptor.get("DepartmentName"),
                "job_category": descriptor.get("JobCategory", [{}])[0].get("Name") if descriptor.get("JobCategory") else None,
            }
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None


class IndeedSource(JobSource):
    """Indeed.com web scraping source."""

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        max_pages = 5

        for page in range(max_pages):
            try:
                start = page * 10
                url = "https://www.indeed.com/jobs"
                params = {
                    "q": "",
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
                    job = self._parse_job_card(card, location)
                    if job:
                        yield job

            except Exception as e:
                logger.error(f"Error scraping Indeed page {page}: {e}")
                break

    def _parse_job_card(self, card, location: str) -> Optional[Dict[str, Any]]:
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

            # Get snippet/description if available
            snippet_elem = card.find('div', class_='job-snippet')
            description = snippet_elem.get_text(strip=True) if snippet_elem else ""

            return {
                "title": title,
                "employer": company,
                "location": location_str,
                "city": location_parts.get("city"),
                "state": location_parts.get("state"),
                "zip_code": location_parts.get("zip_code"),
                "is_remote": "remote" in location_str.lower(),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "description": description,
                "source": "indeed",
                "source_type": "job_board",
                "source_id": link_elem['href'].split('/')[-1].split('?')[0] if link_elem and link_elem.get('href') else None,
                "url": url,
                "sector": "private",
                "confidence_score": 0.7,
                "skills": self.skills_extractor.extract_skills(title + " " + description),
                "metadata": {"salary_text": salary_text}
            }
        except Exception as e:
            logger.error(f"Error parsing Indeed job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> Tuple[Optional[float], Optional[float]]:
        if not salary_text:
            return (None, None)

        numbers = re.findall(r'\$?([\d,]+)', salary_text.replace(',', ''))
        if len(numbers) >= 2:
            return (float(numbers[0]), float(numbers[1]))
        elif len(numbers) == 1:
            return (float(numbers[0]), float(numbers[0]))
        return (None, None)

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        return raw_job


class CareerPageSource(JobSource):
    """Scrape jobs directly from company career pages."""

    @property
    def source_type(self) -> str:
        return JobSourceType.COMPANY_CAREER_PAGE.value

    def fetch_jobs_from_company(self, company: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs from a specific company's career page."""
        career_url = company.get('career_page_url')
        if not career_url:
            return

        try:
            response = self.http_client.get(career_url)
            if response.status_code != 200:
                logger.warning(f"Career page returned {response.status_code}: {career_url}")
                return

            soup = BeautifulSoup(response.content, 'html.parser')

            # Try multiple strategies to find job listings
            jobs = []
            jobs.extend(self._find_jobs_by_common_patterns(soup, career_url, company))
            jobs.extend(self._find_jobs_by_structured_data(soup, career_url, company))

            for job in jobs:
                yield job

        except Exception as e:
            logger.error(f"Error scraping career page {career_url}: {e}")

    def _find_jobs_by_common_patterns(self, soup: BeautifulSoup,
                                      base_url: str, company: Dict) -> List[Dict]:
        """Find jobs using common HTML patterns."""
        jobs = []

        # Common job listing selectors
        selectors = [
            'div.job-listing', 'div.job-post', 'div.position', 'div.opening',
            'li.job-listing', 'li.job-post', 'article.job', 'div.career-listing',
            'tr.job-row', 'div[data-job]', 'a[href*="/job"]', 'a[href*="/career"]',
            'div.job-card', 'div.position-card'
        ]

        for selector in selectors:
            elements = soup.select(selector)
            for elem in elements:
                job = self._extract_job_from_element(elem, base_url, company)
                if job:
                    jobs.append(job)

        # Also look for job links
        job_links = soup.find_all('a', href=re.compile(r'(job|career|position|opening)', re.I))
        for link in job_links[:50]:  # Limit to prevent too many
            href = link.get('href', '')
            title = link.get_text(strip=True)

            if title and len(title) > 5 and len(title) < 200:
                full_url = urljoin(base_url, href)
                job = {
                    'title': title,
                    'employer': company.get('name'),
                    'employer_id': company.get('id'),
                    'location': f"{company.get('headquarters_city', '')}, {company.get('headquarters_state', '')}",
                    'city': company.get('headquarters_city'),
                    'state': company.get('headquarters_state'),
                    'source': 'career_page',
                    'source_type': 'career_page',
                    'source_id': hashlib.md5(full_url.encode()).hexdigest()[:16],
                    'url': full_url,
                    'confidence_score': 0.6,
                    'industry': company.get('industry'),
                    'metadata': {'company_source': company.get('source')}
                }
                jobs.append(job)

        return jobs

    def _find_jobs_by_structured_data(self, soup: BeautifulSoup,
                                      base_url: str, company: Dict) -> List[Dict]:
        """Find jobs using JSON-LD structured data."""
        jobs = []

        # Look for JSON-LD job postings
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'JobPosting':
                            job = self._parse_json_ld_job(item, company)
                            if job:
                                jobs.append(job)
                elif data.get('@type') == 'JobPosting':
                    job = self._parse_json_ld_job(data, company)
                    if job:
                        jobs.append(job)
            except json.JSONDecodeError:
                continue

        return jobs

    def _parse_json_ld_job(self, data: Dict, company: Dict) -> Optional[Dict]:
        """Parse a JSON-LD JobPosting."""
        try:
            location = data.get('jobLocation', {})
            if isinstance(location, list):
                location = location[0] if location else {}

            address = location.get('address', {})

            salary = data.get('baseSalary', {})
            salary_value = salary.get('value', {}) if isinstance(salary, dict) else {}

            description = data.get('description', '')

            return {
                'title': data.get('title', ''),
                'employer': company.get('name') or data.get('hiringOrganization', {}).get('name'),
                'employer_id': company.get('id'),
                'description': BeautifulSoup(description, 'html.parser').get_text() if description else '',
                'location': f"{address.get('addressLocality', '')}, {address.get('addressRegion', '')}",
                'city': address.get('addressLocality'),
                'state': address.get('addressRegion'),
                'zip_code': address.get('postalCode'),
                'is_remote': data.get('jobLocationType') == 'TELECOMMUTE',
                'salary_min': salary_value.get('minValue'),
                'salary_max': salary_value.get('maxValue'),
                'job_type': data.get('employmentType'),
                'posted_date': self._parse_date(data.get('datePosted')),
                'expiration_date': self._parse_date(data.get('validThrough')),
                'source': 'career_page',
                'source_type': 'career_page',
                'source_id': hashlib.md5(data.get('url', str(data)).encode()).hexdigest()[:16],
                'url': data.get('url'),
                'confidence_score': 0.85,
                'skills': self.skills_extractor.extract_skills(description),
                'metadata': {'structured_data': True}
            }
        except Exception as e:
            logger.error(f"Error parsing JSON-LD job: {e}")
            return None

    def _extract_job_from_element(self, elem, base_url: str, company: Dict) -> Optional[Dict]:
        """Extract job info from a DOM element."""
        try:
            # Try to find title
            title_elem = elem.find(['h1', 'h2', 'h3', 'h4', 'a', 'span'],
                                   class_=re.compile(r'(title|name|position)', re.I))
            if not title_elem:
                title_elem = elem.find('a')

            if not title_elem:
                return None

            title = title_elem.get_text(strip=True)
            if not title or len(title) < 3:
                return None

            # Try to find link
            link_elem = elem.find('a', href=True)
            url = urljoin(base_url, link_elem['href']) if link_elem else None

            # Try to find location
            loc_elem = elem.find(class_=re.compile(r'(location|city)', re.I))
            location_str = loc_elem.get_text(strip=True) if loc_elem else None

            return {
                'title': title,
                'employer': company.get('name'),
                'employer_id': company.get('id'),
                'location': location_str or f"{company.get('headquarters_city', '')}, {company.get('headquarters_state', '')}",
                'city': company.get('headquarters_city'),
                'state': company.get('headquarters_state'),
                'source': 'career_page',
                'source_type': 'career_page',
                'source_id': hashlib.md5((url or title).encode()).hexdigest()[:16],
                'url': url,
                'confidence_score': 0.5,
                'industry': company.get('industry'),
                'skills': self.skills_extractor.extract_skills(title),
                'metadata': {'extraction_method': 'dom_pattern'}
            }
        except Exception:
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Not used for career page source - use fetch_jobs_from_company instead."""
        pass

    def normalize_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        return raw_job


class LocalBusinessSource(JobSource):
    """Find jobs from local businesses not on major job boards."""

    @property
    def source_type(self) -> str:
        return JobSourceType.LOCAL_LISTING.value

    def discover_local_businesses(self, city: str, state: str) -> Generator[Dict[str, Any], None, None]:
        """
        Discover local businesses in an area.
        This would integrate with APIs like Google Places, Yelp, or business directories.
        """
        # Placeholder - in production, integrate with:
        # - Google Places API
        # - Yelp Fusion API
        # - Yellow Pages scraping
        # - Chamber of Commerce listings
        # - State business registries

        logger.info(f"Local business discovery for {city}, {state} - API integration needed")
        return
        yield  # Make this a generator

    def fetch_jobs(self, location: str, location_type: str = "zip") -> Generator[Dict[str, Any], None, None]:
        """Fetch jobs from local business sources."""
        # This would be implemented based on the specific local sources available
        return
        yield


# =========================================================
# MAIN ORCHESTRATOR
# =========================================================

class ComprehensiveJobDatabase:
    """Main orchestrator for the comprehensive job database."""

    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config)
        self.http_client = HTTPClient(config)
        self.geo_helper = GeoHelper()
        self.skills_extractor = SkillsExtractor()

        # Initialize sources
        self.sources: List[JobSource] = []
        if config.enable_adzuna:
            self.sources.append(AdzunaSource(config, self.http_client,
                                            self.geo_helper, self.skills_extractor))
        if config.enable_usajobs:
            self.sources.append(USAJobsSource(config, self.http_client,
                                             self.geo_helper, self.skills_extractor))
        if config.enable_indeed:
            self.sources.append(IndeedSource(config, self.http_client,
                                            self.geo_helper, self.skills_extractor))

        # Career page scraper
        self.career_page_source = CareerPageSource(config, self.http_client,
                                                   self.geo_helper, self.skills_extractor)

        # Local business source
        self.local_business_source = LocalBusinessSource(config, self.http_client,
                                                         self.geo_helper, self.skills_extractor)

    def initialize(self):
        """Initialize the database and schema."""
        self.db.connect()
        self.db.create_schema()
        self.db.seed_skills()
        logger.info("Database initialized")

    def shutdown(self):
        """Clean shutdown."""
        self.db.close()

    def process_location(self, location: str, location_type: str = "zip") -> Dict[str, int]:
        """Process all job sources for a single location."""
        stats = {
            'found': 0,
            'inserted': 0,
            'updated': 0,
            'closed': 0,
            'errors': 0
        }

        for source in self.sources:
            source_name = source.source_name
            logger.info(f"Processing {source_name} for {location}")

            try:
                current_hashes = set()

                for job in source.fetch_jobs(location, location_type):
                    stats['found'] += 1

                    # Insert/update job
                    job_id, action = self.db.insert_job(job)

                    if action == 'inserted':
                        stats['inserted'] += 1
                        # Extract and store skills
                        if job_id and job.get('skills'):
                            self.db.insert_job_skills(job_id, job['skills'])
                    elif action == 'updated':
                        stats['updated'] += 1

                    # Track this job hash
                    if job_id:
                        job_hash = self.db._generate_job_hash(job)
                        current_hashes.add(job_hash)

                # Mark jobs not seen as closed
                closed = self.db.mark_jobs_closed(source_name, location, current_hashes)
                stats['closed'] += closed

                logger.info(f"{source_name}: Found {stats['found']}, New: {stats['inserted']}, "
                           f"Updated: {stats['updated']}, Closed: {closed}")

            except Exception as e:
                logger.error(f"Error processing {source_name} for {location}: {e}")
                stats['errors'] += 1

        return stats

    def process_career_pages(self, limit: int = 100) -> Dict[str, int]:
        """Process company career pages for unlisted jobs."""
        stats = {'found': 0, 'inserted': 0, 'updated': 0, 'companies_checked': 0}

        companies = self.db.get_companies_for_scraping(limit)
        logger.info(f"Checking career pages for {len(companies)} companies")

        for company in companies:
            try:
                job_count = 0
                for job in self.career_page_source.fetch_jobs_from_company(company):
                    stats['found'] += 1
                    job_count += 1

                    job_id, action = self.db.insert_job(job)
                    if action == 'inserted':
                        stats['inserted'] += 1
                        if job_id and job.get('skills'):
                            self.db.insert_job_skills(job_id, job['skills'])
                    elif action == 'updated':
                        stats['updated'] += 1

                self.db.update_company_career_check(company['id'], job_count)
                stats['companies_checked'] += 1

            except Exception as e:
                logger.error(f"Error processing career page for {company.get('name')}: {e}")

        logger.info(f"Career pages: Checked {stats['companies_checked']} companies, "
                   f"Found {stats['found']} jobs, New: {stats['inserted']}")
        return stats

    def add_company(self, company_data: Dict[str, Any]) -> Optional[int]:
        """Add a company to the database."""
        return self.db.upsert_company(company_data)

    def add_companies_bulk(self, companies: List[Dict[str, Any]]) -> int:
        """Add multiple companies to the database."""
        added = 0
        for company in companies:
            if self.db.upsert_company(company):
                added += 1
        return added

    def get_statistics(self, state: str = None, city: str = None) -> Dict:
        """Get job database statistics."""
        return self.db.get_job_statistics(state, city)

    def search_jobs(self, query: str, filters: Dict = None, limit: int = 50) -> List[Dict]:
        """Search for jobs."""
        return self.db.search_jobs(query, filters, limit)

    def get_jobs_by_location(self, **kwargs) -> List[Dict]:
        """Get jobs by location."""
        return self.db.get_jobs_by_location(**kwargs)

    def expire_old_jobs(self):
        """Mark old jobs as expired."""
        return self.db.expire_old_jobs(self.config.job_expiry_days)

    def run_full_update(self, locations: List[str] = None):
        """Run a full database update."""
        self.initialize()

        try:
            # Process job board sources
            if locations:
                for location in locations:
                    self.process_location(location)
            else:
                # Use configured target or sample locations
                if self.config.target_zip_codes:
                    for zip_code in self.config.target_zip_codes:
                        self.process_location(zip_code)
                else:
                    # Default sample locations
                    sample_locations = ["10001", "02139", "90210", "60601", "98101"]
                    for loc in sample_locations:
                        self.process_location(loc)

            # Process career pages
            if self.config.enable_career_pages:
                self.process_career_pages()

            # Expire old jobs
            self.expire_old_jobs()

            # Print statistics
            stats = self.get_statistics()
            logger.info(f"Database statistics: {stats}")

        finally:
            self.shutdown()


# =========================================================
# CLI
# =========================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive US Job Database Builder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process specific ZIP codes
  python comprehensive_job_database.py --locations 10001 02139 90210

  # Process a specific city
  python comprehensive_job_database.py --city "Boston" --state "MA"

  # Run full update with all sources
  python comprehensive_job_database.py --full

  # Add companies from a JSON file
  python comprehensive_job_database.py --add-companies companies.json

  # Get statistics for a location
  python comprehensive_job_database.py --stats --state "MA"

  # Search for jobs
  python comprehensive_job_database.py --search "software engineer" --state "CA"
        """
    )

    # Location options
    parser.add_argument("--locations", nargs="+", help="ZIP codes to process")
    parser.add_argument("--city", help="Target city name")
    parser.add_argument("--state", help="Target state (2-letter code)")
    parser.add_argument("--full", action="store_true", help="Run full update")

    # Company options
    parser.add_argument("--add-companies", help="JSON file with companies to add")
    parser.add_argument("--scrape-careers", action="store_true", help="Scrape company career pages")

    # Query options
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--search", help="Search for jobs")

    # Configuration
    parser.add_argument("--config", help="Path to config file (JSON)")

    args = parser.parse_args()

    # Load config
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config_dict = json.load(f)
        config = Config(**config_dict)
    else:
        config = Config()

    # Override with CLI args
    if args.city:
        config.target_city = args.city
    if args.state:
        config.target_state = args.state

    # Create database instance
    db = ComprehensiveJobDatabase(config)

    try:
        if args.stats:
            db.initialize()
            stats = db.get_statistics(args.state, args.city)
            print("\n=== Job Database Statistics ===")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        elif args.search:
            db.initialize()
            filters = {}
            if args.state:
                filters['state'] = args.state
            if args.city:
                filters['city'] = args.city

            jobs = db.search_jobs(args.search, filters)
            print(f"\n=== Found {len(jobs)} jobs ===")
            for job in jobs[:10]:
                print(f"\n  {job['title']}")
                print(f"    {job['employer']} - {job['city']}, {job['state']}")
                if job.get('salary_min'):
                    print(f"    Salary: ${job['salary_min']:,.0f} - ${job.get('salary_max', job['salary_min']):,.0f}")

        elif args.add_companies:
            db.initialize()
            with open(args.add_companies, 'r') as f:
                companies = json.load(f)
            added = db.add_companies_bulk(companies)
            print(f"Added {added} companies")

        elif args.scrape_careers:
            db.initialize()
            stats = db.process_career_pages()
            print(f"Scraped {stats['companies_checked']} career pages, found {stats['found']} jobs")

        else:
            # Default: run full update
            locations = args.locations
            if not locations and args.city and args.state:
                # Get ZIP codes for city
                geo = GeoHelper()
                loc_info = geo.zip_to_location(args.city)
                if loc_info:
                    locations = [loc_info.get('zip_code', args.city)]

            db.run_full_update(locations)

    finally:
        db.shutdown()


if __name__ == "__main__":
    main()
