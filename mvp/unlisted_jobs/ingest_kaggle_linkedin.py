#!/usr/bin/env python3
"""
Kaggle LinkedIn Job Postings Ingestion Script
==============================================

Ingests the LinkedIn Job Postings (2023-2024) dataset from Kaggle.

MANUAL DOWNLOAD REQUIRED:
-------------------------
1. Go to: https://www.kaggle.com/datasets/arshkon/linkedin-job-postings
2. Click "Download" (requires Kaggle account)
3. Extract the ZIP file
4. Move contents to: data/kaggle/linkedin/

Expected files:
- data/kaggle/linkedin/job_postings.csv
- data/kaggle/linkedin/companies/companies.csv
- data/kaggle/linkedin/job_details/benefits.csv
- data/kaggle/linkedin/job_details/salaries.csv
- data/kaggle/linkedin/job_skills/job_skills.csv

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kaggle_linkedin_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'kaggle', 'linkedin')


def check_files_exist() -> bool:
    """Check if required files exist."""
    required = ['job_postings.csv']
    for f in required:
        path = os.path.join(DATA_DIR, f)
        if not os.path.exists(path):
            log.error(f"Required file not found: {path}")
            return False
    return True


def get_or_create_source(cursor, source_name: str) -> int:
    """Get or create a data source."""
    cursor.execute(
        "SELECT id FROM sources WHERE name = %s",
        (source_name,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO sources (name, type, url, description)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (
        source_name,
        'job_board',
        'https://www.kaggle.com/datasets/arshkon/linkedin-job-postings',
        'LinkedIn job postings 2023-2024 from Kaggle'
    ))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, company_name: str, company_data: dict = None) -> int:
    """Get or create a company."""
    if not company_name or pd.isna(company_name):
        return None

    # Normalize company name
    normalized = company_name.strip().lower()

    cursor.execute(
        "SELECT id FROM companies WHERE normalized_name = %s",
        (normalized,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]

    # Insert new company
    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id
    """, (company_name.strip(), normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, city: str, state: str, country: str = 'United States') -> int:
    """Get or create a location."""
    if not city and not state:
        return None

    city = city.strip() if city and not pd.isna(city) else None
    state = state.strip() if state and not pd.isna(state) else None

    cursor.execute("""
        SELECT id FROM locations
        WHERE city = %s AND state = %s AND country = %s
    """, (city, state, country))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (city, state, country))
    return cursor.fetchone()[0]


def parse_salary(row) -> Tuple[Optional[float], Optional[float]]:
    """Parse salary information from job posting."""
    min_sal = row.get('min_salary')
    max_sal = row.get('max_salary')
    med_sal = row.get('med_salary')
    pay_period = str(row.get('pay_period', '')).lower()

    # Convert to annual
    multiplier = 1
    if 'hour' in pay_period:
        multiplier = 2080
    elif 'month' in pay_period:
        multiplier = 12
    elif 'week' in pay_period:
        multiplier = 52

    def safe_float(val):
        if pd.isna(val) or val is None:
            return None
        try:
            return float(val) * multiplier
        except:
            return None

    min_annual = safe_float(min_sal)
    max_annual = safe_float(max_sal)
    med_annual = safe_float(med_sal)

    # Use median if min/max not available
    if min_annual is None and med_annual is not None:
        min_annual = med_annual
    if max_annual is None and med_annual is not None:
        max_annual = med_annual

    return min_annual, max_annual


def load_companies(cursor) -> Dict[str, int]:
    """Load company data and create mapping."""
    companies_file = os.path.join(DATA_DIR, 'companies', 'companies.csv')
    if not os.path.exists(companies_file):
        log.warning("Companies file not found, will create companies on the fly")
        return {}

    log.info("Loading company data...")
    df = pd.read_csv(companies_file)
    log.info(f"Found {len(df)} companies")

    company_map = {}
    for _, row in df.iterrows():
        company_id = row.get('company_id')
        name = row.get('name')
        if pd.isna(company_id) or pd.isna(name):
            continue

        db_id = get_or_create_company(cursor, name)
        if db_id:
            company_map[str(int(company_id))] = db_id

    return company_map


def ingest_jobs(cursor, normalizer: TitleNormalizer, source_id: int, company_map: Dict[str, int]) -> int:
    """Ingest job postings."""
    jobs_file = os.path.join(DATA_DIR, 'job_postings.csv')

    log.info(f"Reading job postings from {jobs_file}...")
    df = pd.read_csv(jobs_file, low_memory=False)
    log.info(f"Found {len(df)} job postings")
    log.info(f"Columns: {list(df.columns)}")

    # Sample a few rows to understand data
    log.info(f"Sample data:\n{df.head(2).to_string()}")

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            # Get job title
            title = row.get('title')
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()

            # Normalize title and get seniority
            normalized_title, seniority, seniority_conf, title_conf = normalizer.normalize_with_confidence(title)

            # Get company
            company_id_str = str(row.get('company_id', ''))
            company_id = company_map.get(company_id_str)

            if not company_id and not pd.isna(row.get('company_id')):
                # Try to get company name from the row if available
                company_name = row.get('company_name') if 'company_name' in row else None
                if company_name and not pd.isna(company_name):
                    company_id = get_or_create_company(cursor, company_name)

            # Parse location
            location = row.get('location', '')
            city, state = None, None
            if not pd.isna(location) and location:
                parts = str(location).split(',')
                if len(parts) >= 2:
                    city = parts[0].strip()
                    state = parts[1].strip()
                elif len(parts) == 1:
                    city = parts[0].strip()

            location_id = get_or_create_location(cursor, city, state) if city or state else None

            # Parse salary
            min_salary, max_salary = parse_salary(row)

            # Skip if no salary (we want quality data)
            if min_salary is None and max_salary is None:
                skipped += 1
                continue

            # Use average for observed_salary
            if min_salary and max_salary:
                observed_salary = (min_salary + max_salary) / 2
            else:
                observed_salary = min_salary or max_salary

            # Skip unrealistic salaries
            if observed_salary and (observed_salary < 15000 or observed_salary > 5000000):
                skipped += 1
                continue

            # Get posting date
            posted_date = None
            if 'listed_time' in row and not pd.isna(row['listed_time']):
                try:
                    # LinkedIn uses milliseconds timestamp
                    ts = int(row['listed_time']) / 1000
                    posted_date = datetime.fromtimestamp(ts)
                except:
                    pass

            # Prepare record
            batch.append((
                normalized_title,
                observed_salary,
                seniority,
                seniority_conf,
                title_conf,
                source_id,
                company_id,
                location_id,
                posted_date,
                row.get('job_id')
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO observed_jobs (
                        raw_title, observed_salary, seniority, seniority_confidence,
                        title_confidence, source_id, company_id, location_id,
                        observation_date, external_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
                batch = []
                log.info(f"  Ingested {count:,} jobs (skipped {skipped:,})...")

        except Exception as e:
            log.warning(f"Error processing row {idx}: {e}")
            skipped += 1
            continue

    # Insert remaining batch
    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, observed_salary, seniority, seniority_confidence,
                title_confidence, source_id, company_id, location_id,
                observation_date, external_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"Ingested {count:,} LinkedIn jobs (skipped {skipped:,} without salary)")
    return count


def main():
    """Main function to ingest LinkedIn data."""
    log.info("=" * 60)
    log.info("KAGGLE LINKEDIN JOB POSTINGS INGESTION")
    log.info("=" * 60)

    if not check_files_exist():
        log.error("\nPlease download the dataset from Kaggle:")
        log.info("  1. Go to: https://www.kaggle.com/datasets/arshkon/linkedin-job-postings")
        log.info("  2. Click 'Download' (requires Kaggle account)")
        log.info("  3. Extract the ZIP file")
        log.info(f"  4. Move contents to: {DATA_DIR}/")
        log.info("\nExpected structure:")
        log.info("  data/kaggle/linkedin/job_postings.csv")
        log.info("  data/kaggle/linkedin/companies/companies.csv")
        return

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer()
    normalizer.load_roles_from_db(conn)

    try:
        # Get or create source
        source_id = get_or_create_source(cursor, 'kaggle_linkedin_2024')
        conn.commit()

        # Load companies
        company_map = load_companies(cursor)
        conn.commit()

        # Ingest jobs
        job_count = ingest_jobs(cursor, normalizer, source_id, company_map)
        conn.commit()

        # Summary
        log.info("\n" + "=" * 60)
        log.info("LINKEDIN INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Jobs ingested: {job_count:,}")

        # Show sample
        cursor.execute("""
            SELECT raw_title, observed_salary, seniority
            FROM observed_jobs
            WHERE source_id = %s
            ORDER BY observed_salary DESC
            LIMIT 10
        """, (source_id,))
        log.info("\nTop 10 highest-paying LinkedIn jobs:")
        for row in cursor.fetchall():
            log.info(f"  {row[0]}: ${row[1]:,.0f} ({row[2]})")

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nLinkedIn ingestion complete!")


if __name__ == "__main__":
    main()
