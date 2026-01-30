#!/usr/bin/env python3
"""
Kaggle Indeed Job Postings Ingestion Script
============================================

Ingests Indeed job posting datasets from Kaggle.

MANUAL DOWNLOAD REQUIRED:
-------------------------
1. Go to: https://www.kaggle.com/datasets/promptcloud/indeed-job-posting-dataset
2. Click "Download" (requires Kaggle account)
3. Extract the ZIP file
4. Move contents to: data/kaggle/indeed/

Alternative datasets to try:
- https://www.kaggle.com/datasets/madhab/jobposts
- https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset

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
import glob

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kaggle_indeed_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'kaggle', 'indeed')


def find_csv_files() -> List[str]:
    """Find all CSV files in the data directory."""
    patterns = [
        os.path.join(DATA_DIR, '*.csv'),
        os.path.join(DATA_DIR, '**/*.csv'),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern, recursive=True))
    return files


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
        'https://www.kaggle.com/datasets/promptcloud/indeed-job-posting-dataset',
        'Indeed job postings from Kaggle'
    ))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, company_name: str) -> Optional[int]:
    """Get or create a company."""
    if not company_name or pd.isna(company_name):
        return None

    normalized = str(company_name).strip().lower()
    if not normalized:
        return None

    cursor.execute(
        "SELECT id FROM companies WHERE normalized_name = %s",
        (normalized,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id
    """, (str(company_name).strip(), normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, city: str, state: str, country: str = 'United States') -> Optional[int]:
    """Get or create a location."""
    city = str(city).strip() if city and not pd.isna(city) else None
    state = str(state).strip() if state and not pd.isna(state) else None

    if not city and not state:
        return None

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


def parse_salary_string(salary_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse salary from string like '$50,000 - $70,000 a year'."""
    if not salary_str or pd.isna(salary_str):
        return None, None

    salary_str = str(salary_str).lower().replace(',', '').replace('$', '')

    # Determine period multiplier
    multiplier = 1
    if 'hour' in salary_str:
        multiplier = 2080
    elif 'month' in salary_str:
        multiplier = 12
    elif 'week' in salary_str:
        multiplier = 52
    elif 'day' in salary_str:
        multiplier = 260

    # Extract numbers
    import re
    numbers = re.findall(r'[\d.]+', salary_str)

    if not numbers:
        return None, None

    try:
        if len(numbers) >= 2:
            min_sal = float(numbers[0]) * multiplier
            max_sal = float(numbers[1]) * multiplier
        else:
            min_sal = max_sal = float(numbers[0]) * multiplier
        return min_sal, max_sal
    except:
        return None, None


def ingest_csv(cursor, filepath: str, normalizer: TitleNormalizer, source_id: int) -> int:
    """Ingest a single CSV file."""
    log.info(f"Processing: {filepath}")

    try:
        df = pd.read_csv(filepath, low_memory=False)
    except Exception as e:
        log.error(f"Failed to read {filepath}: {e}")
        return 0

    log.info(f"  Rows: {len(df)}, Columns: {list(df.columns)}")

    # Try to identify relevant columns
    title_cols = ['title', 'job_title', 'jobtitle', 'position', 'job title', 'Title']
    company_cols = ['company', 'company_name', 'companyname', 'employer', 'Company']
    salary_cols = ['salary', 'salary_offered', 'compensation', 'pay', 'Salary']
    city_cols = ['city', 'location', 'City']
    state_cols = ['state', 'State', 'region']

    def find_column(df, candidates):
        for col in candidates:
            if col in df.columns:
                return col
            # Case-insensitive search
            for actual_col in df.columns:
                if actual_col.lower() == col.lower():
                    return actual_col
        return None

    title_col = find_column(df, title_cols)
    company_col = find_column(df, company_cols)
    salary_col = find_column(df, salary_cols)
    city_col = find_column(df, city_cols)
    state_col = find_column(df, state_cols)

    if not title_col:
        log.warning(f"  No title column found in {filepath}")
        return 0

    log.info(f"  Mapped columns: title={title_col}, company={company_col}, salary={salary_col}")

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            # Get title
            title = row.get(title_col)
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 3:
                skipped += 1
                continue

            # Normalize title
            normalized_title, seniority, seniority_conf, title_conf = normalizer.normalize_with_confidence(title)

            # Get company
            company_id = None
            if company_col:
                company_id = get_or_create_company(cursor, row.get(company_col))

            # Get location
            location_id = None
            if city_col or state_col:
                city = row.get(city_col) if city_col else None
                state = row.get(state_col) if state_col else None
                location_id = get_or_create_location(cursor, city, state)

            # Parse salary
            min_salary, max_salary = None, None
            if salary_col:
                min_salary, max_salary = parse_salary_string(row.get(salary_col))

            # Skip if no salary
            if min_salary is None and max_salary is None:
                skipped += 1
                continue

            # Calculate observed salary
            if min_salary and max_salary:
                observed_salary = (min_salary + max_salary) / 2
            else:
                observed_salary = min_salary or max_salary

            # Skip unrealistic salaries
            if observed_salary < 15000 or observed_salary > 5000000:
                skipped += 1
                continue

            batch.append((
                normalized_title,
                observed_salary,
                seniority,
                seniority_conf,
                title_conf,
                source_id,
                company_id,
                location_id
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO observed_jobs (
                        raw_title, observed_salary, seniority, seniority_confidence,
                        title_confidence, source_id, company_id, location_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
                batch = []

        except Exception as e:
            log.warning(f"  Error on row {idx}: {e}")
            skipped += 1
            continue

    # Insert remaining
    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, observed_salary, seniority, seniority_confidence,
                title_confidence, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"  Ingested {count:,} jobs from {os.path.basename(filepath)} (skipped {skipped:,})")
    return count


def main():
    """Main function to ingest Indeed data."""
    log.info("=" * 60)
    log.info("KAGGLE INDEED JOB POSTINGS INGESTION")
    log.info("=" * 60)

    # Find CSV files
    csv_files = find_csv_files()

    if not csv_files:
        log.error(f"\nNo CSV files found in {DATA_DIR}")
        log.info("\nPlease download one of these datasets from Kaggle:")
        log.info("  1. https://www.kaggle.com/datasets/promptcloud/indeed-job-posting-dataset")
        log.info("  2. https://www.kaggle.com/datasets/madhab/jobposts")
        log.info("  3. https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset")
        log.info(f"\nExtract to: {DATA_DIR}/")
        return

    log.info(f"Found {len(csv_files)} CSV files")

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer()
    normalizer.load_roles_from_db(conn)

    try:
        # Get or create source
        source_id = get_or_create_source(cursor, 'kaggle_indeed')
        conn.commit()

        # Process each file
        total_count = 0
        for csv_file in csv_files:
            count = ingest_csv(cursor, csv_file, normalizer, source_id)
            conn.commit()
            total_count += count

        # Summary
        log.info("\n" + "=" * 60)
        log.info("INDEED INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Total jobs ingested: {total_count:,}")

        if total_count > 0:
            cursor.execute("""
                SELECT raw_title, observed_salary, seniority
                FROM observed_jobs
                WHERE source_id = %s
                ORDER BY observed_salary DESC
                LIMIT 10
            """, (source_id,))
            log.info("\nTop 10 highest-paying Indeed jobs:")
            for row in cursor.fetchall():
                log.info(f"  {row[0]}: ${row[1]:,.0f} ({row[2]})")

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nIndeed ingestion complete!")


if __name__ == "__main__":
    main()
