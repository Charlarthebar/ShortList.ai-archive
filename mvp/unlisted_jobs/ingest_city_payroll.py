#!/usr/bin/env python3
"""
City Payroll Data Ingestion
===========================

Ingests city employee payroll data from Chicago, Philadelphia, and Los Angeles.

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import logging
from typing import Optional
import pandas as pd
import re

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer
from normalize_titles import normalize_title

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('city_payroll_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/city_payroll'


def parse_salary(value) -> Optional[float]:
    """Parse salary from various formats."""
    if pd.isna(value) or value is None:
        return None

    try:
        val_str = str(value).strip()
        val_str = re.sub(r'[$,\s]', '', val_str)

        if not val_str:
            return None

        salary = float(val_str)
        if salary < 15000 or salary > 5000000:
            return None
        return salary
    except (ValueError, TypeError):
        return None


def get_or_create_source(cursor, source_name: str) -> int:
    """Get or create a data source."""
    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO sources (name, type)
        VALUES (%s, %s)
        RETURNING id
    """, (source_name, 'city_payroll'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, agency_name: str) -> Optional[int]:
    """Get or create a company (agency)."""
    if not agency_name or pd.isna(agency_name):
        return None

    agency_name = str(agency_name).strip()
    if not agency_name:
        return None

    normalized = agency_name.lower()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id
    """, (agency_name, normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, city: str, state: str) -> int:
    """Get or create a location."""
    cursor.execute("""
        SELECT id FROM locations
        WHERE city = %s AND state = %s AND country = 'United States'
    """, (city, state))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (city, state, 'United States'))
    return cursor.fetchone()[0]


def ingest_chicago(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest Chicago city payroll data."""
    filepath = os.path.join(DATA_DIR, 'chicago.csv')

    log.info(f"\n{'='*60}")
    log.info("Processing Chicago City Payroll")
    log.info(f"{'='*60}")

    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")

    source_id = get_or_create_source(cursor, 'Chicago City Payroll')
    location_id = get_or_create_location(cursor, 'Chicago', 'IL')

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get('Job Titles')
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            # Handle salary - either Annual Salary or calculate from Hourly Rate
            salary = None
            if pd.notna(row.get('Annual Salary')):
                salary = parse_salary(row['Annual Salary'])
            elif pd.notna(row.get('Hourly Rate')) and pd.notna(row.get('Typical Hours')):
                hourly = parse_salary(row['Hourly Rate'])
                hours = row['Typical Hours']
                if hourly and hours:
                    salary = hourly * hours * 52  # Annual salary from hourly

            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.get('Department'))

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'city_payroll', source_id, company_id, location_id
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO observed_jobs (
                        raw_title, salary_point, seniority, seniority_confidence,
                        title_confidence, source_type, source_id, company_id, location_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
                batch = []
                log.info(f"    Ingested {count:,} records...")

        except Exception as e:
            log.warning(f"  Error on row {idx}: {e}")
            skipped += 1
            continue

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"  Completed: {count:,} records ingested, {skipped:,} skipped")
    return count


def ingest_philadelphia(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest Philadelphia city payroll data."""
    filepath = os.path.join(DATA_DIR, 'philadelphia.csv')

    log.info(f"\n{'='*60}")
    log.info("Processing Philadelphia City Payroll")
    log.info(f"{'='*60}")

    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")

    # Keep only the most recent quarter for each employee to avoid duplicates
    df = df.sort_values(['calendar_year', 'quarter'], ascending=[False, False])
    df = df.drop_duplicates(subset=['last_name', 'first_name', 'title'], keep='first')
    log.info(f"  After deduplication: {len(df):,} rows")

    source_id = get_or_create_source(cursor, 'Philadelphia City Payroll')
    location_id = get_or_create_location(cursor, 'Philadelphia', 'PA')

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get('title')
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            salary = parse_salary(row.get('base_salary'))
            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.get('department_name'))

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'city_payroll', source_id, company_id, location_id
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO observed_jobs (
                        raw_title, salary_point, seniority, seniority_confidence,
                        title_confidence, source_type, source_id, company_id, location_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
                batch = []
                log.info(f"    Ingested {count:,} records...")

        except Exception as e:
            log.warning(f"  Error on row {idx}: {e}")
            skipped += 1
            continue

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"  Completed: {count:,} records ingested, {skipped:,} skipped")
    return count


def ingest_los_angeles(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest Los Angeles city payroll data."""
    filepath = os.path.join(DATA_DIR, 'la_city.csv')

    log.info(f"\n{'='*60}")
    log.info("Processing Los Angeles City Payroll")
    log.info(f"{'='*60}")

    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")

    # Filter to most recent year and active employees
    latest_year = df['PAY_YEAR'].max()
    df = df[df['PAY_YEAR'] == latest_year]
    log.info(f"  Filtered to year {latest_year}: {len(df):,} rows")

    source_id = get_or_create_source(cursor, 'Los Angeles City Payroll')
    location_id = get_or_create_location(cursor, 'Los Angeles', 'CA')

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get('JOB_TITLE')
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            # Use TOTAL_PAY as salary
            salary = parse_salary(row.get('TOTAL_PAY'))
            if salary is None:
                # Try REGULAR_PAY if TOTAL_PAY not available
                salary = parse_salary(row.get('REGULAR_PAY'))

            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.get('DEPARTMENT_TITLE'))

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'city_payroll', source_id, company_id, location_id
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO observed_jobs (
                        raw_title, salary_point, seniority, seniority_confidence,
                        title_confidence, source_type, source_id, company_id, location_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
                batch = []
                log.info(f"    Ingested {count:,} records...")

        except Exception as e:
            log.warning(f"  Error on row {idx}: {e}")
            skipped += 1
            continue

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"  Completed: {count:,} records ingested, {skipped:,} skipped")
    return count


def main():
    """Main function to ingest city payroll data."""
    log.info("=" * 60)
    log.info("CITY PAYROLL DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    total_count = 0

    try:
        # Chicago
        count = ingest_chicago(cursor, normalizer)
        conn.commit()
        total_count += count

        # Philadelphia
        count = ingest_philadelphia(cursor, normalizer)
        conn.commit()
        total_count += count

        # Los Angeles
        count = ingest_los_angeles(cursor, normalizer)
        conn.commit()
        total_count += count

        # Summary
        log.info("\n" + "=" * 60)
        log.info("CITY PAYROLL INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Total records ingested: {total_count:,}")

        cursor.execute("SELECT COUNT(*) FROM observed_jobs")
        total = cursor.fetchone()[0]
        log.info(f"  Total records in database: {total:,}")

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nCity payroll ingestion complete!")


if __name__ == "__main__":
    main()
