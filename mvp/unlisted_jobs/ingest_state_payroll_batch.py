#!/usr/bin/env python3
"""
Batch State Payroll Ingestion Script
=====================================

Ingests multiple state payroll CSV files into the database.
Supports: Ohio, Texas, Florida (and can be extended for more states)

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import logging
from typing import Dict, Optional, Tuple
import pandas as pd
import re

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer
from normalize_titles import normalize_title

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('state_payroll_batch_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'state_payroll')

# State configurations - maps file to column names
STATE_CONFIGS = {
    'ohio': {
        'file': 'ohio.csv',
        'source_name': 'Ohio State Payroll',
        'state': 'Ohio',
        'state_abbr': 'OH',
        'columns': {
            'name': '  Name',
            'title': ' Job Title',
            'agency': 'Agency',
            'salary': 'Amount',
            'hourly_rate': 'Max. Hourly Rate',
        }
    },
    'texas': {
        'file': 'texas.csv',
        'source_name': 'Texas State Payroll',
        'state': 'Texas',
        'state_abbr': 'TX',
        'columns': {
            'name': ['LAST NAME', 'FIRST NAME'],
            'title': 'CLASS TITLE',
            'agency': 'AGENCY NAME',
            'salary': 'ANNUAL',
        }
    },
    'florida': {
        'file': 'florida.csv',
        'source_name': 'Florida State Payroll',
        'state': 'Florida',
        'state_abbr': 'FL',
        'columns': {
            'name': ['Last Name', 'First Name'],
            'title': 'Class Title',
            'agency': 'Agency Name',
            'salary': 'Salary',
            'hourly_rate': 'OPS Hourly Rate',
        }
    },
}


def parse_salary(value) -> Optional[float]:
    """Parse salary from various formats."""
    if pd.isna(value) or value is None:
        return None

    # Convert to string and clean
    val_str = str(value).strip()

    # Remove currency symbols, commas, spaces
    val_str = re.sub(r'[$,\s]', '', val_str)

    # Handle empty strings
    if not val_str or val_str == '':
        return None

    try:
        salary = float(val_str)
        # Filter unrealistic values
        if salary < 15000 or salary > 5000000:
            return None
        return salary
    except (ValueError, TypeError):
        return None


def parse_hourly_to_annual(value) -> Optional[float]:
    """Convert hourly rate to annual salary."""
    if pd.isna(value) or value is None:
        return None

    val_str = str(value).strip()
    val_str = re.sub(r'[$,\s]', '', val_str)

    if not val_str:
        return None

    try:
        hourly = float(val_str)
        if hourly < 1 or hourly > 500:
            return None
        annual = hourly * 2080
        if annual < 15000:
            return None
        return annual
    except (ValueError, TypeError):
        return None


def get_or_create_source(cursor, source_name: str, state: str) -> int:
    """Get or create a data source."""
    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO sources (name, type)
        VALUES (%s, %s)
        RETURNING id
    """, (source_name, 'state_payroll'))
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


def get_or_create_location(cursor, state: str) -> int:
    """Get or create a statewide location."""
    cursor.execute("""
        SELECT id FROM locations
        WHERE state = %s AND city = 'Statewide' AND country = 'United States'
    """, (state,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        RETURNING id
    """, ('Statewide', state, 'United States'))
    return cursor.fetchone()[0]


def ingest_state(cursor, state_key: str, normalizer: TitleNormalizer) -> int:
    """Ingest a single state's payroll data."""
    config = STATE_CONFIGS[state_key]
    filepath = os.path.join(DATA_DIR, config['file'])

    if not os.path.exists(filepath):
        log.warning(f"File not found: {filepath}")
        return 0

    log.info(f"\n{'='*60}")
    log.info(f"Processing {config['source_name']}")
    log.info(f"{'='*60}")

    # Read CSV
    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")
    log.info(f"  Columns: {list(df.columns)}")

    # Get/create source and location
    source_id = get_or_create_source(cursor, config['source_name'], config['state'])
    location_id = get_or_create_location(cursor, config['state'])

    cols = config['columns']
    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            # Get title
            title_col = cols['title']
            title = row.get(title_col)
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            # Normalize title using our simple normalizer
            normalized_title = normalize_title(title)

            # Parse for seniority using TitleNormalizer
            parse_result = normalizer.parse_title(title)
            seniority = parse_result.seniority
            seniority_conf = parse_result.seniority_confidence
            title_conf = parse_result.title_confidence

            # Get salary
            salary = None
            if 'salary' in cols:
                salary = parse_salary(row.get(cols['salary']))

            # Try hourly rate if no annual salary
            if salary is None and 'hourly_rate' in cols:
                salary = parse_hourly_to_annual(row.get(cols['hourly_rate']))

            if salary is None:
                skipped += 1
                continue

            # Get agency/company
            agency_col = cols['agency']
            company_id = get_or_create_company(cursor, row.get(agency_col))

            batch.append((
                normalized_title,
                salary,
                seniority,
                seniority_conf,
                title_conf,
                'state_payroll',
                source_id,
                company_id,
                location_id
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

    # Insert remaining
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
    """Main function to ingest all state payroll files."""
    log.info("=" * 60)
    log.info("BATCH STATE PAYROLL INGESTION")
    log.info("=" * 60)

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    total_count = 0

    try:
        for state_key in STATE_CONFIGS:
            count = ingest_state(cursor, state_key, normalizer)
            conn.commit()
            total_count += count

        # Summary
        log.info("\n" + "=" * 60)
        log.info("INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Total records ingested: {total_count:,}")

        # Show new totals
        cursor.execute("SELECT COUNT(*) FROM observed_jobs")
        total = cursor.fetchone()[0]
        log.info(f"  Total records in database: {total:,}")

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nState payroll ingestion complete!")


if __name__ == "__main__":
    main()
