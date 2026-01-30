#!/usr/bin/env python3
"""
New State Payroll Ingestion Script
===================================

Ingests South Carolina, Oregon, and Massachusetts state payroll data.

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import logging
from typing import Optional
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
        logging.FileHandler('new_states_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll'

# State configurations
STATE_CONFIGS = {
    'southcarolina': {
        'file': 'southcarolina.csv',
        'source_name': 'South Carolina State Payroll',
        'state': 'South Carolina',
        'columns': {
            # Name is split across cols 0,1 due to comma
            'agency': 2,  # AGENCY
            'title': 3,  # POSITION TITLE
            'salary': 4,  # SALARY (has $ and commas)
        },
        'has_header': False,
    },
    'oregon': {
        'file': 'oregon.csv',
        'source_name': 'Oregon State Payroll',
        'state': 'Oregon',
        'columns': {
            'title': 'CLASSIFICATION',
            'agency': 'AGENCY',
            'salary': 'SALARY (ANNUAL) ',
        },
        'has_header': True,
    },
    'massachusetts': {
        'file': 'massachusetts.csv',
        'source_name': 'Massachusetts State Payroll',
        'state': 'Massachusetts',
        'columns': {
            'name_last': 'NAME_LAST',
            'name_first': 'NAME_FIRST',
            'agency': 'DEPARTMENT_DIVISION',
            'title': 'POSITION_TITLE',
            'salary': 'PAY_TOTAL_ACTUAL',
        },
        'has_header': True,
    },
}


def parse_salary(value) -> Optional[float]:
    """Parse salary from various formats."""
    if pd.isna(value) or value is None:
        return None

    val_str = str(value).strip()
    val_str = re.sub(r'[$,\s]', '', val_str)

    if not val_str:
        return None

    try:
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


def ingest_south_carolina(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest South Carolina payroll (no header, positional columns)."""
    config = STATE_CONFIGS['southcarolina']
    filepath = os.path.join(DATA_DIR, config['file'])

    log.info(f"\n{'='*60}")
    log.info(f"Processing {config['source_name']}")
    log.info(f"{'='*60}")

    # Read CSV without header (latin-1 encoding for special chars)
    df = pd.read_csv(filepath, header=None, low_memory=False, encoding='latin-1')
    log.info(f"  Loaded {len(df):,} rows")

    source_id = get_or_create_source(cursor, config['source_name'])
    location_id = get_or_create_location(cursor, config['state'])

    cols = config['columns']
    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.iloc[cols['title']]
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            salary = parse_salary(row.iloc[cols['salary']])
            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.iloc[cols['agency']])

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'state_payroll', source_id, company_id, location_id
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


def ingest_oregon(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest Oregon payroll."""
    config = STATE_CONFIGS['oregon']
    filepath = os.path.join(DATA_DIR, config['file'])

    log.info(f"\n{'='*60}")
    log.info(f"Processing {config['source_name']}")
    log.info(f"{'='*60}")

    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")
    log.info(f"  Columns: {list(df.columns)}")

    source_id = get_or_create_source(cursor, config['source_name'])
    location_id = get_or_create_location(cursor, config['state'])

    cols = config['columns']
    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get(cols['title'])
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            salary = parse_salary(row.get(cols['salary']))
            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.get(cols['agency']))

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'state_payroll', source_id, company_id, location_id
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


def ingest_massachusetts(cursor, normalizer: TitleNormalizer) -> int:
    """Ingest Massachusetts payroll."""
    config = STATE_CONFIGS['massachusetts']
    filepath = os.path.join(DATA_DIR, config['file'])

    log.info(f"\n{'='*60}")
    log.info(f"Processing {config['source_name']}")
    log.info(f"{'='*60}")

    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  Loaded {len(df):,} rows")
    log.info(f"  Columns: {list(df.columns)[:10]}...")

    source_id = get_or_create_source(cursor, config['source_name'])
    location_id = get_or_create_location(cursor, config['state'])

    cols = config['columns']
    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get(cols['title'])
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 2:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            salary = parse_salary(row.get(cols['salary']))
            if salary is None:
                skipped += 1
                continue

            company_id = get_or_create_company(cursor, row.get(cols['agency']))

            batch.append((
                normalized_title, salary, parse_result.seniority,
                parse_result.seniority_confidence, parse_result.title_confidence,
                'state_payroll', source_id, company_id, location_id
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
    """Main function to ingest new state payroll files."""
    log.info("=" * 60)
    log.info("NEW STATE PAYROLL INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    total_count = 0

    try:
        # South Carolina
        count = ingest_south_carolina(cursor, normalizer)
        conn.commit()
        total_count += count

        # Oregon
        count = ingest_oregon(cursor, normalizer)
        conn.commit()
        total_count += count

        # Massachusetts
        count = ingest_massachusetts(cursor, normalizer)
        conn.commit()
        total_count += count

        # Summary
        log.info("\n" + "=" * 60)
        log.info("INGESTION SUMMARY")
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

    log.info("\nNew state payroll ingestion complete!")


if __name__ == "__main__":
    main()
