#!/usr/bin/env python3
"""
Historical H-1B and PERM Visa Data Ingestion
=============================================

Ingests historical H-1B LCA and PERM disclosure data from the Department of Labor
into the jobs database.

Data sources:
- H-1B LCA FY2019-2024: https://www.dol.gov/agencies/eta/foreign-labor/performance
- PERM FY2023-2024: https://www.dol.gov/agencies/eta/foreign-labor/performance

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List
import glob

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
        logging.FileHandler('visa_historical_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'visa', 'historical')


def normalize_salary(wage_from, wage_to, wage_unit: str) -> Optional[float]:
    """Convert wage to annual salary."""
    try:
        if pd.isna(wage_from):
            return None

        wage = float(wage_from)

        if pd.isna(wage_unit):
            return wage if wage > 1000 else None

        unit = str(wage_unit).upper().strip()

        if unit in ['YEAR', 'YEARLY', 'ANNUAL', 'Y']:
            return min(wage, 5000000) if wage > 1000 else None
        elif unit in ['MONTH', 'MONTHLY', 'M']:
            return wage * 12
        elif unit in ['BI-WEEKLY', 'BIWEEKLY', 'BI-WEEK', 'B']:
            return wage * 26
        elif unit in ['WEEK', 'WEEKLY', 'W']:
            return wage * 52
        elif unit in ['HOUR', 'HOURLY', 'H']:
            if wage > 500:
                return min(wage, 5000000)
            else:
                return wage * 2080
        else:
            return wage if wage > 10000 else None
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
    """, (source_name, 'visa'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, company_name: str) -> Optional[int]:
    """Get or create a company."""
    if not company_name or pd.isna(company_name):
        return None

    company_name = str(company_name).strip()
    if not company_name:
        return None

    normalized = company_name.lower()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id
    """, (company_name, normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, city: str, state: str) -> Optional[int]:
    """Get or create a location."""
    city = str(city).strip() if city and not pd.isna(city) else None
    state = str(state).strip() if state and not pd.isna(state) else None

    if not city and not state:
        return None

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


def find_column(df, candidates: List[str]) -> Optional[str]:
    """Find a column by trying multiple candidate names."""
    for col in candidates:
        if col in df.columns:
            return col
        # Case-insensitive search
        for actual_col in df.columns:
            if actual_col.lower() == col.lower():
                return actual_col
    return None


def ingest_h1b_file(cursor, filepath: str, normalizer: TitleNormalizer, source_id: int) -> int:
    """Ingest a single H-1B LCA file."""
    log.info(f"Processing H-1B file: {os.path.basename(filepath)}")

    try:
        df = pd.read_excel(filepath, engine='openpyxl')
    except Exception as e:
        log.error(f"Failed to read {filepath}: {e}")
        return 0

    log.info(f"  Loaded {len(df):,} rows")
    log.info(f"  Columns: {list(df.columns)[:10]}...")

    # Find the status column
    status_col = find_column(df, ['CASE_STATUS', 'STATUS', 'LCA_CASE_STATUS'])
    if status_col:
        # Filter to certified cases
        df = df[df[status_col].str.upper().str.contains('CERTIFIED', na=False)]
        log.info(f"  Filtered to {len(df):,} certified cases")

    # Find relevant columns
    title_col = find_column(df, ['JOB_TITLE', 'LCA_CASE_JOB_TITLE', 'TITLE'])
    employer_col = find_column(df, ['EMPLOYER_NAME', 'LCA_CASE_EMPLOYER_NAME', 'EMPLOYER'])
    city_col = find_column(df, ['WORKSITE_CITY', 'LCA_CASE_WORKLOC1_CITY', 'WORKSITE_CITY_1'])
    state_col = find_column(df, ['WORKSITE_STATE', 'LCA_CASE_WORKLOC1_STATE', 'WORKSITE_STATE_1'])
    wage_from_col = find_column(df, ['WAGE_RATE_OF_PAY_FROM', 'LCA_CASE_WAGE_RATE_FROM', 'WAGE_RATE_OF_PAY_FROM_1'])
    wage_to_col = find_column(df, ['WAGE_RATE_OF_PAY_TO', 'LCA_CASE_WAGE_RATE_TO', 'WAGE_RATE_OF_PAY_TO_1'])
    wage_unit_col = find_column(df, ['WAGE_UNIT_OF_PAY', 'LCA_CASE_WAGE_RATE_UNIT', 'WAGE_UNIT_OF_PAY_1'])

    if not title_col:
        log.warning(f"  No title column found, skipping file")
        return 0

    log.info(f"  Using columns: title={title_col}, employer={employer_col}")

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get(title_col)
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 3:
                skipped += 1
                continue

            # Normalize title
            normalized_title = normalize_title(title)

            # Parse for seniority
            parse_result = normalizer.parse_title(title)
            seniority = parse_result.seniority
            seniority_conf = parse_result.seniority_confidence
            title_conf = parse_result.title_confidence

            # Get salary
            salary = None
            if wage_from_col:
                salary = normalize_salary(
                    row.get(wage_from_col),
                    row.get(wage_to_col) if wage_to_col else None,
                    row.get(wage_unit_col) if wage_unit_col else None
                )

            # Skip if no salary (we want quality data)
            if salary is None:
                skipped += 1
                continue

            # Get company
            company_id = None
            if employer_col:
                company_id = get_or_create_company(cursor, row.get(employer_col))

            # Get location
            location_id = None
            if city_col or state_col:
                city = row.get(city_col) if city_col else None
                state = row.get(state_col) if state_col else None
                location_id = get_or_create_location(cursor, city, state)

            batch.append((
                normalized_title,
                salary,
                seniority,
                seniority_conf,
                title_conf,
                'visa',
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

    log.info(f"  Completed: {count:,} records, {skipped:,} skipped")
    return count


def ingest_perm_file(cursor, filepath: str, normalizer: TitleNormalizer, source_id: int) -> int:
    """Ingest a single PERM file."""
    log.info(f"Processing PERM file: {os.path.basename(filepath)}")

    try:
        df = pd.read_excel(filepath, engine='openpyxl')
    except Exception as e:
        log.error(f"Failed to read {filepath}: {e}")
        return 0

    log.info(f"  Loaded {len(df):,} rows")
    log.info(f"  Columns: {list(df.columns)[:10]}...")

    # Find status column and filter
    status_col = find_column(df, ['CASE_STATUS', 'STATUS'])
    if status_col:
        df = df[df[status_col].str.upper().str.contains('CERTIFIED', na=False)]
        log.info(f"  Filtered to {len(df):,} certified cases")

    # Find relevant columns (PERM uses different naming)
    title_col = find_column(df, ['JOB_INFO_JOB_TITLE', 'JOB_TITLE', 'PW_JOB_TITLE'])
    employer_col = find_column(df, ['EMPLOYER_NAME', 'EMP_BUSINESS_NAME', 'EMPLOYER_BUSINESS_NAME'])
    city_col = find_column(df, ['WORKSITE_CITY', 'JOB_INFO_WORK_CITY', 'PRIMARY_WORKSITE_CITY'])
    state_col = find_column(df, ['WORKSITE_STATE', 'JOB_INFO_WORK_STATE', 'PRIMARY_WORKSITE_STATE'])
    wage_from_col = find_column(df, ['PW_WAGE_1', 'WAGE_OFFER_FROM_9089', 'JOB_INFO_OFFERED_WAGE_FROM'])
    wage_to_col = find_column(df, ['PW_WAGE_2', 'WAGE_OFFER_TO_9089', 'JOB_INFO_OFFERED_WAGE_TO'])
    wage_unit_col = find_column(df, ['PW_UNIT_OF_PAY_1', 'WAGE_OFFER_UNIT_OF_PAY_9089', 'JOB_INFO_OFFERED_WAGE_UNIT'])

    if not title_col:
        log.warning(f"  No title column found, skipping file")
        return 0

    log.info(f"  Using columns: title={title_col}, employer={employer_col}")

    count = 0
    skipped = 0
    batch = []
    batch_size = 1000

    for idx, row in df.iterrows():
        try:
            title = row.get(title_col)
            if pd.isna(title) or not title:
                skipped += 1
                continue

            title = str(title).strip()
            if len(title) < 3:
                skipped += 1
                continue

            # Normalize title
            normalized_title = normalize_title(title)

            # Parse for seniority
            parse_result = normalizer.parse_title(title)
            seniority = parse_result.seniority
            seniority_conf = parse_result.seniority_confidence
            title_conf = parse_result.title_confidence

            # Get salary
            salary = None
            if wage_from_col:
                salary = normalize_salary(
                    row.get(wage_from_col),
                    row.get(wage_to_col) if wage_to_col else None,
                    row.get(wage_unit_col) if wage_unit_col else None
                )

            if salary is None:
                skipped += 1
                continue

            # Get company
            company_id = None
            if employer_col:
                company_id = get_or_create_company(cursor, row.get(employer_col))

            # Get location
            location_id = None
            if city_col or state_col:
                city = row.get(city_col) if city_col else None
                state = row.get(state_col) if state_col else None
                location_id = get_or_create_location(cursor, city, state)

            batch.append((
                normalized_title,
                salary,
                seniority,
                seniority_conf,
                title_conf,
                'visa',
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

    log.info(f"  Completed: {count:,} records, {skipped:,} skipped")
    return count


def main():
    """Main function to ingest all historical visa data."""
    log.info("=" * 60)
    log.info("HISTORICAL VISA DATA INGESTION")
    log.info("=" * 60)

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    total_h1b = 0
    total_perm = 0

    try:
        # Get or create source for historical H-1B
        h1b_source_id = get_or_create_source(cursor, 'h1b_lca_historical')
        perm_source_id = get_or_create_source(cursor, 'perm_historical')
        conn.commit()

        # Find and process all H-1B LCA files
        h1b_files = sorted(glob.glob(os.path.join(DATA_DIR, 'LCA_*.xlsx')))
        log.info(f"\nFound {len(h1b_files)} H-1B LCA files")

        for filepath in h1b_files:
            count = ingest_h1b_file(cursor, filepath, normalizer, h1b_source_id)
            conn.commit()
            total_h1b += count

        # Find and process all PERM files
        perm_files = sorted(glob.glob(os.path.join(DATA_DIR, 'PERM_*.xlsx')))
        log.info(f"\nFound {len(perm_files)} PERM files")

        for filepath in perm_files:
            count = ingest_perm_file(cursor, filepath, normalizer, perm_source_id)
            conn.commit()
            total_perm += count

        # Summary
        log.info("\n" + "=" * 60)
        log.info("HISTORICAL VISA INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  H-1B LCA records ingested: {total_h1b:,}")
        log.info(f"  PERM records ingested: {total_perm:,}")
        log.info(f"  Total visa records: {total_h1b + total_perm:,}")

        # Show total database count
        cursor.execute("SELECT COUNT(*) FROM observed_jobs")
        total = cursor.fetchone()[0]
        log.info(f"\n  Total records in database: {total:,}")

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nHistorical visa ingestion complete!")


if __name__ == "__main__":
    main()
