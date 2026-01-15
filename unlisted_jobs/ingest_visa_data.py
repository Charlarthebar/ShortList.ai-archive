#!/usr/bin/env python3
"""
H-1B and PERM Visa Data Ingestion
==================================

Ingests H-1B LCA and PERM disclosure data from the Department of Labor
into the jobs database.

Data sources:
- H-1B LCA: https://www.dol.gov/agencies/eta/foreign-labor/performance
- PERM: https://www.dol.gov/agencies/eta/foreign-labor/performance

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
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
        logging.FileHandler('visa_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


def normalize_salary(wage_from, wage_to, wage_unit: str) -> Optional[float]:
    """Convert wage to annual salary."""
    try:
        # Use wage_from as the base (minimum offered)
        if pd.isna(wage_from):
            return None

        wage = float(wage_from)

        if pd.isna(wage_unit):
            # Assume annual if not specified and wage > 1000
            return wage if wage > 1000 else None

        unit = str(wage_unit).upper().strip()

        if unit in ['YEAR', 'YEARLY', 'ANNUAL', 'Y']:
            # Cap at $5M - anything higher is likely data error
            return min(wage, 5000000) if wage > 1000 else None
        elif unit in ['MONTH', 'MONTHLY', 'M']:
            return wage * 12
        elif unit in ['BI-WEEKLY', 'BIWEEKLY', 'BI-WEEK', 'B']:
            return wage * 26
        elif unit in ['WEEK', 'WEEKLY', 'W']:
            return wage * 52
        elif unit in ['HOUR', 'HOURLY', 'H']:
            # If "hourly" rate is > $500/hr, it's likely an annual salary miscategorized
            if wage > 500:
                # Treat as annual salary (cap at $5M)
                return min(wage, 5000000)
            else:
                return wage * 2080  # 40 hours/week * 52 weeks
        else:
            # Unknown unit, assume annual if reasonable
            return wage if wage > 10000 else None
    except (ValueError, TypeError):
        return None


def check_existing_data(cursor, source_name: str) -> int:
    """Check how many records already exist for this source."""
    cursor.execute("""
        SELECT COUNT(*) FROM observed_jobs o
        JOIN sources s ON o.source_id = s.id
        WHERE s.name = %s
    """, (source_name,))
    return cursor.fetchone()[0]


def ingest_h1b_lca(file_path: str, db: DatabaseManager, normalizer: TitleNormalizer):
    """Ingest H-1B LCA disclosure data."""
    log.info(f"Reading H-1B LCA file: {file_path}")

    # Read Excel file
    df = pd.read_excel(file_path)
    log.info(f"Loaded {len(df):,} H-1B LCA records")

    # Filter to certified cases only
    df = df[df['CASE_STATUS'] == 'Certified']
    log.info(f"Filtered to {len(df):,} certified cases")

    conn = db.get_connection()
    cursor = conn.cursor()

    # Check for existing data
    existing = check_existing_data(cursor, 'h1b_visa')
    if existing > 0:
        log.warning(f"Found {existing:,} existing H-1B visa records. Skipping ingestion.")
        db.release_connection(conn)
        return

    # Get or create source (reuse existing source name for consistency)
    cursor.execute("SELECT id FROM sources WHERE name = %s", ('h1b_visa',))
    row = cursor.fetchone()
    if row:
        source_id = row[0]
    else:
        cursor.execute("""
            INSERT INTO sources (name, type)
            VALUES (%s, %s)
            RETURNING id
        """, ('h1b_visa', 'visa'))
        source_id = cursor.fetchone()[0]

    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():
        try:
            # Extract fields
            job_title = str(row.get('JOB_TITLE', '')) if pd.notna(row.get('JOB_TITLE')) else None
            employer_name = str(row.get('EMPLOYER_NAME', '')) if pd.notna(row.get('EMPLOYER_NAME')) else None
            city = str(row.get('WORKSITE_CITY', '')) if pd.notna(row.get('WORKSITE_CITY')) else None
            state = str(row.get('WORKSITE_STATE', '')) if pd.notna(row.get('WORKSITE_STATE')) else None

            if not job_title or not employer_name:
                skipped += 1
                continue

            # Normalize salary
            salary = normalize_salary(
                row.get('WAGE_RATE_OF_PAY_FROM'),
                row.get('WAGE_RATE_OF_PAY_TO'),
                row.get('WAGE_UNIT_OF_PAY')
            )

            # Parse title
            result = normalizer.parse_title(job_title)

            # Get or create company
            normalized_name = employer_name.upper().strip()
            cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized_name,))
            row = cursor.fetchone()
            if row:
                company_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO companies (name, normalized_name)
                    VALUES (%s, %s)
                    RETURNING id
                """, (employer_name, normalized_name))
                company_id = cursor.fetchone()[0]

            # Get or create location
            loc_city = city.strip() if city else 'Unknown'
            loc_state = state.strip() if state else 'Unknown'
            cursor.execute("""
                SELECT id FROM locations
                WHERE city = %s AND state = %s AND country = 'USA'
            """, (loc_city, loc_state))
            row = cursor.fetchone()
            if row:
                location_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO locations (city, state, country)
                    VALUES (%s, %s, 'USA')
                    RETURNING id
                """, (loc_city, loc_state))
                location_id = cursor.fetchone()[0]

            # Insert job
            cursor.execute("""
                INSERT INTO observed_jobs (
                    raw_title, canonical_role_id, company_id, location_id,
                    source_id, seniority, seniority_confidence, title_confidence,
                    salary_point, source_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job_title,
                result.canonical_role_id,
                company_id,
                location_id,
                source_id,
                result.seniority,
                result.seniority_confidence,
                result.title_confidence,
                salary,
                'visa'
            ))

            inserted += 1

            if inserted % 10000 == 0:
                conn.commit()
                log.info(f"  Inserted {inserted:,} H-1B records...")

        except Exception as e:
            conn.rollback()  # Rollback to recover from error
            log.warning(f"Error processing row {idx}: {e}")
            skipped += 1
            continue

    conn.commit()
    db.release_connection(conn)
    log.info(f"H-1B LCA ingestion complete: {inserted:,} inserted, {skipped:,} skipped")


def ingest_perm(file_path: str, db: DatabaseManager, normalizer: TitleNormalizer):
    """Ingest PERM disclosure data."""
    log.info(f"Reading PERM file: {file_path}")

    # Read Excel file
    df = pd.read_excel(file_path)
    log.info(f"Loaded {len(df):,} PERM records")

    # Filter to certified cases only
    df = df[df['CASE_STATUS'].isin(['Certified', 'Certified-Expired'])]
    log.info(f"Filtered to {len(df):,} certified cases")

    conn = db.get_connection()
    cursor = conn.cursor()

    # Check for existing data
    existing = check_existing_data(cursor, 'perm_visa')
    if existing > 0:
        log.warning(f"Found {existing:,} existing PERM visa records. Skipping ingestion.")
        db.release_connection(conn)
        return

    # Get or create source (reuse existing source name for consistency)
    cursor.execute("SELECT id FROM sources WHERE name = %s", ('perm_visa',))
    row = cursor.fetchone()
    if row:
        source_id = row[0]
    else:
        cursor.execute("""
            INSERT INTO sources (name, type)
            VALUES (%s, %s)
            RETURNING id
        """, ('perm_visa', 'visa'))
        source_id = cursor.fetchone()[0]

    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():
        try:
            # Extract fields - PERM has different column names
            job_title = str(row.get('JOB_TITLE', '')) if pd.notna(row.get('JOB_TITLE')) else None

            # Try different employer name columns
            employer_name = None
            for col in ['EMP_BUSINESS_NAME', 'EMPLOYER_NAME', 'EMPLOYER_BUSINESS_NAME']:
                if col in row and pd.notna(row.get(col)):
                    employer_name = str(row.get(col))
                    break

            city = str(row.get('PRIMARY_WORKSITE_CITY', '')) if pd.notna(row.get('PRIMARY_WORKSITE_CITY')) else None
            state = str(row.get('PRIMARY_WORKSITE_STATE', '')) if pd.notna(row.get('PRIMARY_WORKSITE_STATE')) else None

            if not job_title or not employer_name:
                skipped += 1
                continue

            # Normalize salary
            salary = normalize_salary(
                row.get('JOB_OPP_WAGE_FROM'),
                row.get('JOB_OPP_WAGE_TO'),
                row.get('JOB_OPP_WAGE_PER')
            )

            # Parse title
            result = normalizer.parse_title(job_title)

            # Get or create company
            normalized_name = employer_name.upper().strip()
            cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized_name,))
            row = cursor.fetchone()
            if row:
                company_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO companies (name, normalized_name)
                    VALUES (%s, %s)
                    RETURNING id
                """, (employer_name, normalized_name))
                company_id = cursor.fetchone()[0]

            # Get or create location
            loc_city = city.strip() if city else 'Unknown'
            loc_state = state.strip() if state else 'Unknown'
            cursor.execute("""
                SELECT id FROM locations
                WHERE city = %s AND state = %s AND country = 'USA'
            """, (loc_city, loc_state))
            row = cursor.fetchone()
            if row:
                location_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO locations (city, state, country)
                    VALUES (%s, %s, 'USA')
                    RETURNING id
                """, (loc_city, loc_state))
                location_id = cursor.fetchone()[0]

            # Insert job
            cursor.execute("""
                INSERT INTO observed_jobs (
                    raw_title, canonical_role_id, company_id, location_id,
                    source_id, seniority, seniority_confidence, title_confidence,
                    salary_point, source_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job_title,
                result.canonical_role_id,
                company_id,
                location_id,
                source_id,
                result.seniority,
                result.seniority_confidence,
                result.title_confidence,
                salary,
                'visa'
            ))

            inserted += 1

            if inserted % 10000 == 0:
                conn.commit()
                log.info(f"  Inserted {inserted:,} PERM records...")

        except Exception as e:
            conn.rollback()  # Rollback to recover from error
            log.warning(f"Error processing row {idx}: {e}")
            skipped += 1
            continue

    conn.commit()
    db.release_connection(conn)
    log.info(f"PERM ingestion complete: {inserted:,} inserted, {skipped:,} skipped")


def main():
    """Main ingestion function."""
    log.info("="*60)
    log.info("VISA DATA INGESTION")
    log.info("="*60)

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    normalizer = TitleNormalizer(db)

    data_dir = Path(__file__).parent / 'data' / 'visa'

    # Ingest H-1B LCA
    h1b_file = data_dir / 'LCA_Disclosure_Data_FY2025_Q4.xlsx'
    if h1b_file.exists():
        ingest_h1b_lca(str(h1b_file), db, normalizer)
    else:
        log.warning(f"H-1B file not found: {h1b_file}")

    # Ingest PERM
    perm_file = data_dir / 'PERM_Disclosure_Data_FY2025_Q4.xlsx'
    if perm_file.exists():
        ingest_perm(str(perm_file), db, normalizer)
    else:
        log.warning(f"PERM file not found: {perm_file}")

    # Summary
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.name, COUNT(*)
        FROM observed_jobs o
        JOIN sources s ON o.source_id = s.id
        WHERE o.source_type = 'visa'
        GROUP BY s.name
        ORDER BY COUNT(*) DESC
    """)

    log.info("\n" + "="*60)
    log.info("VISA DATA SUMMARY")
    log.info("="*60)
    for name, cnt in cursor.fetchall():
        log.info(f"  {name}: {cnt:,}")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs WHERE source_type = 'visa'")
    total = cursor.fetchone()[0]
    log.info(f"\n  TOTAL VISA RECORDS: {total:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
