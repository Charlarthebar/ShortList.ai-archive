#!/usr/bin/env python3
"""
Minnesota State Employee Data Ingestion
========================================

Ingests Minnesota state employee salary data from FY2025 Excel file.
Joins HR Info (job titles) with Earnings (total wages).

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import logging
import pandas as pd
from typing import Optional

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer
from normalize_titles import normalize_title

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def get_or_create_source(cursor, source_name: str) -> int:
    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO sources (name, type) VALUES (%s, %s) RETURNING id
    """, (source_name, 'state_payroll'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, agency: str) -> Optional[int]:
    if not agency or pd.isna(agency):
        return None

    agency = str(agency).strip()
    normalized = agency.lower()[:500]

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (agency[:500], normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, state: str, county: str = None) -> int:
    if county and not pd.isna(county):
        cursor.execute("""
            SELECT id FROM locations WHERE state = %s AND city = %s AND country = 'United States'
        """, (state, county))
        result = cursor.fetchone()
        if result:
            return result[0]
        cursor.execute("""
            INSERT INTO locations (state, city, country) VALUES (%s, %s, 'United States') RETURNING id
        """, (state, county))
        return cursor.fetchone()[0]
    else:
        cursor.execute("""
            SELECT id FROM locations WHERE state = %s AND city IS NULL AND country = 'United States'
        """, (state,))
        result = cursor.fetchone()
        if result:
            return result[0]
        cursor.execute("""
            INSERT INTO locations (state, country) VALUES (%s, 'United States') RETURNING id
        """, (state,))
        return cursor.fetchone()[0]


def main():
    log.info("=" * 60)
    log.info("MINNESOTA STATE EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'Minnesota State Payroll')
    default_location_id = get_or_create_location(cursor, 'MN')

    filepath = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll_new/fiscal-year-2025.xlsx'

    log.info("Loading Excel file...")
    xl = pd.ExcelFile(filepath)

    df_hr = pd.read_excel(xl, sheet_name='FY25 HR INFO')
    df_earn = pd.read_excel(xl, sheet_name='FY25 EARNINGS')

    log.info(f"HR Info rows: {len(df_hr):,}")
    log.info(f"Earnings rows: {len(df_earn):,}")

    # Join on TEMPORARY_ID - keep only records with earnings
    # First deduplicate HR by taking first record per TEMPORARY_ID
    df_hr_dedup = df_hr.drop_duplicates(subset=['TEMPORARY_ID'], keep='first')

    df = df_hr_dedup.merge(df_earn, on='TEMPORARY_ID', how='inner')
    log.info(f"Merged rows: {len(df):,}")

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    for idx, row in df.iterrows():
        title = row.get('JOB_TITLE')
        if pd.isna(title) or not str(title).strip():
            skipped += 1
            continue
        title = str(title).strip()

        salary = row.get('TOTAL_WAGES')
        if pd.isna(salary) or salary < 15000 or salary > 5000000:
            skipped += 1
            continue

        normalized_title = normalize_title(title)
        parse_result = normalizer.parse_title(title)

        company_id = get_or_create_company(cursor, row.get('AGENCY_NAME'))

        county = row.get('LOCATION_COUNTY_NAME')
        if county and not pd.isna(county):
            location_id = get_or_create_location(cursor, 'MN', str(county).strip())
        else:
            location_id = default_location_id

        batch.append((
            normalized_title, float(salary), parse_result.seniority,
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
            log.info(f"  Ingested {count:,} records...")

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    conn.commit()

    log.info(f"\nCompleted: {count:,} records ingested, {skipped:,} skipped")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs")
    total = cursor.fetchone()[0]
    log.info(f"Total records in database: {total:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
