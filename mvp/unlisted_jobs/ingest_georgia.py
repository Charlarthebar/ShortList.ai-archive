#!/usr/bin/env python3
"""
Georgia State Employee Data Ingestion
======================================

Ingests Georgia state employee salary data from Open.Georgia.gov export.
2025 salary and travel data for state employees and local boards of education.

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import logging
import csv
from typing import Optional
import re

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer
from normalize_titles import normalize_title

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def parse_salary(salary_str) -> Optional[float]:
    """Parse salary - Georgia format has quotes around numbers."""
    if not salary_str:
        return None

    try:
        # Remove quotes and clean up
        salary_str = str(salary_str).strip().strip("'\"").replace(',', '').replace('$', '')
        salary = float(salary_str)

        if salary < 15000 or salary > 5000000:
            return None

        return salary
    except (ValueError, TypeError):
        return None


def get_or_create_source(cursor, source_name: str) -> int:
    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO sources (name, type) VALUES (%s, %s) RETURNING id
    """, (source_name, 'state_payroll'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, org: str) -> Optional[int]:
    if not org:
        return None

    org = str(org).strip()
    normalized = org.lower()[:500]

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (org[:500], normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, state: str) -> int:
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
    log.info("GEORGIA STATE EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'Georgia State Payroll')
    location_id = get_or_create_location(cursor, 'GA')

    filepath = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll_new/SalaryTravelDataExport2025.txt'

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='latin-1') as f:
        reader = csv.DictReader(f)

        for row in reader:
            title = row.get('TITLE', '').strip()
            if not title:
                skipped += 1
                continue

            salary = parse_salary(row.get('SALARY'))
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, row.get('ORGANIZATION'))

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
                if count % 50000 == 0:
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
