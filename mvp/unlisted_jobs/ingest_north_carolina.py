#!/usr/bin/env python3
"""
North Carolina State Employee Data Ingestion
=============================================

Ingests North Carolina state employee salary data.
Source: State Of NC Salary Lookup.csv (~83K records)

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


def parse_salary(salary_str: str) -> Optional[float]:
    """Parse salary string like '$67,447.00' to float."""
    if not salary_str:
        return None
    try:
        cleaned = re.sub(r'[\$,\s"\']', '', salary_str)
        salary = float(cleaned)
        if salary < 15000 or salary > 1000000:
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


def get_or_create_company(cursor, agency: str) -> Optional[int]:
    if not agency:
        return None
    agency = str(agency).strip()
    company_name = f"State of North Carolina - {agency}"
    normalized = company_name.lower()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (company_name, normalized))
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
    log.info("NORTH CAROLINA STATE EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'North Carolina State Payroll')
    location_id = get_or_create_location(cursor, 'NC')

    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'state_payroll_new', 'State Of NC Salary Lookup.csv')

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    # Read CSV with UTF-8-BOM encoding
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            agency = row.get('Agency', '').strip()
            job_title = row.get('Job Title', '').strip()
            position_title = row.get('Position Title', '').strip()
            salary_str = row.get('Current Salary', '')

            # Use Job Title, fall back to Position Title
            title = job_title or position_title
            if not title:
                skipped += 1
                continue

            salary = parse_salary(salary_str)
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, agency)

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
