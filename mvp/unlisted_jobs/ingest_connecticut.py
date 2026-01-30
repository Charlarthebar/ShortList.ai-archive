#!/usr/bin/env python3
"""
Connecticut State Employee Data Ingestion
==========================================

Ingests Connecticut state employee salary data from data.ct.gov.
Data is bi-weekly paycheck records - we use Annual Rate and deduplicate.

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
    """Parse salary."""
    if not salary_str:
        return None

    try:
        salary_str = str(salary_str).strip().replace(',', '').replace('$', '')
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


def get_or_create_company(cursor, agency: str, dept: str = None) -> Optional[int]:
    if not agency:
        return None

    company_name = agency.strip()
    if dept and dept.strip():
        company_name = f"{agency.strip()} - {dept.strip()}"

    normalized = company_name.lower()[:500]

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (company_name[:500], normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, state: str, city: str = None) -> int:
    if city:
        cursor.execute("""
            SELECT id FROM locations WHERE state = %s AND city = %s AND country = 'United States'
        """, (state, city))
    else:
        cursor.execute("""
            SELECT id FROM locations WHERE state = %s AND city IS NULL AND country = 'United States'
        """, (state,))

    result = cursor.fetchone()
    if result:
        return result[0]

    if city:
        cursor.execute("""
            INSERT INTO locations (state, city, country) VALUES (%s, %s, 'United States') RETURNING id
        """, (state, city))
    else:
        cursor.execute("""
            INSERT INTO locations (state, country) VALUES (%s, 'United States') RETURNING id
        """, (state,))
    return cursor.fetchone()[0]


def main():
    log.info("=" * 60)
    log.info("CONNECTICUT STATE EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'Connecticut State Payroll')
    default_location_id = get_or_create_location(cursor, 'CT')

    filepath = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll_new/connecticut.csv'

    # Deduplicate by employee ID - keep first occurrence per employee
    # (they all have same Annual Rate anyway)
    seen_employees = set()

    count = 0
    skipped = 0
    duplicate = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            empl_id = row.get('EmplId-Empl Rcd', '').strip()

            # Skip if we've already seen this employee
            if empl_id in seen_employees:
                duplicate += 1
                continue
            seen_employees.add(empl_id)

            title = row.get('Job Cd Descr', '').strip()
            if not title:
                skipped += 1
                continue

            salary = parse_salary(row.get('Annual Rate'))
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, row.get('Agency'), row.get('Department'))

            city = row.get('City', '').strip()
            if city:
                location_id = get_or_create_location(cursor, 'CT', city)
            else:
                location_id = default_location_id

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
                if count % 10000 == 0:
                    log.info(f"  Ingested {count:,} records... ({duplicate:,} duplicates)")

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    conn.commit()

    log.info(f"\nCompleted: {count:,} records ingested")
    log.info(f"  Duplicates skipped: {duplicate:,}")
    log.info(f"  Other skipped: {skipped:,}")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs")
    total = cursor.fetchone()[0]
    log.info(f"Total records in database: {total:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
