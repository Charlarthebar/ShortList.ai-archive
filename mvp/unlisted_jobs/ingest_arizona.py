#!/usr/bin/env python3
"""
Arizona City Employee Data Ingestion
=====================================

Ingests Arizona city employee salary data.
Source: employee-compensation-report-2025.csv (~16.5K records)

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
    """Parse salary string like '110,190.59' to float."""
    if not salary_str:
        return None
    try:
        cleaned = re.sub(r'[\$,\s"\']', '', str(salary_str))
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
    """, (source_name, 'city_payroll'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, city_name: str, department: str) -> Optional[int]:
    if not department:
        company_name = city_name
    else:
        department = str(department).strip()
        company_name = f"{city_name} - {department}"
    normalized = company_name.lower()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (company_name[:500], normalized[:500]))
    return cursor.fetchone()[0]


def get_or_create_location(cursor, city: str, state: str) -> int:
    cursor.execute("""
        SELECT id FROM locations WHERE state = %s AND city = %s AND country = 'United States'
    """, (state, city))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO locations (state, city, country) VALUES (%s, %s, 'United States') RETURNING id
    """, (state, city))
    return cursor.fetchone()[0]


def main():
    log.info("=" * 60)
    log.info("ARIZONA CITY EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    # Assuming this is Phoenix based on department names
    source_id = get_or_create_source(cursor, 'Phoenix AZ City Payroll')
    location_id = get_or_create_location(cursor, 'Phoenix', 'AZ')

    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'state_payroll_new', 'employee-compensation-report-2025.csv')

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            department = row.get('Department', '').strip()
            title = row.get('Job Title', '').strip()
            salary_str = row.get('Compensation', '')

            if not title:
                skipped += 1
                continue

            salary = parse_salary(salary_str)
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, 'City of Phoenix', department)

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
