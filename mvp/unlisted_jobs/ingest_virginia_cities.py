#!/usr/bin/env python3
"""
Virginia City Employee Data Ingestion
======================================

Ingests Virginia city employee salary data.
File 1: Employee_Salaries.csv - appears to be state or Richmond data (~7.7K records)
File 2: Employee_Salaries_20260115.csv - Norfolk city data (~4.9K records)

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
    """Parse salary string to float."""
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


def ingest_file1(db, cursor, normalizer):
    """Ingest Employee_Salaries.csv (Virginia state/Richmond style)"""
    log.info("Processing Employee_Salaries.csv...")

    # This appears to be Virginia state data based on AGR, AUD codes
    source_id = get_or_create_source(cursor, 'Virginia State Payroll')
    location_id = get_or_create_location(cursor, 'Richmond', 'VA')

    filepath = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll_new/Employee_Salaries.csv'

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            department = row.get('Department', '').strip()
            title = row.get('Position_Title', '').strip()
            salary_str = row.get('Salary', '')

            if not title:
                skipped += 1
                continue

            salary = parse_salary(salary_str)
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, 'State of Virginia', department)

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
                log.info(f"  File 1: Ingested {count:,} records...")

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"File 1 complete: {count:,} records, {skipped:,} skipped")
    return count, skipped


def ingest_file2(db, cursor, normalizer):
    """Ingest Employee_Salaries_20260115.csv (Norfolk city)"""
    log.info("Processing Employee_Salaries_20260115.csv (Norfolk)...")

    source_id = get_or_create_source(cursor, 'Norfolk VA City Payroll')
    location_id = get_or_create_location(cursor, 'Norfolk', 'VA')

    filepath = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/state_payroll_new/Employee_Salaries_20260115.csv'

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            department = row.get('Department  ', '').strip()  # Note: has trailing spaces in header
            title = row.get('Position Title', '').strip()
            salary_str = row.get('Base Salary', '')

            if not title:
                skipped += 1
                continue

            salary = parse_salary(salary_str)
            if not salary:
                skipped += 1
                continue

            normalized_title = normalize_title(title)
            parse_result = normalizer.parse_title(title)

            company_id = get_or_create_company(cursor, 'City of Norfolk', department)

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
                log.info(f"  File 2: Ingested {count:,} records...")

    if batch:
        cursor.executemany("""
            INSERT INTO observed_jobs (
                raw_title, salary_point, seniority, seniority_confidence,
                title_confidence, source_type, source_id, company_id, location_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        count += len(batch)

    log.info(f"File 2 complete: {count:,} records, {skipped:,} skipped")
    return count, skipped


def main():
    log.info("=" * 60)
    log.info("VIRGINIA CITY/STATE EMPLOYEE DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    total_count = 0
    total_skipped = 0

    # File 1: Virginia state data
    c1, s1 = ingest_file1(db, cursor, normalizer)
    total_count += c1
    total_skipped += s1

    # File 2: Norfolk city data
    c2, s2 = ingest_file2(db, cursor, normalizer)
    total_count += c2
    total_skipped += s2

    conn.commit()

    log.info(f"\nTotal: {total_count:,} records ingested, {total_skipped:,} skipped")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs")
    total = cursor.fetchone()[0]
    log.info(f"Total records in database: {total:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
