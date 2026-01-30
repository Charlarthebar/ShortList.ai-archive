#!/usr/bin/env python3
"""
NY State Authorities Salary Data Ingestion
==========================================

Ingests NY State Authorities employee salary data from data.ny.gov.
Includes MTA, hospitals, and other public authorities.

Author: ShortList.ai
Date: 2026-01-14
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ny_authorities_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

DATA_FILE = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/university_payroll/ny_state_authorities_salary.csv'


def parse_salary(value) -> Optional[float]:
    """Parse salary from various formats."""
    if not value or value.strip() == '':
        return None

    try:
        val_str = str(value).strip()
        val_str = re.sub(r'[$,\s]', '', val_str)

        if not val_str:
            return None

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
    """, (source_name, 'state_authority'))
    return cursor.fetchone()[0]


def get_or_create_company(cursor, authority_name: str, department: str = None) -> Optional[int]:
    """Get or create a company (authority/department)."""
    if not authority_name:
        return None

    # Combine authority and department for more specific company names
    company_name = authority_name.strip()
    if department and department.strip():
        company_name = f"{authority_name.strip()} - {department.strip()}"

    if not company_name:
        return None

    normalized = company_name.lower()[:500]  # Limit length

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id
    """, (company_name[:500], normalized))
    return cursor.fetchone()[0]


def get_or_create_location(cursor) -> int:
    """Get or create NY state location."""
    cursor.execute("""
        SELECT id FROM locations
        WHERE state = 'NY' AND city IS NULL AND country = 'United States'
    """)
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO locations (state, country)
        VALUES (%s, %s)
        RETURNING id
    """, ('NY', 'United States'))
    return cursor.fetchone()[0]


def main():
    """Main function to ingest NY State Authorities data."""
    log.info("=" * 60)
    log.info("NY STATE AUTHORITIES SALARY DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'NY State Authorities')
    location_id = get_or_create_location(cursor)

    # Get most recent fiscal year for each employee to avoid duplicates
    # We'll track by (authority, last_name, first_name, title) and keep most recent
    seen_employees = set()

    count = 0
    skipped = 0
    duplicate = 0
    batch = []
    batch_size = 1000

    log.info(f"Reading from {DATA_FILE}")

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for idx, row in enumerate(reader):
            try:
                title = row.get('Title', '').strip()
                if not title or len(title) < 2:
                    skipped += 1
                    continue

                # Check for duplicates (same person, same title)
                authority = row.get('Authority Name', '').strip()
                last_name = row.get('Last Name', '').strip()
                first_name = row.get('First Name', '').strip()

                employee_key = (authority, last_name, first_name, title)
                if employee_key in seen_employees:
                    duplicate += 1
                    continue
                seen_employees.add(employee_key)

                normalized_title = normalize_title(title)
                parse_result = normalizer.parse_title(title)

                # Use Base Annualized Salary
                salary = parse_salary(row.get('Base Annualized Salary'))
                if salary is None:
                    skipped += 1
                    continue

                department = row.get('Department', '').strip()
                company_id = get_or_create_company(cursor, authority, department)

                batch.append((
                    normalized_title, salary, parse_result.seniority,
                    parse_result.seniority_confidence, parse_result.title_confidence,
                    'state_authority', source_id, company_id, location_id
                ))

                if len(batch) >= batch_size:
                    cursor.executemany("""
                        INSERT INTO observed_jobs (
                            raw_title, salary_point, seniority, seniority_confidence,
                            title_confidence, source_type, source_id, company_id, location_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    count += len(batch)
                    batch = []

                    if count % 50000 == 0:
                        log.info(f"  Ingested {count:,} records...")

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
        conn.commit()
        count += len(batch)

    log.info(f"\nCompleted:")
    log.info(f"  Records ingested: {count:,}")
    log.info(f"  Duplicates skipped: {duplicate:,}")
    log.info(f"  Other skipped: {skipped:,}")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs")
    total = cursor.fetchone()[0]
    log.info(f"  Total records in database: {total:,}")

    db.release_connection(conn)
    log.info("\nNY State Authorities ingestion complete!")


if __name__ == "__main__":
    main()
