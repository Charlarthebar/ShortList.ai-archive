#!/usr/bin/env python3
"""
Michigan Scraped Data Ingestion
===============================

Ingests Michigan state employee salary data scraped from Mackinac Center.

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import logging
import csv
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


def get_or_create_company(cursor, employer: str) -> Optional[int]:
    if not employer:
        return None
    employer = str(employer).strip()
    normalized = employer.lower()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO companies (name, normalized_name) VALUES (%s, %s) RETURNING id
    """, (employer, normalized))
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
    log.info("MICHIGAN SCRAPED DATA INGESTION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    normalizer = TitleNormalizer(db)

    source_id = get_or_create_source(cursor, 'Michigan State Payroll (Mackinac)')
    location_id = get_or_create_location(cursor, 'MI')

    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'michigan', 'michigan_state.csv')

    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        return

    count = 0
    skipped = 0
    batch = []
    batch_size = 500

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            position = row.get('position', '').strip()
            if not position:
                skipped += 1
                continue

            try:
                salary = float(row.get('salary', 0))
                if salary < 15000 or salary > 5000000:
                    skipped += 1
                    continue
            except (ValueError, TypeError):
                skipped += 1
                continue

            normalized_title = normalize_title(position)
            parse_result = normalizer.parse_title(position)

            company_id = get_or_create_company(cursor, row.get('employer'))

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
