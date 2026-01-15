#!/usr/bin/env python3
"""
University Salary Data Ingestion
=================================

Ingests public university salary data from California Public Pay
(UC and CSU systems) into the jobs database.

Data sources:
- https://publicpay.ca.gov/RawExport/

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('university_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


# University system configurations
UNIVERSITY_CONFIGS = {
    'UC': {
        'file': '2024_UniversityOfCalifornia.csv',
        'source_name': 'uc_salaries_2024',
        'employer_column': 'EmployerName',
        'title_column': 'Position',
        'salary_column': 'TotalWages',
        'department_column': 'DepartmentOrSubdivision',
    },
    'CSU': {
        'file': '2024_CaliforniaStateUniversity.csv',
        'source_name': 'csu_salaries_2024',
        'employer_column': 'EmployerName',
        'title_column': 'Position',
        'salary_column': 'TotalWages',
        'department_column': 'DepartmentOrSubdivision',
    },
}


def check_existing_data(cursor, source_name: str) -> int:
    """Check how many records already exist for this source."""
    cursor.execute("""
        SELECT COUNT(*) FROM observed_jobs o
        JOIN sources s ON o.source_id = s.id
        WHERE s.name = %s
    """, (source_name,))
    return cursor.fetchone()[0]


def ingest_university_data(config: dict, data_dir: Path, db: DatabaseManager, normalizer: TitleNormalizer):
    """Ingest university salary data from a CSV file."""
    file_path = data_dir / config['file']
    source_name = config['source_name']

    log.info(f"Reading {source_name} from {file_path}")

    if not file_path.exists():
        log.warning(f"File not found: {file_path}")
        return

    # Read CSV
    df = pd.read_csv(file_path)
    log.info(f"Loaded {len(df):,} records")

    conn = db.get_connection()
    cursor = conn.cursor()

    # Check for existing data
    existing = check_existing_data(cursor, source_name)
    if existing > 0:
        log.warning(f"Found {existing:,} existing {source_name} records. Skipping ingestion.")
        db.release_connection(conn)
        return

    # Get or create source
    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
    row = cursor.fetchone()
    if row:
        source_id = row[0]
    else:
        cursor.execute("""
            INSERT INTO sources (name, type)
            VALUES (%s, %s)
            RETURNING id
        """, (source_name, 'university'))
        source_id = cursor.fetchone()[0]

    inserted = 0
    skipped = 0

    for idx, row_data in df.iterrows():
        try:
            # Extract fields
            job_title = str(row_data.get(config['title_column'], ''))
            employer_name = str(row_data.get(config['employer_column'], ''))

            if not job_title or job_title == 'nan' or not employer_name:
                skipped += 1
                continue

            # Get salary (TotalWages)
            salary = row_data.get(config['salary_column'])
            if pd.notna(salary):
                try:
                    salary = float(salary)
                    # Filter out very low salaries (likely part-time/student workers)
                    if salary < 5000:
                        salary = None
                except (ValueError, TypeError):
                    salary = None
            else:
                salary = None

            # Parse title
            result = normalizer.parse_title(job_title)

            # Get or create company (university)
            normalized_name = employer_name.upper().strip()
            cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized_name,))
            comp_row = cursor.fetchone()
            if comp_row:
                company_id = comp_row[0]
            else:
                cursor.execute("""
                    INSERT INTO companies (name, normalized_name)
                    VALUES (%s, %s)
                    RETURNING id
                """, (employer_name, normalized_name))
                company_id = cursor.fetchone()[0]

            # Get location from employer name (extract campus)
            # UC campuses are like "University of California, Berkeley"
            state = 'CA'
            city = 'Unknown'
            if ',' in employer_name:
                city = employer_name.split(',')[-1].strip()

            cursor.execute("""
                SELECT id FROM locations
                WHERE city = %s AND state = %s AND country = 'USA'
            """, (city, state))
            loc_row = cursor.fetchone()
            if loc_row:
                location_id = loc_row[0]
            else:
                cursor.execute("""
                    INSERT INTO locations (city, state, country)
                    VALUES (%s, %s, 'USA')
                    RETURNING id
                """, (city, state))
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
                'university'
            ))

            inserted += 1

            if inserted % 25000 == 0:
                conn.commit()
                log.info(f"  Inserted {inserted:,} records...")

        except Exception as e:
            conn.rollback()
            log.warning(f"Error processing row {idx}: {e}")
            skipped += 1
            continue

    conn.commit()
    db.release_connection(conn)
    log.info(f"{source_name} ingestion complete: {inserted:,} inserted, {skipped:,} skipped")


def main():
    """Main ingestion function."""
    log.info("="*60)
    log.info("UNIVERSITY SALARY DATA INGESTION")
    log.info("="*60)

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    normalizer = TitleNormalizer(db)

    data_dir = Path(__file__).parent / 'data' / 'university'

    # Ingest each university system
    for system_name, system_config in UNIVERSITY_CONFIGS.items():
        log.info(f"\nProcessing {system_name}...")
        ingest_university_data(system_config, data_dir, db, normalizer)

    # Summary
    conn = db.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT s.name, COUNT(*), ROUND(AVG(o.salary_point)) as avg_sal
        FROM observed_jobs o
        JOIN sources s ON o.source_id = s.id
        WHERE o.source_type = 'university'
        GROUP BY s.name
        ORDER BY COUNT(*) DESC
    """)

    log.info("\n" + "="*60)
    log.info("UNIVERSITY DATA SUMMARY")
    log.info("="*60)
    for name, cnt, avg_sal in cursor.fetchall():
        sal_str = f"${avg_sal:,.0f}" if avg_sal else "N/A"
        log.info(f"  {name}: {cnt:,} records, avg salary {sal_str}")

    cursor.execute("SELECT COUNT(*) FROM observed_jobs WHERE source_type = 'university'")
    total = cursor.fetchone()[0]
    log.info(f"\n  TOTAL UNIVERSITY RECORDS: {total:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
