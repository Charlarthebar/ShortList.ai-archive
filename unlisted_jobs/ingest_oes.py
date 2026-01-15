#!/usr/bin/env python3
"""
OES (Occupational Employment and Wage Statistics) Integration Script
=====================================================================

Ingests BLS OES wage data to provide occupation-level salary benchmarks.

MANUAL DOWNLOAD REQUIRED:
-------------------------
BLS blocks automated downloads. Please manually download the files from:

1. National data:
   https://www.bls.gov/oes/special.requests/oesm24nat.xlsx
   Save to: data/oes/oesm24nat.xlsx

2. State data (optional):
   https://www.bls.gov/oes/special.requests/oesm24st.xlsx
   Save to: data/oes/oesm24st.xlsx

3. Metro area data (optional):
   https://www.bls.gov/oes/special.requests/oesm24ma.xlsx
   Save to: data/oes/oesm24ma.xlsx

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import logging
from typing import Dict, List, Optional
import pandas as pd

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oes_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

OES_DATA_DIR = '/Users/noahhopkins/ShortList.ai/unlisted_jobs/data/oes'


def create_oes_tables(cursor):
    """Create OES reference tables in the database."""
    log.info("Creating OES reference tables...")

    # National wage estimates by occupation
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oes_wages_national (
            id SERIAL PRIMARY KEY,
            soc_code VARCHAR(10),
            occupation_title VARCHAR(255),
            employment INTEGER,
            employment_rse NUMERIC(6,2),
            hourly_mean NUMERIC(10,2),
            annual_mean NUMERIC(12,2),
            hourly_p10 NUMERIC(10,2),
            hourly_p25 NUMERIC(10,2),
            hourly_median NUMERIC(10,2),
            hourly_p75 NUMERIC(10,2),
            hourly_p90 NUMERIC(10,2),
            annual_p10 NUMERIC(12,2),
            annual_p25 NUMERIC(12,2),
            annual_median NUMERIC(12,2),
            annual_p75 NUMERIC(12,2),
            annual_p90 NUMERIC(12,2),
            data_year INTEGER DEFAULT 2024,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_oes_nat_soc ON oes_wages_national(soc_code)
    """)

    # State-level wage estimates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oes_wages_state (
            id SERIAL PRIMARY KEY,
            state_code VARCHAR(2),
            state_name VARCHAR(50),
            soc_code VARCHAR(10),
            occupation_title VARCHAR(255),
            employment INTEGER,
            hourly_mean NUMERIC(10,2),
            annual_mean NUMERIC(12,2),
            hourly_p10 NUMERIC(10,2),
            hourly_p25 NUMERIC(10,2),
            hourly_median NUMERIC(10,2),
            hourly_p75 NUMERIC(10,2),
            hourly_p90 NUMERIC(10,2),
            annual_p10 NUMERIC(12,2),
            annual_p25 NUMERIC(12,2),
            annual_median NUMERIC(12,2),
            annual_p75 NUMERIC(12,2),
            annual_p90 NUMERIC(12,2),
            data_year INTEGER DEFAULT 2024,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_oes_state_soc ON oes_wages_state(soc_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_oes_state_code ON oes_wages_state(state_code)
    """)

    # Metro area wage estimates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oes_wages_metro (
            id SERIAL PRIMARY KEY,
            area_code VARCHAR(10),
            area_name VARCHAR(100),
            soc_code VARCHAR(10),
            occupation_title VARCHAR(255),
            employment INTEGER,
            hourly_mean NUMERIC(10,2),
            annual_mean NUMERIC(12,2),
            hourly_p10 NUMERIC(10,2),
            hourly_p25 NUMERIC(10,2),
            hourly_median NUMERIC(10,2),
            hourly_p75 NUMERIC(10,2),
            hourly_p90 NUMERIC(10,2),
            annual_p10 NUMERIC(12,2),
            annual_p25 NUMERIC(12,2),
            annual_median NUMERIC(12,2),
            annual_p75 NUMERIC(12,2),
            annual_p90 NUMERIC(12,2),
            data_year INTEGER DEFAULT 2024,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_oes_metro_soc ON oes_wages_metro(soc_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_oes_metro_area ON oes_wages_metro(area_code)
    """)

    log.info("OES tables created successfully")


def safe_int(value) -> Optional[int]:
    """Convert value to int safely."""
    if pd.isna(value) or value in ('*', '**', '#', '~'):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def safe_float(value) -> Optional[float]:
    """Convert value to float safely."""
    if pd.isna(value) or value in ('*', '**', '#', '~'):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def ingest_national_data(cursor) -> int:
    """Ingest national OES wage data."""
    filepath = os.path.join(OES_DATA_DIR, 'oesm24nat.xlsx')

    if not os.path.exists(filepath):
        log.warning(f"National data file not found: {filepath}")
        log.info("Please download from: https://www.bls.gov/oes/special.requests/oesm24nat.xlsx")
        return 0

    log.info(f"Reading national OES data from {filepath}...")
    df = pd.read_excel(filepath)

    log.info(f"Found {len(df)} rows in national data")
    log.info(f"Columns: {list(df.columns)}")

    # Clear existing data
    cursor.execute("DELETE FROM oes_wages_national WHERE data_year = 2024")

    count = 0
    for _, row in df.iterrows():
        soc_code = str(row.get('OCC_CODE', '')).strip()
        if not soc_code or soc_code == 'nan':
            continue

        cursor.execute("""
            INSERT INTO oes_wages_national (
                soc_code, occupation_title, employment, employment_rse,
                hourly_mean, annual_mean,
                hourly_p10, hourly_p25, hourly_median, hourly_p75, hourly_p90,
                annual_p10, annual_p25, annual_median, annual_p75, annual_p90,
                data_year
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2024
            )
        """, (
            soc_code,
            row.get('OCC_TITLE', ''),
            safe_int(row.get('TOT_EMP')),
            safe_float(row.get('EMP_RSE')),
            safe_float(row.get('H_MEAN')),
            safe_float(row.get('A_MEAN')),
            safe_float(row.get('H_PCT10')),
            safe_float(row.get('H_PCT25')),
            safe_float(row.get('H_MEDIAN')),
            safe_float(row.get('H_PCT75')),
            safe_float(row.get('H_PCT90')),
            safe_float(row.get('A_PCT10')),
            safe_float(row.get('A_PCT25')),
            safe_float(row.get('A_MEDIAN')),
            safe_float(row.get('A_PCT75')),
            safe_float(row.get('A_PCT90')),
        ))
        count += 1

    log.info(f"Ingested {count} national occupation records")
    return count


def ingest_state_data(cursor) -> int:
    """Ingest state-level OES wage data."""
    filepath = os.path.join(OES_DATA_DIR, 'oesm24st.xlsx')

    if not os.path.exists(filepath):
        log.warning(f"State data file not found: {filepath}")
        log.info("Please download from: https://www.bls.gov/oes/special.requests/oesm24st.xlsx")
        return 0

    log.info(f"Reading state OES data from {filepath}...")
    df = pd.read_excel(filepath)

    log.info(f"Found {len(df)} rows in state data")

    # Clear existing data
    cursor.execute("DELETE FROM oes_wages_state WHERE data_year = 2024")

    count = 0
    for _, row in df.iterrows():
        soc_code = str(row.get('OCC_CODE', '')).strip()
        if not soc_code or soc_code == 'nan':
            continue

        cursor.execute("""
            INSERT INTO oes_wages_state (
                state_code, state_name, soc_code, occupation_title,
                employment, hourly_mean, annual_mean,
                hourly_p10, hourly_p25, hourly_median, hourly_p75, hourly_p90,
                annual_p10, annual_p25, annual_median, annual_p75, annual_p90,
                data_year
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2024
            )
        """, (
            row.get('ST', ''),
            row.get('STATE', ''),
            soc_code,
            row.get('OCC_TITLE', ''),
            safe_int(row.get('TOT_EMP')),
            safe_float(row.get('H_MEAN')),
            safe_float(row.get('A_MEAN')),
            safe_float(row.get('H_PCT10')),
            safe_float(row.get('H_PCT25')),
            safe_float(row.get('H_MEDIAN')),
            safe_float(row.get('H_PCT75')),
            safe_float(row.get('H_PCT90')),
            safe_float(row.get('A_PCT10')),
            safe_float(row.get('A_PCT25')),
            safe_float(row.get('A_MEDIAN')),
            safe_float(row.get('A_PCT75')),
            safe_float(row.get('A_PCT90')),
        ))
        count += 1

        if count % 10000 == 0:
            log.info(f"  Processed {count} state records...")

    log.info(f"Ingested {count} state-level occupation records")
    return count


def ingest_metro_data(cursor) -> int:
    """Ingest metro area OES wage data."""
    filepath = os.path.join(OES_DATA_DIR, 'oesm24ma.xlsx')

    if not os.path.exists(filepath):
        log.warning(f"Metro data file not found: {filepath}")
        log.info("Please download from: https://www.bls.gov/oes/special.requests/oesm24ma.xlsx")
        return 0

    log.info(f"Reading metro OES data from {filepath}...")
    df = pd.read_excel(filepath)

    log.info(f"Found {len(df)} rows in metro data")

    # Clear existing data
    cursor.execute("DELETE FROM oes_wages_metro WHERE data_year = 2024")

    count = 0
    for _, row in df.iterrows():
        soc_code = str(row.get('OCC_CODE', '')).strip()
        if not soc_code or soc_code == 'nan':
            continue

        cursor.execute("""
            INSERT INTO oes_wages_metro (
                area_code, area_name, soc_code, occupation_title,
                employment, hourly_mean, annual_mean,
                hourly_p10, hourly_p25, hourly_median, hourly_p75, hourly_p90,
                annual_p10, annual_p25, annual_median, annual_p75, annual_p90,
                data_year
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2024
            )
        """, (
            row.get('AREA', ''),
            row.get('AREA_TITLE', ''),
            soc_code,
            row.get('OCC_TITLE', ''),
            safe_int(row.get('TOT_EMP')),
            safe_float(row.get('H_MEAN')),
            safe_float(row.get('A_MEAN')),
            safe_float(row.get('H_PCT10')),
            safe_float(row.get('H_PCT25')),
            safe_float(row.get('H_MEDIAN')),
            safe_float(row.get('H_PCT75')),
            safe_float(row.get('H_PCT90')),
            safe_float(row.get('A_PCT10')),
            safe_float(row.get('A_PCT25')),
            safe_float(row.get('A_MEDIAN')),
            safe_float(row.get('A_PCT75')),
            safe_float(row.get('A_PCT90')),
        ))
        count += 1

        if count % 50000 == 0:
            log.info(f"  Processed {count} metro records...")

    log.info(f"Ingested {count} metro-level occupation records")
    return count


def show_sample_data(cursor):
    """Display sample data to verify ingestion."""
    log.info("\n" + "=" * 60)
    log.info("SAMPLE OES DATA")
    log.info("=" * 60)

    # Sample national data
    cursor.execute("""
        SELECT soc_code, occupation_title, employment, annual_median, annual_p90
        FROM oes_wages_national
        WHERE soc_code LIKE '15-1%'  -- Software/tech jobs
        ORDER BY employment DESC NULLS LAST
        LIMIT 5
    """)
    results = cursor.fetchall()
    if results:
        log.info("\nSample Tech Jobs (National):")
        for row in results:
            log.info(f"  {row[0]}: {row[1]}")
            log.info(f"    Employment: {row[2]:,}, Median: ${row[3]:,.0f}, 90th: ${row[4]:,.0f}")

    # Count totals
    cursor.execute("SELECT COUNT(*) FROM oes_wages_national WHERE data_year = 2024")
    nat_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM oes_wages_state WHERE data_year = 2024")
    state_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM oes_wages_metro WHERE data_year = 2024")
    metro_count = cursor.fetchone()[0]

    log.info(f"\nTotal OES Records:")
    log.info(f"  National: {nat_count:,}")
    log.info(f"  State:    {state_count:,}")
    log.info(f"  Metro:    {metro_count:,}")


def main():
    """Main function to ingest OES data."""
    log.info("=" * 60)
    log.info("OES WAGE DATA INTEGRATION")
    log.info("=" * 60)

    # Check if any data files exist
    files = ['oesm24nat.xlsx', 'oesm24st.xlsx', 'oesm24ma.xlsx']
    found_files = [f for f in files if os.path.exists(os.path.join(OES_DATA_DIR, f))]

    if not found_files:
        log.error("\nNo OES data files found!")
        log.info("\nPlease manually download the following files from BLS:")
        log.info("  1. https://www.bls.gov/oes/special.requests/oesm24nat.xlsx")
        log.info("     -> Save to: data/oes/oesm24nat.xlsx")
        log.info("  2. https://www.bls.gov/oes/special.requests/oesm24st.xlsx (optional)")
        log.info("     -> Save to: data/oes/oesm24st.xlsx")
        log.info("  3. https://www.bls.gov/oes/special.requests/oesm24ma.xlsx (optional)")
        log.info("     -> Save to: data/oes/oesm24ma.xlsx")
        log.info("\nThen re-run this script.")
        return

    log.info(f"\nFound data files: {found_files}")

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        # Create tables
        create_oes_tables(cursor)
        conn.commit()

        # Ingest available data
        nat_count = ingest_national_data(cursor)
        conn.commit()

        state_count = ingest_state_data(cursor)
        conn.commit()

        metro_count = ingest_metro_data(cursor)
        conn.commit()

        # Summary
        log.info("\n" + "=" * 60)
        log.info("OES INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  National records: {nat_count:,}")
        log.info(f"  State records:    {state_count:,}")
        log.info(f"  Metro records:    {metro_count:,}")
        log.info(f"  Total:            {nat_count + state_count + metro_count:,}")

        if nat_count > 0:
            show_sample_data(cursor)

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nOES integration complete!")


if __name__ == "__main__":
    main()
