#!/usr/bin/env python3
"""
BLS OEWS Data Ingestion
=======================

Ingests Occupational Employment and Wage Statistics from BLS into PostgreSQL.

Data source: https://download.bls.gov/pub/time.series/oe/
Files needed:
  - oe.data.0.Current (main data file, ~332MB)
  - oe.area (area codes)
  - oe.occupation (occupation codes)
  - oe.industry (industry codes)
  - oe.datatype (data type codes)

Usage:
    python ingest_oews.py                    # Download and ingest
    python ingest_oews.py --skip-download    # Use existing files

Author: ShortList.ai
"""

import os
import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
import argparse

# Setup environment
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

from database import DatabaseManager, Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oews_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


# ============================================================================
# BLS DOWNLOAD
# ============================================================================

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/oe/"

FILES_TO_DOWNLOAD = [
    "oe.data.0.Current",  # Main data file (~332MB)
    "oe.area",            # Area codes
    "oe.occupation",      # Occupation codes
    "oe.industry",        # Industry codes
    "oe.datatype",        # Data type codes
    "oe.areatype",        # Area type codes
]


def download_oews_files(data_dir: Path) -> bool:
    """Download OEWS files from BLS."""
    data_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
    }

    success = True
    for filename in FILES_TO_DOWNLOAD:
        filepath = data_dir / filename
        url = BLS_BASE_URL + filename

        # Skip if already exists and is large enough
        if filepath.exists():
            size = filepath.stat().st_size
            if filename == "oe.data.0.Current" and size > 100_000_000:
                log.info(f"✓ {filename}: Already downloaded ({size:,} bytes)")
                continue
            elif size > 100:
                log.info(f"✓ {filename}: Already downloaded ({size:,} bytes)")
                continue

        log.info(f"⬇️  Downloading {filename}...")

        try:
            response = requests.get(url, headers=headers, timeout=600, stream=True)
            response.raise_for_status()

            total = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (50 * 1024 * 1024) == 0:  # Log every 50MB
                        log.info(f"  Downloaded {total / 1024 / 1024:.0f} MB...")

            log.info(f"  ✓ Downloaded {total:,} bytes")

        except Exception as e:
            log.error(f"  ❌ Failed to download {filename}: {e}")
            success = False

    return success


# ============================================================================
# DATA PARSING
# ============================================================================

def parse_series_id(series_id: str) -> Dict:
    """
    Parse BLS OEWS series ID into components.

    Format: OE U M 0000400 000000 000000 01
            ^  ^ ^    ^      ^      ^    ^
            |  | |    |      |      |    datatype_code (01-17)
            |  | |    |      |      occupation_code
            |  | |    |      industry_code
            |  | |    area_code
            |  | areatype (N/S/M)
            |  seasonal (U=unseasonal)
            survey prefix
    """
    if len(series_id) < 26:
        return None

    # Remove leading/trailing whitespace
    series_id = series_id.strip()

    return {
        'survey': series_id[0:2],        # OE
        'seasonal': series_id[2],         # U
        'areatype': series_id[3],         # M/S/N
        'area_code': series_id[4:11],     # 0000400
        'industry_code': series_id[11:17], # 000000
        'occ_code': series_id[17:23],     # 000000
        'datatype_code': series_id[23:25], # 01
    }


# Datatype code to column name mapping
DATATYPE_MAP = {
    '01': 'employment',
    '02': 'employment_rse',
    '03': 'wage_hourly_mean',
    '04': 'wage_annual_mean',
    '05': 'wage_rse',
    '06': 'wage_hourly_p10',
    '07': 'wage_hourly_p25',
    '08': 'wage_hourly_median',
    '09': 'wage_hourly_p75',
    '10': 'wage_hourly_p90',
    '11': 'wage_annual_p10',
    '12': 'wage_annual_p25',
    '13': 'wage_annual_median',
    '14': 'wage_annual_p75',
    '15': 'wage_annual_p90',
    '16': 'employment_per_1000',
    '17': 'location_quotient',
}


def load_reference_tables(data_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load OEWS reference/mapping files."""
    refs = {}

    # Load area codes
    area_file = data_dir / "oe.area"
    if area_file.exists():
        refs['areas'] = pd.read_csv(area_file, sep='\t')
        log.info(f"Loaded {len(refs['areas'])} area codes")

    # Load occupation codes
    occ_file = data_dir / "oe.occupation"
    if occ_file.exists():
        refs['occupations'] = pd.read_csv(occ_file, sep='\t')
        log.info(f"Loaded {len(refs['occupations'])} occupation codes")

    # Load industry codes
    ind_file = data_dir / "oe.industry"
    if ind_file.exists():
        refs['industries'] = pd.read_csv(ind_file, sep='\t')
        log.info(f"Loaded {len(refs['industries'])} industry codes")

    return refs


def process_data_file(data_dir: Path, year_filter: int = 2024) -> Dict:
    """
    Process the main OEWS data file.

    Groups data by (area_code, occ_code, industry_code, year) and
    pivots datatypes into columns.
    """
    data_file = data_dir / "oe.data.0.Current"

    log.info(f"Processing {data_file}...")
    log.info(f"Filtering for year={year_filter}")

    # Read in chunks due to file size
    chunk_size = 500_000
    estimates = defaultdict(dict)
    total_rows = 0
    filtered_rows = 0

    for chunk in pd.read_csv(data_file, sep='\t', chunksize=chunk_size):
        # Strip whitespace from column names
        chunk.columns = chunk.columns.str.strip()
        total_rows += len(chunk)

        # Filter to desired year
        chunk = chunk[chunk['year'] == year_filter]
        filtered_rows += len(chunk)

        # Process each row
        for _, row in chunk.iterrows():
            series_id = row['series_id']
            parsed = parse_series_id(series_id)

            if not parsed:
                continue

            # Create unique key
            key = (
                parsed['area_code'],
                parsed['occ_code'],
                parsed['industry_code'],
                row['year']
            )

            # Map datatype to column
            datatype = parsed['datatype_code']
            col_name = DATATYPE_MAP.get(datatype)

            if col_name:
                value = row['value']
                # Handle special values
                if pd.notna(value):
                    try:
                        value = float(str(value).strip())
                        estimates[key][col_name] = value
                    except ValueError:
                        pass

        log.info(f"  Processed {total_rows:,} rows, {len(estimates):,} unique estimates...")

    log.info(f"Total rows: {total_rows:,}")
    log.info(f"Filtered rows (year={year_filter}): {filtered_rows:,}")
    log.info(f"Unique estimates: {len(estimates):,}")

    return estimates


# ============================================================================
# DATABASE INGESTION
# ============================================================================

def ingest_reference_tables(db: DatabaseManager, refs: Dict[str, pd.DataFrame]):
    """Ingest OEWS reference tables into database."""

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Ingest areas
        if 'areas' in refs:
            log.info("Ingesting area codes...")
            cur.execute("DELETE FROM oews_areas")

            for _, row in refs['areas'].iterrows():
                cur.execute("""
                    INSERT INTO oews_areas (area_code, state_code, areatype_code, area_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (area_code) DO UPDATE SET area_name = EXCLUDED.area_name
                """, (
                    row['area_code'],
                    row['state_code'],
                    row['areatype_code'],
                    row['area_name']
                ))

            conn.commit()
            log.info(f"  Ingested {len(refs['areas'])} areas")

        # Ingest occupations
        if 'occupations' in refs:
            log.info("Ingesting occupation codes...")
            cur.execute("DELETE FROM oews_occupations")

            for _, row in refs['occupations'].iterrows():
                cur.execute("""
                    INSERT INTO oews_occupations (occ_code, occ_title, occ_description, display_level, is_selectable)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (occ_code) DO UPDATE SET occ_title = EXCLUDED.occ_title
                """, (
                    row['occupation_code'],
                    row['occupation_name'],
                    row.get('occupation_description', ''),
                    row.get('display_level', 0),
                    row.get('selectable', 'T') == 'T'
                ))

            conn.commit()
            log.info(f"  Ingested {len(refs['occupations'])} occupations")

        # Ingest industries
        if 'industries' in refs:
            log.info("Ingesting industry codes...")
            cur.execute("DELETE FROM oews_industries")

            for _, row in refs['industries'].iterrows():
                cur.execute("""
                    INSERT INTO oews_industries (industry_code, industry_title, display_level)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (industry_code) DO UPDATE SET industry_title = EXCLUDED.industry_title
                """, (
                    row['industry_code'],
                    row['industry_name'],
                    row.get('display_level', 0)
                ))

            conn.commit()
            log.info(f"  Ingested {len(refs['industries'])} industries")

        cur.close()


def ingest_estimates(db: DatabaseManager, estimates: Dict):
    """Ingest OEWS estimates into database."""

    log.info(f"Ingesting {len(estimates):,} estimates...")

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Clear existing data for this year
        if estimates:
            sample_key = list(estimates.keys())[0]
            year = sample_key[3]
            cur.execute("DELETE FROM oews_estimates WHERE year = %s", (year,))
            log.info(f"  Cleared existing data for year {year}")

        batch_size = 10000
        batch = []
        inserted = 0

        for key, data in estimates.items():
            area_code, occ_code, industry_code, year = key

            row = {
                'area_code': area_code,
                'occ_code': occ_code,
                'industry_code': industry_code,
                'year': year,
                **data
            }
            batch.append(row)

            if len(batch) >= batch_size:
                _insert_batch(cur, batch)
                inserted += len(batch)
                batch = []
                log.info(f"  Inserted {inserted:,} estimates...")

        # Insert remaining
        if batch:
            _insert_batch(cur, batch)
            inserted += len(batch)

        conn.commit()
        cur.close()

        log.info(f"  ✓ Inserted {inserted:,} total estimates")


def _insert_batch(cur, batch: List[Dict]):
    """Insert a batch of estimates."""
    for row in batch:
        cur.execute("""
            INSERT INTO oews_estimates (
                area_code, occ_code, industry_code, year,
                employment, employment_rse, employment_per_1000, location_quotient,
                wage_annual_mean, wage_annual_p10, wage_annual_p25, wage_annual_median,
                wage_annual_p75, wage_annual_p90,
                wage_hourly_mean, wage_hourly_p10, wage_hourly_p25, wage_hourly_median,
                wage_hourly_p75, wage_hourly_p90, wage_rse
            ) VALUES (
                %(area_code)s, %(occ_code)s, %(industry_code)s, %(year)s,
                %(employment)s, %(employment_rse)s, %(employment_per_1000)s, %(location_quotient)s,
                %(wage_annual_mean)s, %(wage_annual_p10)s, %(wage_annual_p25)s, %(wage_annual_median)s,
                %(wage_annual_p75)s, %(wage_annual_p90)s,
                %(wage_hourly_mean)s, %(wage_hourly_p10)s, %(wage_hourly_p25)s, %(wage_hourly_median)s,
                %(wage_hourly_p75)s, %(wage_hourly_p90)s, %(wage_rse)s
            )
            ON CONFLICT (area_code, occ_code, industry_code, year) DO UPDATE SET
                employment = EXCLUDED.employment,
                wage_annual_median = EXCLUDED.wage_annual_median
        """, {
            'area_code': row['area_code'],
            'occ_code': row['occ_code'],
            'industry_code': row['industry_code'],
            'year': row['year'],
            'employment': row.get('employment'),
            'employment_rse': row.get('employment_rse'),
            'employment_per_1000': row.get('employment_per_1000'),
            'location_quotient': row.get('location_quotient'),
            'wage_annual_mean': row.get('wage_annual_mean'),
            'wage_annual_p10': row.get('wage_annual_p10'),
            'wage_annual_p25': row.get('wage_annual_p25'),
            'wage_annual_median': row.get('wage_annual_median'),
            'wage_annual_p75': row.get('wage_annual_p75'),
            'wage_annual_p90': row.get('wage_annual_p90'),
            'wage_hourly_mean': row.get('wage_hourly_mean'),
            'wage_hourly_p10': row.get('wage_hourly_p10'),
            'wage_hourly_p25': row.get('wage_hourly_p25'),
            'wage_hourly_median': row.get('wage_hourly_median'),
            'wage_hourly_p75': row.get('wage_hourly_p75'),
            'wage_hourly_p90': row.get('wage_hourly_p90'),
            'wage_rse': row.get('wage_rse'),
        })


# ============================================================================
# SOC TO CANONICAL ROLE MAPPING
# ============================================================================

# Initial mapping of SOC codes to our canonical roles
# This is a starting point - can be refined based on job title patterns
SOC_TO_ROLE_MAPPING = {
    # Software & Tech
    '151251': 'Software Engineer',        # Computer Programmers
    '151252': 'Software Engineer',        # Software Developers
    '151253': 'Software Engineer',        # Software Quality Assurance
    '151254': 'Software Engineer',        # Web Developers
    '151255': 'Software Engineer',        # Web and Digital Interface Designers
    '151211': 'Data Analyst',             # Computer Systems Analysts
    '151212': 'Security Engineer',        # Information Security Analysts
    '151221': 'Security Engineer',        # Computer and Information Security
    '152031': 'Data Scientist',           # Operations Research Analysts
    '152051': 'Data Scientist',           # Data Scientists
    '152099': 'Data Scientist',           # Mathematical Science Occupations, All Other
    '151241': 'Database Administrator',   # Database Architects
    '151242': 'Database Administrator',   # Database Administrators
    '151243': 'Network Engineer',         # Network Architects
    '151244': 'Network Engineer',         # Network Administrators
    '151231': 'Network Engineer',         # Computer Network Support
    '151232': 'IT Support Specialist',    # Computer User Support
    '151299': 'Software Engineer',        # Computer Occupations, All Other
    '113021': 'Engineering Manager',      # Computer and Information Systems Managers

    # Data
    '152041': 'Statistician',             # Statisticians
    '152098': 'Data Scientist',           # Data Scientists and Mathematical Science

    # Design
    '271024': 'UX Designer',              # Graphic Designers
    '271021': 'Commercial Photographer',  # Commercial and Industrial Designers
    '271026': 'UX Designer',              # Merchandise Displayers

    # Management
    '111011': 'CEO',                      # Chief Executives
    '111021': 'Operations Manager',       # General and Operations Managers
    '112021': 'Marketing Manager',        # Marketing Managers
    '112022': 'Sales Manager',            # Sales Managers
    '113031': 'Finance Manager',          # Financial Managers
    '113051': 'Operations Manager',       # Industrial Production Managers
    '113121': 'HR Manager',               # Human Resources Managers
    '119041': 'Engineering Manager',      # Engineering Managers
    '119111': 'Medical Director',         # Medical and Health Services Managers

    # Finance
    '132011': 'Accountant',               # Accountants and Auditors
    '132051': 'Financial Analyst',        # Financial and Investment Analysts
    '132052': 'Personal Financial Advisor', # Personal Financial Advisors
    '132053': 'Insurance Underwriter',    # Insurance Underwriters
    '132061': 'Financial Analyst',        # Financial Examiners
    '132072': 'Loan Officer',             # Loan Officers
    '132099': 'Financial Analyst',        # Financial Specialists, All Other
    '433031': 'Bookkeeper',               # Bookkeeping, Accounting, and Auditing

    # HR
    '131071': 'HR Specialist',            # Human Resources Specialists
    '131141': 'Compensation Analyst',     # Compensation, Benefits Specialists
    '131151': 'Recruiter',                # Training and Development Specialists
    '131161': 'Recruiter',                # Market Research Analysts

    # Marketing & Sales
    '411011': 'Sales Representative',     # Advertising Sales Agents
    '411012': 'Insurance Agent',          # Insurance Sales Agents
    '412021': 'Sales Representative',     # Counter and Rental Clerks
    '412031': 'Retail Sales Associate',   # Retail Salespersons
    '413011': 'Advertising Account Executive', # Advertising Sales Agents
    '413021': 'Insurance Agent',          # Insurance Sales Agents
    '413031': 'Financial Services Representative', # Securities and Commodities
    '274021': 'Photographer',             # Photographers
    '273031': 'Public Relations Specialist', # Public Relations Specialists

    # Engineering
    '172011': 'Aerospace Engineer',       # Aerospace Engineers
    '172041': 'Chemical Engineer',        # Chemical Engineers
    '172051': 'Civil Engineer',           # Civil Engineers
    '172061': 'Computer Hardware Engineer', # Computer Hardware Engineers
    '172071': 'Electrical Engineer',      # Electrical Engineers
    '172072': 'Electronics Engineer',     # Electronics Engineers
    '172081': 'Environmental Engineer',   # Environmental Engineers
    '172112': 'Industrial Engineer',      # Industrial Engineers
    '172131': 'Materials Engineer',       # Materials Engineers
    '172141': 'Mechanical Engineer',      # Mechanical Engineers
    '172199': 'Engineer',                 # Engineers, All Other

    # Healthcare
    '291210': 'Physician',                # Physicians
    '291215': 'Physician',                # Family Medicine Physicians
    '291216': 'Physician',                # General Internal Medicine Physicians
    '291217': 'Surgeon',                  # Surgeons
    '291218': 'Physician',                # Obstetricians and Gynecologists
    '291221': 'Physician',                # Pediatricians, General
    '291228': 'Physician',                # Physicians, All Other
    '291141': 'Registered Nurse',         # Registered Nurses
    '291171': 'Nurse Practitioner',       # Nurse Practitioners
    '312021': 'Physical Therapist',       # Physical Therapist Assistants
    '292010': 'Clinical Lab Technician',  # Clinical Laboratory Technologists
    '292081': 'Medical Lab Technician',   # Opticians, Dispensing

    # Legal
    '231011': 'Attorney',                 # Lawyers
    '231012': 'Judicial Law Clerk',       # Judicial Law Clerks
    '232011': 'Paralegal',                # Paralegals and Legal Assistants

    # Education
    '251011': 'Professor',                # Business Teachers, Postsecondary
    '252021': 'Teacher',                  # Elementary School Teachers
    '252031': 'Teacher',                  # Secondary School Teachers

    # Administrative
    '434051': 'Customer Service Representative', # Customer Service Representatives
    '436014': 'Executive Assistant',      # Secretaries and Admin Assistants
    '433021': 'Billing Clerk',            # Billing and Posting Clerks

    # Trades
    '472111': 'Electrician',              # Electricians
    '472152': 'Plumber',                  # Plumbers, Pipefitters, Steamfitters
    '472031': 'Carpenter',                # Carpenters
    '474011': 'Construction Supervisor',  # First-Line Supervisors of Construction
    '474021': 'Elevator Installer',       # Elevator and Escalator Installers
    '491011': 'Maintenance Supervisor',   # First-Line Supervisors of Mechanics
    '493023': 'Auto Mechanic',            # Automotive Service Technicians
    '499021': 'HVAC Technician',          # Heating, Air Conditioning Mechanics
    '499071': 'Maintenance Worker',       # Maintenance and Repair Workers
}


def create_role_mappings(db: DatabaseManager):
    """Create initial SOC to canonical role mappings."""

    log.info("Creating SOC to canonical role mappings...")

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Get canonical role IDs
        cur.execute("SELECT id, name FROM canonical_roles")
        role_map = {row[1]: row[0] for row in cur.fetchall()}

        # Get valid SOC codes
        cur.execute("SELECT occ_code FROM oews_occupations")
        valid_socs = {row[0] for row in cur.fetchall()}

        inserted = 0
        for soc_code, role_name in SOC_TO_ROLE_MAPPING.items():
            if soc_code not in valid_socs:
                log.debug(f"  Skipping {soc_code} - not in OEWS occupations")
                continue

            if role_name not in role_map:
                log.debug(f"  Skipping {soc_code} -> {role_name} - role not found")
                continue

            try:
                cur.execute("""
                    INSERT INTO oews_role_mapping (occ_code, canonical_role_id, confidence, is_primary)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (occ_code, canonical_role_id) DO NOTHING
                """, (soc_code, role_map[role_name], 0.9, True))
                inserted += 1
            except Exception as e:
                log.warning(f"  Could not map {soc_code} -> {role_name}: {e}")

        conn.commit()
        cur.close()

        log.info(f"  Created {inserted} SOC to role mappings")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Ingest BLS OEWS data')
    parser.add_argument('--skip-download', action='store_true', help='Skip downloading files')
    parser.add_argument('--year', type=int, default=2024, help='Year to filter data')
    parser.add_argument('--data-dir', type=str, default='data/bls_oews', help='Data directory')
    args = parser.parse_args()

    log.info("="*70)
    log.info("BLS OEWS INGESTION")
    log.info("="*70)

    data_dir = Path(args.data_dir)

    # Download files
    if not args.skip_download:
        log.info("\n" + "="*70)
        log.info("DOWNLOADING OEWS FILES")
        log.info("="*70)
        if not download_oews_files(data_dir):
            log.error("Failed to download all files")
            return

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    # Run schema
    log.info("\n" + "="*70)
    log.info("CREATING SCHEMA")
    log.info("="*70)
    schema_file = Path(__file__).parent / "schema_oews.sql"
    if schema_file.exists():
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(open(schema_file).read())
            conn.commit()
            cur.close()
        log.info("  ✓ Schema created")
    else:
        log.warning(f"  Schema file not found: {schema_file}")

    # Load reference tables
    log.info("\n" + "="*70)
    log.info("LOADING REFERENCE DATA")
    log.info("="*70)
    refs = load_reference_tables(data_dir)

    # Ingest reference tables
    log.info("\n" + "="*70)
    log.info("INGESTING REFERENCE TABLES")
    log.info("="*70)
    ingest_reference_tables(db, refs)

    # Process main data file
    log.info("\n" + "="*70)
    log.info(f"PROCESSING DATA (year={args.year})")
    log.info("="*70)
    estimates = process_data_file(data_dir, year_filter=args.year)

    # Ingest estimates
    log.info("\n" + "="*70)
    log.info("INGESTING ESTIMATES")
    log.info("="*70)
    ingest_estimates(db, estimates)

    # Create role mappings
    log.info("\n" + "="*70)
    log.info("CREATING ROLE MAPPINGS")
    log.info("="*70)
    create_role_mappings(db)

    # Summary
    log.info("\n" + "="*70)
    log.info("SUMMARY")
    log.info("="*70)

    with db.get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM oews_areas")
        log.info(f"Areas: {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM oews_occupations")
        log.info(f"Occupations: {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM oews_estimates WHERE year = %s", (args.year,))
        log.info(f"Estimates (year={args.year}): {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM oews_role_mapping")
        log.info(f"Role mappings: {cur.fetchone()[0]:,}")

        # Sample query
        cur.execute("""
            SELECT oa.area_name, oo.occ_title, oe.employment, oe.wage_annual_median
            FROM oews_estimates oe
            JOIN oews_areas oa ON oe.area_code = oa.area_code
            JOIN oews_occupations oo ON oe.occ_code = oo.occ_code
            WHERE oe.year = %s
              AND oa.areatype_code = 'M'
              AND oo.occ_code = '151252'  -- Software Developers
            ORDER BY oe.employment DESC
            LIMIT 10
        """, (args.year,))

        log.info("\nTop 10 metros for Software Developers:")
        for row in cur.fetchall():
            log.info(f"  {row[0]}: {row[2]:,} jobs @ ${row[3]:,}/year")

        cur.close()

    log.info("\n" + "="*70)
    log.info("OEWS INGESTION COMPLETE")
    log.info("="*70)


if __name__ == '__main__':
    main()
