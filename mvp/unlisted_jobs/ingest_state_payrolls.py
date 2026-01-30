#!/usr/bin/env python3
"""
Multi-State Payroll Ingestion System
=====================================

Ingests payroll data from all 50 US states into the jobs database.

Each state has different:
- Data source URLs
- Column naming conventions
- Data formats

This script handles the variations and normalizes to a common schema.

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import pandas as pd
import requests
from pathlib import Path
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('state_payroll_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ============================================================================
# STATE CONFIGURATIONS - Based on actual data formats
# ============================================================================

STATE_CONFIGS = {
    # ==========================================================================
    # STATES/CITIES WITH WORKING API URLs (AUTO-DOWNLOAD)
    # ==========================================================================

    # --- STATE PAYROLLS ---
    'MO': {
        'name': 'Missouri',
        'url': 'https://data.mo.gov/api/views/przm-8aj2/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': 'Employee Name',
            'title': 'Position Title',
            'department': 'Agency Name',
            'salary': 'YTD Gross Pay',
        },
        'employer': 'State of Missouri',
    },
    'IA': {
        'name': 'Iowa',
        'url': 'https://data.iowa.gov/api/views/s3p7-wy6w/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': 'Name',
            'title': 'Position',
            'department': 'Department',
            'salary': 'Total Salary Paid',
        },
        'employer': 'State of Iowa',
    },
    'DC': {
        'name': 'District of Columbia',
        'url': 'https://opendata.dc.gov/datasets/DCGIS::dc-public-employee-salary.csv',
        'columns': {
            'name': ['FIRST_NAME', 'LAST_NAME'],
            'title': 'JOBTITLE',
            'department': 'DESCRSHORT',
            'salary': 'COMPRATE',
        },
        'employer': 'District of Columbia',
    },

    # --- CITY PAYROLLS (LARGE CITIES) ---
    'NYC': {
        'name': 'New York City',
        'url': 'https://data.cityofnewyork.us/api/views/k397-673e/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': ['First Name', 'Last Name'],
            'title': 'Title Description',
            'department': 'Agency Name',
            'salary': 'Base Salary',
        },
        'employer': 'City of New York',
    },
    'CHI': {
        'name': 'Chicago',
        'url': 'https://data.cityofchicago.org/api/views/xzkq-xp2w/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': 'Name',
            'title': 'Job Titles',
            'department': 'Department',
            'salary': 'Annual Salary',
        },
        'employer': 'City of Chicago',
    },
    'SF': {
        'name': 'San Francisco',
        'url': 'https://data.sfgov.org/api/views/88g8-5mnd/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': 'Employee Name',
            'title': 'Job',
            'department': 'Department',
            'salary': 'Total Salary',
        },
        'employer': 'City of San Francisco',
    },
    'SEA': {
        'name': 'Seattle',
        'url': 'https://cos-data.seattle.gov/api/views/2khk-5ukd/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': ['First Name', 'Last Name'],
            'title': 'Job Title',
            'department': 'Department',
            'salary': 'Hourly Rate ',  # Note: trailing space in actual column name
        },
        'salary_multiplier': 2080,  # Hourly to annual (40hrs * 52 weeks)
        'employer': 'City of Seattle',
    },

    # --- COUNTY PAYROLLS ---
    'COOK': {
        'name': 'Cook County IL',
        'url': 'https://datacatalog.cookcountyil.gov/api/views/xu6t-uvny/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': ['First Name', 'Last Name'],
            'title': 'Job Title',
            'department': 'Office Name',
            'salary': 'Base Pay',
        },
        'salary_multiplier': 4,  # Quarterly to annual
        'employer': 'Cook County, Illinois',
    },
    'MONT': {
        'name': 'Montgomery County MD',
        'url': 'https://data.montgomerycountymd.gov/api/views/2nq6-auk8/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': None,  # No name column
            'title': 'Department Name',  # Use department as proxy (no job title column)
            'department': 'Department Name',
            'salary': 'Base Salary',
        },
        'employer': 'Montgomery County, Maryland',
        'skip': True,  # Skip - no job title column
    },

    # --- TRANSIT AGENCIES ---
    'MTA': {
        'name': 'MTA New York',
        'url': 'https://data.ny.gov/api/views/kcjb-nf3e/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': 'Name',
            'title': 'Title',
            'department': 'Department',
            'salary': 'Total Earnings',
        },
        'employer': 'MTA New York',
    },

    # --- NEW AUTO-DOWNLOAD STATES ---
    'OR': {
        'name': 'Oregon',
        'url': 'https://data.oregon.gov/api/views/4cmg-5yp4/rows.csv?accessType=DOWNLOAD',
        'columns': {
            'name': None,  # No name column in this dataset
            'title': 'CLASSIFICATION',
            'department': 'AGENCY',
            'salary': 'SALARY (ANNUAL) ',  # Note: trailing space
        },
        'employer': 'State of Oregon',
    },
    'SC': {
        'name': 'South Carolina',
        'url': 'https://www.admin.sc.gov/sites/admin/files/SalaryUpload/Transparency%20Salary%20Data%2012-29-2025.csv',
        'columns': {
            'name': None,
            'title': None,
            'department': None,
            'salary': None,
        },
        'employer': 'State of South Carolina',
        'skip': True,  # Data doesn't have job titles - only department/division names
        'skip_reason': 'CSV contains department names, not job titles',
    },
    'BOS': {
        'name': 'Boston',
        'url': 'https://data.boston.gov/dataset/418983dc-7cae-42bb-88e4-d56f5adcf869/resource/579a4be3-9ca7-4183-bc95-7d67ee715b6d/download/employee_earnings_report_2024.csv',
        'columns': {
            'name': 'NAME',
            'title': 'TITLE',
            'department': 'DEPARTMENT_NAME',
            'salary': 'TOTAL GROSS',
        },
        'employer': 'City of Boston',
    },

    # ==========================================================================
    # STATES WITH MANUAL DOWNLOAD (HAVE JOB TITLES)
    # ==========================================================================
    'NY': {
        'name': 'New York State',
        'manual_download': True,
        'download_url': 'https://www.seethroughny.net/payrolls',
        'columns': {
            'name': 'Name',
            'title': 'Title',
            'department': 'Agency/Authority/Public Employer',
            'salary': 'Total Wages',
        },
        'employer': 'State of New York',
    },
    'TX': {
        'name': 'Texas',
        'manual_download': True,
        'download_url': 'https://salaries.texastribune.org/',
        'columns': {
            'name': ['FIRST NAME', 'LAST NAME'],
            'title': 'CLASS TITLE',
            'department': 'AGENCY NAME',
            'salary': 'ANNUAL',
        },
        'employer': 'State of Texas',
    },
    'FL': {
        'name': 'Florida',
        'manual_download': True,
        'download_url': 'https://floridahasarighttoknow.com/',
        'columns': {
            'name': 'Name',
            'title': 'Class Title',
            'department': 'Agency',
            'salary': 'Annual Salary',
        },
        'employer': 'State of Florida',
    },
    'OH': {
        'name': 'Ohio',
        'manual_download': True,
        'download_url': 'https://checkbook.ohio.gov/',
        'columns': {
            'name': 'Employee Name',
            'title': 'Job Title',
            'department': 'Agency',
            'salary': 'YTD Gross',
        },
        'employer': 'State of Ohio',
    },
    'NC': {
        'name': 'North Carolina',
        'manual_download': True,
        'download_url': 'https://www.newsobserver.com/news/databases/state-pay/',
        'columns': {
            'name': 'Name',
            'title': 'Job Title',
            'department': 'Agency',
            'salary': 'Annual Salary',
        },
        'employer': 'State of North Carolina',
    },
    'MI': {
        'name': 'Michigan',
        'manual_download': True,
        'download_url': 'https://www.michigan.gov/dtmb/budget/state-employee-info',
        'columns': {
            'name': 'Employee Name',
            'title': 'Class Title',
            'department': 'Department',
            'salary': 'Annual Rate',
        },
        'employer': 'State of Michigan',
    },
}


def auto_detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Automatically detect column mappings based on common patterns.
    """
    columns = df.columns.str.lower().tolist()
    mapping = {}

    # Name detection
    name_patterns = ['name', 'employee', 'last_name', 'full_name']
    for col in df.columns:
        if any(p in col.lower() for p in name_patterns):
            mapping['name'] = col
            break

    # Title detection
    title_patterns = ['title', 'position', 'job', 'class', 'occupation', 'description']
    for col in df.columns:
        col_lower = col.lower()
        if 'title' in col_lower or 'position' in col_lower or 'job' in col_lower:
            mapping['title'] = col
            break
        elif 'class' in col_lower and 'title' in col_lower:
            mapping['title'] = col
            break
        elif 'description' in col_lower and 'account' not in col_lower:
            mapping['title'] = col
            break

    # Department detection
    dept_patterns = ['agency', 'department', 'dept', 'organization', 'org']
    for col in df.columns:
        if any(p in col.lower() for p in dept_patterns):
            mapping['department'] = col
            break

    # Salary detection
    salary_patterns = ['salary', 'wage', 'pay', 'gross', 'annual', 'compensation', 'amount']
    for col in df.columns:
        col_lower = col.lower()
        if 'salary' in col_lower or 'annual' in col_lower:
            mapping['salary'] = col
            break
        elif 'wage' in col_lower or 'gross' in col_lower:
            mapping['salary'] = col
            break
        elif 'amount' in col_lower:
            mapping['salary'] = col
            break

    return mapping


def download_state_data(state_code: str, output_dir: Path) -> Optional[Path]:
    """Download payroll data for a state."""
    config = STATE_CONFIGS.get(state_code)
    if not config:
        log.error(f"❌ No configuration for state: {state_code}")
        return None

    # Check if source should be skipped
    if config.get('skip'):
        log.info(f"⏭️  {config['name']}: Skipped (no job title column)")
        return None

    output_file = output_dir / f"{state_code.lower()}_payroll_2024.csv"

    # Check if manual download required
    if config.get('manual_download'):
        if output_file.exists():
            log.info(f"✓ {config['name']}: Using existing manual download")
            return output_file
        else:
            log.warning(f"⚠️  {config['name']}: Manual download required from {config.get('download_url')}")
            return None

    if not config.get('url'):
        log.warning(f"⚠️  {config['name']}: No URL configured")
        return None

    if output_file.exists():
        log.info(f"✓ {config['name']}: Data already downloaded")
        return output_file

    log.info(f"⬇️  Downloading {config['name']} payroll data...")

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        # Longer timeout for large files (NYC can be 500MB+)
        response = requests.get(config['url'], timeout=600, headers=headers, stream=True)
        response.raise_for_status()

        # Check if we got HTML instead of CSV
        content_type = response.headers.get('content-type', '')
        if 'text/html' in content_type or response.text.strip().startswith('<!DOCTYPE'):
            log.error(f"❌ {config['name']}: Got HTML instead of CSV (may need manual download)")
            return None

        with open(output_file, 'wb') as f:
            f.write(response.content)

        log.info(f"✓ {config['name']}: Downloaded successfully ({len(response.content):,} bytes)")
        return output_file

    except Exception as e:
        log.error(f"❌ {config['name']}: Download failed - {str(e)}")
        return None


def load_and_standardize(state_code: str, file_path: Path) -> Optional[pd.DataFrame]:
    """Load state payroll data and standardize column names."""
    config = STATE_CONFIGS.get(state_code)
    if not config:
        return None

    log.info(f"📂 Loading {config['name']} data from {file_path}...")

    try:
        # Check if file has no header
        has_header = not config.get('no_header', False)

        # Try reading with different encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                if has_header:
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                else:
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False, header=None)
                break
            except UnicodeDecodeError:
                continue
        else:
            log.error(f"❌ {config['name']}: Could not decode file")
            return None

        log.info(f"  Raw records: {len(df):,}")
        log.info(f"  Columns found: {list(df.columns)}")

        # Handle position-based columns (e.g., South Carolina with no headers)
        column_positions = config.get('column_positions')
        if column_positions:
            log.info(f"  Using position-based column mapping: {column_positions}")
            standardized = pd.DataFrame()

            # Map by position
            if 'name' in column_positions:
                standardized['name'] = df.iloc[:, column_positions['name']]
            else:
                standardized['name'] = 'Unknown'

            if 'title' in column_positions:
                standardized['title'] = df.iloc[:, column_positions['title']]
            else:
                standardized['title'] = 'Unknown'

            if 'department' in column_positions:
                standardized['department'] = df.iloc[:, column_positions['department']]
            else:
                standardized['department'] = config.get('employer', 'Unknown')

            if 'salary' in column_positions:
                standardized['salary'] = pd.to_numeric(
                    df.iloc[:, column_positions['salary']].astype(str).str.replace(r'[\$,]', '', regex=True),
                    errors='coerce'
                )
            else:
                standardized['salary'] = None
        else:
            # Try configured column mapping first
            column_map = config.get('columns', {})

            # If configured columns don't exist, try auto-detection
            if not all(
                (v is None) or
                (isinstance(v, str) and v in df.columns) or
                (isinstance(v, list) and all(c in df.columns for c in v))
                for v in column_map.values()
            ):
                log.info(f"  Configured columns not found, trying auto-detection...")
                column_map = auto_detect_columns(df)
                log.info(f"  Auto-detected: {column_map}")

            standardized = pd.DataFrame()

            # Handle name (might need concatenation)
            name_col = column_map.get('name')
            if isinstance(name_col, list):
                # Concatenate multiple name columns
                standardized['name'] = df[name_col].fillna('').astype(str).agg(' '.join, axis=1)
            elif name_col and name_col in df.columns:
                standardized['name'] = df[name_col]
            else:
                standardized['name'] = 'Unknown'

            # Handle title
            title_col = column_map.get('title')
            if title_col and title_col in df.columns:
                standardized['title'] = df[title_col]
            else:
                log.warning(f"  ⚠️ No title column found")
                standardized['title'] = 'Unknown'

            # Handle department
            dept_col = column_map.get('department')
            if dept_col and dept_col in df.columns:
                standardized['department'] = df[dept_col]
            else:
                standardized['department'] = config.get('employer', 'Unknown')

            # Handle salary
            salary_col = column_map.get('salary')
            if salary_col and salary_col in df.columns:
                # Clean salary data - remove $ and commas
                standardized['salary'] = pd.to_numeric(
                    df[salary_col].astype(str).str.replace(r'[\$,]', '', regex=True),
                    errors='coerce'
                )

                # Apply multiplier if needed (e.g., biweekly to annual)
                multiplier = config.get('salary_multiplier', 1)
                if multiplier > 1:
                    standardized['salary'] = standardized['salary'] * multiplier
                    log.info(f"  Applied salary multiplier: {multiplier}x")
            else:
                log.warning(f"  ⚠️ No salary column found")
            standardized['salary'] = None

        # Add metadata
        standardized['state'] = state_code
        standardized['employer'] = config.get('employer', f'State of {config["name"]}')
        standardized['source'] = f"{config['name']} State Payroll"

        # Remove rows with missing titles or very low salaries
        initial_count = len(standardized)
        standardized = standardized[standardized['title'].notna() & (standardized['title'] != '')]
        standardized = standardized[standardized['title'] != 'Unknown']

        if 'salary' in standardized.columns:
            # Keep rows with salary > $10k (filter out hourly/per-period that weren't multiplied correctly)
            standardized = standardized[
                (standardized['salary'].isna()) |
                (standardized['salary'] > 10000)
            ]

        removed = initial_count - len(standardized)

        log.info(f"  ✓ Standardized: {len(standardized):,} records")
        if removed > 0:
            log.info(f"  Removed {removed:,} rows with missing/invalid data")

        # Show sample
        if len(standardized) > 0:
            log.info(f"  Sample titles: {standardized['title'].head(5).tolist()}")

        return standardized

    except Exception as e:
        log.error(f"❌ {config['name']}: Load failed - {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def check_existing_data(state_code: str, db: DatabaseManager) -> int:
    """Check how many records already exist for this employer."""
    config = STATE_CONFIGS[state_code]
    employer = config.get('employer', f"State of {config['name']}")
    normalized_employer = employer.lower().strip()
    normalized_employer = ''.join(c for c in normalized_employer if c.isalnum() or c.isspace()).strip()

    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM observed_jobs o
            JOIN companies c ON o.company_id = c.id
            WHERE c.normalized_name = %s AND o.source_type = 'state_payroll'
        """, (normalized_employer,))
        count = cursor.fetchone()[0]
        return count
    finally:
        db.release_connection(conn)


def ingest_state_payroll(state_code: str, df: pd.DataFrame, normalizer: TitleNormalizer,
                         db: DatabaseManager, force: bool = False) -> Dict[str, int]:
    """Ingest standardized state payroll data into database."""
    config = STATE_CONFIGS[state_code]
    state_name = config['name']

    # Check if data already exists
    existing_count = check_existing_data(state_code, db)
    if existing_count > 0 and not force:
        log.info(f"⏭️  {state_name}: Already has {existing_count:,} records in database - skipping")
        log.info(f"   (Use --force to re-ingest)")
        return {'total_records': 0, 'normalized': 0, 'inserted': 0, 'skipped': 0, 'errors': 0, 'existing': existing_count}

    log.info(f"\n{'='*70}")
    log.info(f"INGESTING {state_name.upper()} PAYROLL")
    log.info(f"{'='*70}")

    stats = {
        'total_records': len(df),
        'normalized': 0,
        'inserted': 0,
        'skipped': 0,
        'errors': 0
    }

    # Get or create source
    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO sources (name, type, reliability_tier, base_reliability)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING
        """, (f"{state_name} State Payroll", 'state_payroll', 'tier_1', 0.95))
        conn.commit()

        cursor.execute("SELECT id FROM sources WHERE name = %s", (f"{state_name} State Payroll",))
        source_id = cursor.fetchone()[0]

        # Get or create company (state government)
        employer = config.get('employer', f'State of {state_name}')
        normalized_employer = employer.lower().strip()
        normalized_employer = ''.join(c for c in normalized_employer if c.isalnum() or c.isspace()).strip()

        cursor.execute("""
            INSERT INTO companies (name, normalized_name, industry, size_category, is_public)
            VALUES (%s, %s, 'Government', 'Large', false)
            ON CONFLICT (normalized_name) DO NOTHING
        """, (employer, normalized_employer))
        conn.commit()

        cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized_employer,))
        company_id = cursor.fetchone()[0]

        # Get or create location
        cursor.execute("""
            INSERT INTO locations (city, state, country)
            VALUES (%s, %s, 'United States')
            ON CONFLICT (city, state, country) DO NOTHING
        """, ('Statewide', state_code))
        conn.commit()

        cursor.execute("SELECT id FROM locations WHERE city = %s AND state = %s", ('Statewide', state_code))
        location_id = cursor.fetchone()[0]

        # Process records in batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                try:
                    title = str(row.get('title', '')).strip()
                    if not title or title == 'nan' or title == 'Unknown':
                        stats['skipped'] += 1
                        continue

                    # Normalize title
                    result = normalizer.parse_title(title)

                    if not result.canonical_role_id:
                        stats['skipped'] += 1
                        continue

                    stats['normalized'] += 1

                    # Get salary
                    salary = row.get('salary')
                    if pd.isna(salary) or salary <= 0:
                        salary = None

                    # Insert job
                    cursor.execute("""
                        INSERT INTO observed_jobs (
                            raw_title, canonical_role_id, company_id, location_id,
                            source_id, seniority, seniority_confidence, title_confidence,
                            salary_point, source_type
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        title,
                        result.canonical_role_id,
                        company_id,
                        location_id,
                        source_id,
                        result.seniority,
                        result.seniority_confidence,
                        result.title_confidence,
                        salary,
                        'state_payroll'
                    ))

                    stats['inserted'] += 1

                except Exception as e:
                    stats['errors'] += 1
                    if stats['errors'] <= 5:  # Log first 5 errors
                        log.warning(f"  Error processing row: {str(e)}")

            # Commit batch
            conn.commit()

            # Progress update
            if (i + batch_size) % 10000 == 0:
                log.info(f"  Processed {i + batch_size:,} / {len(df):,} records...")

    finally:
        db.release_connection(conn)

    # Calculate match rate
    match_rate = (stats['normalized'] / stats['total_records'] * 100) if stats['total_records'] > 0 else 0

    log.info(f"\n{'='*70}")
    log.info(f"{state_name.upper()} INGESTION COMPLETE")
    log.info(f"{'='*70}")
    log.info(f"Total records:     {stats['total_records']:>10,}")
    log.info(f"Normalized:        {stats['normalized']:>10,} ({match_rate:.1f}%)")
    log.info(f"Inserted:          {stats['inserted']:>10,}")
    log.info(f"Skipped:           {stats['skipped']:>10,}")
    log.info(f"Errors:            {stats['errors']:>10,}")
    log.info("")

    return stats


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    log.info("="*70)
    log.info("MULTI-STATE PAYROLL INGESTION SYSTEM")
    log.info("="*70)
    log.info("")

    # Setup
    data_dir = Path('data/state_payrolls')
    data_dir.mkdir(parents=True, exist_ok=True)

    # Initialize
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()
    normalizer = TitleNormalizer(db)  # Pass db so it can load canonical roles

    # States to process - prioritize those with working URLs
    priority_states = ['OK', 'PA', 'IL']  # States with known working data.gov URLs

    # Also try states where we might have manual downloads
    all_states = list(STATE_CONFIGS.keys())

    log.info(f"Configured states: {', '.join(all_states)}")
    log.info(f"Priority states (auto-download): {', '.join(priority_states)}")
    log.info("")

    all_stats = {}

    for state_code in all_states:
        config_data = STATE_CONFIGS.get(state_code)
        if not config_data:
            continue

        log.info(f"\n{'='*70}")
        log.info(f"PROCESSING {config_data['name'].upper()}")
        log.info(f"{'='*70}\n")

        # Download data
        file_path = download_state_data(state_code, data_dir)

        if not file_path:
            log.info(f"⚠️  Skipping {config_data['name']} - no data file")
            continue

        # Load and standardize
        df = load_and_standardize(state_code, file_path)

        if df is None or len(df) == 0:
            log.info(f"⚠️  Skipping {config_data['name']} - no valid data loaded")
            continue

        # Ingest
        stats = ingest_state_payroll(state_code, df, normalizer, db)
        all_stats[state_code] = stats

    # Close database
    db.close_all_connections()

    # Final summary
    log.info("\n" + "="*70)
    log.info("FINAL SUMMARY - ALL STATES")
    log.info("="*70)

    if all_stats:
        total_records = sum(s['total_records'] for s in all_stats.values())
        total_inserted = sum(s['inserted'] for s in all_stats.values())
        total_normalized = sum(s['normalized'] for s in all_stats.values())

        log.info(f"\nStates processed:  {len(all_stats)}")
        log.info(f"Total records:     {total_records:,}")
        log.info(f"Total normalized:  {total_normalized:,}")
        log.info(f"Total inserted:    {total_inserted:,}")
        if total_records > 0:
            log.info(f"Overall match rate: {total_normalized/total_records*100:.1f}%")

        log.info("\nBy State:")
        for state_code, stats in all_stats.items():
            state_name = STATE_CONFIGS[state_code]['name']
            match_rate = stats['normalized'] / stats['total_records'] * 100 if stats['total_records'] > 0 else 0
            log.info(f"  {state_name:20s}: {stats['inserted']:>8,} jobs ({match_rate:>5.1f}% match)")
    else:
        log.info("\nNo states were successfully processed.")
        log.info("Consider manually downloading data for states with manual_download=True")

    log.info("\n" + "="*70)
    log.info("INGESTION COMPLETE!")
    log.info("="*70)


if __name__ == '__main__':
    main()
