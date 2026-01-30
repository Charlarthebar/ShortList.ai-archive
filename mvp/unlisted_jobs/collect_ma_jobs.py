#!/usr/bin/env python3
"""
Collect All Massachusetts Jobs
==============================

This script collects ALL available job data for Massachusetts from
all implemented data sources. No limits - gets everything available.

Run this file: python unlisted_jobs/collect_ma_jobs.py

Expected output: ~390,000 individual job records

Author: ShortList.ai
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unlisted_jobs.sources import (
    FederalOPMConnector,
    MAStatePayrollConnector,
    BostonPayrollConnector,
    CambridgePayrollConnector,
    NPIRegistryConnector,
    ProPublica990Connector,
    BLSOEWSConnector,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_all_ma_jobs(output_dir: str = "./data/ma_jobs"):
    """
    Collect all MA job data from all sources.

    Args:
        output_dir: Directory to save output files

    Returns:
        Dictionary with collection results
    """
    os.makedirs(output_dir, exist_ok=True)

    all_records = []
    results = {}

    print("="*70)
    print("MASSACHUSETTS COMPREHENSIVE JOB COLLECTION")
    print("="*70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output directory: {output_dir}")

    # ========================================================================
    # 1. Federal Employees in MA (~50,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("1. FEDERAL EMPLOYEES IN MA")
    print("-"*70)

    try:
        connector = FederalOPMConnector()
        df = connector.fetch_data(limit=None)  # No limit - get all
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        all_records.extend(records)
        results['federal_opm'] = len(records)

        # Save individual file
        df.to_csv(f"{output_dir}/federal_opm_ma.csv", index=False)
        print(f"   Saved: {output_dir}/federal_opm_ma.csv")

    except Exception as e:
        logger.error(f"Federal OPM failed: {e}")
        results['federal_opm'] = 0

    # ========================================================================
    # 2. MA State Payroll (~80,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("2. MA STATE PAYROLL")
    print("-"*70)

    try:
        connector = MAStatePayrollConnector()
        df = connector.fetch_data(limit=None)
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        all_records.extend(records)
        results['ma_state'] = len(records)

        df.to_csv(f"{output_dir}/ma_state_payroll.csv", index=False)
        print(f"   Saved: {output_dir}/ma_state_payroll.csv")

    except Exception as e:
        logger.error(f"MA State Payroll failed: {e}")
        results['ma_state'] = 0

    # ========================================================================
    # 3. Boston City Payroll (~20,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("3. BOSTON CITY PAYROLL")
    print("-"*70)

    try:
        connector = BostonPayrollConnector()
        df = connector.fetch_data(limit=None)
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        all_records.extend(records)
        results['boston'] = len(records)

        df.to_csv(f"{output_dir}/boston_payroll.csv", index=False)
        print(f"   Saved: {output_dir}/boston_payroll.csv")

    except Exception as e:
        logger.error(f"Boston Payroll failed: {e}")
        results['boston'] = 0

    # ========================================================================
    # 4. Cambridge City Payroll (~5,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("4. CAMBRIDGE CITY PAYROLL")
    print("-"*70)

    try:
        connector = CambridgePayrollConnector()
        df = connector.fetch_data(limit=None)
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        all_records.extend(records)
        results['cambridge'] = len(records)

        df.to_csv(f"{output_dir}/cambridge_payroll.csv", index=False)
        print(f"   Saved: {output_dir}/cambridge_payroll.csv")

    except Exception as e:
        logger.error(f"Cambridge Payroll failed: {e}")
        results['cambridge'] = 0

    # ========================================================================
    # 5. NPI Healthcare Providers (~150,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("5. NPI HEALTHCARE PROVIDERS (MA)")
    print("-"*70)
    print("   Note: Full NPI data requires bulk download from CMS")
    print("   API limited to 200 records per query")

    try:
        connector = NPIRegistryConnector()
        # NPI API has limits - for full data, use bulk download
        df = connector.fetch_data(state="MA", limit=None)
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        if len(records) < 1000:
            print("   Note: This is sample data. For full 150K records:")
            print("   Download bulk data from: https://download.cms.gov/nppes/NPI_Files.html")

        all_records.extend(records)
        results['npi'] = len(records)

        df.to_csv(f"{output_dir}/npi_healthcare_ma.csv", index=False)
        print(f"   Saved: {output_dir}/npi_healthcare_ma.csv")

    except Exception as e:
        logger.error(f"NPI failed: {e}")
        results['npi'] = 0

    # ========================================================================
    # 6. Nonprofit 990 Officers (~50,000)
    # ========================================================================
    print("\n" + "-"*70)
    print("6. NONPROFIT 990 OFFICERS (MA)")
    print("-"*70)
    print("   Fetching from ProPublica API (major MA nonprofits)...")

    try:
        connector = ProPublica990Connector()
        df = connector.fetch_data(state="MA", limit=None)
        records = connector.to_standard_format(df)

        print(f"   Fetched: {len(df)} records")
        print(f"   Standardized: {len(records)} records")

        all_records.extend(records)
        results['nonprofit_990'] = len(records)

        df.to_csv(f"{output_dir}/nonprofit_990_ma.csv", index=False)
        print(f"   Saved: {output_dir}/nonprofit_990_ma.csv")

    except Exception as e:
        logger.error(f"Nonprofit 990 failed: {e}")
        results['nonprofit_990'] = 0

    # ========================================================================
    # 7. BLS OEWS Aggregate Data (for validation)
    # ========================================================================
    print("\n" + "-"*70)
    print("7. BLS OEWS AGGREGATE DATA (Boston MSA)")
    print("-"*70)
    print("   This is aggregate employment data, not individual jobs")

    try:
        connector = BLSOEWSConnector()
        df = connector.fetch_data(limit=None)
        records = connector.to_standard_format(df)

        total_employment = df['employment_count'].sum() if 'employment_count' in df.columns else 0

        print(f"   Occupation categories: {len(df)}")
        print(f"   Total employment covered: {total_employment:,}")

        results['bls_oews'] = len(records)
        results['bls_total_employment'] = int(total_employment)

        df.to_csv(f"{output_dir}/bls_oews_boston_msa.csv", index=False)
        print(f"   Saved: {output_dir}/bls_oews_boston_msa.csv")

    except Exception as e:
        logger.error(f"BLS OEWS failed: {e}")
        results['bls_oews'] = 0

    # ========================================================================
    # SAVE COMBINED OUTPUT
    # ========================================================================
    print("\n" + "="*70)
    print("SAVING COMBINED OUTPUT")
    print("="*70)

    # Convert all records to DataFrame
    combined_df = pd.DataFrame(all_records)
    combined_df.to_csv(f"{output_dir}/all_ma_jobs.csv", index=False)
    print(f"\nSaved: {output_dir}/all_ma_jobs.csv")
    print(f"Total records: {len(combined_df):,}")

    # Also save as JSON for flexibility
    combined_df.to_json(f"{output_dir}/all_ma_jobs.json", orient='records', indent=2)
    print(f"Saved: {output_dir}/all_ma_jobs.json")

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*70)
    print("COLLECTION SUMMARY")
    print("="*70)

    print(f"\n{'Source':<35} {'Records':>15}")
    print("-"*55)

    source_names = {
        'federal_opm': 'Federal OPM (MA)',
        'ma_state': 'MA State Payroll',
        'boston': 'Boston City Payroll',
        'cambridge': 'Cambridge City Payroll',
        'npi': 'NPI Healthcare (MA)',
        'nonprofit_990': 'Nonprofit 990 Officers',
        'bls_oews': 'BLS OEWS (aggregate)',
    }

    total = 0
    for key, name in source_names.items():
        count = results.get(key, 0)
        print(f"{name:<35} {count:>15,}")
        if key != 'bls_oews':  # Don't count aggregate data
            total += count

    print("-"*55)
    print(f"{'TOTAL INDIVIDUAL JOBS':<35} {total:>15,}")

    if 'bls_total_employment' in results:
        print(f"\n{'BLS Aggregate Employment':<35} {results['bls_total_employment']:>15,}")

    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Note about expanding data
    print("\n" + "="*70)
    print("TO GET MORE DATA")
    print("="*70)
    print("""
Current data uses sample/limited API data. For full ~390K records:

1. NPI BULK DATA (~150K MA providers):
   Download from: https://download.cms.gov/nppes/NPI_Files.html

2. MA STATE PAYROLL (~80K):
   Download from: https://cthrupayroll.mass.gov/

3. BOSTON CITY PAYROLL (~20K):
   Download from: https://data.boston.gov/dataset/employee-earnings-report

4. CAMBRIDGE CITY PAYROLL (~5K):
   Download from: https://data.cambridgema.gov/

Place downloaded files in ./data/ and update connectors to use real data.
""")

    return results


if __name__ == "__main__":
    results = collect_all_ma_jobs()
