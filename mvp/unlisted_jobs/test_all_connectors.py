#!/usr/bin/env python3
"""
Test All Data Source Connectors
================================

Verifies all implemented data source connectors work correctly.
Demonstrates the full data collection capability for Massachusetts.

Expected MA Record Counts:
| Source                      | Records    | Confidence |
|----------------------------|------------|------------|
| Federal employees (MA)      | ~50,000    | 0.95       |
| MA State payroll           | ~80,000    | 0.95       |
| City payroll (Boston, etc) | ~30,000    | 0.90       |
| NPI Healthcare (MA)        | ~150,000   | 0.90       |
| Nonprofit 990 officers     | ~50,000    | 0.80       |
| H-1B/PERM (MA)            | ~30,000    | 0.85       |
| **MA TOTAL**              | **~390,000** |          |

Plus BLS OEWS aggregate data for validation (~160M jobs nationally).

Author: ShortList.ai
"""

import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mvp.unlisted_jobs.sources import (
    # Base classes
    BaseConnector,
    GovernmentPayrollConnector,
    LicensedProfessionalConnector,
    NonprofitConnector,
    # Tier A
    H1BVisaConnector,
    PERMVisaConnector,
    MAStatePayrollConnector,
    NPIRegistryConnector,
    FederalOPMConnector,
    BostonPayrollConnector,
    CambridgePayrollConnector,
    # Tier B
    ProPublica990Connector,
    # Tier C
    BLSOEWSConnector,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_connector(name: str, connector, limit: int = 50):
    """Test a single connector and return summary stats."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print('='*60)

    try:
        # Fetch data
        df = connector.fetch_data(limit=limit)
        print(f"  Records fetched: {len(df)}")

        if len(df) == 0:
            print("  WARNING: No records returned!")
            return {'name': name, 'records': 0, 'success': False}

        # Show columns
        print(f"  Columns: {list(df.columns)[:8]}...")

        # Convert to standard format
        records = connector.to_standard_format(df)
        print(f"  Standardized records: {len(records)}")

        # Show sample
        if records:
            sample = records[0]
            print(f"\n  Sample Record:")
            print(f"    Title: {sample.get('raw_title', 'N/A')}")
            print(f"    Company: {sample.get('raw_company', 'N/A')}")
            print(f"    Location: {sample.get('raw_location', 'N/A')}")
            print(f"    Salary: {sample.get('raw_salary_text', 'N/A')}")
            print(f"    Confidence: {sample.get('confidence_score', 'N/A')}")
            print(f"    Status: {sample.get('job_status', 'N/A')}")

        return {
            'name': name,
            'records': len(records),
            'success': True,
            'columns': list(df.columns),
        }

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {'name': name, 'records': 0, 'success': False, 'error': str(e)}


def main():
    """Test all connectors."""
    print("="*60)
    print("COMPREHENSIVE JOB DATABASE - CONNECTOR TEST SUITE")
    print("="*60)
    print("\nTesting all data source connectors for Massachusetts...")

    results = []

    # ========================================================================
    # TIER A: High Reliability (0.85-0.95)
    # ========================================================================

    print("\n\n" + "#"*60)
    print("# TIER A: HIGH RELIABILITY SOURCES (0.85-0.95)")
    print("#"*60)

    # 1. Federal OPM (federal employees in MA)
    connector = FederalOPMConnector()
    results.append(test_connector("Federal OPM (MA)", connector, limit=100))

    # 2. MA State Payroll
    connector = MAStatePayrollConnector()
    results.append(test_connector("MA State Payroll", connector, limit=100))

    # 3. Boston City Payroll
    connector = BostonPayrollConnector()
    results.append(test_connector("Boston City Payroll", connector, limit=100))

    # 4. Cambridge City Payroll
    connector = CambridgePayrollConnector()
    results.append(test_connector("Cambridge City Payroll", connector, limit=100))

    # 5. NPI Healthcare Registry
    connector = NPIRegistryConnector()
    results.append(test_connector("NPI Healthcare (MA)", connector, limit=100))

    # 6. H-1B Visa (if available)
    try:
        connector = H1BVisaConnector()
        results.append(test_connector("H-1B Visa (MA)", connector, limit=100))
    except Exception as e:
        print(f"\nH-1B Visa connector not available: {e}")

    # 7. PERM Visa (if available)
    try:
        connector = PERMVisaConnector()
        results.append(test_connector("PERM Visa (MA)", connector, limit=100))
    except Exception as e:
        print(f"\nPERM Visa connector not available: {e}")

    # ========================================================================
    # TIER B: Medium Reliability (0.70-0.85)
    # ========================================================================

    print("\n\n" + "#"*60)
    print("# TIER B: MEDIUM RELIABILITY SOURCES (0.70-0.85)")
    print("#"*60)

    # 8. ProPublica 990 Nonprofits
    connector = ProPublica990Connector()
    results.append(test_connector("ProPublica 990 (MA Nonprofits)", connector, limit=50))

    # ========================================================================
    # TIER C: Macro/Aggregate Data (Validation)
    # ========================================================================

    print("\n\n" + "#"*60)
    print("# TIER C: MACRO/AGGREGATE DATA (Validation)")
    print("#"*60)

    # 9. BLS OEWS
    connector = BLSOEWSConnector()
    results.append(test_connector("BLS OEWS (Boston MSA)", connector, limit=50))

    # ========================================================================
    # SUMMARY
    # ========================================================================

    print("\n\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    total_records = 0
    successful = 0
    failed = 0

    print(f"\n{'Source':<35} {'Records':>10} {'Status':>10}")
    print("-"*60)

    for r in results:
        status = "OK" if r['success'] else "FAILED"
        print(f"{r['name']:<35} {r['records']:>10} {status:>10}")
        total_records += r['records']
        if r['success']:
            successful += 1
        else:
            failed += 1

    print("-"*60)
    print(f"{'TOTAL':<35} {total_records:>10}")

    print(f"\n\nConnectors tested: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")

    # Estimate full collection
    print("\n\n" + "="*60)
    print("ESTIMATED FULL COLLECTION (Massachusetts)")
    print("="*60)

    estimates = [
        ("Federal employees (MA)", 50000, 0.95),
        ("MA State payroll", 80000, 0.95),
        ("Boston City payroll", 20000, 0.95),
        ("Cambridge City payroll", 5000, 0.95),
        ("Other city payroll", 5000, 0.90),
        ("NPI Healthcare (MA)", 150000, 0.90),
        ("H-1B/PERM Visa (MA)", 30000, 0.85),
        ("Nonprofit 990 officers (MA)", 50000, 0.80),
    ]

    print(f"\n{'Source':<35} {'Est. Records':>15} {'Confidence':>12}")
    print("-"*65)

    grand_total = 0
    for name, count, conf in estimates:
        print(f"{name:<35} {count:>15,} {conf:>12.2f}")
        grand_total += count

    print("-"*65)
    print(f"{'MA INDIVIDUAL RECORDS':<35} {grand_total:>15,}")

    print("\n\nPlus BLS OEWS aggregate data:")
    print("  - Boston-Cambridge MSA: ~2.9M total employment")
    print("  - 800+ occupation codes with wage distributions")

    print("\n\nNational expansion potential:")
    print("  - ~50M individual job records (all states)")
    print("  - ~160M employment count from BLS aggregates")

    return results


if __name__ == "__main__":
    results = main()
    sys.exit(0 if all(r['success'] for r in results) else 1)
