#!/usr/bin/env python3
"""
Delete and Re-ingest All Data Sources
======================================

Deletes existing observed_jobs from H-1B, PERM, and MA Payroll sources,
then re-ingests all data with the expanded 90 canonical roles.

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def delete_existing_jobs():
    """Delete observed_jobs from all 3 sources."""

    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    print("="*70)
    print("STEP 1: Delete Existing Observed Jobs")
    print("="*70)

    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        # Get source IDs
        cursor.execute("""
            SELECT id, name FROM sources
            WHERE name IN ('h1b_visa', 'perm_visa', 'ma_state_payroll')
        """)
        sources = cursor.fetchall()

        print(f"\nFound {len(sources)} sources:")
        for source_id, name in sources:
            print(f"  - {name} (ID: {source_id})")

        # Count current jobs
        cursor.execute("""
            SELECT COUNT(*) FROM observed_jobs
            WHERE source_id IN (
                SELECT id FROM sources
                WHERE name IN ('h1b_visa', 'perm_visa', 'ma_state_payroll')
            )
        """)
        current_count = cursor.fetchone()[0]
        print(f"\nCurrent observed_jobs: {current_count:,}")

        # Delete
        print("\nDeleting existing jobs...")
        cursor.execute("""
            DELETE FROM observed_jobs
            WHERE source_id IN (
                SELECT id FROM sources
                WHERE name IN ('h1b_visa', 'perm_visa', 'ma_state_payroll')
            )
        """)
        deleted = cursor.rowcount
        conn.commit()

        print(f"✓ Deleted {deleted:,} jobs")

        # Verify
        cursor.execute("""
            SELECT COUNT(*) FROM observed_jobs
            WHERE source_id IN (
                SELECT id FROM sources
                WHERE name IN ('h1b_visa', 'perm_visa', 'ma_state_payroll')
            )
        """)
        remaining = cursor.fetchone()[0]
        print(f"✓ Remaining jobs: {remaining:,}")

        print("\n" + "="*70)
        print("DELETION COMPLETE")
        print("="*70)

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        db.close_all_connections()

if __name__ == "__main__":
    delete_existing_jobs()

    print("\n" + "="*70)
    print("NEXT STEPS: Re-ingest data")
    print("="*70)
    print("\n1. Re-ingest H-1B (expected: ~72k jobs, ~10 min):")
    print("   DB_USER=noahhopkins python3 ingest_h1b.py --year 2024")
    print("\n2. Re-ingest PERM (expected: ~51k jobs, ~5 min):")
    print("   DB_USER=noahhopkins python3 ingest_perm.py --year 2024")
    print("\n3. Re-ingest MA Payroll (expected: ~92k jobs, ~30 min):")
    print("   DB_USER=noahhopkins python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv")
    print("\n" + "="*70)
