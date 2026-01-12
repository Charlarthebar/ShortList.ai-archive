#!/usr/bin/env python3
"""
Re-ingest PERM Data Only
=========================

Deletes existing PERM observed_jobs and re-ingests with 105 canonical roles
to capture blue-collar jobs.

Author: ShortList.ai
Date: 2026-01-12
"""

import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def delete_perm_jobs():
    """Delete observed_jobs from PERM source."""

    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    print("="*70)
    print("STEP 1: Delete Existing PERM Observed Jobs")
    print("="*70)

    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        # Get PERM source ID
        cursor.execute("""
            SELECT id FROM sources WHERE name = 'perm_visa'
        """)
        source = cursor.fetchone()
        if not source:
            print("ERROR: perm_visa source not found")
            return

        source_id = source[0]
        print(f"\nFound PERM source (ID: {source_id})")

        # Count current jobs
        cursor.execute("""
            SELECT COUNT(*) FROM observed_jobs WHERE source_id = %s
        """, (source_id,))
        current_count = cursor.fetchone()[0]
        print(f"Current PERM observed_jobs: {current_count:,}")

        # Delete
        print("\nDeleting existing PERM jobs...")
        cursor.execute("""
            DELETE FROM observed_jobs WHERE source_id = %s
        """, (source_id,))
        deleted = cursor.rowcount
        conn.commit()

        print(f"✓ Deleted {deleted:,} PERM jobs")

        # Verify
        cursor.execute("""
            SELECT COUNT(*) FROM observed_jobs WHERE source_id = %s
        """, (source_id,))
        remaining = cursor.fetchone()[0]
        print(f"✓ Remaining PERM jobs: {remaining:,}")

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
    delete_perm_jobs()

    print("\n" + "="*70)
    print("NEXT STEP: Re-ingest PERM data")
    print("="*70)
    print("\nRun: DB_USER=noahhopkins python3 ingest_perm.py --year 2024")
    print("\nExpected: 47,845 → ~53,000 jobs (+10-13%)")
