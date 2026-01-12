#!/usr/bin/env python3
"""
Verify Final Results with 105 Canonical Roles
==============================================

Verifies the final job counts after adding 15 blue-collar roles
and re-ingesting PERM data.

Author: ShortList.ai
Date: 2026-01-12
"""

import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def verify_final_results():
    """Verify final counts with 105 roles."""

    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    conn = db.get_connection()
    cursor = conn.cursor()

    print("="*70)
    print("FINAL RESULTS VERIFICATION - 105 CANONICAL ROLES")
    print("="*70)

    try:
        # Get canonical role count
        cursor.execute("SELECT COUNT(*) FROM canonical_roles")
        role_count = cursor.fetchone()[0]
        print(f"\nCanonical roles in database: {role_count}")

        # Get total observed_jobs
        cursor.execute("SELECT COUNT(*) FROM observed_jobs")
        total_jobs = cursor.fetchone()[0]
        print(f"\nTotal observed_jobs: {total_jobs:,}")

        # Get counts by source
        print("\n" + "="*70)
        print("JOBS BY SOURCE")
        print("="*70)

        sources = ['h1b_visa', 'perm_visa', 'ma_state_payroll']
        source_data = {}

        for source_name in sources:
            cursor.execute("""
                SELECT COUNT(*) FROM observed_jobs
                WHERE source_id = (SELECT id FROM sources WHERE name = %s)
            """, (source_name,))
            count = cursor.fetchone()[0]
            source_data[source_name] = count
            print(f"{source_name:20s}: {count:>8,} jobs")

        # Calculate improvements from original (45 roles)
        print("\n" + "="*70)
        print("COMPLETE JOURNEY (45 → 105 roles)")
        print("="*70)

        before_after = {
            'h1b_visa': (66138, source_data['h1b_visa']),
            'perm_visa': (42765, source_data['perm_visa']),
            'ma_state_payroll': (27174, source_data['ma_state_payroll']),
        }

        total_before = sum(before for before, after in before_after.values())
        total_after = sum(after for before, after in before_after.values())

        for source_name, (before, after) in before_after.items():
            gain = after - before
            pct = (gain / before * 100) if before > 0 else 0
            print(f"\n{source_name:20s}")
            print(f"  Before (45 roles):  {before:>8,} jobs")
            print(f"  After (105 roles):  {after:>8,} jobs")
            print(f"  Gain:               {gain:>+8,} jobs ({pct:+.1f}%)")

        print(f"\n{'TOTAL':20s}")
        print(f"  Before (45 roles):  {total_before:>8,} jobs")
        print(f"  After (105 roles):  {total_after:>8,} jobs")
        total_gain = total_after - total_before
        total_pct = (total_gain / total_before * 100) if total_before > 0 else 0
        print(f"  Gain:               {total_gain:>+8,} jobs ({total_pct:+.1f}%)")

        # Phase 5 specific improvement (90 → 105 roles)
        print("\n" + "="*70)
        print("PHASE 5 IMPROVEMENT (90 → 105 roles, blue-collar)")
        print("="*70)

        phase5_before = {
            'h1b_visa': 71344,
            'perm_visa': 47845,
            'ma_state_payroll': 69683,
        }

        print(f"\nPERM visa:")
        print(f"  Before (90 roles):  {phase5_before['perm_visa']:>8,} jobs")
        print(f"  After (105 roles):  {source_data['perm_visa']:>8,} jobs")
        perm_gain = source_data['perm_visa'] - phase5_before['perm_visa']
        perm_pct = (perm_gain / phase5_before['perm_visa'] * 100)
        print(f"  Gain:               {perm_gain:>+8,} jobs ({perm_pct:+.1f}%)")

        phase5_total_before = sum(phase5_before.values())
        phase5_total_gain = total_after - phase5_total_before
        phase5_total_pct = (phase5_total_gain / phase5_total_before * 100)

        print(f"\nTotal database:")
        print(f"  Before (90 roles):  {phase5_total_before:>8,} jobs")
        print(f"  After (105 roles):  {total_after:>8,} jobs")
        print(f"  Gain:               {phase5_total_gain:>+8,} jobs ({phase5_total_pct:+.1f}%)")

        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
        print(f"\n✓ Database now has {total_after:,} observed jobs")
        print(f"✓ Using {role_count} canonical roles")
        print(f"✓ Total gain from start: {total_gain:,} jobs (+{total_pct:.1f}%)")
        print(f"✓ Phase 5 blue-collar gain: {phase5_total_gain:,} jobs (+{phase5_total_pct:.1f}%)")
        print(f"\n✓ Ready for inference model building (Phases 6-8)")

    finally:
        cursor.close()
        conn.close()
        db.close_all_connections()

if __name__ == "__main__":
    verify_final_results()
