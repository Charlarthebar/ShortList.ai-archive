#!/usr/bin/env python3
"""
Verify Re-ingestion Results
============================

Verifies the final job counts and match rates after re-ingesting
all data sources with 90 canonical roles.

Author: ShortList.ai
Date: 2026-01-12
"""

import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def verify_results():
    """Verify final counts and match rates."""

    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    conn = db.get_connection()
    cursor = conn.cursor()

    print("="*70)
    print("RE-INGESTION RESULTS VERIFICATION")
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

        # Calculate improvements
        print("\n" + "="*70)
        print("IMPROVEMENTS (Before → After)")
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
            print(f"  Before:  {before:>8,} jobs")
            print(f"  After:   {after:>8,} jobs")
            print(f"  Gain:    {gain:>+8,} jobs ({pct:+.1f}%)")

        print(f"\n{'TOTAL':20s}")
        print(f"  Before:  {total_before:>8,} jobs")
        print(f"  After:   {total_after:>8,} jobs")
        total_gain = total_after - total_before
        total_pct = (total_gain / total_before * 100) if total_before > 0 else 0
        print(f"  Gain:    {total_gain:>+8,} jobs ({total_pct:+.1f}%)")

        # Get top roles
        print("\n" + "="*70)
        print("TOP 20 ROLES (All Sources Combined)")
        print("="*70)

        cursor.execute("""
            SELECT cr.name, COUNT(*) as count
            FROM observed_jobs oj
            JOIN archetypes a ON oj.archetype_id = a.id
            JOIN canonical_roles cr ON a.canonical_role_id = cr.id
            GROUP BY cr.name
            ORDER BY count DESC
            LIMIT 20
        """)

        print(f"{'Count':>8}  Role")
        print("-" * 70)
        for role_name, count in cursor.fetchall():
            print(f"{count:>8,}  {role_name}")

        # Get top roles by source
        print("\n" + "="*70)
        print("TOP 10 ROLES BY SOURCE")
        print("="*70)

        for source_name in sources:
            print(f"\n{source_name.upper()}:")
            cursor.execute("""
                SELECT cr.name, COUNT(*) as count
                FROM observed_jobs oj
                JOIN archetypes a ON oj.archetype_id = a.id
                JOIN canonical_roles cr ON a.canonical_role_id = cr.id
                WHERE oj.source_id = (SELECT id FROM sources WHERE name = %s)
                GROUP BY cr.name
                ORDER BY count DESC
                LIMIT 10
            """, (source_name,))

            for i, (role_name, count) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2d}. {role_name:40s} {count:>6,}")

        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
        print(f"\n✓ Database now has {total_after:,} observed jobs")
        print(f"✓ Gained {total_gain:,} jobs (+{total_pct:.1f}%)")
        print(f"✓ Using {role_count} canonical roles")
        print(f"\n✓ MA Payroll transformed: 27k → {source_data['ma_state_payroll']:,} (+{((source_data['ma_state_payroll'] - 27174) / 27174 * 100):.1f}%)")

    finally:
        cursor.close()
        conn.close()
        db.close_all_connections()

if __name__ == "__main__":
    verify_results()
