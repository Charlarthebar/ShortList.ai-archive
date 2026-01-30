#!/usr/bin/env python3
"""
Add Phase 4 Canonical Roles (Roles 81-90)
==========================================

Adds 10 final canonical roles to push match rates even higher:
- Focus: Government edge cases, admin, healthcare, tech infrastructure

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def add_phase4_roles():
    """Add 10 Phase 4 roles to canonical_roles table."""

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    print("="*70)
    print("PHASE 4: Adding 10 Final Canonical Roles (81-90)")
    print("="*70)

    # Define Phase 4 roles
    new_roles = [
        # Government edge cases
        ('Developmental Services Worker', '21-1093', '21-1093.00', 'Social Service', 'Community and Social Service'),
        ('Child Support Enforcement Specialist', '23-1099', '23-1099.00', 'Legal Support', 'Legal'),
        ('Captain', '33-1011', '33-1011.00', 'Protective Service', 'Protective Service'),
        ('Caseworker', '21-1094', '21-1094.00', 'Social Service', 'Community and Social Service'),
        ('Inspector', '13-1041', '13-1041.00', 'Compliance', 'Business and Financial Operations'),

        # Tech infrastructure
        ('System Administrator', '15-1244', '15-1244.00', 'IT Operations', 'Computer and Mathematical'),

        # Administrative
        ('Administrative Secretary', '43-6014', '43-6014.00', 'Administrative Support', 'Office and Administrative Support'),

        # Healthcare expansion
        ('Recreational Therapist', '29-1125', '29-1125.00', 'Therapy', 'Healthcare Practitioners and Technical'),
        ('Nurse Practitioner', '29-1171', '29-1171.00', 'Advanced Practice Nursing', 'Healthcare Practitioners and Technical'),

        # Analytics
        ('Statistician', '15-2041', '15-2041.00', 'Data Science', 'Computer and Mathematical'),
    ]

    print(f"\nRoles to add: {len(new_roles)}")
    print("\nChecking which roles already exist...")

    conn = db.get_connection()
    cursor = conn.cursor()

    added_count = 0
    skipped_count = 0

    try:
        for name, soc_code, onet_code, role_family, category in new_roles:
            # Check if role already exists
            cursor.execute("""
                SELECT id FROM canonical_roles WHERE name = %s
            """, (name,))

            if cursor.fetchone():
                print(f"  ✓ Skipped: {name} (already exists)")
                skipped_count += 1
            else:
                # Insert new role
                cursor.execute("""
                    INSERT INTO canonical_roles
                    (soc_code, onet_code, name, role_family, category, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    RETURNING id
                """, (soc_code, onet_code, name, role_family, category))

                role_id = cursor.fetchone()[0]
                print(f"  ✓ Added: {name} (ID: {role_id})")
                added_count += 1

        conn.commit()

        print("\n" + "="*70)
        print("PHASE 4 COMPLETE")
        print("="*70)
        print(f"Roles added:   {added_count}")
        print(f"Roles skipped: {skipped_count}")
        print(f"Total roles:   {added_count + skipped_count}")

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM canonical_roles")
        total = cursor.fetchone()[0]
        print(f"\nCanonical roles in database: {total}")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        db.close_all_connections()

    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("1. Test match rates with 90 roles:")
    print("   python3 analyze_unmatched_all.py --source ma_state_payroll --limit 10000")
    print("   python3 analyze_unmatched_all.py --source h1b_visa --limit 10000")
    print("   python3 analyze_unmatched_all.py --source perm_visa --limit 10000")
    print("\n2. Re-ingest all data to capture additional jobs:")
    print("   python3 ingest_h1b.py --year 2024")
    print("   python3 ingest_perm.py --year 2024")
    print("   python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv")

if __name__ == "__main__":
    add_phase4_roles()
