#!/usr/bin/env python3
"""
Add Phase 5 Canonical Roles (Roles 91-105) - Blue-Collar & Service
===================================================================

Adds 15 blue-collar and service roles to capture PERM visa data:
- Transportation: Truck Driver
- Warehousing: Warehouse Worker
- Hospitality: Housekeeper, Server
- Food Service: Cook, Food Service Worker
- Care: Caregiver, Nanny
- Manufacturing: General Laborer, Production Worker, Poultry Worker
- Maintenance: Landscape Laborer, Janitor
- Animals: Animal Caretaker
- Textiles: Sewing Machine Operator

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

def add_phase5_roles():
    """Add 15 Phase 5 blue-collar/service roles to canonical_roles table."""

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    print("="*70)
    print("PHASE 5: Adding 15 Blue-Collar & Service Roles (91-105)")
    print("="*70)

    # Define Phase 5 roles
    new_roles = [
        # Transportation & Warehousing
        ('Truck Driver', '53-3032', '53-3032.00', 'Transportation', 'Transportation and Material Moving'),
        ('Warehouse Worker', '53-7065', '53-7065.00', 'Material Handling', 'Transportation and Material Moving'),

        # Hospitality & Cleaning
        ('Housekeeper', '37-2012', '37-2012.00', 'Cleaning & Maintenance', 'Building and Grounds Cleaning and Maintenance'),
        ('Janitor', '37-2011', '37-2011.00', 'Cleaning & Maintenance', 'Building and Grounds Cleaning and Maintenance'),

        # Food Service
        ('Cook', '35-2014', '35-2014.00', 'Food Preparation', 'Food Preparation and Serving'),
        ('Server', '35-3031', '35-3031.00', 'Food Service', 'Food Preparation and Serving'),
        ('Food Service Worker', '35-3023', '35-3023.00', 'Food Service', 'Food Preparation and Serving'),

        # Personal Care
        ('Caregiver', '39-9021', '39-9021.00', 'Personal Care', 'Personal Care and Service'),
        ('Nanny', '39-9011', '39-9011.00', 'Childcare', 'Personal Care and Service'),

        # Manufacturing & Production
        ('General Laborer', '51-9199', '51-9199.00', 'Production', 'Production Occupations'),
        ('Production Worker', '51-2090', '51-2090.00', 'Manufacturing', 'Production Occupations'),
        ('Poultry Worker', '51-3022', '51-3022.00', 'Food Processing', 'Production Occupations'),

        # Grounds Maintenance
        ('Landscape Laborer', '37-3011', '37-3011.00', 'Grounds Maintenance', 'Building and Grounds Cleaning and Maintenance'),

        # Animal Care
        ('Animal Caretaker', '39-2021', '39-2021.00', 'Animal Care', 'Personal Care and Service'),

        # Textile Production
        ('Sewing Machine Operator', '51-6031', '51-6031.00', 'Textile Production', 'Production Occupations'),
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
        print("PHASE 5 COMPLETE")
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
    print("1. Re-ingest PERM data to capture blue-collar jobs:")
    print("   DB_USER=noahhopkins python3 ingest_perm.py --year 2024")
    print("\n2. Expected improvement:")
    print("   PERM: 47,845 → ~53,000 jobs (+5,000-6,000 jobs, +10-13%)")
    print("   Total: 188,872 → ~194,000 jobs (+3%)")
    print("\n3. Then proceed to building inference models (Phases 6-8)")

if __name__ == "__main__":
    add_phase5_roles()
