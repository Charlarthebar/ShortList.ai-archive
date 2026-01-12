#!/usr/bin/env python3
"""Add Phase 3 roles (21-35)."""
import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

new_roles = [
    ('Management Analyst', '13-1111', '13-1111.00', 'Business & Management', 'Business and Financial Operations'),
    ('Environmental Engineer', '17-2081', '17-2081.00', 'Engineering', 'Architecture and Engineering'),
    ('Paralegal', '23-2011', '23-2011.00', 'Legal', 'Legal'),
    ('Vocational Rehabilitation Counselor', '21-1015', '21-1015.00', 'Social Service', 'Community and Social Service'),
    ('Supervisor', '43-1011', '43-1011.00', 'Management', 'Management'),
    ('Highway Maintenance Worker', '47-4051', '47-4051.00', 'Construction & Maintenance', 'Construction and Extraction'),
    ('Lieutenant', '33-1012', '33-1012.00', 'Protective Service', 'Protective Service'),
    ('Sergeant', '33-1012', '33-1012.00', 'Protective Service', 'Protective Service'),
    ('Research Associate', '19-4061', '19-4061.00', 'Research', 'Life, Physical, and Social Science'),
    ('Solutions Architect', '15-1199', '15-1199.09', 'Engineering', 'Computer and Mathematical'),
    ('Librarian', '25-4021', '25-4021.00', 'Education & Library', 'Education, Training, and Library'),
    ('Firefighter', '33-2011', '33-2011.00', 'Protective Service', 'Protective Service'),
    ('Compliance Officer', '13-1041', '13-1041.07', 'Business & Compliance', 'Business and Financial Operations'),
    ('Tax Examiner', '13-2081', '13-2081.00', 'Accounting & Finance', 'Business and Financial Operations'),
    ('Mechanic', '49-3023', '49-3023.00', 'Maintenance & Repair', 'Installation, Maintenance, and Repair'),
]

config = Config()
db = DatabaseManager(config)
db.initialize_pool()

conn = db.get_connection()
cursor = conn.cursor()

added = 0
skipped = 0

for name, soc, onet, family, category in new_roles:
    cursor.execute("SELECT id FROM canonical_roles WHERE name = %s", (name,))
    if cursor.fetchone():
        print(f"- Skipped: {name} (already exists)")
        skipped += 1
        continue
    
    cursor.execute("""
        INSERT INTO canonical_roles (soc_code, onet_code, name, role_family, category)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (soc, onet, name, family, category))
    role_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✓ Added: {name} (id={role_id})")
    added += 1

cursor.execute("SELECT COUNT(*) FROM canonical_roles")
total = cursor.fetchone()[0]
print(f"\nTotal canonical roles: {total}")
print(f"Added: {added}, Skipped: {skipped}")

db.release_connection(conn)
db.close_all_connections()
