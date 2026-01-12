#!/usr/bin/env python3
"""Add 10 new roles directly."""
import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

new_roles = [
    ('Social Worker', '21-1021', '21-1021.00', 'Healthcare & Social Service', 'Community and Social Service'),
    ('Correction Officer', '33-3012', '33-3012.00', 'Protective Service', 'Protective Service'),
    ('Police Officer', '33-3051', '33-3051.00', 'Protective Service', 'Protective Service'),
    ('Licensed Practical Nurse', '29-2061', '29-2061.00', 'Healthcare', 'Healthcare Practitioners'),
    ('Program Coordinator', '13-1199', '13-1199.00', 'Administrative', 'Business and Financial Operations'),
    ('Environmental Analyst', '19-2041', '19-2041.00', 'Science', 'Life, Physical, and Social Science'),
    ('Human Services Coordinator', '21-1093', '21-1093.00', 'Social Service', 'Community and Social Service'),
    ('Mental Health Worker', '21-1014', '21-1014.00', 'Healthcare & Social Service', 'Community and Social Service'),
    ('Technical Program Manager', '11-9199', '11-9199.00', 'Management', 'Management'),
    ('Staff Engineer', '15-1252', '15-1252.00', 'Engineering', 'Computer and Mathematical'),
]

config = Config()
db = DatabaseManager(config)
db.initialize_pool()

conn = db.get_connection()
cursor = conn.cursor()

for name, soc, onet, family, category in new_roles:
    # Check if exists
    cursor.execute("SELECT id FROM canonical_roles WHERE name = %s", (name,))
    if cursor.fetchone():
        print(f"- Skipped: {name} (already exists)")
        continue
    
    # Insert
    cursor.execute("""
        INSERT INTO canonical_roles (soc_code, onet_code, name, role_family, category)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (soc, onet, name, family, category))
    role_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✓ Added: {name} (id={role_id})")

# Count total
cursor.execute("SELECT COUNT(*) FROM canonical_roles")
total = cursor.fetchone()[0]
print(f"\nTotal canonical roles: {total}")

db.release_connection(conn)
db.close_all_connections()
