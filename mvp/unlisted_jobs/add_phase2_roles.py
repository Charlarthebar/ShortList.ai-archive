#!/usr/bin/env python3
"""Add Phase 2 roles (11-20)."""
import os
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

new_roles = [
    ('Applied Scientist', '15-2099', '15-2099.01', 'Data Science & Research', 'Computer and Mathematical'),
    ('Site Reliability Engineer', '15-1252', '15-1252.02', 'Engineering', 'Computer and Mathematical'),
    ('Nursing Assistant', '31-1131', '31-1131.00', 'Healthcare', 'Healthcare Support'),
    ('Occupational Therapist', '29-1122', '29-1122.00', 'Healthcare', 'Healthcare Practitioners'),
    ('Market Research Analyst', '13-1161', '13-1161.00', 'Business & Marketing', 'Business and Financial Operations'),
    ('Quantitative Analyst', '15-2099', '15-2099.01', 'Finance & Analysis', 'Computer and Mathematical'),
    ('Tax Specialist', '13-2082', '13-2082.00', 'Accounting & Finance', 'Business and Financial Operations'),
    ('Attorney', '23-1011', '23-1011.00', 'Legal', 'Legal'),
    ('Clerk', '43-9061', '43-9061.00', 'Administrative', 'Office and Administrative Support'),
    ('Technical Specialist', '15-1299', '15-1299.09', 'Technical Support', 'Computer and Mathematical'),
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
