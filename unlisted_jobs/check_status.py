#!/usr/bin/env python3
"""
Quick database status check.

Shows current counts of jobs, companies, and other key metrics.
"""

import os
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

from database import DatabaseManager, Config

config = Config()
db = DatabaseManager(config)
db.initialize_pool()

conn = db.get_connection()
cursor = conn.cursor()

print("="*60)
print("DATABASE STATUS")
print("="*60)

# Canonical roles
cursor.execute("SELECT COUNT(*) FROM canonical_roles")
print(f"\nCanonical roles: {cursor.fetchone()[0]}")

# Companies
cursor.execute("SELECT COUNT(*) FROM companies")
print(f"Companies: {cursor.fetchone()[0]}")

# Locations
cursor.execute("SELECT COUNT(*) FROM locations")
print(f"Locations: {cursor.fetchone()[0]}")

# Sources
cursor.execute("SELECT name, COUNT(*) as jobs FROM sources s LEFT JOIN observed_jobs o ON s.id = o.source_id GROUP BY s.name")
print(f"\nJobs by source:")
for row in cursor.fetchall():
    print(f"  {row[0]:<20} {row[1]:,} jobs")

# Total observed jobs
cursor.execute("SELECT COUNT(*) FROM observed_jobs")
total_jobs = cursor.fetchone()[0]
print(f"\nTotal observed jobs: {total_jobs:,}")

# Jobs with salary
cursor.execute("SELECT COUNT(*) FROM observed_jobs WHERE salary_min IS NOT NULL")
jobs_with_salary = cursor.fetchone()[0]
print(f"Jobs with salary: {jobs_with_salary:,} ({jobs_with_salary/total_jobs*100:.1f}%)")

# Salary stats
cursor.execute("""
    SELECT
        AVG(salary_min) as avg,
        MIN(salary_min) as min,
        MAX(salary_min) as max
    FROM observed_jobs
    WHERE salary_min IS NOT NULL
""")
stats = cursor.fetchone()
print(f"\nSalary statistics:")
print(f"  Average: ${stats[0]:,.0f}")
print(f"  Range: ${stats[1]:,.0f} - ${stats[2]:,.0f}")

# Top roles
print(f"\nTop 10 roles:")
cursor.execute("""
    SELECT r.name, COUNT(*) as count
    FROM observed_jobs o
    JOIN canonical_roles r ON o.canonical_role_id = r.id
    GROUP BY r.name
    ORDER BY count DESC
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"  {row[1]:6,}  {row[0]}")

db.release_connection(conn)
db.close_all_connections()

print()
