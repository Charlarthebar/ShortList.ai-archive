#!/usr/bin/env python3
"""
Populate Posting Targets
========================

Adds all known-ATS companies from company_targets.py to the posting_targets table.

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import logging

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from sources.job_postings.company_targets import (
    GREENHOUSE_COMPANIES,
    LEVER_COMPANIES,
    SMARTRECRUITERS_COMPANIES,
    WORKDAY_COMPANIES,
    ASHBY_COMPANIES,
    RIPPLING_COMPANIES
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def get_or_create_company(cursor, company_name: str) -> int:
    """Get or create a company in the companies table."""
    normalized = company_name.lower().strip()

    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO companies (name, normalized_name)
        VALUES (%s, %s) RETURNING id
    """, (company_name, normalized))
    return cursor.fetchone()[0]


def add_posting_target(cursor, company_id_ats: str, company_name: str, ats_type: str,
                       careers_url: str, db_company_id: int = None):
    """Add a posting target if it doesn't exist."""
    # Check if already exists
    cursor.execute("""
        SELECT id FROM posting_targets
        WHERE company_id_ats = %s AND ats_type = %s
    """, (company_id_ats, ats_type))

    if cursor.fetchone():
        return False  # Already exists

    cursor.execute("""
        INSERT INTO posting_targets
        (company_id_ats, company_name, ats_type, careers_url, db_company_id, enabled, fetch_frequency_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (company_id_ats, company_name, ats_type, careers_url, db_company_id, True, 24))

    return True


def main():
    log.info("=" * 60)
    log.info("POPULATING POSTING TARGETS")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    added = 0
    skipped = 0

    # Add Greenhouse companies
    log.info(f"\nAdding {len(GREENHOUSE_COMPANIES)} Greenhouse companies...")
    for company_id, company_name in GREENHOUSE_COMPANIES.items():
        careers_url = f"https://boards.greenhouse.io/{company_id}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "greenhouse", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    # Add Lever companies
    log.info(f"Adding {len(LEVER_COMPANIES)} Lever companies...")
    for company_id, company_name in LEVER_COMPANIES.items():
        careers_url = f"https://jobs.lever.co/{company_id}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "lever", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    # Add SmartRecruiters companies
    log.info(f"Adding {len(SMARTRECRUITERS_COMPANIES)} SmartRecruiters companies...")
    for company_id, company_name in SMARTRECRUITERS_COMPANIES.items():
        careers_url = f"https://jobs.smartrecruiters.com/{company_id}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "smartrecruiters", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    # Add Workday companies
    log.info(f"Adding {len(WORKDAY_COMPANIES)} Workday companies...")
    for (company_id, workday_host, tenant), company_name in WORKDAY_COMPANIES.items():
        careers_url = f"https://{company_id}.{workday_host}/en-US/{tenant}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "workday", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    # Add Ashby companies
    log.info(f"Adding {len(ASHBY_COMPANIES)} Ashby companies...")
    for company_id, company_name in ASHBY_COMPANIES.items():
        careers_url = f"https://jobs.ashbyhq.com/{company_id}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "ashby", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    # Add Rippling companies
    log.info(f"Adding {len(RIPPLING_COMPANIES)} Rippling companies...")
    for company_id, company_name in RIPPLING_COMPANIES.items():
        careers_url = f"https://ats.rippling.com/{company_id}"
        db_company_id = get_or_create_company(cursor, company_name)

        if add_posting_target(cursor, company_id, company_name, "rippling", careers_url, db_company_id):
            added += 1
        else:
            skipped += 1

    conn.commit()

    log.info(f"\n{'=' * 60}")
    log.info(f"COMPLETE: {added} added, {skipped} already existed")

    # Show summary
    cursor.execute("""
        SELECT ats_type, COUNT(*), SUM(CASE WHEN enabled THEN 1 ELSE 0 END) as enabled_count
        FROM posting_targets
        GROUP BY ats_type
        ORDER BY COUNT(*) DESC
    """)
    log.info("\nPosting targets by ATS type:")
    for row in cursor.fetchall():
        log.info(f"  {row[0]}: {row[1]} total, {row[2]} enabled")

    cursor.execute("SELECT COUNT(*) FROM posting_targets")
    log.info(f"\nTotal posting targets: {cursor.fetchone()[0]}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
