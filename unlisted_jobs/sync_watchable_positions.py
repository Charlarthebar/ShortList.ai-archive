#!/usr/bin/env python3
"""
Sync Watchable Positions
========================

Syncs job postings from observed_jobs into watchable_positions
for the role-watching feature.

This should be run after:
1. The platform schema is created (schema_platform.sql)
2. Job postings have been refreshed (refresh_postings.py)

Author: ShortList.ai
Date: 2026-01-16
"""

import os
import sys
import logging
from datetime import datetime

os.environ['DB_USER'] = 'noahhopkins'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import DatabaseManager, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def create_platform_tables(cursor):
    """Create platform tables if they don't exist."""
    log.info("Checking platform tables...")

    # Check if tables already exist
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'platform_users'
        )
    """)
    if cursor.fetchone()[0]:
        log.info("Platform tables already exist, skipping schema creation")
        return

    # Read and execute schema
    schema_path = os.path.join(os.path.dirname(__file__), 'schema_platform.sql')
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        cursor.execute(schema_sql)
        log.info("Platform schema created")
    else:
        log.warning(f"Schema file not found: {schema_path}")


def sync_positions(cursor):
    """Sync observed_jobs to watchable_positions."""
    log.info("Syncing positions from observed_jobs...")

    # Get count of existing watchable positions
    cursor.execute("SELECT COUNT(*) FROM watchable_positions")
    existing_count = cursor.fetchone()[0]
    log.info(f"Existing watchable positions: {existing_count}")

    # Sync active postings from observed_jobs
    cursor.execute("""
        INSERT INTO watchable_positions (
            observed_job_id,
            company_id,
            company_name,
            title,
            department,
            location,
            salary_range,
            employment_type,
            description,
            status,
            apply_url,
            posted_at,
            created_at,
            updated_at
        )
        SELECT
            oj.id,
            oj.company_id,
            COALESCE(c.name, oj.raw_company),
            oj.raw_title,
            NULL as department,
            COALESCE(l.city || ', ' || l.state, oj.raw_location),
            CASE
                WHEN oj.salary_min IS NOT NULL AND oj.salary_max IS NOT NULL
                THEN '$' || (oj.salary_min / 1000)::int || 'k - $' || (oj.salary_max / 1000)::int || 'k'
                WHEN oj.salary_min IS NOT NULL
                THEN '$' || (oj.salary_min / 1000)::int || 'k+'
                ELSE NULL
            END,
            oj.employment_type,
            LEFT(oj.description, 5000),
            CASE WHEN oj.status = 'active' THEN 'open' ELSE 'filled' END,
            (oj.metadata->>'url')::text,
            COALESCE(oj.posted_date, oj.first_seen),
            oj.created_at,
            oj.updated_at
        FROM observed_jobs oj
        LEFT JOIN companies c ON oj.company_id = c.id
        LEFT JOIN locations l ON oj.location_id = l.id
        WHERE oj.source_type LIKE 'job_posting_%'
          AND NOT EXISTS (
              SELECT 1 FROM watchable_positions wp
              WHERE wp.observed_job_id = oj.id
          )
    """)

    new_count = cursor.rowcount
    log.info(f"Synced {new_count} new positions")

    # Update existing positions
    cursor.execute("""
        UPDATE watchable_positions wp
        SET
            status = CASE WHEN oj.status = 'active' THEN 'open' ELSE 'filled' END,
            updated_at = CURRENT_TIMESTAMP
        FROM observed_jobs oj
        WHERE wp.observed_job_id = oj.id
          AND wp.status != (CASE WHEN oj.status = 'active' THEN 'open' ELSE 'filled' END)
    """)

    updated_count = cursor.rowcount
    log.info(f"Updated {updated_count} existing positions")

    return new_count, updated_count


def update_watcher_counts(cursor):
    """Update watcher counts on positions."""
    log.info("Updating watcher counts...")

    cursor.execute("""
        UPDATE watchable_positions wp
        SET watcher_count = (
            SELECT COUNT(*) FROM job_watches jw
            WHERE jw.position_id = wp.id
        )
        WHERE wp.watcher_count != (
            SELECT COUNT(*) FROM job_watches jw
            WHERE jw.position_id = wp.id
        )
    """)

    log.info(f"Updated watcher counts for {cursor.rowcount} positions")


def get_stats(cursor):
    """Get current platform stats."""
    stats = {}

    cursor.execute("SELECT COUNT(*) FROM watchable_positions")
    stats['total_positions'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM watchable_positions WHERE status = 'open'")
    stats['open_positions'] = cursor.fetchone()[0]

    cursor.execute("""
        SELECT company_name, COUNT(*) as count
        FROM watchable_positions
        WHERE status = 'open'
        GROUP BY company_name
        ORDER BY count DESC
        LIMIT 10
    """)
    stats['top_companies'] = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM platform_users")
    stats['total_users'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM job_watches")
    stats['total_watches'] = cursor.fetchone()[0]

    return stats


def main():
    log.info("=" * 60)
    log.info("SYNC WATCHABLE POSITIONS")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()

    try:
        cursor = conn.cursor()

        # Create platform tables
        create_platform_tables(cursor)
        conn.commit()

        # Sync positions
        new_count, updated_count = sync_positions(cursor)
        conn.commit()

        # Update watcher counts
        update_watcher_counts(cursor)
        conn.commit()

        # Get stats
        stats = get_stats(cursor)

        log.info("\n" + "=" * 60)
        log.info("SYNC COMPLETE")
        log.info("=" * 60)
        log.info(f"Total watchable positions: {stats['total_positions']}")
        log.info(f"Open positions: {stats['open_positions']}")
        log.info(f"Total users: {stats['total_users']}")
        log.info(f"Total watches: {stats['total_watches']}")

        if stats['top_companies']:
            log.info("\nTop companies by open positions:")
            for company, count in stats['top_companies']:
                log.info(f"  {company}: {count}")

    except Exception as e:
        conn.rollback()
        log.error(f"Error during sync: {e}")
        raise
    finally:
        db.release_connection(conn)


if __name__ == "__main__":
    main()
