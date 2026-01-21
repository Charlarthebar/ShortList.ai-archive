#!/usr/bin/env python3
"""
Migrate to Shortlist System
============================

Runs the schema migration from role-watching to shortlist system.

Author: ShortList.ai
Date: 2026-01-16
"""

import os
import sys
import logging

os.environ['DB_USER'] = 'noahhopkins'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import DatabaseManager, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def run_migration(cursor):
    """Run the shortlist migration SQL."""
    log.info("Running shortlist migration...")

    # Read and execute migration
    migration_path = os.path.join(os.path.dirname(__file__), 'schema_shortlist_migration.sql')
    if os.path.exists(migration_path):
        with open(migration_path, 'r') as f:
            migration_sql = f.read()

        # Split by semicolons and execute each statement
        # (needed because some statements can't be in multi-statement execute)
        statements = []
        current = []
        in_function = False

        for line in migration_sql.split('\n'):
            stripped = line.strip()

            # Track if we're inside a function definition
            if 'CREATE OR REPLACE FUNCTION' in line or 'CREATE FUNCTION' in line:
                in_function = True
            if in_function and stripped == '$$ LANGUAGE plpgsql;':
                in_function = False
                current.append(line)
                statements.append('\n'.join(current))
                current = []
                continue

            if not in_function and stripped.endswith(';') and not stripped.startswith('--'):
                current.append(line)
                statements.append('\n'.join(current))
                current = []
            else:
                current.append(line)

        # Execute each statement
        for i, stmt in enumerate(statements):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                try:
                    cursor.execute(stmt)
                    log.debug(f"Executed statement {i+1}")
                except Exception as e:
                    # Log but continue on expected errors (like "already exists")
                    if 'already exists' in str(e).lower() or 'does not exist' in str(e).lower():
                        log.debug(f"Skipped (already done): {str(e)[:50]}")
                    else:
                        log.warning(f"Statement {i+1} warning: {e}")

        log.info("Migration SQL executed")
    else:
        raise FileNotFoundError(f"Migration file not found: {migration_path}")


def update_monitoring_flags(cursor):
    """Update is_monitored flags based on posting_targets."""
    log.info("Updating monitoring flags...")

    # Set is_monitored=TRUE for positions where company is in posting_targets
    cursor.execute("""
        UPDATE watchable_positions wp
        SET is_monitored = TRUE,
            data_source = 'ats'
        WHERE EXISTS (
            SELECT 1 FROM posting_targets pt
            WHERE pt.company_id = wp.company_id AND pt.enabled = TRUE
        )
        AND (is_monitored IS NULL OR is_monitored = FALSE)
    """)
    monitored_count = cursor.rowcount
    log.info(f"Marked {monitored_count} positions as monitored (from ATS)")

    # Mark positions without posting_targets as historical
    cursor.execute("""
        UPDATE watchable_positions wp
        SET is_monitored = FALSE,
            data_source = 'historical'
        WHERE NOT EXISTS (
            SELECT 1 FROM posting_targets pt
            WHERE pt.company_id = wp.company_id AND pt.enabled = TRUE
        )
        AND (is_monitored IS NULL OR is_monitored = TRUE)
        AND data_source IS NULL
    """)
    historical_count = cursor.rowcount
    log.info(f"Marked {historical_count} positions as historical (not monitored)")


def get_stats(cursor):
    """Get current system stats."""
    stats = {}

    cursor.execute("SELECT COUNT(*) FROM watchable_positions")
    stats['total_positions'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM watchable_positions WHERE is_monitored = TRUE")
    stats['monitored_positions'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM watchable_positions WHERE is_monitored = FALSE OR is_monitored IS NULL")
    stats['historical_positions'] = cursor.fetchone()[0]

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'shortlist_applications'
        )
    """)
    stats['shortlist_table_exists'] = cursor.fetchone()[0]

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'role_configurations'
        )
    """)
    stats['role_config_table_exists'] = cursor.fetchone()[0]

    if stats['shortlist_table_exists']:
        cursor.execute("SELECT COUNT(*) FROM shortlist_applications")
        stats['total_applications'] = cursor.fetchone()[0]
    else:
        stats['total_applications'] = 0

    return stats


def main():
    log.info("=" * 60)
    log.info("SHORTLIST SYSTEM MIGRATION")
    log.info("=" * 60)

    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()

    try:
        cursor = conn.cursor()

        # Run migration
        run_migration(cursor)
        conn.commit()

        # Update monitoring flags
        update_monitoring_flags(cursor)
        conn.commit()

        # Get stats
        stats = get_stats(cursor)

        log.info("\n" + "=" * 60)
        log.info("MIGRATION COMPLETE")
        log.info("=" * 60)
        log.info(f"Total positions: {stats['total_positions']}")
        log.info(f"  Monitored (ATS): {stats['monitored_positions']}")
        log.info(f"  Historical: {stats['historical_positions']}")
        log.info(f"Shortlist table exists: {stats['shortlist_table_exists']}")
        log.info(f"Role config table exists: {stats['role_config_table_exists']}")
        log.info(f"Total applications: {stats['total_applications']}")

    except Exception as e:
        conn.rollback()
        log.error(f"Error during migration: {e}")
        raise
    finally:
        db.release_connection(conn)


if __name__ == "__main__":
    main()
