#!/usr/bin/env python3
"""
Job Posting Refresh Scheduler
==============================

Refreshes job postings from all enabled targets. Designed to be run via cron.

Usage:
    # Run all targets
    python refresh_postings.py

    # Run specific ATS type
    python refresh_postings.py --ats greenhouse

    # Run specific company
    python refresh_postings.py --company stripe

    # Dry run (don't save)
    python refresh_postings.py --dry-run

Cron example (run every 6 hours):
    0 */6 * * * cd /Users/noahhopkins/ShortList.ai/unlisted_jobs && python3 refresh_postings.py >> /var/log/posting_refresh.log 2>&1

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from sources.job_postings.posting_ingestion import PostingIngestionManager, IngestionTarget
from sources.job_postings.greenhouse import GreenhouseConnector
from sources.job_postings.lever import LeverConnector
from sources.job_postings.smartrecruiters import SmartRecruitersConnector
from sources.job_postings.workday import WorkdayConnector
from sources.job_postings.ashby import AshbyConnector
from sources.job_postings.rippling import RipplingConnector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


class PostingRefresher:
    """
    Handles scheduled refresh of job postings.
    """

    # Extended connector registry
    CONNECTOR_CLASSES = {
        'greenhouse': GreenhouseConnector,
        'lever': LeverConnector,
        'smartrecruiters': SmartRecruitersConnector,
        'workday': WorkdayConnector,
        'ashby': AshbyConnector,
        'rippling': RipplingConnector,
    }

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config()
        self.db = DatabaseManager(self.config)
        self.manager = PostingIngestionManager()

        # Extend manager's connector classes
        self.manager.CONNECTOR_CLASSES = self.CONNECTOR_CLASSES

        self.stats = {
            'targets_due': 0,
            'targets_processed': 0,
            'targets_skipped': 0,
            'postings_fetched': 0,
            'postings_new': 0,
            'errors': 0,
        }

    def close(self):
        self.manager.close()

    def get_targets_due_for_refresh(self, ats_type: str = None,
                                     company_id: str = None) -> List[IngestionTarget]:
        """
        Get targets that need refreshing based on fetch_frequency_hours.
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT id, company_name, company_id_ats, ats_type, careers_url,
                           enabled, last_fetched, db_company_id, fetch_frequency_hours
                    FROM posting_targets
                    WHERE enabled = true
                """
                params = []

                if ats_type:
                    query += " AND ats_type = %s"
                    params.append(ats_type)

                if company_id:
                    query += " AND company_id_ats = %s"
                    params.append(company_id)

                # Only get targets due for refresh
                query += """
                    AND (last_fetched IS NULL
                         OR last_fetched < NOW() - (fetch_frequency_hours || ' hours')::interval)
                """

                query += " ORDER BY last_fetched ASC NULLS FIRST"

                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [
                    IngestionTarget(
                        id=row[0],
                        company_name=row[1],
                        company_id_ats=row[2],
                        ats_type=row[3],
                        careers_url=row[4],
                        enabled=row[5],
                        last_fetched=row[6],
                        db_company_id=row[7]
                    )
                    for row in rows
                ]
        finally:
            self.db.release_connection(conn)

    def refresh_target(self, target: IngestionTarget) -> dict:
        """
        Refresh a single target.
        """
        connector_class = self.CONNECTOR_CLASSES.get(target.ats_type)
        if not connector_class:
            log.warning(f"Unknown ATS type: {target.ats_type} for {target.company_name}")
            return {'error': f'Unknown ATS type: {target.ats_type}'}

        try:
            # Create connector
            if target.ats_type == 'workday':
                # Workday needs special handling - extract host/tenant from URL
                connector = connector_class(target.company_id_ats, target.company_name)
            else:
                connector = connector_class(target.company_id_ats, target.company_name)

            # Fetch jobs
            postings = connector.fetch_jobs()
            log.info(f"  Fetched {len(postings)} postings from {target.company_name}")

            if self.dry_run:
                return {
                    'fetched': len(postings),
                    'new': 0,
                    'dry_run': True
                }

            # Use the manager's ingest method
            result = self.manager.ingest_target(target)
            return result

        except Exception as e:
            log.error(f"  Error refreshing {target.company_name}: {e}")
            return {'error': str(e)}

    def run(self, ats_type: str = None, company_id: str = None,
            force_all: bool = False, limit: int = None):
        """
        Run the refresh for all due targets.
        """
        log.info("=" * 70)
        log.info(f"POSTING REFRESH - {datetime.now().isoformat()}")
        log.info("=" * 70)

        if self.dry_run:
            log.info("DRY RUN MODE - No changes will be saved")

        start_time = datetime.now()

        # Get targets
        if force_all:
            targets = self.manager.get_enabled_targets(ats_type)
            log.info(f"Force refresh: {len(targets)} targets")
        else:
            targets = self.get_targets_due_for_refresh(ats_type, company_id)
            log.info(f"Targets due for refresh: {len(targets)}")

        if limit:
            targets = targets[:limit]
            log.info(f"Limited to {limit} targets")

        self.stats['targets_due'] = len(targets)

        # Process each target
        for i, target in enumerate(targets, 1):
            log.info(f"\n[{i}/{len(targets)}] {target.company_name} ({target.ats_type})")

            # Skip unsupported ATS types for now
            if target.ats_type not in self.CONNECTOR_CLASSES:
                log.info(f"  Skipping - {target.ats_type} connector not ready")
                self.stats['targets_skipped'] += 1
                continue

            result = self.refresh_target(target)

            if 'error' in result:
                self.stats['errors'] += 1
            else:
                self.stats['targets_processed'] += 1
                self.stats['postings_fetched'] += result.get('fetched', 0)
                self.stats['postings_new'] += result.get('new', 0)

        # Update lifecycle for closed postings
        if not self.dry_run:
            log.info("\nUpdating posting lifecycle...")
            self.manager.update_lifecycle_status()

        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        log.info("\n" + "=" * 70)
        log.info("REFRESH COMPLETE")
        log.info("=" * 70)
        log.info(f"Duration: {duration:.1f} seconds")
        log.info(f"Targets due: {self.stats['targets_due']}")
        log.info(f"Targets processed: {self.stats['targets_processed']}")
        log.info(f"Targets skipped: {self.stats['targets_skipped']}")
        log.info(f"Postings fetched: {self.stats['postings_fetched']}")
        log.info(f"Postings new: {self.stats['postings_new']}")
        log.info(f"Errors: {self.stats['errors']}")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Refresh job postings from ATS targets')
    parser.add_argument('--ats', type=str, help='Only refresh specific ATS type (greenhouse, lever, etc.)')
    parser.add_argument('--company', type=str, help='Only refresh specific company ID')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not save')
    parser.add_argument('--force', action='store_true', help='Refresh all targets regardless of schedule')
    parser.add_argument('--limit', type=int, help='Limit number of targets to process')

    args = parser.parse_args()

    refresher = PostingRefresher(dry_run=args.dry_run)

    try:
        stats = refresher.run(
            ats_type=args.ats,
            company_id=args.company,
            force_all=args.force,
            limit=args.limit
        )
        return 0 if stats['errors'] == 0 else 1
    finally:
        refresher.close()


if __name__ == "__main__":
    sys.exit(main())
