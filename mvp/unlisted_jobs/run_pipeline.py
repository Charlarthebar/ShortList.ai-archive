#!/usr/bin/env python3
"""
ShortList Complete Pipeline
============================

Runs the complete job refresh and notification pipeline:
1. Refresh job postings from ATS targets
2. Detect new job openings
3. Sync openings to watchable_positions
4. Notify shortlist applicants about newly opened roles
5. Send notifications to employers with ready shortlists

Usage:
    # Run complete pipeline
    python run_pipeline.py

    # Run specific steps
    python run_pipeline.py --step refresh
    python run_pipeline.py --step detect
    python run_pipeline.py --step notify

    # Dry run (no changes)
    python run_pipeline.py --dry-run

Cron example (run every 6 hours):
    0 */6 * * * cd /path/to/unlisted_jobs && python3 run_pipeline.py >> /var/log/shortlist_pipeline.log 2>&1

Author: ShortList.ai
Date: 2026-01-19
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Import pipeline components
from refresh_postings import PostingRefresher
from detect_openings import OpeningDetector
from sync_watchable_positions import sync_positions, update_watcher_counts

# Import email service
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'api'))
    from email_service import get_email_service
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


class ShortlistPipeline:
    """
    Orchestrates the complete job refresh and notification pipeline.
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config()
        self.db = DatabaseManager(self.config)
        self.stats = {
            'postings_refreshed': 0,
            'openings_detected': 0,
            'positions_synced': 0,
            'candidates_notified': 0,
            'employers_notified': 0,
            'errors': []
        }

    def close(self):
        self.db.close_all_connections()

    def step_refresh_postings(self):
        """Step 1: Refresh job postings from ATS targets."""
        log.info("\n" + "=" * 70)
        log.info("STEP 1: REFRESH POSTINGS")
        log.info("=" * 70)

        try:
            refresher = PostingRefresher(dry_run=self.dry_run)
            result = refresher.run()
            refresher.close()

            self.stats['postings_refreshed'] = result.get('postings_new', 0)
            log.info(f"New postings: {self.stats['postings_refreshed']}")

        except Exception as e:
            log.error(f"Error in refresh step: {e}")
            self.stats['errors'].append(f"refresh: {e}")

    def step_detect_openings(self, since_hours: int = 24):
        """Step 2: Detect new job openings from recent refreshes."""
        log.info("\n" + "=" * 70)
        log.info("STEP 2: DETECT OPENINGS")
        log.info("=" * 70)

        try:
            detector = OpeningDetector()
            detector.ensure_opening_events_table()

            openings = detector.detect_new_openings(since_hours=since_hours)
            log.info(f"Found {len(openings)} new openings")

            if not self.dry_run and openings:
                recorded = detector.record_opening_events(openings)
                self.stats['openings_detected'] = recorded
                log.info(f"Recorded {recorded} opening events")

            detector.close()

        except Exception as e:
            log.error(f"Error in detect step: {e}")
            self.stats['errors'].append(f"detect: {e}")

    def step_sync_positions(self):
        """Step 3: Sync observed_jobs to watchable_positions."""
        log.info("\n" + "=" * 70)
        log.info("STEP 3: SYNC WATCHABLE POSITIONS")
        log.info("=" * 70)

        if self.dry_run:
            log.info("Skipping sync in dry run mode")
            return

        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            new_count, updated_count = sync_positions(cursor)
            self.stats['positions_synced'] = new_count

            update_watcher_counts(cursor)

            conn.commit()
            cursor.close()
            self.db.release_connection(conn)

            log.info(f"Synced {new_count} new positions, updated {updated_count}")

        except Exception as e:
            log.error(f"Error in sync step: {e}")
            self.stats['errors'].append(f"sync: {e}")

    def step_notify_candidates(self):
        """Step 4: Notify shortlist applicants about newly opened roles."""
        log.info("\n" + "=" * 70)
        log.info("STEP 4: NOTIFY CANDIDATES")
        log.info("=" * 70)

        if self.dry_run:
            log.info("Skipping notifications in dry run mode")
            return

        if not EMAIL_AVAILABLE:
            log.warning("Email service not available, skipping candidate notifications")
            return

        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            # Find positions that just opened (status changed to 'open')
            # and have shortlist applicants who haven't been notified
            cursor.execute("""
                SELECT DISTINCT
                    sa.id as application_id,
                    sa.user_id,
                    pu.email,
                    pu.first_name,
                    wp.id as position_id,
                    wp.title,
                    wp.company_name
                FROM shortlist_applications sa
                JOIN platform_users pu ON sa.user_id = pu.id
                JOIN watchable_positions wp ON sa.position_id = wp.id
                WHERE wp.status = 'open'
                  AND wp.is_monitored = TRUE
                  AND sa.screening_passed = TRUE
                  AND sa.notified_role_opened = FALSE
                ORDER BY wp.id, sa.ai_score DESC
            """)

            candidates_to_notify = cursor.fetchall()
            log.info(f"Found {len(candidates_to_notify)} candidates to notify")

            email_service = get_email_service()
            notified_ids = []

            for row in candidates_to_notify:
                app_id, user_id, email, first_name, position_id, title, company_name = row

                try:
                    success = email_service.send_role_opened_notification(
                        to_email=email,
                        to_name=first_name,
                        position_title=title,
                        company_name=company_name,
                        position_id=position_id
                    )

                    if success:
                        notified_ids.append(app_id)
                        log.info(f"  Notified {email} about {title} at {company_name}")

                except Exception as e:
                    log.error(f"  Failed to notify {email}: {e}")

            # Mark as notified
            if notified_ids:
                cursor.execute("""
                    UPDATE shortlist_applications
                    SET notified_role_opened = TRUE,
                        notified_role_opened_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(%s)
                """, (notified_ids,))

            conn.commit()
            cursor.close()
            self.db.release_connection(conn)

            self.stats['candidates_notified'] = len(notified_ids)
            log.info(f"Notified {len(notified_ids)} candidates")

        except Exception as e:
            log.error(f"Error in candidate notification step: {e}")
            self.stats['errors'].append(f"notify_candidates: {e}")

    def step_notify_employers(self):
        """Step 5: Notify employers with ready shortlists."""
        log.info("\n" + "=" * 70)
        log.info("STEP 5: NOTIFY EMPLOYERS")
        log.info("=" * 70)

        if self.dry_run:
            log.info("Skipping notifications in dry run mode")
            return

        if not EMAIL_AVAILABLE:
            log.warning("Email service not available, skipping employer notifications")
            return

        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            # Find positions that just opened and have qualified candidates
            # We look for positions where status is 'open' and we haven't
            # notified the employer yet about the shortlist
            cursor.execute("""
                SELECT DISTINCT ON (wp.id)
                    wp.id as position_id,
                    wp.title,
                    wp.company_name,
                    wp.company_id,
                    (
                        SELECT COUNT(*)
                        FROM shortlist_applications sa
                        LEFT JOIN role_configurations rc ON sa.position_id = rc.position_id
                        WHERE sa.position_id = wp.id
                          AND sa.screening_passed = TRUE
                          AND sa.ai_score >= COALESCE(rc.score_threshold, 70)
                    ) as qualified_count,
                    pu.email as owner_email,
                    pu.first_name as owner_name
                FROM watchable_positions wp
                JOIN company_profiles cp ON wp.company_id = cp.company_id
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                JOIN platform_users pu ON ctm.user_id = pu.id
                WHERE wp.status = 'open'
                  AND wp.is_monitored = TRUE
                  AND ctm.role IN ('owner', 'admin')
                  AND wp.updated_at > NOW() - INTERVAL '24 hours'
                  AND NOT EXISTS (
                      SELECT 1 FROM notifications n
                      WHERE n.related_position_id = wp.id
                        AND n.type = 'shortlist_ready'
                        AND n.created_at > NOW() - INTERVAL '24 hours'
                  )
            """)

            employers_to_notify = cursor.fetchall()
            log.info(f"Found {len(employers_to_notify)} employers to notify")

            email_service = get_email_service()
            notified_count = 0

            for row in employers_to_notify:
                position_id, title, company_name, company_id, qualified_count, owner_email, owner_name = row

                if qualified_count == 0:
                    continue

                try:
                    success = email_service.send_shortlist_ready_notification(
                        to_email=owner_email,
                        to_name=owner_name,
                        position_title=title,
                        candidate_count=qualified_count,
                        position_id=position_id
                    )

                    if success:
                        notified_count += 1
                        log.info(f"  Notified {owner_email} - {qualified_count} candidates for {title}")

                        # Record notification to prevent duplicates
                        cursor.execute("""
                            INSERT INTO notifications (
                                user_id, type, title, message,
                                related_position_id, related_company_id
                            )
                            SELECT ctm.user_id, 'shortlist_ready',
                                   %s, %s, %s, %s
                            FROM company_team_members ctm
                            JOIN company_profiles cp ON ctm.company_profile_id = cp.id
                            WHERE cp.company_id = %s AND ctm.role IN ('owner', 'admin')
                        """, (
                            f'Shortlist ready for {title}',
                            f'{qualified_count} qualified candidates ready to review',
                            position_id,
                            company_id,
                            company_id
                        ))

                except Exception as e:
                    log.error(f"  Failed to notify {owner_email}: {e}")

            conn.commit()
            cursor.close()
            self.db.release_connection(conn)

            self.stats['employers_notified'] = notified_count
            log.info(f"Notified {notified_count} employers")

        except Exception as e:
            log.error(f"Error in employer notification step: {e}")
            self.stats['errors'].append(f"notify_employers: {e}")

    def run(self, steps: Optional[list] = None):
        """
        Run the complete pipeline or specific steps.

        Args:
            steps: List of step names to run, or None for all
        """
        start_time = datetime.now()

        log.info("=" * 70)
        log.info(f"SHORTLIST PIPELINE - {start_time.isoformat()}")
        log.info("=" * 70)

        if self.dry_run:
            log.info("DRY RUN MODE - No changes will be saved")

        all_steps = ['refresh', 'detect', 'sync', 'notify_candidates', 'notify_employers']
        steps_to_run = steps or all_steps

        if 'refresh' in steps_to_run:
            self.step_refresh_postings()

        if 'detect' in steps_to_run:
            self.step_detect_openings()

        if 'sync' in steps_to_run:
            self.step_sync_positions()

        if 'notify_candidates' in steps_to_run or 'notify' in steps_to_run:
            self.step_notify_candidates()

        if 'notify_employers' in steps_to_run or 'notify' in steps_to_run:
            self.step_notify_employers()

        # Summary
        duration = (datetime.now() - start_time).total_seconds()

        log.info("\n" + "=" * 70)
        log.info("PIPELINE COMPLETE")
        log.info("=" * 70)
        log.info(f"Duration: {duration:.1f} seconds")
        log.info(f"Postings refreshed: {self.stats['postings_refreshed']}")
        log.info(f"Openings detected: {self.stats['openings_detected']}")
        log.info(f"Positions synced: {self.stats['positions_synced']}")
        log.info(f"Candidates notified: {self.stats['candidates_notified']}")
        log.info(f"Employers notified: {self.stats['employers_notified']}")

        if self.stats['errors']:
            log.warning(f"Errors: {len(self.stats['errors'])}")
            for err in self.stats['errors']:
                log.warning(f"  - {err}")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Run ShortList pipeline')
    parser.add_argument('--step', type=str, action='append',
                        choices=['refresh', 'detect', 'sync', 'notify', 'notify_candidates', 'notify_employers'],
                        help='Run specific step(s) only')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run without making changes')

    args = parser.parse_args()

    pipeline = ShortlistPipeline(dry_run=args.dry_run)

    try:
        pipeline.run(steps=args.step)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
