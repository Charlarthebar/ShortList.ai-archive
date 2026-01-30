#!/usr/bin/env python3
"""
Opening Detection System
========================

Detects new job openings by comparing current postings to previous state.
Generates opening events that can trigger user notifications.

Usage:
    # Detect openings from recent refresh
    python detect_openings.py

    # Detect openings for specific company
    python detect_openings.py --company stripe

    # Show openings from last N hours
    python detect_openings.py --hours 24

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


@dataclass
class OpeningEvent:
    """Represents a detected job opening."""
    id: int
    company_id: int
    company_name: str
    job_title: str
    canonical_role_id: Optional[int]
    location: str
    is_remote: bool
    posting_url: str
    first_seen: datetime
    salary_min: Optional[float]
    salary_max: Optional[float]


class OpeningDetector:
    """
    Detects new job openings from refreshed posting data.
    """

    def __init__(self):
        self.config = Config()
        self.db = DatabaseManager(self.config)

    def close(self):
        self.db.close_all_connections()

    def ensure_opening_events_table(self):
        """Create opening_events table if it doesn't exist."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS opening_events (
                        id SERIAL PRIMARY KEY,
                        observed_job_id INTEGER REFERENCES observed_jobs(id),
                        company_id INTEGER REFERENCES companies(id),
                        canonical_role_id INTEGER,
                        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notification_sent BOOLEAN DEFAULT FALSE,
                        notification_sent_at TIMESTAMP,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE INDEX IF NOT EXISTS idx_opening_events_company
                    ON opening_events(company_id);

                    CREATE INDEX IF NOT EXISTS idx_opening_events_role
                    ON opening_events(canonical_role_id);

                    CREATE INDEX IF NOT EXISTS idx_opening_events_detected
                    ON opening_events(detected_at);

                    CREATE INDEX IF NOT EXISTS idx_opening_events_notification
                    ON opening_events(notification_sent, detected_at);
                """)
            conn.commit()
            log.info("Opening events table ready")
        finally:
            self.db.release_connection(conn)

    def detect_new_openings(self, since_hours: int = 24,
                            company_id: int = None) -> List[OpeningEvent]:
        """
        Detect new job openings from recent posting refreshes.

        A posting is considered a "new opening" if:
        1. It was first_seen within the time window
        2. It's from a refreshable source (job_posting_*)
        3. It hasn't already been recorded as an opening event

        Args:
            since_hours: Look for openings in the last N hours
            company_id: Optional filter by company

        Returns:
            List of new OpeningEvent objects
        """
        conn = self.db.get_connection()
        try:
            cutoff = datetime.now() - timedelta(hours=since_hours)

            with conn.cursor() as cursor:
                query = """
                    SELECT
                        oj.id,
                        oj.company_id,
                        c.name as company_name,
                        oj.raw_title,
                        oj.canonical_role_id,
                        COALESCE(l.city, 'Unknown') || ', ' || COALESCE(l.state, '') as location,
                        COALESCE(l.is_remote, false) as is_remote,
                        COALESCE(sdr.source_url, '') as posting_url,
                        oj.first_seen,
                        oj.salary_min,
                        oj.salary_max
                    FROM observed_jobs oj
                    JOIN companies c ON oj.company_id = c.id
                    LEFT JOIN locations l ON oj.location_id = l.id
                    LEFT JOIN source_data_raw sdr ON oj.source_data_id = sdr.id
                    WHERE oj.source_type LIKE 'job_posting_%%'
                      AND oj.first_seen >= %s
                      AND oj.status = 'active'
                      AND NOT EXISTS (
                          SELECT 1 FROM opening_events oe
                          WHERE oe.observed_job_id = oj.id
                      )
                """
                params = [cutoff]

                if company_id:
                    query += " AND oj.company_id = %s"
                    params.append(company_id)

                query += " ORDER BY oj.first_seen DESC"

                cursor.execute(query, params)
                rows = cursor.fetchall()

                openings = []
                for row in rows:
                    openings.append(OpeningEvent(
                        id=row[0],
                        company_id=row[1],
                        company_name=row[2],
                        job_title=row[3],
                        canonical_role_id=row[4],
                        location=row[5],
                        is_remote=row[6],
                        posting_url=row[7],
                        first_seen=row[8],
                        salary_min=row[9],
                        salary_max=row[10]
                    ))

                return openings

        finally:
            self.db.release_connection(conn)

    def record_opening_events(self, openings: List[OpeningEvent]) -> int:
        """
        Record opening events for notification processing.

        Returns:
            Number of events recorded
        """
        if not openings:
            return 0

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                recorded = 0
                for opening in openings:
                    metadata = {
                        'job_title': opening.job_title,
                        'location': opening.location,
                        'is_remote': opening.is_remote,
                        'posting_url': opening.posting_url,
                        'company_name': opening.company_name,
                    }
                    if opening.salary_min:
                        metadata['salary_min'] = float(opening.salary_min)
                    if opening.salary_max:
                        metadata['salary_max'] = float(opening.salary_max)

                    cursor.execute("""
                        INSERT INTO opening_events
                        (observed_job_id, company_id, canonical_role_id, detected_at, metadata)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        opening.id,
                        opening.company_id,
                        opening.canonical_role_id,
                        datetime.now(),
                        json.dumps(metadata)
                    ))
                    recorded += cursor.rowcount

            conn.commit()
            return recorded

        finally:
            self.db.release_connection(conn)

    def get_pending_notifications(self, limit: int = 100) -> List[Dict]:
        """
        Get opening events that haven't been notified yet.

        Returns events with user subscription info for notification processing.
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        oe.id,
                        oe.observed_job_id,
                        oe.company_id,
                        oe.canonical_role_id,
                        oe.detected_at,
                        oe.metadata
                    FROM opening_events oe
                    WHERE oe.notification_sent = false
                    ORDER BY oe.detected_at ASC
                    LIMIT %s
                """, (limit,))

                return [
                    {
                        'event_id': row[0],
                        'observed_job_id': row[1],
                        'company_id': row[2],
                        'canonical_role_id': row[3],
                        'detected_at': row[4],
                        'metadata': row[5]
                    }
                    for row in cursor.fetchall()
                ]

        finally:
            self.db.release_connection(conn)

    def mark_notifications_sent(self, event_ids: List[int]):
        """Mark opening events as notified."""
        if not event_ids:
            return

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE opening_events
                    SET notification_sent = true,
                        notification_sent_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(%s)
                """, (event_ids,))
            conn.commit()
        finally:
            self.db.release_connection(conn)

    def get_opening_stats(self, since_hours: int = 24) -> Dict:
        """Get statistics about recent openings."""
        conn = self.db.get_connection()
        try:
            cutoff = datetime.now() - timedelta(hours=since_hours)

            with conn.cursor() as cursor:
                # Total openings
                cursor.execute("""
                    SELECT COUNT(*) FROM opening_events
                    WHERE detected_at >= %s
                """, (cutoff,))
                total = cursor.fetchone()[0]

                # By company
                cursor.execute("""
                    SELECT c.name, COUNT(*)
                    FROM opening_events oe
                    JOIN companies c ON oe.company_id = c.id
                    WHERE oe.detected_at >= %s
                    GROUP BY c.name
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                """, (cutoff,))
                by_company = cursor.fetchall()

                # Pending notifications
                cursor.execute("""
                    SELECT COUNT(*) FROM opening_events
                    WHERE notification_sent = false
                """)
                pending = cursor.fetchone()[0]

                return {
                    'total_openings': total,
                    'pending_notifications': pending,
                    'by_company': by_company,
                    'since_hours': since_hours
                }

        finally:
            self.db.release_connection(conn)


def main():
    parser = argparse.ArgumentParser(description='Detect new job openings')
    parser.add_argument('--hours', type=int, default=24,
                        help='Look for openings in the last N hours')
    parser.add_argument('--company', type=int, help='Filter by company ID')
    parser.add_argument('--stats-only', action='store_true',
                        help='Only show statistics, do not record new events')

    args = parser.parse_args()

    detector = OpeningDetector()

    try:
        # Ensure table exists
        detector.ensure_opening_events_table()

        if args.stats_only:
            stats = detector.get_opening_stats(args.hours)
            log.info(f"\n=== Opening Statistics (last {args.hours} hours) ===")
            log.info(f"Total openings detected: {stats['total_openings']}")
            log.info(f"Pending notifications: {stats['pending_notifications']}")
            if stats['by_company']:
                log.info("\nTop companies by openings:")
                for company, count in stats['by_company']:
                    log.info(f"  {company}: {count}")
            return

        # Detect new openings
        log.info(f"Detecting openings from last {args.hours} hours...")
        openings = detector.detect_new_openings(args.hours, args.company)
        log.info(f"Found {len(openings)} new openings")

        if openings:
            # Show sample
            log.info("\nSample openings:")
            for opening in openings[:10]:
                salary_str = ""
                if opening.salary_min:
                    salary_str = f" (${opening.salary_min:,.0f}-${opening.salary_max:,.0f})"
                log.info(f"  [{opening.company_name}] {opening.job_title} - {opening.location}{salary_str}")

            # Record events
            recorded = detector.record_opening_events(openings)
            log.info(f"\nRecorded {recorded} new opening events")

        # Show final stats
        stats = detector.get_opening_stats(args.hours)
        log.info(f"\nTotal opening events: {stats['total_openings']}")
        log.info(f"Pending notifications: {stats['pending_notifications']}")

    finally:
        detector.close()


if __name__ == "__main__":
    main()
