#!/usr/bin/env python3
"""
Job Posting Ingestion System
============================

Manages the ingestion pipeline for job postings from ATS platforms:
1. Fetches postings from enabled targets
2. Normalizes and stores in observed_jobs
3. Tracks posting lifecycle (open → closed)
4. Updates archetypes with posting evidence
5. Handles deduplication

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer
from sources.job_postings.base_connector import JobPosting
from sources.job_postings.greenhouse import GreenhouseConnector
from sources.job_postings.lever import LeverConnector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('posting_ingestion.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


@dataclass
class IngestionTarget:
    """
    Represents an employer career site to track.
    """
    id: int
    company_name: str
    company_id_ats: str  # The ID used by the ATS (e.g., 'stripe' for Greenhouse)
    ats_type: str  # greenhouse, lever, smartrecruiters, workday, json_ld
    careers_url: str
    enabled: bool = True
    last_fetched: datetime = None
    db_company_id: int = None  # Our internal company ID


class PostingIngestionManager:
    """
    Manages the end-to-end posting ingestion pipeline.
    """

    # Days until a posting is considered closed
    DAYS_UNTIL_CLOSED = 7

    # Connector registry
    CONNECTOR_CLASSES = {
        'greenhouse': GreenhouseConnector,
        'lever': LeverConnector,
    }

    def __init__(self):
        self.config = Config()
        self.db = DatabaseManager(self.config)
        self.db.initialize_pool()
        self.normalizer = TitleNormalizer()

        # Statistics
        self.stats = {
            'targets_processed': 0,
            'postings_fetched': 0,
            'postings_new': 0,
            'postings_updated': 0,
            'postings_closed': 0,
            'postings_matched': 0,
            'errors': 0,
        }

    def close(self):
        """Clean up resources."""
        self.db.close_all_connections()

    # =========================================================================
    # TARGET MANAGEMENT
    # =========================================================================

    def create_targets_table(self):
        """Create the posting_targets table if it doesn't exist."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS posting_targets (
                        id SERIAL PRIMARY KEY,
                        company_name TEXT NOT NULL,
                        company_id_ats TEXT NOT NULL,
                        ats_type TEXT NOT NULL,
                        careers_url TEXT,
                        enabled BOOLEAN DEFAULT true,
                        last_fetched TIMESTAMP,
                        db_company_id INTEGER REFERENCES companies(id),
                        fetch_frequency_hours INTEGER DEFAULT 24,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(company_id_ats, ats_type)
                    );

                    CREATE INDEX IF NOT EXISTS idx_posting_targets_enabled
                    ON posting_targets(enabled, ats_type);

                    CREATE INDEX IF NOT EXISTS idx_posting_targets_company
                    ON posting_targets(db_company_id);
                """)
            conn.commit()
            log.info("Created/verified posting_targets table")
        finally:
            self.db.release_connection(conn)

    def add_target(self, company_name: str, company_id_ats: str, ats_type: str,
                   careers_url: str = None, enabled: bool = True) -> int:
        """
        Add a new employer career site to track.

        Returns:
            Target ID
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO posting_targets (company_name, company_id_ats, ats_type, careers_url, enabled)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (company_id_ats, ats_type) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        careers_url = EXCLUDED.careers_url,
                        enabled = EXCLUDED.enabled,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (company_name, company_id_ats, ats_type, careers_url, enabled))
                target_id = cursor.fetchone()[0]
            conn.commit()
            log.info(f"Added target: {company_name} ({ats_type})")
            return target_id
        finally:
            self.db.release_connection(conn)

    def get_enabled_targets(self, ats_type: str = None) -> List[IngestionTarget]:
        """Get all enabled targets, optionally filtered by ATS type."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT id, company_name, company_id_ats, ats_type, careers_url,
                           enabled, last_fetched, db_company_id
                    FROM posting_targets
                    WHERE enabled = true
                """
                params = []
                if ats_type:
                    query += " AND ats_type = %s"
                    params.append(ats_type)

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

    # =========================================================================
    # POSTING INGESTION
    # =========================================================================

    def ingest_target(self, target: IngestionTarget) -> Dict[str, int]:
        """
        Ingest all postings from a single target.

        Returns:
            Statistics dict with counts of new, updated, matched postings
        """
        target_stats = {
            'fetched': 0,
            'new': 0,
            'updated': 0,
            'matched': 0,
            'errors': 0,
        }

        # Get connector class
        connector_class = self.CONNECTOR_CLASSES.get(target.ats_type)
        if not connector_class:
            log.error(f"Unknown ATS type: {target.ats_type}")
            return target_stats

        # Create connector and fetch jobs
        try:
            connector = connector_class(target.company_id_ats, target.company_name)
            postings = connector.fetch_jobs()
            target_stats['fetched'] = len(postings)
            log.info(f"Fetched {len(postings)} postings from {target.company_name}")
        except Exception as e:
            log.error(f"Error fetching from {target.company_name}: {e}")
            target_stats['errors'] += 1
            return target_stats

        # Get or create source
        source_id = self._get_or_create_source(target.ats_type, target.company_name)

        # Get or create company
        db_company_id = self._get_or_create_company(target.company_name)

        # Process each posting
        for posting in postings:
            try:
                result = self._process_posting(posting, source_id, db_company_id, target.id)
                if result == 'new':
                    target_stats['new'] += 1
                elif result == 'updated':
                    target_stats['updated'] += 1
                if result in ('new', 'updated'):
                    target_stats['matched'] += 1
            except Exception as e:
                log.warning(f"Error processing posting {posting.external_id}: {e}")
                target_stats['errors'] += 1

        # Update target's last_fetched
        self._update_target_last_fetched(target.id)

        return target_stats

    def _process_posting(self, posting: JobPosting, source_id: int,
                        company_id: int, target_id: int) -> str:
        """
        Process a single posting: insert or update.

        Returns:
            'new', 'updated', or 'unchanged'
        """
        conn = self.db.get_connection()
        try:
            # Normalize title
            norm_result = self.normalizer.parse_title(posting.title)
            canonical_role_id = norm_result.canonical_role_id
            seniority = norm_result.seniority

            # Get or create location
            location_id = self._get_or_create_location(
                posting.city, posting.state, posting.is_remote
            )

            with conn.cursor() as cursor:
                # Check if posting exists in posting_lifecycle
                cursor.execute("""
                    SELECT id, last_seen FROM posting_lifecycle
                    WHERE external_id = %s AND source_id = %s
                """, (posting.external_id, source_id))

                existing = cursor.fetchone()
                now = datetime.now()

                if existing:
                    # Update last_seen
                    lifecycle_id = existing[0]
                    cursor.execute("""
                        UPDATE posting_lifecycle
                        SET last_seen = %s, updated_at = %s
                        WHERE id = %s
                    """, (now, now, lifecycle_id))

                    # Also update observed_jobs last_seen
                    cursor.execute("""
                        UPDATE observed_jobs
                        SET last_seen = %s, updated_at = %s
                        WHERE metadata->>'lifecycle_id' = %s
                    """, (now, now, str(lifecycle_id)))

                    conn.commit()
                    return 'updated'
                else:
                    # Insert into posting_lifecycle
                    cursor.execute("""
                        INSERT INTO posting_lifecycle (
                            external_id, source_id, company_id, canonical_role_id,
                            first_seen, last_seen
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (posting.external_id, source_id, company_id,
                          canonical_role_id, now, now))

                    lifecycle_id = cursor.fetchone()[0]

                    # Store raw data
                    raw_data_id = self._store_raw_data(posting, source_id, cursor)

                    # Insert into observed_jobs
                    metadata = {
                        'lifecycle_id': lifecycle_id,
                        'target_id': target_id,
                        'ats_type': posting.ats_type,
                        'content_hash': posting.content_hash(),
                    }

                    cursor.execute("""
                        INSERT INTO observed_jobs (
                            company_id, location_id, canonical_role_id,
                            raw_title, raw_company, raw_location,
                            seniority, description, requirements,
                            salary_min, salary_max, salary_currency, salary_period,
                            source_id, source_data_id, source_type,
                            status, posted_date, first_seen, last_seen,
                            metadata
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING id
                    """, (
                        company_id, location_id, canonical_role_id,
                        posting.title, posting.company_name, posting.location_raw,
                        seniority, posting.description, posting.requirements,
                        posting.salary_min, posting.salary_max,
                        posting.salary_currency, posting.salary_period,
                        source_id, raw_data_id, f"job_posting_{posting.ats_type}",
                        'active', posting.posted_date, now, now,
                        json.dumps(metadata)
                    ))

                    conn.commit()
                    return 'new'

        except Exception as e:
            conn.rollback()
            raise
        finally:
            self.db.release_connection(conn)

    def _store_raw_data(self, posting: JobPosting, source_id: int, cursor) -> int:
        """Store raw posting data for provenance."""
        cursor.execute("""
            INSERT INTO source_data_raw (
                source_id, raw_company, raw_location, raw_title, raw_description,
                source_url, source_document_id, as_of_date, raw_data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            source_id,
            posting.company_name,
            posting.location_raw,
            posting.title,
            posting.description,
            posting.url,
            posting.external_id,
            datetime.now().date(),
            json.dumps(posting.raw_data, default=str)
        ))
        return cursor.fetchone()[0]

    # =========================================================================
    # LIFECYCLE MANAGEMENT
    # =========================================================================

    def update_lifecycle_status(self):
        """
        Update posting lifecycle status.

        Marks postings as closed if not seen for N days.
        Calculates filled probability.
        """
        conn = self.db.get_connection()
        try:
            cutoff_date = datetime.now() - timedelta(days=self.DAYS_UNTIL_CLOSED)

            with conn.cursor() as cursor:
                # Find postings that haven't been seen recently and aren't already closed
                cursor.execute("""
                    SELECT id, external_id, company_id, canonical_role_id,
                           first_seen, last_seen
                    FROM posting_lifecycle
                    WHERE last_seen < %s
                      AND disappeared_date IS NULL
                """, (cutoff_date,))

                stale_postings = cursor.fetchall()
                closed_count = 0

                for row in stale_postings:
                    lifecycle_id = row[0]
                    first_seen = row[4]
                    last_seen = row[5]

                    # Calculate posting duration
                    duration_days = (last_seen - first_seen).days if first_seen else 0

                    # Simple filled probability heuristic:
                    # - Short duration (< 14 days): likely filled (0.7)
                    # - Medium duration (14-30 days): moderate (0.5)
                    # - Long duration (> 30 days): possibly cancelled (0.3)
                    if duration_days < 14:
                        filled_prob = 0.7
                        closure_reason = 'likely_filled'
                    elif duration_days < 30:
                        filled_prob = 0.5
                        closure_reason = 'possibly_filled'
                    else:
                        filled_prob = 0.3
                        closure_reason = 'possibly_cancelled'

                    # Update lifecycle record
                    cursor.execute("""
                        UPDATE posting_lifecycle
                        SET disappeared_date = %s,
                            filled_probability = %s,
                            closure_reason = %s,
                            posting_duration_days = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (last_seen, filled_prob, closure_reason, duration_days, lifecycle_id))

                    # Update observed_jobs status
                    cursor.execute("""
                        UPDATE observed_jobs
                        SET status = 'closed', updated_at = CURRENT_TIMESTAMP
                        WHERE metadata->>'lifecycle_id' = %s
                    """, (str(lifecycle_id),))

                    closed_count += 1

                conn.commit()
                log.info(f"Closed {closed_count} stale postings")
                self.stats['postings_closed'] = closed_count

        finally:
            self.db.release_connection(conn)

    # =========================================================================
    # DEDUPLICATION
    # =========================================================================

    def dedupe_postings(self):
        """
        Identify and link duplicate postings.

        Uses content hash to find similar postings at the same company.
        Doesn't delete anything - just marks duplicates in metadata.
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                # Find postings with same content hash at same company
                cursor.execute("""
                    WITH content_hashes AS (
                        SELECT
                            id,
                            company_id,
                            metadata->>'content_hash' as content_hash,
                            first_seen,
                            ROW_NUMBER() OVER (
                                PARTITION BY company_id, metadata->>'content_hash'
                                ORDER BY first_seen ASC
                            ) as rn
                        FROM observed_jobs
                        WHERE source_type LIKE 'job_posting_%'
                          AND metadata->>'content_hash' IS NOT NULL
                    )
                    SELECT id, company_id, content_hash
                    FROM content_hashes
                    WHERE rn > 1
                """)

                duplicates = cursor.fetchall()
                dupe_count = 0

                for row in duplicates:
                    job_id = row[0]
                    company_id = row[1]
                    content_hash = row[2]

                    # Find the original posting
                    cursor.execute("""
                        SELECT id FROM observed_jobs
                        WHERE company_id = %s
                          AND metadata->>'content_hash' = %s
                          AND id != %s
                        ORDER BY first_seen ASC
                        LIMIT 1
                    """, (company_id, content_hash, job_id))

                    original = cursor.fetchone()
                    if original:
                        original_id = original[0]

                        # Mark as duplicate (don't delete)
                        cursor.execute("""
                            UPDATE observed_jobs
                            SET metadata = metadata || %s::jsonb
                            WHERE id = %s
                        """, (json.dumps({'is_duplicate_of': original_id}), job_id))
                        dupe_count += 1

                conn.commit()
                log.info(f"Identified {dupe_count} duplicate postings")

        finally:
            self.db.release_connection(conn)

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_or_create_source(self, ats_type: str, company_name: str) -> int:
        """Get or create source record."""
        source_name = f"Job Postings - {ats_type.title()}"
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sources (name, type, reliability_tier, base_reliability)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id
                """, (source_name, 'job_posting', 'tier_1', 0.85))
                result = cursor.fetchone()
                if result:
                    source_id = result[0]
                else:
                    # Already exists, fetch the ID
                    cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
                    source_id = cursor.fetchone()[0]
            conn.commit()
            return source_id
        finally:
            self.db.release_connection(conn)

    def _get_or_create_company(self, company_name: str) -> int:
        """Get or create company record."""
        # Normalize company name (same logic as DatabaseManager)
        normalized = company_name.lower().strip()
        for suffix in ['Inc.', 'Inc', 'LLC', 'L.L.C.', 'Corp.', 'Corporation', 'Ltd.', 'Limited', 'Co.', 'Company']:
            normalized = normalized.replace(suffix.lower(), '')
        normalized = ''.join(c for c in normalized if c.isalnum() or c.isspace()).strip()

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO companies (name, normalized_name, industry, size_category, is_public)
                    VALUES (%s, %s, 'Technology', 'Unknown', false)
                    ON CONFLICT (normalized_name) DO NOTHING
                    RETURNING id
                """, (company_name, normalized))
                result = cursor.fetchone()
                if result:
                    company_id = result[0]
                else:
                    # Already exists, fetch the ID
                    cursor.execute("SELECT id FROM companies WHERE normalized_name = %s", (normalized,))
                    company_id = cursor.fetchone()[0]
            conn.commit()
            return company_id
        finally:
            self.db.release_connection(conn)

    def _get_or_create_location(self, city: str, state: str, is_remote: bool) -> int:
        """Get or create location record."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                city = city or ('Remote' if is_remote else 'Unknown')
                state = state or 'Unknown'

                cursor.execute("""
                    INSERT INTO locations (city, state, country, is_remote)
                    VALUES (%s, %s, 'United States', %s)
                    ON CONFLICT (city, state, country)
                    DO UPDATE SET is_remote = EXCLUDED.is_remote
                    RETURNING id
                """, (city, state, is_remote))
                location_id = cursor.fetchone()[0]
            conn.commit()
            return location_id
        finally:
            self.db.release_connection(conn)

    def _update_target_last_fetched(self, target_id: int):
        """Update target's last_fetched timestamp."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE posting_targets
                    SET last_fetched = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (target_id,))
            conn.commit()
        finally:
            self.db.release_connection(conn)

    # =========================================================================
    # MAIN PIPELINE
    # =========================================================================

    def run_full_pipeline(self, ats_type: str = None):
        """
        Run the complete posting ingestion pipeline.

        1. Fetch all enabled targets
        2. Ingest postings from each target
        3. Update lifecycle status
        4. Run deduplication
        """
        log.info("="*70)
        log.info("STARTING POSTING INGESTION PIPELINE")
        log.info("="*70)

        start_time = datetime.now()

        # Get targets
        targets = self.get_enabled_targets(ats_type)
        log.info(f"Found {len(targets)} enabled targets")

        # Process each target
        for target in targets:
            log.info(f"\n--- Processing {target.company_name} ({target.ats_type}) ---")
            try:
                target_stats = self.ingest_target(target)
                self.stats['targets_processed'] += 1
                self.stats['postings_fetched'] += target_stats['fetched']
                self.stats['postings_new'] += target_stats['new']
                self.stats['postings_updated'] += target_stats['updated']
                self.stats['postings_matched'] += target_stats['matched']
                self.stats['errors'] += target_stats['errors']
            except Exception as e:
                log.error(f"Error processing target {target.company_name}: {e}")
                self.stats['errors'] += 1

        # Update lifecycle
        log.info("\n--- Updating posting lifecycle ---")
        self.update_lifecycle_status()

        # Run deduplication
        log.info("\n--- Running deduplication ---")
        self.dedupe_postings()

        # Final summary
        duration = (datetime.now() - start_time).total_seconds()
        log.info("\n" + "="*70)
        log.info("PIPELINE COMPLETE")
        log.info("="*70)
        log.info(f"Duration: {duration:.1f} seconds")
        log.info(f"Targets processed: {self.stats['targets_processed']}")
        log.info(f"Postings fetched: {self.stats['postings_fetched']}")
        log.info(f"Postings new: {self.stats['postings_new']}")
        log.info(f"Postings updated: {self.stats['postings_updated']}")
        log.info(f"Postings closed: {self.stats['postings_closed']}")
        log.info(f"Postings matched: {self.stats['postings_matched']}")
        log.info(f"Errors: {self.stats['errors']}")

        return self.stats


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def setup_initial_targets(manager: PostingIngestionManager):
    """Set up initial tracking targets for major companies."""

    # Greenhouse companies
    greenhouse_targets = [
        ("Stripe", "stripe"),
        ("Airbnb", "airbnb"),
        ("Figma", "figma"),
        ("DoorDash", "doordash"),
        ("Coinbase", "coinbase"),
        ("Dropbox", "dropbox"),
        ("MongoDB", "mongodb"),
        ("Twilio", "twilio"),
        ("GitLab", "gitlab"),
        ("Datadog", "datadog"),
    ]

    # Lever companies
    lever_targets = [
        ("Spotify", "spotify"),
        ("Lyft", "lyft"),
        ("Asana", "asana"),
        ("Flexport", "flexport"),
        ("Rippling", "rippling"),
        ("Brex", "brex"),
        ("Affirm", "affirm"),
        ("Ramp", "ramp"),
        ("Gusto", "gusto"),
        ("Scale AI", "scale"),
    ]

    log.info("Setting up initial targets...")

    for company_name, company_id in greenhouse_targets:
        manager.add_target(company_name, company_id, "greenhouse",
                          f"https://boards.greenhouse.io/{company_id}")

    for company_name, company_id in lever_targets:
        manager.add_target(company_name, company_id, "lever",
                          f"https://jobs.lever.co/{company_id}")

    log.info(f"Added {len(greenhouse_targets)} Greenhouse + {len(lever_targets)} Lever targets")


def main():
    """Main entry point."""
    manager = PostingIngestionManager()

    try:
        # Create targets table
        manager.create_targets_table()

        # Set up initial targets (only if needed)
        targets = manager.get_enabled_targets()
        if not targets:
            setup_initial_targets(manager)

        # Run the pipeline
        stats = manager.run_full_pipeline()

        return stats

    finally:
        manager.close()


if __name__ == "__main__":
    main()
