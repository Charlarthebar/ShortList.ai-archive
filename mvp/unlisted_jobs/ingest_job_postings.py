#!/usr/bin/env python3
"""
Job Posting Ingestion Orchestrator
===================================

Orchestrates job posting ingestion from multiple ATS platforms at scale.

Features:
- Auto-detects ATS type for unknown companies
- Parallel fetching with rate limiting
- Progress tracking and resumption
- Database upsert with change detection
- Logging and error handling

Usage:
    python ingest_job_postings.py                    # Full run
    python ingest_job_postings.py --known-only      # Only known ATS companies
    python ingest_job_postings.py --detect-only     # Only detect ATS types
    python ingest_job_postings.py --company stripe  # Single company

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import json
import logging
import argparse
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
import psycopg2
from psycopg2.extras import execute_values

# Setup environment for database
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sources.job_postings import (
    GreenhouseConnector,
    LeverConnector,
    SmartRecruitersConnector,
    WorkdayConnector,
    JobPosting,
)
from sources.job_postings.company_targets import (
    get_all_known_targets,
    get_auto_detect_targets,
    get_all_targets,
    CompanyTarget,
    WORKDAY_COMPANIES,
)
from title_normalizer import TitleNormalizer
from database import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('posting_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Result of a single company ingestion."""
    company_id: str
    company_name: str
    ats_type: str
    jobs_found: int
    jobs_inserted: int
    jobs_updated: int
    duration_seconds: float
    error: Optional[str] = None


class ATSDetector:
    """Detects which ATS platform a company uses."""

    @staticmethod
    def detect(company_id: str) -> Optional[str]:
        """
        Detect which ATS a company uses by probing each platform.

        Returns:
            ATS type string or None if not found
        """
        # Try Greenhouse first (most common for tech)
        if GreenhouseConnector.discover_company(company_id):
            logger.info(f"Detected Greenhouse for {company_id}")
            return "greenhouse"

        # Try Lever
        if LeverConnector.discover_company(company_id):
            logger.info(f"Detected Lever for {company_id}")
            return "lever"

        # Try SmartRecruiters
        if SmartRecruitersConnector.discover_company(company_id):
            logger.info(f"Detected SmartRecruiters for {company_id}")
            return "smartrecruiters"

        # Workday is harder - would need to try multiple hosts
        # For now, skip auto-detection for Workday

        logger.debug(f"No ATS detected for {company_id}")
        return None


class JobPostingIngester:
    """
    Orchestrates job posting ingestion from multiple ATS platforms.
    """

    def __init__(self, db_url: str = None, max_workers: int = 5):
        """
        Initialize the ingester.

        Args:
            db_url: PostgreSQL connection URL
            max_workers: Maximum parallel workers for fetching
        """
        self.db_url = db_url
        self.max_workers = max_workers
        self.normalizer = TitleNormalizer()
        self.config = Config()

        # Track ATS detections for caching
        self.detected_ats: Dict[str, str] = {}

        # Rate limiting
        self.request_delay = 0.5  # seconds between requests per worker

    def get_connector(self, target: CompanyTarget):
        """
        Get the appropriate connector for a company target.

        Returns:
            Connector instance or None if not found
        """
        ats_type = target.ats_type

        # If no ATS type, try to detect
        if not ats_type:
            if target.company_id in self.detected_ats:
                ats_type = self.detected_ats[target.company_id]
            else:
                ats_type = ATSDetector.detect(target.company_id)
                if ats_type:
                    self.detected_ats[target.company_id] = ats_type

        if not ats_type:
            return None

        # Create connector based on type
        if ats_type == "greenhouse":
            return GreenhouseConnector(target.company_id, target.company_name)

        elif ats_type == "lever":
            return LeverConnector(target.company_id, target.company_name)

        elif ats_type == "smartrecruiters":
            return SmartRecruitersConnector(target.company_id, target.company_name)

        elif ats_type == "workday":
            # Workday needs additional configuration
            for (company_id, host, tenant), name in WORKDAY_COMPANIES.items():
                if company_id.lower() == target.company_id.lower():
                    return WorkdayConnector(
                        company_id=company_id,
                        company_name=name,
                        workday_host=host,
                        tenant=tenant
                    )
            # Try auto-discovery for unknown Workday
            return WorkdayConnector(target.company_id, target.company_name)

        return None

    def fetch_company_jobs(self, target: CompanyTarget) -> Tuple[List[JobPosting], str, Optional[str]]:
        """
        Fetch jobs for a single company.

        Returns:
            Tuple of (jobs list, ats_type, error message or None)
        """
        start_time = time.time()

        try:
            connector = self.get_connector(target)
            if not connector:
                return [], None, "No ATS detected"

            jobs = connector.fetch_jobs()
            ats_type = connector.ATS_TYPE

            # Rate limiting
            time.sleep(self.request_delay)

            return jobs, ats_type, None

        except Exception as e:
            logger.error(f"Error fetching {target.company_name}: {e}")
            return [], target.ats_type, str(e)

    def ingest_company(self, target: CompanyTarget) -> IngestionResult:
        """
        Fetch and ingest jobs for a single company.

        Returns:
            IngestionResult with statistics
        """
        start_time = time.time()
        logger.info(f"Processing {target.company_name} ({target.company_id})")

        # Fetch jobs
        jobs, ats_type, error = self.fetch_company_jobs(target)

        if error:
            return IngestionResult(
                company_id=target.company_id,
                company_name=target.company_name,
                ats_type=ats_type or "unknown",
                jobs_found=0,
                jobs_inserted=0,
                jobs_updated=0,
                duration_seconds=time.time() - start_time,
                error=error
            )

        if not jobs:
            return IngestionResult(
                company_id=target.company_id,
                company_name=target.company_name,
                ats_type=ats_type,
                jobs_found=0,
                jobs_inserted=0,
                jobs_updated=0,
                duration_seconds=time.time() - start_time
            )

        # Ingest to database
        inserted, updated = self.upsert_jobs(jobs, target.company_name)

        return IngestionResult(
            company_id=target.company_id,
            company_name=target.company_name,
            ats_type=ats_type,
            jobs_found=len(jobs),
            jobs_inserted=inserted,
            jobs_updated=updated,
            duration_seconds=time.time() - start_time
        )

    def upsert_jobs(self, jobs: List[JobPosting], company_name: str) -> Tuple[int, int]:
        """
        Upsert jobs to database.

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not jobs:
            return 0, 0

        inserted = 0
        updated = 0

        try:
            if self.db_url:
                conn = psycopg2.connect(self.db_url)
            else:
                conn = psycopg2.connect(
                    host=self.config.db_host,
                    port=self.config.db_port,
                    database=self.config.db_name,
                    user=self.config.db_user,
                    password=self.config.db_password
                )
            cur = conn.cursor()

            for job in jobs:
                try:
                    # Get or create company
                    company_id = self._get_or_create_company(cur, company_name)

                    # Get or create location
                    location_id = self._get_or_create_location(cur, job)

                    # Normalize title
                    title_result = self.normalizer.parse_title(job.title)
                    canonical_role_id = self._get_canonical_role_id(cur, title_result.canonical_role_name)

                    # Upsert job
                    was_inserted = self._upsert_job(
                        cur, job, company_id, location_id, canonical_role_id, title_result
                    )

                    if was_inserted:
                        inserted += 1
                    else:
                        updated += 1

                except Exception as e:
                    logger.warning(f"Error upserting job {job.external_id}: {e}")
                    continue

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Database error: {e}")

        return inserted, updated

    def _get_or_create_company(self, cur, company_name: str) -> int:
        """Get or create company record."""
        normalized_name = company_name.lower().strip()

        # Try to find existing
        cur.execute(
            "SELECT id FROM companies WHERE normalized_name = %s",
            (normalized_name,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # Create new
        cur.execute(
            """
            INSERT INTO companies (name, normalized_name)
            VALUES (%s, %s)
            ON CONFLICT (normalized_name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (company_name, normalized_name)
        )
        return cur.fetchone()[0]

    def _get_or_create_location(self, cur, job: JobPosting) -> Optional[int]:
        """Get or create location record."""
        if not job.city and not job.state:
            return None

        country = job.country or 'US'

        # Try to find existing
        cur.execute(
            """
            SELECT id FROM locations
            WHERE city = %s AND state = %s AND country = %s
            """,
            (job.city, job.state, country)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # Create new
        cur.execute(
            """
            INSERT INTO locations (city, state, country)
            VALUES (%s, %s, %s)
            ON CONFLICT (city, state, country) DO NOTHING
            RETURNING id
            """,
            (job.city, job.state, country)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # Fetch the existing one
        cur.execute(
            "SELECT id FROM locations WHERE city = %s AND state = %s AND country = %s",
            (job.city, job.state, country)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _get_canonical_role_id(self, cur, canonical_title: str) -> Optional[int]:
        """Get canonical role ID for a title."""
        if not canonical_title:
            return None

        cur.execute(
            "SELECT id FROM canonical_roles WHERE name = %s",
            (canonical_title,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _upsert_job(self, cur, job: JobPosting, company_id: int,
                    location_id: Optional[int], canonical_role_id: Optional[int],
                    title_result) -> bool:
        """
        Upsert a single job posting.

        Uses metadata->external_id for deduplication since the table
        doesn't have a dedicated external_id column.

        Returns:
            True if inserted, False if updated
        """
        import json

        # Check if exists by looking in metadata
        cur.execute(
            """
            SELECT id FROM observed_jobs
            WHERE metadata->>'external_id' = %s
              AND source_type = %s
            """,
            (job.external_id, f"ats_{job.ats_type}")
        )
        existing = cur.fetchone()

        # Build metadata JSON
        metadata = {
            'external_id': job.external_id,
            'url': job.url,
            'is_remote': job.is_remote,
            'ats_type': job.ats_type
        }

        if existing:
            # Update existing
            cur.execute(
                """
                UPDATE observed_jobs SET
                    raw_title = %s,
                    canonical_role_id = %s,
                    location_id = %s,
                    salary_min = %s,
                    salary_max = %s,
                    title_confidence = %s,
                    last_seen = NOW(),
                    updated_at = NOW(),
                    metadata = %s
                WHERE id = %s
                """,
                (
                    job.title,
                    canonical_role_id,
                    location_id,
                    job.salary_min,
                    job.salary_max,
                    title_result.title_confidence if title_result else None,
                    json.dumps(metadata),
                    existing[0]
                )
            )
            return False
        else:
            # Insert new
            cur.execute(
                """
                INSERT INTO observed_jobs (
                    company_id, raw_title, canonical_role_id, location_id,
                    salary_min, salary_max, title_confidence,
                    source_type, posted_date, first_seen, last_seen,
                    metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s
                )
                """,
                (
                    company_id,
                    job.title,
                    canonical_role_id,
                    location_id,
                    job.salary_min,
                    job.salary_max,
                    title_result.title_confidence if title_result else None,
                    f"ats_{job.ats_type}",
                    job.posted_date,
                    json.dumps(metadata)
                )
            )
            return True

    def run_detection(self, targets: List[CompanyTarget]) -> Dict[str, str]:
        """
        Run ATS detection for targets without known ATS.

        Returns:
            Dict mapping company_id to detected ATS type
        """
        results = {}

        for target in targets:
            if target.ats_type:
                results[target.company_id] = target.ats_type
                continue

            ats_type = ATSDetector.detect(target.company_id)
            if ats_type:
                results[target.company_id] = ats_type
                logger.info(f"Detected {ats_type} for {target.company_name}")
            else:
                logger.debug(f"No ATS found for {target.company_name}")

            # Rate limiting
            time.sleep(0.3)

        return results

    def run_ingestion(self, targets: List[CompanyTarget],
                      parallel: bool = True) -> List[IngestionResult]:
        """
        Run full ingestion for all targets.

        Args:
            targets: List of company targets
            parallel: Whether to run in parallel

        Returns:
            List of IngestionResult objects
        """
        results = []

        if parallel and len(targets) > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_target = {
                    executor.submit(self.ingest_company, target): target
                    for target in targets
                }

                for future in as_completed(future_to_target):
                    target = future_to_target[future]
                    try:
                        result = future.result()
                        results.append(result)
                        if result.error:
                            logger.warning(f"{target.company_name}: {result.error}")
                        else:
                            logger.info(
                                f"{target.company_name}: {result.jobs_found} jobs "
                                f"({result.jobs_inserted} new, {result.jobs_updated} updated)"
                            )
                    except Exception as e:
                        logger.error(f"Error processing {target.company_name}: {e}")
                        results.append(IngestionResult(
                            company_id=target.company_id,
                            company_name=target.company_name,
                            ats_type=target.ats_type or "unknown",
                            jobs_found=0,
                            jobs_inserted=0,
                            jobs_updated=0,
                            duration_seconds=0,
                            error=str(e)
                        ))
        else:
            for target in targets:
                result = self.ingest_company(target)
                results.append(result)
                if result.error:
                    logger.warning(f"{target.company_name}: {result.error}")
                else:
                    logger.info(
                        f"{target.company_name}: {result.jobs_found} jobs "
                        f"({result.jobs_inserted} new, {result.jobs_updated} updated)"
                    )

        return results


def main():
    parser = argparse.ArgumentParser(
        description="Ingest job postings from ATS platforms"
    )
    parser.add_argument(
        '--known-only',
        action='store_true',
        help='Only process companies with known ATS type'
    )
    parser.add_argument(
        '--detect-only',
        action='store_true',
        help='Only run ATS detection, do not ingest'
    )
    parser.add_argument(
        '--company',
        type=str,
        help='Process single company by ID'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of companies to process'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of parallel workers'
    )
    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing'
    )

    args = parser.parse_args()

    # Get targets
    if args.company:
        # Single company
        targets = [CompanyTarget(
            company_id=args.company,
            company_name=args.company.title(),
            ats_type=None
        )]
    elif args.known_only:
        targets = get_all_known_targets()
    else:
        targets = get_all_targets()

    if args.limit:
        targets = targets[:args.limit]

    logger.info(f"Processing {len(targets)} company targets")

    # Initialize ingester
    ingester = JobPostingIngester(max_workers=args.workers)

    if args.detect_only:
        # Just run detection
        results = ingester.run_detection(targets)
        print(f"\nATS Detection Results:")
        print(f"Detected: {len([v for v in results.values() if v])}")
        for company_id, ats_type in sorted(results.items()):
            if ats_type:
                print(f"  {company_id}: {ats_type}")
        return

    # Run full ingestion
    start_time = time.time()
    results = ingester.run_ingestion(
        targets,
        parallel=not args.no_parallel
    )
    duration = time.time() - start_time

    # Summary
    total_jobs = sum(r.jobs_found for r in results)
    total_inserted = sum(r.jobs_inserted for r in results)
    total_updated = sum(r.jobs_updated for r in results)
    successful = len([r for r in results if not r.error])
    failed = len([r for r in results if r.error])

    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    print(f"Companies processed: {len(results)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"\nJobs:")
    print(f"  Total found: {total_jobs:,}")
    print(f"  Inserted: {total_inserted:,}")
    print(f"  Updated: {total_updated:,}")
    print(f"\nDuration: {duration:.1f}s")
    print(f"Rate: {total_jobs / max(duration, 1):.1f} jobs/sec")

    # Top companies by job count
    top_companies = sorted(results, key=lambda x: x.jobs_found, reverse=True)[:10]
    if top_companies:
        print(f"\nTop companies by job count:")
        for r in top_companies:
            if r.jobs_found > 0:
                print(f"  {r.company_name}: {r.jobs_found:,} ({r.ats_type})")

    # Log errors
    errors = [r for r in results if r.error]
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for r in errors[:10]:
            print(f"  {r.company_name}: {r.error}")


if __name__ == "__main__":
    main()
