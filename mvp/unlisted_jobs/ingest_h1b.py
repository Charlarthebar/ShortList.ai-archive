#!/usr/bin/env python3
"""
H-1B Data Ingestion Script
===========================

Fetches H-1B visa data and loads it into the comprehensive job database.

This script:
1. Downloads H-1B LCA disclosure data
2. Normalizes it to standard format
3. Creates companies, locations, and canonical roles
4. Creates observed jobs (Tier A source)
5. Creates compensation observations

Usage:
    python ingest_h1b.py --year 2024 --limit 1000

Author: ShortList.ai
"""

import sys
import os
import logging
import argparse
from datetime import date

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sources.h1b_visa import H1BVisaConnector
from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class H1BIngestionPipeline:
    """Pipeline for ingesting H-1B data into the database."""

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.title_normalizer = TitleNormalizer(db)
        self.h1b_connector = H1BVisaConnector()

        # Get or create H-1B source
        self.source_id = self._get_or_create_source()

        # Stats
        self.stats = {
            'fetched': 0,
            'processed': 0,
            'observed_jobs_created': 0,
            'comp_observations_created': 0,
            'errors': 0,
            'skipped_no_role_match': 0,
        }

    def _get_or_create_source(self) -> int:
        """Get or create H-1B source in database."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM sources WHERE name = 'h1b_visa'")
                result = cursor.fetchone()

                if result:
                    return result[0]

                # Create new source
                cursor.execute("""
                    INSERT INTO sources (name, type, reliability_tier, base_reliability, is_active)
                    VALUES ('h1b_visa', 'visa', 'A', 0.85, TRUE)
                    RETURNING id
                """)
                source_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Created H-1B source with ID {source_id}")
                return source_id
        finally:
            self.db.release_connection(conn)

    def run(self, year: int = 2024, limit: int = None):
        """
        Run the H-1B ingestion pipeline.

        Args:
            year: Fiscal year to fetch
            limit: Optional limit on records (for testing)
        """
        logger.info("="*60)
        logger.info("H-1B DATA INGESTION PIPELINE")
        logger.info("="*60)
        logger.info(f"Year: {year}")
        logger.info(f"Limit: {limit if limit else 'None (all records)'}")
        logger.info("")

        # Step 1: Fetch H-1B data
        logger.info("Step 1: Fetching H-1B data...")
        df = self.h1b_connector.fetch_year(year=year, limit=limit)
        self.stats['fetched'] = len(df)
        logger.info(f"✓ Fetched {len(df)} records")

        # Step 2: Convert to standard format
        logger.info("\nStep 2: Converting to standard format...")
        records = self.h1b_connector.to_standard_format(df)
        self.stats['processed'] = len(records)
        logger.info(f"✓ Converted {len(records)} certified records")

        # Step 3: Ingest into database
        logger.info("\nStep 3: Ingesting into database...")
        self._ingest_records(records)

        # Step 4: Show stats
        self._print_stats()

    def _ingest_records(self, records: list):
        """Ingest H-1B records into database."""
        for i, record in enumerate(records):
            try:
                if (i + 1) % 100 == 0:
                    logger.info(f"  Processing record {i+1}/{len(records)}...")

                # Step 1: Insert raw source data
                source_data_id = self.db.insert_source_data_raw(
                    source_id=self.source_id,
                    raw_data=record
                )

                # Step 2: Normalize company
                company_id = self.db.insert_company(
                    name=record['raw_company']
                )

                # Step 3: Normalize location
                city, state = self._parse_location(record['raw_location'])
                if not city or not state:
                    logger.debug(f"Skipping - invalid location: {record['raw_location']}")
                    self.stats['errors'] += 1
                    continue

                location_id = self.db.insert_location(
                    city=city,
                    state=state
                )

                # Step 4: Normalize title
                title_result = self.title_normalizer.parse_title(record['raw_title'])

                if not title_result.canonical_role_id:
                    logger.debug(f"Skipping - no role match: {record['raw_title']}")
                    self.stats['skipped_no_role_match'] += 1
                    continue

                # Step 5: Create observed job
                job_data = {
                    'company_id': company_id,
                    'location_id': location_id,
                    'canonical_role_id': title_result.canonical_role_id,
                    'raw_title': record['raw_title'],
                    'raw_company': record['raw_company'],
                    'raw_location': record['raw_location'],
                    'title_confidence': title_result.title_confidence,
                    'seniority': title_result.seniority,
                    'seniority_confidence': title_result.seniority_confidence,
                    'employment_type': 'FT' if record['raw_data'].get('full_time') == 'Y' else None,
                    'salary_min': record['raw_salary_min'],
                    'salary_max': record['raw_salary_max'],
                    'salary_currency': 'USD',
                    'salary_period': 'annual',
                    'salary_type': 'base',
                    'source_id': self.source_id,
                    'source_data_id': source_data_id,
                    'source_type': 'visa',
                    'observation_weight': 0.85,  # Tier A
                    'status': 'filled',  # H-1B means position was filled
                    'metadata': {
                        'soc_code': record['raw_data'].get('soc_code'),
                        'soc_title': record['raw_data'].get('soc_title'),
                        'case_status': record['raw_data'].get('case_status'),
                    }
                }

                job_id = self.db.insert_observed_job(job_data)
                self.stats['observed_jobs_created'] += 1

                # Step 6: Create compensation observation
                if record['raw_salary_min']:
                    comp_data = {
                        'company_id': company_id,
                        'location_id': location_id,
                        'canonical_role_id': title_result.canonical_role_id,
                        'seniority': title_result.seniority,
                        'pay_type': 'base',
                        'value_min': record['raw_salary_min'],
                        'value_max': record['raw_salary_max'],
                        'value_point': (record['raw_salary_min'] + record['raw_salary_max']) / 2 if record['raw_salary_max'] else record['raw_salary_min'],
                        'currency': 'USD',
                        'annualized_base': record['raw_salary_min'],
                        'source_id': self.source_id,
                        'source_type': 'visa',
                        'observation_weight': 0.85,
                        'observed_date': record['as_of_date'] or date.today(),
                        'employment_type': 'FT'
                    }

                    self.db.insert_compensation_observation(comp_data)
                    self.stats['comp_observations_created'] += 1

            except Exception as e:
                logger.error(f"Error processing record: {e}")
                logger.debug(f"Record: {record}")
                self.stats['errors'] += 1
                continue

    def _parse_location(self, location_str: str) -> tuple:
        """Parse 'City, State' string."""
        if not location_str:
            return (None, None)

        parts = location_str.split(',')
        if len(parts) != 2:
            return (None, None)

        city = parts[0].strip()
        state = parts[1].strip()

        return (city, state)

    def _print_stats(self):
        """Print ingestion statistics."""
        logger.info("\n" + "="*60)
        logger.info("INGESTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Records fetched:              {self.stats['fetched']:,}")
        logger.info(f"Records processed:            {self.stats['processed']:,}")
        logger.info(f"Observed jobs created:        {self.stats['observed_jobs_created']:,}")
        logger.info(f"Compensation obs created:     {self.stats['comp_observations_created']:,}")
        logger.info(f"Skipped (no role match):      {self.stats['skipped_no_role_match']:,}")
        logger.info(f"Errors:                       {self.stats['errors']:,}")
        logger.info("")

        if self.stats['observed_jobs_created'] > 0:
            logger.info("✓ SUCCESS: H-1B data loaded into database")
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Run: psql jobs_comprehensive")
            logger.info("  2. Query: SELECT * FROM observed_jobs WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa');")
            logger.info("  3. Or use Python: from database import DatabaseManager; db = DatabaseManager(); ...")
        else:
            logger.warning("⚠ No jobs were created. Check errors above.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ingest H-1B visa data")
    parser.add_argument('--year', type=int, default=2024, help='Fiscal year (2022-2024)')
    parser.add_argument('--limit', type=int, help='Limit number of records (for testing)')
    parser.add_argument('--init-schema', action='store_true', help='Initialize schema first')
    parser.add_argument('--seed-roles', action='store_true', help='Seed canonical roles first')

    args = parser.parse_args()

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    # Initialize schema if requested
    if args.init_schema:
        logger.info("Initializing database schema...")
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        db.execute_schema_file(schema_path)

    # Seed roles if requested
    if args.seed_roles:
        logger.info("Seeding canonical roles...")
        from title_normalizer import seed_canonical_roles
        seed_canonical_roles(db)

    # Run ingestion
    pipeline = H1BIngestionPipeline(db)
    pipeline.run(year=args.year, limit=args.limit)

    # Cleanup
    db.close_all_connections()


if __name__ == "__main__":
    main()
