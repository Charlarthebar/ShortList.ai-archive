#!/usr/bin/env python3
"""
Massachusetts State Payroll Ingestion Pipeline
==============================================

Loads MA state employee payroll data into the comprehensive job database.

Usage:
    python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv
    python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv --limit 1000

Author: ShortList.ai
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Dict, List

from sources.ma_state_payroll import MAStatePayrollConnector
from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MAPayrollIngestionPipeline:
    """
    Pipeline for ingesting MA state payroll data into the database.
    """

    def __init__(self, database_manager: DatabaseManager):
        """
        Initialize the ingestion pipeline.

        Args:
            database_manager: Database manager instance
        """
        self.db = database_manager
        self.normalizer = TitleNormalizer(database_manager)
        self.connector = MAStatePayrollConnector()

        # Track statistics
        self.stats = {
            'fetched': 0,
            'processed': 0,
            'jobs_created': 0,
            'compensation_created': 0,
            'skipped': 0,
            'errors': 0,
        }

        # Get or create source
        self.source_id = self._get_or_create_source()

    def _get_or_create_source(self) -> int:
        """Get or create the ma_state_payroll source."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM sources WHERE name = 'ma_state_payroll'
                """)
                result = cursor.fetchone()

                if result:
                    return result[0]

                # Create source
                cursor.execute("""
                    INSERT INTO sources (
                        name, type, reliability_tier, base_reliability
                    ) VALUES (
                        'ma_state_payroll',
                        'payroll',
                        'A',
                        0.90
                    )
                    RETURNING id
                """)
                source_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Created source: ma_state_payroll (id={source_id})")
                return source_id
        finally:
            self.db.release_connection(conn)

    def run(self, csv_path: str, limit: int = None):
        """
        Run the full ingestion pipeline.

        Args:
            csv_path: Path to MA payroll CSV file
            limit: Optional limit on records to process
        """
        logger.info("="*60)
        logger.info("MA STATE PAYROLL INGESTION PIPELINE")
        logger.info("="*60)
        logger.info(f"CSV file: {csv_path}")
        logger.info(f"Limit: {limit or 'None (all records)'}")
        logger.info("")

        # Step 1: Load CSV
        logger.info("Step 1: Loading MA state payroll data...")
        df = self.connector.load_from_csv(csv_path, limit=limit)
        self.stats['fetched'] = len(df)
        logger.info(f"✓ Loaded {len(df)} records")

        # Step 2: Convert to standard format
        logger.info("\nStep 2: Converting to standard format...")
        records = self.connector.to_standard_format(df)
        self.stats['processed'] = len(records)
        logger.info(f"✓ Converted {len(records)} records with salaries")

        # Step 3: Ingest into database
        logger.info("\nStep 3: Ingesting into database...")
        self._ingest_records(records)

        # Step 4: Summary
        self._print_summary()

    def _ingest_records(self, records: List[Dict]):
        """
        Ingest MA payroll records into database.

        Args:
            records: List of standardized payroll records
        """
        for i, record in enumerate(records):
            if (i + 1) % 100 == 0:
                logger.info(f"  Processing record {i + 1}/{len(records)}...")

            try:
                # Insert raw source data
                source_data_id = self.db.insert_source_data_raw(
                    source_id=self.source_id,
                    raw_data=record
                )

                # Normalize company (always "Commonwealth of Massachusetts")
                company_id = self.db.insert_company(
                    name=record['raw_company']
                )

                # Normalize location
                city = record['raw_location'].split(',')[0].strip() if ',' in record['raw_location'] else 'Boston'
                location_id = self.db.insert_location(
                    city=city,
                    state='MA'
                )

                # Normalize title
                title_result = self.normalizer.parse_title(record['raw_title'])

                # Skip if no role match
                if title_result.canonical_role_id is None:
                    self.stats['skipped'] += 1
                    continue

                # Create observed job
                job_id = self.db.insert_observed_job({
                    'source_id': self.source_id,
                    'source_data_id': source_data_id,
                    'source_type': 'payroll',
                    'company_id': company_id,
                    'location_id': location_id,
                    'canonical_role_id': title_result.canonical_role_id,
                    'seniority': title_result.seniority,
                    'raw_company': record['raw_company'],
                    'raw_location': record['raw_location'],
                    'raw_title': record['raw_title'],
                    'raw_description': record['raw_description'],
                    'salary_min': record['raw_salary_min'],
                    'salary_max': record['raw_salary_max'],
                    'salary_currency': 'USD',
                    'title_confidence': title_result.title_confidence,
                    'seniority_confidence': title_result.seniority_confidence,
                    'as_of_date': record['as_of_date'],
                })

                self.stats['jobs_created'] += 1

                # Create compensation observation
                if record['raw_salary_min']:
                    self.db.insert_compensation_observation({
                        'source_id': self.source_id,
                        'company_id': company_id,
                        'location_id': location_id,
                        'canonical_role_id': title_result.canonical_role_id,
                        'seniority': title_result.seniority,
                        'salary_min': record['raw_salary_min'],
                        'salary_max': record['raw_salary_max'],
                        'salary_currency': 'USD',
                        'as_of_date': record['as_of_date'],
                        'sample_size': 1,
                        'is_reported': True,
                    })
                    self.stats['compensation_created'] += 1

            except Exception as e:
                logger.error(f"Error processing record: {e}")
                self.stats['errors'] += 1
                continue

    def _print_summary(self):
        """Print ingestion summary."""
        logger.info("")
        logger.info("="*60)
        logger.info("INGESTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Records fetched:              {self.stats['fetched']:,}")
        logger.info(f"Records processed:            {self.stats['processed']:,}")
        logger.info(f"Observed jobs created:        {self.stats['jobs_created']:,}")
        logger.info(f"Compensation obs created:     {self.stats['compensation_created']:,}")
        logger.info(f"Skipped (no role match):      {self.stats['skipped']:,}")
        logger.info(f"Errors:                       {self.stats['errors']:,}")
        logger.info("")

        if self.stats['jobs_created'] > 0:
            logger.info("✓ SUCCESS: MA state payroll data loaded into database")
        else:
            logger.warning("⚠ No jobs were created. Check errors above.")

        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Run: python3 check_status.py")
        logger.info("  2. Query: SELECT * FROM observed_jobs WHERE source_id = (SELECT id FROM sources WHERE name = 'ma_state_payroll');")
        logger.info("  3. Or use Python: from database import DatabaseManager; db = DatabaseManager(); ...")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest MA state payroll data into job database"
    )
    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Path to MA payroll CSV file (downloaded from CTHRU)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of records to process (for testing)'
    )

    args = parser.parse_args()

    # Check file exists
    if not os.path.exists(args.file):
        logger.error(f"File not found: {args.file}")
        logger.info("\nTo download MA payroll data:")
        logger.info("  1. Go to: https://cthrupayroll.mass.gov/")
        logger.info("  2. Click 'Export' or 'Download Data'")
        logger.info("  3. Select year: 2024")
        logger.info("  4. Save CSV to: data/ma_payroll_2024.csv")
        sys.exit(1)

    # Initialize database
    config = Config()
    db = DatabaseManager(config)

    try:
        db.initialize_pool()

        # Run pipeline
        pipeline = MAPayrollIngestionPipeline(db)
        pipeline.run(csv_path=args.file, limit=args.limit)

    finally:
        db.close_all_connections()
        logger.info("All database connections closed")


if __name__ == "__main__":
    main()
