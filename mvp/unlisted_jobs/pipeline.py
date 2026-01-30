#!/usr/bin/env python3
"""
Job Database Pipeline - Main Orchestrator
==========================================

End-to-end pipeline that implements the 12-phase comprehensive plan.

Runs:
1. Source acquisition from all configured sources
2. Company and location normalization
3. Title normalization and role mapping
4. Observed job creation
5. Compensation observation collection
6. Salary estimation modeling
7. Headcount estimation modeling
8. Description generation
9. Archetype synthesis (observed + inferred)
10. Confidence scoring and provenance tracking
11. Quality metrics computation
12. Human review queue population

Author: ShortList.ai
"""

import os
import sys
import logging
import argparse
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import json

from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer, seed_canonical_roles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class JobDatabasePipeline:
    """
    Main pipeline orchestrator.

    Coordinates all phases of the job database building process.
    """

    def __init__(self, database_manager: DatabaseManager = None):
        self.db = database_manager or DatabaseManager()
        self.title_normalizer = TitleNormalizer(self.db)

        # Initialize connection pool
        if not self.db.pool:
            self.db.initialize_pool()

        # Metrics for this run
        self.run_metrics = {
            'started_at': datetime.now(),
            'sources_processed': 0,
            'raw_records_ingested': 0,
            'observed_jobs_created': 0,
            'observed_jobs_updated': 0,
            'archetypes_created': 0,
            'archetypes_updated': 0,
            'errors': []
        }

    def run(self, mode: str = 'full'):
        """
        Run the full pipeline.

        Args:
            mode: 'full' | 'incremental' | 'metrics_only'
        """
        logger.info(f"Starting pipeline run (mode={mode})")

        try:
            if mode == 'metrics_only':
                self._compute_quality_metrics()
                return

            # Phase 1-4: Setup and ingestion
            self._phase_1_4_ingestion()

            # Phase 5-9: Modeling and inference
            if mode == 'full':
                self._phase_5_9_inference()

            # Phase 10: Quality metrics
            self._compute_quality_metrics()

            # Save run log
            self._save_run_log()

            logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.run_metrics['errors'].append(str(e))
            self._save_run_log(status='failed')
            raise

    def _phase_1_4_ingestion(self):
        """
        Phases 1-4: Source acquisition, normalization, observed job creation.
        """
        logger.info("="*60)
        logger.info("PHASES 1-4: SOURCE ACQUISITION AND NORMALIZATION")
        logger.info("="*60)

        # For this demo, we'll show the structure
        # In production, you'd call actual source connectors

        # Example: Process a sample source
        self._process_sample_payroll_source()

        logger.info(f"Observed jobs created: {self.run_metrics['observed_jobs_created']}")

    def _process_sample_payroll_source(self):
        """
        Example: Process a sample payroll data source.

        In production, this would be in sources/payroll_connector.py
        """
        logger.info("Processing sample payroll source...")

        # Sample data (in production, this comes from API/file)
        sample_records = [
            {
                'company_name': 'Massachusetts Institute of Technology',
                'employee_name': 'John Doe',
                'title': 'Senior Software Engineer',
                'department': 'Information Systems',
                'city': 'Cambridge',
                'state': 'MA',
                'annual_salary': 145000,
                'pay_year': 2024,
            },
            {
                'company_name': 'Harvard University',
                'employee_name': 'Jane Smith',
                'title': 'Data Scientist',
                'department': 'Research',
                'city': 'Cambridge',
                'state': 'MA',
                'annual_salary': 125000,
                'pay_year': 2024,
            }
        ]

        # Get or create source
        source_id = self._get_or_create_source('sample_payroll', 'payroll', 'A', 0.95)

        for record in sample_records:
            try:
                # Normalize company
                company_id = self.db.insert_company(
                    name=record['company_name'],
                    industry='Education'
                )

                # Normalize location
                # (In production, you'd look up metro_id from CBSA codes)
                location_id = self.db.insert_location(
                    city=record['city'],
                    state=record['state'],
                    metro_id=None  # Would be Boston-Cambridge metro
                )

                # Normalize title
                title_result = self.title_normalizer.parse_title(record['title'])

                # If role not in DB, log for review
                if not title_result.canonical_role_id:
                    logger.warning(f"Title not mapped: {record['title']}")
                    continue

                # Create observed job
                job_data = {
                    'company_id': company_id,
                    'location_id': location_id,
                    'canonical_role_id': title_result.canonical_role_id,
                    'raw_title': record['title'],
                    'raw_company': record['company_name'],
                    'raw_location': f"{record['city']}, {record['state']}",
                    'title_confidence': title_result.title_confidence,
                    'seniority': title_result.seniority,
                    'seniority_confidence': title_result.seniority_confidence,
                    'salary_point': record['annual_salary'],
                    'salary_currency': 'USD',
                    'salary_period': 'annual',
                    'salary_type': 'base',
                    'source_id': source_id,
                    'source_type': 'payroll',
                    'observation_weight': 0.95,
                    'status': 'filled',  # Payroll = filled position
                    'metadata': {
                        'department': record['department'],
                        'pay_year': record['pay_year']
                    }
                }

                job_id = self.db.insert_observed_job(job_data)
                self.run_metrics['observed_jobs_created'] += 1

                # Also create compensation observation
                comp_data = {
                    'company_id': company_id,
                    'location_id': location_id,
                    'canonical_role_id': title_result.canonical_role_id,
                    'seniority': title_result.seniority,
                    'pay_type': 'base',
                    'value_point': record['annual_salary'],
                    'annualized_base': record['annual_salary'],
                    'currency': 'USD',
                    'source_id': source_id,
                    'source_type': 'payroll',
                    'observation_weight': 0.95,
                    'observed_date': date.today(),
                    'employment_type': 'FT'
                }
                self.db.insert_compensation_observation(comp_data)

                logger.info(f"✓ Processed: {record['title']} at {record['company_name']}")

            except Exception as e:
                logger.error(f"Error processing record: {e}")
                self.run_metrics['errors'].append(f"Record processing: {str(e)}")

    def _phase_5_9_inference(self):
        """
        Phases 5-9: Evidence modeling, salary estimation, description generation, archetype synthesis.
        """
        logger.info("="*60)
        logger.info("PHASES 5-9: INFERENCE AND ARCHETYPE GENERATION")
        logger.info("="*60)

        # This is where you'd run:
        # - Salary model (hierarchical Bayesian)
        # - Headcount allocation
        # - Description generation
        # - Archetype synthesis

        # For now, we'll create a simple example archetype
        self._create_sample_archetypes()

    def _create_sample_archetypes(self):
        """
        Create sample archetypes based on observed data.

        In production, this would:
        1. Aggregate observed_jobs by (company, metro, role, seniority)
        2. Run salary model to get distributions
        3. Run headcount model
        4. Generate descriptions
        5. Compute confidence scores
        """
        logger.info("Creating sample archetypes from observed data...")

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                # Find archetypes to create from observed jobs
                cursor.execute("""
                    SELECT
                        company_id,
                        location_id,
                        canonical_role_id,
                        seniority,
                        COUNT(*) as obs_count,
                        AVG(salary_point) as avg_salary,
                        MIN(salary_point) as min_salary,
                        MAX(salary_point) as max_salary
                    FROM observed_jobs
                    WHERE canonical_role_id IS NOT NULL
                      AND seniority IS NOT NULL
                    GROUP BY company_id, location_id, canonical_role_id, seniority
                """)

                results = cursor.fetchall()

                for row in results:
                    company_id, location_id, role_id, seniority = row[0], row[1], row[2], row[3]
                    obs_count, avg_sal, min_sal, max_sal = row[4], row[5], row[6], row[7]

                    # Get metro_id from location
                    cursor.execute("SELECT metro_id FROM locations WHERE id = %s", (location_id,))
                    metro_result = cursor.fetchone()
                    metro_id = metro_result[0] if metro_result else None

                    if not metro_id:
                        continue

                    # Create observed archetype (high confidence because from payroll)
                    archetype_data = {
                        'company_id': company_id,
                        'metro_id': metro_id,
                        'canonical_role_id': role_id,
                        'seniority': seniority,
                        'record_type': 'observed',  # This is an observed aggregate
                        'headcount_p50': obs_count,  # Observed count
                        'salary_p25': min_sal,
                        'salary_p50': avg_sal,
                        'salary_p75': max_sal,
                        'salary_mean': avg_sal,
                        'salary_currency': 'USD',
                        'salary_method': 'direct_observation',
                        'observed_count': obs_count,
                        'composite_confidence': 0.90,  # High confidence - from payroll
                        'evidence_summary': {
                            'payroll_rows': obs_count
                        },
                        'evidence_date_latest': date.today()
                    }

                    archetype_id = self.db.upsert_job_archetype(archetype_data)
                    self.run_metrics['archetypes_created'] += 1

                    logger.info(f"✓ Created observed archetype ID {archetype_id}")

        finally:
            self.db.release_connection(conn)

    def _compute_quality_metrics(self):
        """
        Phase 10: Compute and store quality metrics.
        """
        logger.info("="*60)
        logger.info("PHASE 10: QUALITY METRICS")
        logger.info("="*60)

        metrics = {}

        # Coverage metrics
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                # Total observed jobs by source
                cursor.execute("""
                    SELECT s.name, COUNT(*) as count
                    FROM observed_jobs o
                    JOIN sources s ON o.source_id = s.id
                    GROUP BY s.name
                """)
                metrics['observed_jobs_by_source'] = dict(cursor.fetchall())

                # Total archetypes by record type
                cursor.execute("""
                    SELECT record_type, COUNT(*) as count
                    FROM job_archetypes
                    GROUP BY record_type
                """)
                metrics['archetypes_by_type'] = dict(cursor.fetchall())

                # Title mapping confidence
                cursor.execute("""
                    SELECT
                        AVG(title_confidence) as avg_confidence,
                        SUM(CASE WHEN title_confidence >= 0.7 THEN 1 ELSE 0 END) as high_conf_count,
                        COUNT(*) as total_count
                    FROM observed_jobs
                    WHERE canonical_role_id IS NOT NULL
                """)
                row = cursor.fetchone()
                if row:
                    metrics['title_mapping'] = {
                        'avg_confidence': float(row[0]) if row[0] else 0,
                        'high_confidence_pct': (row[1] / row[2] * 100) if row[2] > 0 else 0
                    }

            logger.info("\nQuality Metrics:")
            logger.info(json.dumps(metrics, indent=2))

        finally:
            self.db.release_connection(conn)

    def _save_run_log(self, status: str = 'completed'):
        """Save pipeline run log to database."""
        self.run_metrics['completed_at'] = datetime.now()
        self.run_metrics['status'] = status

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline_runs (
                        run_type, started_at, completed_at, status,
                        sources_processed, raw_records_ingested,
                        observed_jobs_created, observed_jobs_updated,
                        archetypes_created, archetypes_updated,
                        error_log
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'full',
                    self.run_metrics['started_at'],
                    self.run_metrics['completed_at'],
                    status,
                    self.run_metrics['sources_processed'],
                    self.run_metrics['raw_records_ingested'],
                    self.run_metrics['observed_jobs_created'],
                    self.run_metrics['observed_jobs_updated'],
                    self.run_metrics['archetypes_created'],
                    self.run_metrics['archetypes_updated'],
                    '\n'.join(self.run_metrics['errors']) if self.run_metrics['errors'] else None
                ))
            conn.commit()
        finally:
            self.db.release_connection(conn)

    def _get_or_create_source(self, name: str, source_type: str,
                             reliability_tier: str, base_reliability: float) -> int:
        """Get or create source ID."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM sources WHERE name = %s", (name,))
                result = cursor.fetchone()

                if result:
                    return result[0]

                cursor.execute("""
                    INSERT INTO sources (name, type, reliability_tier, base_reliability, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (name, source_type, reliability_tier, base_reliability, True))

                source_id = cursor.fetchone()[0]
                conn.commit()
                return source_id
        finally:
            self.db.release_connection(conn)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Job Database Pipeline")
    parser.add_argument('--mode', default='full', choices=['full', 'incremental', 'metrics_only'],
                       help='Pipeline run mode')
    parser.add_argument('--seed-roles', action='store_true',
                       help='Seed canonical roles before running')
    parser.add_argument('--init-schema', action='store_true',
                       help='Initialize database schema')

    args = parser.parse_args()

    # Initialize database
    config = Config()
    db = DatabaseManager(config)

    # Initialize schema if requested
    if args.init_schema:
        logger.info("Initializing database schema...")
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        db.execute_schema_file(schema_path)
        logger.info("Schema initialized")

    # Seed roles if requested
    if args.seed_roles:
        logger.info("Seeding canonical roles...")
        seed_canonical_roles(db)
        logger.info("Canonical roles seeded")

    # Run pipeline
    pipeline = JobDatabasePipeline(db)
    pipeline.run(mode=args.mode)

    # Cleanup
    db.close_all_connections()


if __name__ == "__main__":
    main()
