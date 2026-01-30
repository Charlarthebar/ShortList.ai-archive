#!/usr/bin/env python3
"""
Quality Metrics Dashboard
=========================

Comprehensive quality metrics for the labor market intelligence platform.

Tracks:
1. Coverage metrics - How much of the labor market we observe
2. Data quality metrics - Confidence scores, mapping rates
3. Inference validation - Compare inferred vs. OEWS macro totals
4. Freshness metrics - Data recency and update rates

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup environment for database
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

from database import DatabaseManager, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CoverageMetrics:
    """Coverage metrics across different dimensions."""
    total_observed_jobs: int
    total_companies: int
    total_locations: int
    total_canonical_roles: int

    # By source
    jobs_by_source: Dict[str, int]

    # Coverage rates
    jobs_with_salary: int
    jobs_with_title_mapping: int
    salary_coverage_pct: float
    title_mapping_pct: float

    # Metro coverage
    metros_covered: int
    top_metros: List[Tuple[str, int]]


@dataclass
class DataQualityMetrics:
    """Data quality metrics."""
    # Title mapping quality
    high_confidence_titles_pct: float
    low_confidence_titles_count: int
    unmapped_titles_count: int

    # Salary quality
    salary_outliers_count: int
    salary_range_avg: float  # Average max-min range

    # Company matching
    companies_with_aliases: int
    potential_duplicates: int


@dataclass
class InferenceValidationMetrics:
    """Validation of inferred vs. macro data."""
    # Headcount validation
    total_inferred_headcount: int
    total_oews_employment: int
    headcount_coverage_ratio: float

    # By metro
    metros_with_inference: int
    metro_coverage_ratios: List[Tuple[str, float]]

    # Salary validation
    salary_mae: Optional[float]  # Mean absolute error vs. OEWS
    salary_calibration: Optional[float]


@dataclass
class FreshnessMetrics:
    """Data freshness metrics."""
    oldest_posting: datetime
    newest_posting: datetime
    avg_posting_age_days: float

    # Update rates
    jobs_added_last_7_days: int
    jobs_added_last_30_days: int

    # Source freshness
    source_last_update: Dict[str, datetime]


class QualityDashboard:
    """
    Quality metrics dashboard for the labor market intelligence platform.
    """

    def __init__(self, db_url: str = None):
        """Initialize with database connection."""
        self.db_url = db_url
        self.conn = None
        self.config = Config()

    def connect(self):
        """Establish database connection."""
        if not self.conn or self.conn.closed:
            if self.db_url:
                self.conn = psycopg2.connect(self.db_url)
            else:
                self.conn = psycopg2.connect(
                    host=self.config.db_host,
                    port=self.config.db_port,
                    database=self.config.db_name,
                    user=self.config.db_user,
                    password=self.config.db_password
                )

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def get_coverage_metrics(self) -> CoverageMetrics:
        """Compute coverage metrics."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Total counts
        cur.execute("SELECT COUNT(*) as cnt FROM observed_jobs")
        total_jobs = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM companies")
        total_companies = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM locations")
        total_locations = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM canonical_roles")
        total_roles = cur.fetchone()['cnt']

        # Jobs by source
        cur.execute("""
            SELECT source_type, COUNT(*) as cnt
            FROM observed_jobs
            GROUP BY source_type
            ORDER BY cnt DESC
        """)
        jobs_by_source = {row['source_type']: row['cnt'] for row in cur.fetchall()}

        # Coverage rates
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL) as with_salary,
                COUNT(*) FILTER (WHERE canonical_role_id IS NOT NULL) as with_mapping
            FROM observed_jobs
        """)
        row = cur.fetchone()
        jobs_with_salary = row['with_salary']
        jobs_with_mapping = row['with_mapping']

        # Metro coverage
        cur.execute("""
            SELECT COUNT(DISTINCT l.metro_id) as cnt
            FROM observed_jobs oj
            JOIN locations l ON oj.location_id = l.id
            WHERE l.metro_id IS NOT NULL
        """)
        metros_covered = cur.fetchone()['cnt']

        # Top metros
        cur.execute("""
            SELECT m.name, COUNT(*) as cnt
            FROM observed_jobs oj
            JOIN locations l ON oj.location_id = l.id
            JOIN metro_areas m ON l.metro_id = m.id
            GROUP BY m.name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        top_metros = [(row['name'], row['cnt']) for row in cur.fetchall()]

        cur.close()

        return CoverageMetrics(
            total_observed_jobs=total_jobs,
            total_companies=total_companies,
            total_locations=total_locations,
            total_canonical_roles=total_roles,
            jobs_by_source=jobs_by_source,
            jobs_with_salary=jobs_with_salary,
            jobs_with_title_mapping=jobs_with_mapping,
            salary_coverage_pct=(jobs_with_salary / total_jobs * 100) if total_jobs > 0 else 0,
            title_mapping_pct=(jobs_with_mapping / total_jobs * 100) if total_jobs > 0 else 0,
            metros_covered=metros_covered,
            top_metros=top_metros
        )

    def get_data_quality_metrics(self) -> DataQualityMetrics:
        """Compute data quality metrics."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Title mapping confidence
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE title_confidence >= 0.8) as high_conf,
                COUNT(*) FILTER (WHERE title_confidence < 0.5 AND title_confidence IS NOT NULL) as low_conf,
                COUNT(*) FILTER (WHERE canonical_role_id IS NULL) as unmapped
            FROM observed_jobs
        """)
        row = cur.fetchone()
        total_with_conf = row['high_conf'] + row['low_conf']
        high_conf_pct = (row['high_conf'] / total_with_conf * 100) if total_with_conf > 0 else 0

        # Salary quality
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE salary_min < 10000 OR salary_max > 1000000) as outliers,
                AVG(salary_max - salary_min) FILTER (WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL) as avg_range
            FROM observed_jobs
        """)
        salary_row = cur.fetchone()

        # Company matching
        cur.execute("SELECT COUNT(DISTINCT company_id) FROM company_aliases")
        companies_with_aliases = cur.fetchone()['count']

        # Potential duplicates (similar normalized names)
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT normalized_name, COUNT(*) as cnt
                FROM companies
                GROUP BY normalized_name
                HAVING COUNT(*) > 1
            ) dups
        """)
        potential_dups = cur.fetchone()['count']

        cur.close()

        return DataQualityMetrics(
            high_confidence_titles_pct=high_conf_pct,
            low_confidence_titles_count=row['low_conf'],
            unmapped_titles_count=row['unmapped'],
            salary_outliers_count=salary_row['outliers'] or 0,
            salary_range_avg=float(salary_row['avg_range'] or 0),
            companies_with_aliases=companies_with_aliases,
            potential_duplicates=potential_dups
        )

    def get_inference_validation(self) -> InferenceValidationMetrics:
        """Validate inferred data against OEWS macro totals."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Total inferred headcount
        cur.execute("""
            SELECT COALESCE(SUM(headcount_p50), 0) as total
            FROM job_archetypes
            WHERE record_type = 'inferred'
        """)
        total_inferred = cur.fetchone()['total']

        # Total OEWS employment (for metros we have data on)
        cur.execute("""
            SELECT COALESCE(SUM(employment), 0) as total
            FROM oews_estimates
            WHERE industry_code = '000000'
              AND employment IS NOT NULL
        """)
        total_oews = cur.fetchone()['total']

        coverage_ratio = total_inferred / total_oews if total_oews > 0 else 0

        # Metros with inference
        cur.execute("""
            SELECT COUNT(DISTINCT metro_id) FROM job_archetypes
            WHERE record_type = 'inferred'
        """)
        metros_with_inf = cur.fetchone()['count']

        # Coverage by metro (compare inferred to OEWS)
        cur.execute("""
            WITH inferred_by_metro AS (
                SELECT
                    m.name as metro_name,
                    SUM(ja.headcount_p50) as inferred_total
                FROM job_archetypes ja
                JOIN metro_areas m ON ja.metro_id = m.id
                WHERE ja.record_type = 'inferred'
                GROUP BY m.name
            ),
            oews_by_metro AS (
                SELECT
                    oa.area_name as metro_name,
                    SUM(oe.employment) as oews_total
                FROM oews_estimates oe
                JOIN oews_areas oa ON oe.area_code = oa.area_code
                WHERE oe.industry_code = '000000'
                  AND oa.areatype_code = 'M'
                GROUP BY oa.area_name
            )
            SELECT
                i.metro_name,
                i.inferred_total::float / NULLIF(o.oews_total, 0) as coverage_ratio
            FROM inferred_by_metro i
            LEFT JOIN oews_by_metro o ON i.metro_name ILIKE '%' || SPLIT_PART(o.metro_name, ',', 1) || '%'
            WHERE o.oews_total > 0
            ORDER BY coverage_ratio DESC
            LIMIT 10
        """)
        metro_ratios = [(row['metro_name'], row['coverage_ratio']) for row in cur.fetchall() if row['coverage_ratio']]

        cur.close()

        return InferenceValidationMetrics(
            total_inferred_headcount=total_inferred,
            total_oews_employment=total_oews,
            headcount_coverage_ratio=coverage_ratio,
            metros_with_inference=metros_with_inf,
            metro_coverage_ratios=metro_ratios,
            salary_mae=None,  # Would need validation data
            salary_calibration=None
        )

    def get_freshness_metrics(self) -> FreshnessMetrics:
        """Compute data freshness metrics."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Posting dates
        cur.execute("""
            SELECT
                MIN(first_seen) as oldest,
                MAX(first_seen) as newest,
                AVG(EXTRACT(EPOCH FROM (NOW() - first_seen)) / 86400) as avg_age_days
            FROM observed_jobs
            WHERE first_seen IS NOT NULL
        """)
        row = cur.fetchone()
        oldest = row['oldest'] or datetime.now()
        newest = row['newest'] or datetime.now()
        avg_age = row['avg_age_days'] or 0

        # Recent additions
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE first_seen >= NOW() - INTERVAL '7 days') as last_7,
                COUNT(*) FILTER (WHERE first_seen >= NOW() - INTERVAL '30 days') as last_30
            FROM observed_jobs
        """)
        row = cur.fetchone()
        last_7 = row['last_7']
        last_30 = row['last_30']

        # Source freshness
        cur.execute("""
            SELECT source_type, MAX(first_seen) as last_update
            FROM observed_jobs
            GROUP BY source_type
        """)
        source_freshness = {row['source_type']: row['last_update'] for row in cur.fetchall()}

        cur.close()

        return FreshnessMetrics(
            oldest_posting=oldest,
            newest_posting=newest,
            avg_posting_age_days=avg_age,
            jobs_added_last_7_days=last_7,
            jobs_added_last_30_days=last_30,
            source_last_update=source_freshness
        )

    def get_oews_mapping_stats(self) -> Dict:
        """Get statistics on OEWS role mapping coverage."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Total SOC codes vs mapped
        cur.execute("SELECT COUNT(*) FROM oews_occupations")
        total_soc = cur.fetchone()['count']

        cur.execute("SELECT COUNT(DISTINCT occ_code) FROM oews_role_mapping")
        mapped_soc = cur.fetchone()['count']

        # Canonical roles with OEWS mapping
        cur.execute("""
            SELECT COUNT(DISTINCT canonical_role_id) FROM oews_role_mapping
        """)
        roles_with_mapping = cur.fetchone()['count']

        cur.execute("SELECT COUNT(*) FROM canonical_roles")
        total_canonical = cur.fetchone()['count']

        cur.close()

        return {
            'total_soc_codes': total_soc,
            'mapped_soc_codes': mapped_soc,
            'soc_mapping_pct': (mapped_soc / total_soc * 100) if total_soc > 0 else 0,
            'total_canonical_roles': total_canonical,
            'roles_with_oews_mapping': roles_with_mapping,
            'role_mapping_pct': (roles_with_mapping / total_canonical * 100) if total_canonical > 0 else 0
        }

    def print_dashboard(self):
        """Print full dashboard to console."""
        print("\n" + "=" * 80)
        print(" LABOR MARKET INTELLIGENCE - QUALITY DASHBOARD")
        print(" " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        # Coverage Metrics
        print("\n" + "-" * 40)
        print(" COVERAGE METRICS")
        print("-" * 40)
        try:
            coverage = self.get_coverage_metrics()
            print(f"Total observed jobs:     {coverage.total_observed_jobs:>12,}")
            print(f"Total companies:         {coverage.total_companies:>12,}")
            print(f"Total locations:         {coverage.total_locations:>12,}")
            print(f"Canonical roles:         {coverage.total_canonical_roles:>12,}")
            print(f"Metros covered:          {coverage.metros_covered:>12,}")
            print(f"\nSalary coverage:         {coverage.salary_coverage_pct:>11.1f}%")
            print(f"Title mapping rate:      {coverage.title_mapping_pct:>11.1f}%")

            print("\nJobs by source:")
            for source, count in sorted(coverage.jobs_by_source.items(), key=lambda x: -x[1])[:8]:
                print(f"  {source:25s} {count:>10,}")

            if coverage.top_metros:
                print("\nTop metros:")
                for metro, count in coverage.top_metros[:5]:
                    print(f"  {metro[:35]:35s} {count:>10,}")
        except Exception as e:
            print(f"  Error: {e}")

        # Data Quality
        print("\n" + "-" * 40)
        print(" DATA QUALITY")
        print("-" * 40)
        try:
            quality = self.get_data_quality_metrics()
            print(f"High-confidence titles:  {quality.high_confidence_titles_pct:>11.1f}%")
            print(f"Low-confidence titles:   {quality.low_confidence_titles_count:>12,}")
            print(f"Unmapped titles:         {quality.unmapped_titles_count:>12,}")
            print(f"Salary outliers:         {quality.salary_outliers_count:>12,}")
            print(f"Avg salary range:        ${quality.salary_range_avg:>10,.0f}")
            print(f"Companies with aliases:  {quality.companies_with_aliases:>12,}")
            print(f"Potential duplicates:    {quality.potential_duplicates:>12,}")
        except Exception as e:
            print(f"  Error: {e}")

        # OEWS Mapping
        print("\n" + "-" * 40)
        print(" OEWS INTEGRATION")
        print("-" * 40)
        try:
            oews = self.get_oews_mapping_stats()
            print(f"SOC codes in OEWS:       {oews['total_soc_codes']:>12,}")
            print(f"SOC codes mapped:        {oews['mapped_soc_codes']:>12,}")
            print(f"SOC mapping coverage:    {oews['soc_mapping_pct']:>11.1f}%")
            print(f"Roles with OEWS mapping: {oews['roles_with_oews_mapping']:>12,}")
            print(f"Role mapping coverage:   {oews['role_mapping_pct']:>11.1f}%")
        except Exception as e:
            print(f"  Error: {e}")

        # Inference Validation
        print("\n" + "-" * 40)
        print(" INFERENCE VALIDATION")
        print("-" * 40)
        try:
            inference = self.get_inference_validation()
            print(f"Inferred headcount:      {inference.total_inferred_headcount:>12,}")
            print(f"OEWS total employment:   {inference.total_oews_employment:>12,}")
            print(f"Coverage ratio:          {inference.headcount_coverage_ratio:>11.2%}")
            print(f"Metros with inference:   {inference.metros_with_inference:>12,}")

            if inference.metro_coverage_ratios:
                print("\nTop metro coverage ratios:")
                for metro, ratio in inference.metro_coverage_ratios[:5]:
                    print(f"  {metro[:35]:35s} {ratio:>10.2%}")
        except Exception as e:
            print(f"  Error: {e}")

        # Freshness
        print("\n" + "-" * 40)
        print(" DATA FRESHNESS")
        print("-" * 40)
        try:
            freshness = self.get_freshness_metrics()
            print(f"Oldest posting:          {freshness.oldest_posting.strftime('%Y-%m-%d')}")
            print(f"Newest posting:          {freshness.newest_posting.strftime('%Y-%m-%d')}")
            print(f"Avg posting age:         {freshness.avg_posting_age_days:>10.1f} days")
            print(f"Added last 7 days:       {freshness.jobs_added_last_7_days:>12,}")
            print(f"Added last 30 days:      {freshness.jobs_added_last_30_days:>12,}")

            if freshness.source_last_update:
                print("\nSource last update:")
                for source, dt in sorted(freshness.source_last_update.items(), key=lambda x: x[1] or datetime.min, reverse=True)[:5]:
                    if dt:
                        print(f"  {source:25s} {dt.strftime('%Y-%m-%d %H:%M')}")
        except Exception as e:
            print(f"  Error: {e}")

        print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Quality metrics dashboard for labor market intelligence"
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output metrics as JSON'
    )

    args = parser.parse_args()

    dashboard = QualityDashboard()

    try:
        if args.json:
            import json
            metrics = {
                'coverage': dashboard.get_coverage_metrics().__dict__,
                'quality': dashboard.get_data_quality_metrics().__dict__,
                'oews': dashboard.get_oews_mapping_stats(),
                'inference': dashboard.get_inference_validation().__dict__,
                'freshness': {
                    k: str(v) if isinstance(v, datetime) else v
                    for k, v in dashboard.get_freshness_metrics().__dict__.items()
                }
            }
            print(json.dumps(metrics, indent=2, default=str))
        else:
            dashboard.print_dashboard()

    finally:
        dashboard.close()


if __name__ == "__main__":
    main()
