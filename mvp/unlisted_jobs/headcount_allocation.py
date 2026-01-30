#!/usr/bin/env python3
"""
Headcount Allocation Model
==========================

Allocates employment headcount to companies using OEWS macro totals as constraints.

The model:
1. Gets OEWS employment total for each metro × role combination
2. Computes company evidence scores from observed data (postings, H-1B, payrolls)
3. Applies Bayesian shrinkage toward uniform prior to avoid extreme allocations
4. Ensures allocated headcounts sum to OEWS total (hard constraint)

Key features:
- OEWS-constrained: company allocations must sum to macro totals
- Evidence-weighted: more evidence = more confidence in company share
- Shrinkage: small companies shrink toward uniform; large companies less so
- Distribution-based: returns P10/P50/P90 headcount estimates

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import logging
import argparse
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from scipy import stats
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup environment for database
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

from database import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CompanyEvidence:
    """Evidence summary for a company in a metro × role."""
    company_id: int
    company_name: str
    posting_count: int = 0
    h1b_count: int = 0
    payroll_count: int = 0
    total_evidence: float = 0.0
    evidence_weight: float = 0.0


@dataclass
class HeadcountEstimate:
    """Headcount estimate for a company × metro × role."""
    company_id: int
    company_name: str
    metro_id: int
    metro_name: str
    canonical_role_id: int
    role_name: str

    # Estimates
    headcount_p10: int
    headcount_p50: int
    headcount_p90: int

    # Evidence summary
    evidence_score: float
    share_of_metro: float

    # Methodology
    method: str
    oews_total: int
    companies_in_metro: int


class HeadcountAllocator:
    """
    Allocates employment headcount using OEWS constraints and observed evidence.
    """

    # Evidence weights (tunable)
    POSTING_WEIGHT = 0.5        # Weight per job posting
    H1B_WEIGHT = 2.0            # Weight per H-1B (higher - actual hire)
    PAYROLL_WEIGHT = 3.0        # Weight per payroll record (highest - confirmed employee)

    # Shrinkage parameters
    PRIOR_WEIGHT = 5.0          # Effective sample size of uniform prior
    MIN_EVIDENCE_FOR_ALLOCATION = 1  # Minimum evidence to include company

    # Concentration parameter for Dirichlet
    CONCENTRATION_ALPHA = 1.0   # Higher = more uniform, lower = more peaked

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

    def get_oews_priors(self, year: int = 2024) -> Dict[Tuple[str, int], int]:
        """
        Get OEWS employment totals by area × canonical_role.

        Returns:
            Dict mapping (area_code, canonical_role_id) to employment count
        """
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Get mapped roles from oews_role_mapping
        cur.execute("""
            SELECT
                oe.area_code,
                oa.area_name,
                orm.canonical_role_id,
                cr.name as role_name,
                SUM(oe.employment) as total_employment
            FROM oews_estimates oe
            JOIN oews_areas oa ON oe.area_code = oa.area_code
            JOIN oews_role_mapping orm ON oe.occ_code = orm.occ_code
            JOIN canonical_roles cr ON orm.canonical_role_id = cr.id
            WHERE oe.year = %s
              AND oe.industry_code = '000000'
              AND oe.employment IS NOT NULL
              AND orm.is_primary = TRUE
              AND oa.areatype_code = 'M'  -- Metro areas only
            GROUP BY oe.area_code, oa.area_name, orm.canonical_role_id, cr.name
        """, (year,))

        results = {}
        for row in cur.fetchall():
            key = (row['area_code'], row['canonical_role_id'])
            results[key] = {
                'area_name': row['area_name'],
                'role_name': row['role_name'],
                'employment': row['total_employment']
            }

        cur.close()
        logger.info(f"Loaded {len(results)} OEWS metro × role priors")
        return results

    def get_company_evidence(self, area_code: str, canonical_role_id: int) -> List[CompanyEvidence]:
        """
        Get evidence for companies in a specific metro × role.

        Aggregates evidence from:
        - Job postings (observed_jobs with matching role)
        - H-1B data (if available)
        - State payroll records
        """
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Get metro area name for location matching
        cur.execute("""
            SELECT area_name FROM oews_areas WHERE area_code = %s
        """, (area_code,))
        row = cur.fetchone()
        if not row:
            return []
        area_name = row['area_name']

        # Extract primary city/state from OEWS area name
        # Format: "City-City-City, State" or "City, State"
        metro_parts = area_name.split(',')
        metro_state = metro_parts[-1].strip().split('-')[0].strip() if len(metro_parts) > 1 else None
        metro_cities = metro_parts[0].split('-') if metro_parts else []

        # Build evidence query
        # This joins observed_jobs with companies and locations
        cur.execute("""
            WITH posting_evidence AS (
                SELECT
                    oj.company_id,
                    COUNT(*) as posting_count
                FROM observed_jobs oj
                JOIN locations l ON oj.location_id = l.id
                WHERE oj.canonical_role_id = %s
                  AND (l.state = %s OR l.city = ANY(%s))
                  AND oj.source_type LIKE 'ats_%%'
                GROUP BY oj.company_id
            ),
            payroll_evidence AS (
                SELECT
                    oj.company_id,
                    COUNT(*) as payroll_count
                FROM observed_jobs oj
                JOIN locations l ON oj.location_id = l.id
                WHERE oj.canonical_role_id = %s
                  AND (l.state = %s OR l.city = ANY(%s))
                  AND oj.source_type IN ('state_payroll', 'payroll')
                GROUP BY oj.company_id
            ),
            h1b_evidence AS (
                SELECT
                    oj.company_id,
                    COUNT(*) as h1b_count
                FROM observed_jobs oj
                JOIN locations l ON oj.location_id = l.id
                WHERE oj.canonical_role_id = %s
                  AND (l.state = %s OR l.city = ANY(%s))
                  AND oj.source_type = 'h1b_visa'
                GROUP BY oj.company_id
            )
            SELECT
                c.id as company_id,
                c.name as company_name,
                COALESCE(pe.posting_count, 0) as posting_count,
                COALESCE(pa.payroll_count, 0) as payroll_count,
                COALESCE(h1.h1b_count, 0) as h1b_count
            FROM companies c
            LEFT JOIN posting_evidence pe ON c.id = pe.company_id
            LEFT JOIN payroll_evidence pa ON c.id = pa.company_id
            LEFT JOIN h1b_evidence h1 ON c.id = h1.company_id
            WHERE COALESCE(pe.posting_count, 0) + COALESCE(pa.payroll_count, 0) + COALESCE(h1.h1b_count, 0) > 0
        """, (
            canonical_role_id, metro_state, metro_cities,
            canonical_role_id, metro_state, metro_cities,
            canonical_role_id, metro_state, metro_cities
        ))

        evidence_list = []
        for row in cur.fetchall():
            # Compute weighted evidence score
            total_evidence = (
                row['posting_count'] * self.POSTING_WEIGHT +
                row['h1b_count'] * self.H1B_WEIGHT +
                row['payroll_count'] * self.PAYROLL_WEIGHT
            )

            if total_evidence >= self.MIN_EVIDENCE_FOR_ALLOCATION:
                evidence_list.append(CompanyEvidence(
                    company_id=row['company_id'],
                    company_name=row['company_name'],
                    posting_count=row['posting_count'],
                    h1b_count=row['h1b_count'],
                    payroll_count=row['payroll_count'],
                    total_evidence=total_evidence
                ))

        cur.close()
        return evidence_list

    def allocate_headcount(
        self,
        oews_total: int,
        evidence_list: List[CompanyEvidence],
        uncertainty_samples: int = 1000
    ) -> List[Tuple[CompanyEvidence, int, int, int]]:
        """
        Allocate OEWS total headcount to companies based on evidence.

        Uses Bayesian approach:
        1. Evidence counts define Dirichlet concentration parameters
        2. Shrinkage toward uniform prior based on evidence strength
        3. Sample from posterior to get uncertainty estimates

        Args:
            oews_total: Total employment from OEWS for this metro × role
            evidence_list: List of company evidence
            uncertainty_samples: Number of Monte Carlo samples

        Returns:
            List of (evidence, headcount_p10, headcount_p50, headcount_p90)
        """
        if not evidence_list:
            return []

        n_companies = len(evidence_list)

        # Compute evidence weights
        total_evidence = sum(e.total_evidence for e in evidence_list)
        for e in evidence_list:
            e.evidence_weight = e.total_evidence / total_evidence if total_evidence > 0 else 1/n_companies

        # Dirichlet concentration parameters
        # alpha = prior_weight * (1/n) + evidence_weight * total_evidence
        # This shrinks toward uniform when evidence is weak
        alphas = np.array([
            self.PRIOR_WEIGHT / n_companies + e.total_evidence * self.CONCENTRATION_ALPHA
            for e in evidence_list
        ])

        # Sample from Dirichlet to get share distributions
        # Each sample is a probability distribution over companies
        share_samples = np.random.dirichlet(alphas, size=uncertainty_samples)

        # Convert shares to headcounts
        headcount_samples = np.round(share_samples * oews_total).astype(int)

        # Ensure each sample sums to exactly oews_total (adjust rounding)
        for i in range(uncertainty_samples):
            diff = oews_total - headcount_samples[i].sum()
            if diff != 0:
                # Add/subtract from company with highest share
                max_idx = np.argmax(share_samples[i])
                headcount_samples[i, max_idx] += diff

        # Compute percentiles for each company
        results = []
        for j, evidence in enumerate(evidence_list):
            company_headcounts = headcount_samples[:, j]
            p10 = int(np.percentile(company_headcounts, 10))
            p50 = int(np.percentile(company_headcounts, 50))
            p90 = int(np.percentile(company_headcounts, 90))

            # Ensure at least 1 if company has evidence
            p10 = max(1, p10)
            p50 = max(1, p50)
            p90 = max(p50, p90)  # P90 >= P50

            results.append((evidence, p10, p50, p90))

        return results

    def run_allocation(
        self,
        year: int = 2024,
        limit_areas: int = None,
        limit_roles: int = None
    ) -> List[HeadcountEstimate]:
        """
        Run headcount allocation for all metro × role combinations.

        Args:
            year: OEWS reference year
            limit_areas: Limit to first N areas (for testing)
            limit_roles: Limit to first N roles (for testing)

        Returns:
            List of HeadcountEstimate objects
        """
        logger.info("Starting headcount allocation...")

        # Get OEWS priors
        oews_priors = self.get_oews_priors(year)

        # Track unique areas and roles for limiting
        areas_seen = set()
        roles_seen = set()

        all_estimates = []
        processed = 0

        for (area_code, role_id), oews_data in oews_priors.items():
            # Apply limits
            if limit_areas and len(areas_seen) >= limit_areas and area_code not in areas_seen:
                continue
            if limit_roles and len(roles_seen) >= limit_roles and role_id not in roles_seen:
                continue

            areas_seen.add(area_code)
            roles_seen.add(role_id)

            oews_total = oews_data['employment']
            if oews_total is None or oews_total <= 0:
                continue

            # Get company evidence
            evidence_list = self.get_company_evidence(area_code, role_id)

            if not evidence_list:
                logger.debug(f"No evidence for {oews_data['area_name']} × {oews_data['role_name']}")
                continue

            # Allocate headcount
            allocations = self.allocate_headcount(oews_total, evidence_list)

            # Create estimates
            for evidence, p10, p50, p90 in allocations:
                estimate = HeadcountEstimate(
                    company_id=evidence.company_id,
                    company_name=evidence.company_name,
                    metro_id=None,  # Would need to look up
                    metro_name=oews_data['area_name'],
                    canonical_role_id=role_id,
                    role_name=oews_data['role_name'],
                    headcount_p10=p10,
                    headcount_p50=p50,
                    headcount_p90=p90,
                    evidence_score=evidence.total_evidence,
                    share_of_metro=evidence.evidence_weight,
                    method='dirichlet_shrinkage',
                    oews_total=oews_total,
                    companies_in_metro=len(evidence_list)
                )
                all_estimates.append(estimate)

            processed += 1
            if processed % 100 == 0:
                logger.info(f"Processed {processed} metro × role combinations")

        logger.info(f"Generated {len(all_estimates)} headcount estimates")
        return all_estimates

    def save_to_archetypes(self, estimates: List[HeadcountEstimate]):
        """
        Save headcount estimates to job_archetypes table.
        """
        self.connect()
        cur = self.conn.cursor()

        inserted = 0
        updated = 0

        for est in estimates:
            try:
                # Get or create metro_id
                cur.execute("""
                    SELECT id FROM metro_areas WHERE name ILIKE %s LIMIT 1
                """, (f"%{est.metro_name.split(',')[0]}%",))
                row = cur.fetchone()
                metro_id = row[0] if row else None

                if not metro_id:
                    # Create metro area
                    cur.execute("""
                        INSERT INTO metro_areas (name) VALUES (%s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (est.metro_name,))
                    row = cur.fetchone()
                    metro_id = row[0] if row else None

                if not metro_id:
                    continue

                # Upsert archetype
                cur.execute("""
                    INSERT INTO job_archetypes (
                        company_id, metro_id, canonical_role_id, seniority,
                        record_type, headcount_p10, headcount_p50, headcount_p90,
                        headcount_method, evidence_summary, updated_at
                    ) VALUES (
                        %s, %s, %s, 'mid',
                        'inferred', %s, %s, %s,
                        %s, %s, NOW()
                    )
                    ON CONFLICT (company_id, metro_id, canonical_role_id, seniority, record_type)
                    DO UPDATE SET
                        headcount_p10 = EXCLUDED.headcount_p10,
                        headcount_p50 = EXCLUDED.headcount_p50,
                        headcount_p90 = EXCLUDED.headcount_p90,
                        headcount_method = EXCLUDED.headcount_method,
                        evidence_summary = EXCLUDED.evidence_summary,
                        updated_at = NOW()
                """, (
                    est.company_id,
                    metro_id,
                    est.canonical_role_id,
                    est.headcount_p10,
                    est.headcount_p50,
                    est.headcount_p90,
                    est.method,
                    {
                        'oews_total': est.oews_total,
                        'evidence_score': est.evidence_score,
                        'share_of_metro': est.share_of_metro,
                        'companies_in_metro': est.companies_in_metro
                    }
                ))

                if cur.rowcount > 0:
                    inserted += 1
                else:
                    updated += 1

            except Exception as e:
                logger.warning(f"Error saving archetype: {e}")
                continue

        self.conn.commit()
        cur.close()

        logger.info(f"Saved {inserted} new archetypes, updated {updated}")
        return inserted, updated


def main():
    parser = argparse.ArgumentParser(
        description="Allocate headcount to companies using OEWS constraints"
    )
    parser.add_argument(
        '--year',
        type=int,
        default=2024,
        help='OEWS reference year'
    )
    parser.add_argument(
        '--limit-areas',
        type=int,
        default=None,
        help='Limit to first N metro areas (for testing)'
    )
    parser.add_argument(
        '--limit-roles',
        type=int,
        default=None,
        help='Limit to first N roles (for testing)'
    )
    parser.add_argument(
        '--save',
        action='store_true',
        help='Save results to job_archetypes table'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    allocator = HeadcountAllocator()

    try:
        # Run allocation
        estimates = allocator.run_allocation(
            year=args.year,
            limit_areas=args.limit_areas,
            limit_roles=args.limit_roles
        )

        # Print summary
        print("\n" + "=" * 70)
        print("HEADCOUNT ALLOCATION SUMMARY")
        print("=" * 70)
        print(f"Total estimates generated: {len(estimates):,}")

        if estimates:
            # Group by metro
            metros = {}
            for est in estimates:
                if est.metro_name not in metros:
                    metros[est.metro_name] = []
                metros[est.metro_name].append(est)

            print(f"Metros covered: {len(metros)}")

            # Show top metros by estimate count
            print("\nTop metros by company coverage:")
            sorted_metros = sorted(metros.items(), key=lambda x: len(x[1]), reverse=True)
            for metro_name, metro_ests in sorted_metros[:10]:
                total_headcount = sum(e.headcount_p50 for e in metro_ests)
                print(f"  {metro_name}: {len(metro_ests)} companies, {total_headcount:,} estimated jobs")

            # Show sample estimates
            print("\nSample estimates (top by evidence score):")
            top_estimates = sorted(estimates, key=lambda x: x.evidence_score, reverse=True)[:10]
            for est in top_estimates:
                print(f"  {est.company_name} | {est.role_name} | {est.metro_name}")
                print(f"    Headcount: {est.headcount_p10}-{est.headcount_p50}-{est.headcount_p90}")
                print(f"    Evidence: {est.evidence_score:.1f}, Share: {est.share_of_metro:.1%}")

        # Save if requested
        if args.save and estimates:
            print("\nSaving to database...")
            inserted, updated = allocator.save_to_archetypes(estimates)
            print(f"Saved {inserted} new, updated {updated} existing archetypes")

    finally:
        allocator.close()


if __name__ == "__main__":
    main()
