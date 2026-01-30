#!/usr/bin/env python3
"""
Salary Inference Model with OEWS Priors
=======================================

Infers salary distributions using Bayesian shrinkage toward OEWS wage priors.

The model:
1. Gets OEWS wage percentiles (P10/P25/P50/P75/P90) for metro × role
2. Collects company-specific salary observations (postings, H-1B, payrolls)
3. Applies Bayesian shrinkage: more observations = closer to observed data;
   fewer observations = shrink toward OEWS prior
4. Outputs posterior salary distributions

Key features:
- OEWS-informed priors: starts with macro wage distribution
- Evidence-weighted: H-1B/payroll weighted higher than posting ranges
- Shrinkage: small sample sizes shrink toward prior
- Uncertainty quantification: returns full percentile distribution

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
class OEWSWagePrior:
    """OEWS wage prior for a metro × role."""
    area_code: str
    area_name: str
    canonical_role_id: int
    role_name: str
    wage_p10: float
    wage_p25: float
    wage_p50: float
    wage_p75: float
    wage_p90: float
    wage_mean: float


@dataclass
class SalaryObservation:
    """A single salary observation."""
    source_type: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    salary_point: Optional[float]
    weight: float


@dataclass
class SalaryEstimate:
    """Salary estimate for a company × metro × role."""
    company_id: int
    company_name: str
    metro_name: str
    canonical_role_id: int
    role_name: str

    # Posterior estimates
    salary_p10: float
    salary_p25: float
    salary_p50: float
    salary_p75: float
    salary_p90: float
    salary_mean: float
    salary_stddev: float

    # Methodology
    method: str
    observation_count: int
    effective_sample_size: float
    shrinkage_factor: float

    # Prior used
    oews_median: float


class SalaryInferenceModel:
    """
    Infers salary distributions using Bayesian shrinkage to OEWS priors.
    """

    # Observation weights by source type
    SOURCE_WEIGHTS = {
        'h1b_visa': 5.0,          # Actual offered salary
        'perm_visa': 5.0,         # Prevailing wage
        'state_payroll': 5.0,     # Actual salary
        'payroll': 5.0,           # Actual salary
        'cba_pay_table': 4.0,     # Negotiated wages
        'ats_greenhouse': 2.0,    # Posted range
        'ats_lever': 2.0,
        'ats_smartrecruiters': 2.0,
        'ats_workday': 2.0,
        'posting': 1.5,           # Posted range (generic)
        'default': 1.0
    }

    # Shrinkage parameters
    PRIOR_EFFECTIVE_N = 10.0    # Effective sample size of prior
    MIN_OBSERVATIONS = 1        # Minimum observations to generate estimate

    # Assumed coefficients of variation for priors
    PRIOR_CV = 0.25             # 25% CV for wage distributions

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

    def get_oews_wage_priors(self, year: int = 2024) -> Dict[Tuple[str, int], OEWSWagePrior]:
        """
        Get OEWS wage priors by area × canonical_role.

        Returns:
            Dict mapping (area_code, canonical_role_id) to OEWSWagePrior
        """
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                oe.area_code,
                oa.area_name,
                orm.canonical_role_id,
                cr.name as role_name,
                AVG(oe.wage_annual_p10) as wage_p10,
                AVG(oe.wage_annual_p25) as wage_p25,
                AVG(oe.wage_annual_median) as wage_p50,
                AVG(oe.wage_annual_p75) as wage_p75,
                AVG(oe.wage_annual_p90) as wage_p90,
                AVG(oe.wage_annual_mean) as wage_mean
            FROM oews_estimates oe
            JOIN oews_areas oa ON oe.area_code = oa.area_code
            JOIN oews_role_mapping orm ON oe.occ_code = orm.occ_code
            JOIN canonical_roles cr ON orm.canonical_role_id = cr.id
            WHERE oe.year = %s
              AND oe.industry_code = '000000'
              AND oe.wage_annual_median IS NOT NULL
              AND orm.is_primary = TRUE
              AND oa.areatype_code = 'M'  -- Metro areas only
            GROUP BY oe.area_code, oa.area_name, orm.canonical_role_id, cr.name
        """, (year,))

        results = {}
        for row in cur.fetchall():
            key = (row['area_code'], row['canonical_role_id'])
            results[key] = OEWSWagePrior(
                area_code=row['area_code'],
                area_name=row['area_name'],
                canonical_role_id=row['canonical_role_id'],
                role_name=row['role_name'],
                wage_p10=float(row['wage_p10'] or 0),
                wage_p25=float(row['wage_p25'] or 0),
                wage_p50=float(row['wage_p50'] or 0),
                wage_p75=float(row['wage_p75'] or 0),
                wage_p90=float(row['wage_p90'] or 0),
                wage_mean=float(row['wage_mean'] or 0)
            )

        cur.close()
        logger.info(f"Loaded {len(results)} OEWS wage priors")
        return results

    def get_company_salary_observations(
        self,
        area_code: str,
        canonical_role_id: int,
        company_id: int
    ) -> List[SalaryObservation]:
        """
        Get salary observations for a specific company × metro × role.
        """
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Get metro area info
        cur.execute("SELECT area_name FROM oews_areas WHERE area_code = %s", (area_code,))
        row = cur.fetchone()
        if not row:
            return []
        area_name = row['area_name']

        # Parse metro for location matching
        metro_parts = area_name.split(',')
        metro_state = metro_parts[-1].strip().split('-')[0].strip() if len(metro_parts) > 1 else None
        metro_cities = metro_parts[0].split('-') if metro_parts else []

        cur.execute("""
            SELECT
                oj.source_type,
                oj.salary_min,
                oj.salary_max,
                oj.salary_point
            FROM observed_jobs oj
            JOIN locations l ON oj.location_id = l.id
            WHERE oj.company_id = %s
              AND oj.canonical_role_id = %s
              AND (l.state = %s OR l.city = ANY(%s))
              AND (oj.salary_min IS NOT NULL OR oj.salary_max IS NOT NULL OR oj.salary_point IS NOT NULL)
        """, (company_id, canonical_role_id, metro_state, metro_cities))

        observations = []
        for row in cur.fetchall():
            weight = self.SOURCE_WEIGHTS.get(row['source_type'], self.SOURCE_WEIGHTS['default'])
            observations.append(SalaryObservation(
                source_type=row['source_type'],
                salary_min=float(row['salary_min']) if row['salary_min'] else None,
                salary_max=float(row['salary_max']) if row['salary_max'] else None,
                salary_point=float(row['salary_point']) if row['salary_point'] else None,
                weight=weight
            ))

        cur.close()
        return observations

    def estimate_posterior(
        self,
        prior: OEWSWagePrior,
        observations: List[SalaryObservation]
    ) -> Tuple[Dict[str, float], float, float]:
        """
        Compute posterior salary distribution using Bayesian shrinkage.

        Uses conjugate normal-normal model:
        - Prior: N(mu_prior, sigma_prior^2 / n_prior)
        - Likelihood: N(mu_obs, sigma_obs^2 / n_obs)
        - Posterior: N(mu_post, sigma_post^2)

        Args:
            prior: OEWS wage prior
            observations: Company salary observations

        Returns:
            Tuple of (posterior_dict, effective_n, shrinkage_factor)
        """
        # Prior parameters
        mu_prior = prior.wage_p50
        sigma_prior = mu_prior * self.PRIOR_CV  # Assume 25% CV
        n_prior = self.PRIOR_EFFECTIVE_N

        # Observation parameters
        if not observations:
            # No observations - return prior
            return {
                'p10': prior.wage_p10,
                'p25': prior.wage_p25,
                'p50': prior.wage_p50,
                'p75': prior.wage_p75,
                'p90': prior.wage_p90,
                'mean': prior.wage_mean,
                'stddev': sigma_prior
            }, 0, 1.0

        # Compute weighted mean and effective sample size from observations
        total_weight = 0
        weighted_sum = 0
        obs_values = []

        for obs in observations:
            # Get point estimate from observation
            if obs.salary_point:
                value = obs.salary_point
            elif obs.salary_min and obs.salary_max:
                value = (obs.salary_min + obs.salary_max) / 2
            elif obs.salary_min:
                value = obs.salary_min * 1.1  # Assume min is 10% below median
            elif obs.salary_max:
                value = obs.salary_max * 0.9  # Assume max is 10% above median
            else:
                continue

            # Filter unrealistic values
            if value < 20000 or value > 1000000:
                continue

            weighted_sum += value * obs.weight
            total_weight += obs.weight
            obs_values.append(value)

        if total_weight == 0:
            # No valid observations
            return {
                'p10': prior.wage_p10,
                'p25': prior.wage_p25,
                'p50': prior.wage_p50,
                'p75': prior.wage_p75,
                'p90': prior.wage_p90,
                'mean': prior.wage_mean,
                'stddev': sigma_prior
            }, 0, 1.0

        mu_obs = weighted_sum / total_weight
        n_obs = total_weight  # Effective sample size

        # Compute observation variance
        if len(obs_values) > 1:
            sigma_obs = np.std(obs_values)
        else:
            sigma_obs = sigma_prior  # Use prior variance if single observation

        # Bayesian update (precision weighting)
        precision_prior = n_prior / (sigma_prior ** 2)
        precision_obs = n_obs / (sigma_obs ** 2 + 1e-6)

        precision_post = precision_prior + precision_obs
        mu_post = (precision_prior * mu_prior + precision_obs * mu_obs) / precision_post
        sigma_post = np.sqrt(1 / precision_post)

        # Shrinkage factor (how much we moved toward observations)
        shrinkage = precision_obs / precision_post

        # Generate posterior percentiles
        # Assume lognormal for salaries (can't be negative)
        # Convert to lognormal parameters
        log_mu = np.log(mu_post) - 0.5 * (sigma_post / mu_post) ** 2
        log_sigma = np.sqrt(np.log(1 + (sigma_post / mu_post) ** 2))

        posterior = {
            'p10': float(stats.lognorm.ppf(0.10, s=log_sigma, scale=np.exp(log_mu))),
            'p25': float(stats.lognorm.ppf(0.25, s=log_sigma, scale=np.exp(log_mu))),
            'p50': float(stats.lognorm.ppf(0.50, s=log_sigma, scale=np.exp(log_mu))),
            'p75': float(stats.lognorm.ppf(0.75, s=log_sigma, scale=np.exp(log_mu))),
            'p90': float(stats.lognorm.ppf(0.90, s=log_sigma, scale=np.exp(log_mu))),
            'mean': float(mu_post),
            'stddev': float(sigma_post)
        }

        return posterior, n_obs, shrinkage

    def get_companies_with_evidence(
        self,
        area_code: str,
        canonical_role_id: int
    ) -> List[Tuple[int, str]]:
        """Get companies with salary evidence for a metro × role."""
        self.connect()
        cur = self.conn.cursor(cursor_factory=RealDictCursor)

        # Get metro area info
        cur.execute("SELECT area_name FROM oews_areas WHERE area_code = %s", (area_code,))
        row = cur.fetchone()
        if not row:
            return []
        area_name = row['area_name']

        metro_parts = area_name.split(',')
        metro_state = metro_parts[-1].strip().split('-')[0].strip() if len(metro_parts) > 1 else None
        metro_cities = metro_parts[0].split('-') if metro_parts else []

        cur.execute("""
            SELECT DISTINCT
                c.id as company_id,
                c.name as company_name
            FROM observed_jobs oj
            JOIN companies c ON oj.company_id = c.id
            JOIN locations l ON oj.location_id = l.id
            WHERE oj.canonical_role_id = %s
              AND (l.state = %s OR l.city = ANY(%s))
              AND (oj.salary_min IS NOT NULL OR oj.salary_max IS NOT NULL OR oj.salary_point IS NOT NULL)
        """, (canonical_role_id, metro_state, metro_cities))

        results = [(row['company_id'], row['company_name']) for row in cur.fetchall()]
        cur.close()
        return results

    def run_inference(
        self,
        year: int = 2024,
        limit_areas: int = None,
        limit_roles: int = None
    ) -> List[SalaryEstimate]:
        """
        Run salary inference for all metro × role × company combinations.
        """
        logger.info("Starting salary inference...")

        # Get OEWS priors
        oews_priors = self.get_oews_wage_priors(year)

        areas_seen = set()
        roles_seen = set()
        all_estimates = []
        processed = 0

        for (area_code, role_id), prior in oews_priors.items():
            # Apply limits
            if limit_areas and len(areas_seen) >= limit_areas and area_code not in areas_seen:
                continue
            if limit_roles and len(roles_seen) >= limit_roles and role_id not in roles_seen:
                continue

            areas_seen.add(area_code)
            roles_seen.add(role_id)

            # Get companies with salary evidence
            companies = self.get_companies_with_evidence(area_code, role_id)

            for company_id, company_name in companies:
                # Get observations
                observations = self.get_company_salary_observations(
                    area_code, role_id, company_id
                )

                if len(observations) < self.MIN_OBSERVATIONS:
                    continue

                # Compute posterior
                posterior, eff_n, shrinkage = self.estimate_posterior(prior, observations)

                estimate = SalaryEstimate(
                    company_id=company_id,
                    company_name=company_name,
                    metro_name=prior.area_name,
                    canonical_role_id=role_id,
                    role_name=prior.role_name,
                    salary_p10=posterior['p10'],
                    salary_p25=posterior['p25'],
                    salary_p50=posterior['p50'],
                    salary_p75=posterior['p75'],
                    salary_p90=posterior['p90'],
                    salary_mean=posterior['mean'],
                    salary_stddev=posterior['stddev'],
                    method='bayesian_shrinkage',
                    observation_count=len(observations),
                    effective_sample_size=eff_n,
                    shrinkage_factor=shrinkage,
                    oews_median=prior.wage_p50
                )
                all_estimates.append(estimate)

            processed += 1
            if processed % 100 == 0:
                logger.info(f"Processed {processed} metro × role combinations")

        logger.info(f"Generated {len(all_estimates)} salary estimates")
        return all_estimates

    def save_to_archetypes(self, estimates: List[SalaryEstimate]):
        """
        Update job_archetypes table with salary estimates.
        """
        self.connect()
        cur = self.conn.cursor()

        updated = 0

        for est in estimates:
            try:
                # Find matching archetype
                cur.execute("""
                    SELECT ja.id FROM job_archetypes ja
                    JOIN metro_areas m ON ja.metro_id = m.id
                    WHERE ja.company_id = %s
                      AND ja.canonical_role_id = %s
                      AND m.name ILIKE %s
                    LIMIT 1
                """, (
                    est.company_id,
                    est.canonical_role_id,
                    f"%{est.metro_name.split(',')[0]}%"
                ))
                row = cur.fetchone()

                if row:
                    # Update existing archetype
                    cur.execute("""
                        UPDATE job_archetypes SET
                            salary_p25 = %s,
                            salary_p50 = %s,
                            salary_p75 = %s,
                            salary_mean = %s,
                            salary_stddev = %s,
                            salary_method = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        est.salary_p25,
                        est.salary_p50,
                        est.salary_p75,
                        est.salary_mean,
                        est.salary_stddev,
                        est.method,
                        row[0]
                    ))
                    updated += 1

            except Exception as e:
                logger.warning(f"Error updating archetype: {e}")
                continue

        self.conn.commit()
        cur.close()

        logger.info(f"Updated {updated} archetypes with salary data")
        return updated


def main():
    parser = argparse.ArgumentParser(
        description="Infer salaries using OEWS priors and Bayesian shrinkage"
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

    model = SalaryInferenceModel()

    try:
        # Run inference
        estimates = model.run_inference(
            year=args.year,
            limit_areas=args.limit_areas,
            limit_roles=args.limit_roles
        )

        # Print summary
        print("\n" + "=" * 70)
        print("SALARY INFERENCE SUMMARY")
        print("=" * 70)
        print(f"Total estimates generated: {len(estimates):,}")

        if estimates:
            # Aggregate statistics
            salaries = [e.salary_p50 for e in estimates]
            print(f"\nOverall salary distribution (P50 estimates):")
            print(f"  Min: ${min(salaries):,.0f}")
            print(f"  P25: ${np.percentile(salaries, 25):,.0f}")
            print(f"  Median: ${np.median(salaries):,.0f}")
            print(f"  P75: ${np.percentile(salaries, 75):,.0f}")
            print(f"  Max: ${max(salaries):,.0f}")

            # Shrinkage statistics
            shrinkages = [e.shrinkage_factor for e in estimates]
            print(f"\nShrinkage factor distribution:")
            print(f"  Mean: {np.mean(shrinkages):.2f}")
            print(f"  Min: {min(shrinkages):.2f} (most toward prior)")
            print(f"  Max: {max(shrinkages):.2f} (most toward observations)")

            # Show sample estimates
            print("\nSample estimates (highest observation counts):")
            top_estimates = sorted(estimates, key=lambda x: x.observation_count, reverse=True)[:10]
            for est in top_estimates:
                diff_pct = (est.salary_p50 - est.oews_median) / est.oews_median * 100
                direction = "+" if diff_pct > 0 else ""
                print(f"  {est.company_name} | {est.role_name}")
                print(f"    ${est.salary_p25:,.0f} - ${est.salary_p50:,.0f} - ${est.salary_p75:,.0f}")
                print(f"    vs OEWS ${est.oews_median:,.0f} ({direction}{diff_pct:.1f}%)")
                print(f"    Obs: {est.observation_count}, Shrinkage: {est.shrinkage_factor:.2f}")

        # Save if requested
        if args.save and estimates:
            print("\nSaving to database...")
            updated = model.save_to_archetypes(estimates)
            print(f"Updated {updated} archetypes")

    finally:
        model.close()


if __name__ == "__main__":
    main()
