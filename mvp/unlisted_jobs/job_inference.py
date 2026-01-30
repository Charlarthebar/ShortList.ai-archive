#!/usr/bin/env python3
"""
Job Inference Engine
=====================

Infers filled jobs by combining:
1. BLS OEWS occupation data (how many of each job type exist)
2. Employer database (which companies employ people)
3. Industry-occupation matrices (what % of healthcare workers are nurses)

Output: Job archetypes with employer, location, salary range, and confidence scores

Author: ShortList.ai
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging
import random

from employer_database import EmployerDatabase, build_employer_database, Employer

logger = logging.getLogger(__name__)


# Industry-Occupation Distribution Matrix
# What percentage of each industry's workforce is in each occupation category
INDUSTRY_OCCUPATION_MIX = {
    'healthcare': {
        'registered_nurses': 0.25,
        'physicians': 0.08,
        'medical_technicians': 0.12,
        'healthcare_support': 0.15,
        'administrative': 0.15,
        'management': 0.05,
        'facilities': 0.05,
        'other': 0.15,
    },
    'higher_education': {
        'faculty': 0.25,
        'research_staff': 0.15,
        'administrative': 0.20,
        'student_services': 0.10,
        'facilities': 0.10,
        'it_staff': 0.08,
        'management': 0.05,
        'other': 0.07,
    },
    'technology': {
        'software_developers': 0.40,
        'data_scientists': 0.08,
        'product_managers': 0.08,
        'designers': 0.05,
        'sales': 0.12,
        'management': 0.08,
        'administrative': 0.08,
        'support': 0.06,
        'other': 0.05,
    },
    'finance': {
        'financial_analysts': 0.15,
        'accountants': 0.12,
        'software_developers': 0.15,
        'customer_service': 0.12,
        'sales': 0.10,
        'management': 0.10,
        'administrative': 0.12,
        'compliance': 0.08,
        'other': 0.06,
    },
    'biotech': {
        'scientists': 0.35,
        'research_associates': 0.15,
        'manufacturing': 0.12,
        'quality_control': 0.08,
        'regulatory': 0.05,
        'software_developers': 0.08,
        'management': 0.07,
        'administrative': 0.05,
        'other': 0.05,
    },
    'consulting': {
        'consultants': 0.50,
        'analysts': 0.20,
        'management': 0.10,
        'administrative': 0.10,
        'it_staff': 0.05,
        'other': 0.05,
    },
    'defense': {
        'engineers': 0.35,
        'scientists': 0.15,
        'technicians': 0.15,
        'manufacturing': 0.10,
        'management': 0.08,
        'administrative': 0.08,
        'security': 0.05,
        'other': 0.04,
    },
    'retail': {
        'sales_associates': 0.45,
        'cashiers': 0.15,
        'stock_handlers': 0.12,
        'management': 0.10,
        'customer_service': 0.08,
        'administrative': 0.05,
        'other': 0.05,
    },
    'government': {
        'police': 0.12,
        'fire': 0.05,
        'teachers': 0.25,
        'administrative': 0.20,
        'social_workers': 0.08,
        'maintenance': 0.10,
        'management': 0.08,
        'other': 0.12,
    },
    'other': {
        'administrative': 0.25,
        'sales': 0.15,
        'customer_service': 0.15,
        'management': 0.10,
        'skilled_trades': 0.15,
        'other': 0.20,
    },
}

# Occupation to SOC code mapping
OCCUPATION_SOC_CODES = {
    'registered_nurses': ('29-1141', 'Registered Nurses'),
    'physicians': ('29-1216', 'Physicians'),
    'software_developers': ('15-1252', 'Software Developers'),
    'data_scientists': ('15-2051', 'Data Scientists'),
    'financial_analysts': ('13-2051', 'Financial Analysts'),
    'accountants': ('13-2011', 'Accountants and Auditors'),
    'management': ('11-1021', 'General and Operations Managers'),
    'administrative': ('43-6014', 'Secretaries and Administrative Assistants'),
    'sales': ('41-0000', 'Sales and Related Occupations'),
    'teachers': ('25-2021', 'Elementary School Teachers'),
    'engineers': ('17-2000', 'Engineers'),
    'scientists': ('19-1042', 'Medical Scientists'),
}


@dataclass
class InferredJob:
    """Represents an inferred job position."""
    employer_name: str
    employer_city: str
    employer_state: str
    occupation: str
    soc_code: Optional[str]
    soc_title: Optional[str]

    # Headcount for this occupation at this employer
    estimated_headcount: int
    headcount_confidence: float

    # Salary range
    salary_min: float
    salary_median: float
    salary_max: float
    salary_confidence: float

    # Location confidence
    location_confidence: float

    # Overall confidence (product of component confidences)
    overall_confidence: float

    # Source of inference
    inference_source: str

    # Industry
    industry: str

    # Is this an archetype (aggregated) or individual estimate
    record_type: str = 'inferred_archetype'


class JobInferenceEngine:
    """
    Engine for inferring filled jobs from available data.
    """

    def __init__(self, employer_db: EmployerDatabase, bls_data: pd.DataFrame):
        self.employer_db = employer_db
        self.bls_data = bls_data
        self.salary_lookup = self._build_salary_lookup()

    def _build_salary_lookup(self) -> Dict[str, Dict]:
        """Build salary lookup from BLS data."""
        lookup = {}

        for _, row in self.bls_data.iterrows():
            soc_code = row.get('soc_code', '')
            if soc_code:
                lookup[soc_code] = {
                    'title': row.get('soc_title', ''),
                    'employment': row.get('employment_count', 0),
                    'mean_wage': row.get('mean_wage', 0),
                    'median_wage': row.get('median_wage', 0),
                    'pct_10': row.get('pct_10_wage', 0),
                    'pct_90': row.get('pct_90_wage', 0),
                }

        return lookup

    def get_salary_range(self, soc_code: str) -> Tuple[float, float, float]:
        """Get salary range (10th, median, 90th percentile) for an occupation."""
        if soc_code in self.salary_lookup:
            data = self.salary_lookup[soc_code]
            return (
                data.get('pct_10', 40000),
                data.get('median_wage', 60000),
                data.get('pct_90', 100000)
            )

        # Default fallback
        return (40000, 60000, 100000)

    def infer_jobs_for_employer(self, employer: Employer) -> List[InferredJob]:
        """
        Infer job positions for a specific employer.

        Uses industry-occupation mix to distribute headcount.
        """
        jobs = []

        industry = employer.industry
        if industry not in INDUSTRY_OCCUPATION_MIX:
            industry = 'other'

        occupation_mix = INDUSTRY_OCCUPATION_MIX[industry]

        for occupation, pct in occupation_mix.items():
            # Calculate estimated headcount for this occupation
            headcount = int(employer.employee_count * pct)

            if headcount < 1:
                continue

            # Get SOC code and salary data
            soc_info = OCCUPATION_SOC_CODES.get(occupation)
            if soc_info:
                soc_code, soc_title = soc_info
            else:
                soc_code = None
                soc_title = occupation.replace('_', ' ').title()

            # Get salary range
            salary_min, salary_median, salary_max = self.get_salary_range(
                soc_code or '00-0000'
            )

            # Calculate confidence scores
            headcount_conf = employer.employee_count_confidence * 0.7  # Industry mix adds uncertainty
            salary_conf = 0.85 if soc_code else 0.60  # BLS data is reliable
            location_conf = 0.90  # We know the employer's location

            overall_conf = headcount_conf * salary_conf * location_conf

            job = InferredJob(
                employer_name=employer.name,
                employer_city=employer.city,
                employer_state=employer.state,
                occupation=occupation,
                soc_code=soc_code,
                soc_title=soc_title,
                estimated_headcount=headcount,
                headcount_confidence=round(headcount_conf, 3),
                salary_min=salary_min,
                salary_median=salary_median,
                salary_max=salary_max,
                salary_confidence=round(salary_conf, 3),
                location_confidence=round(location_conf, 3),
                overall_confidence=round(overall_conf, 3),
                inference_source=f"employer_db:{employer.source}",
                industry=employer.industry,
            )

            jobs.append(job)

        return jobs

    def infer_all_jobs(self) -> List[InferredJob]:
        """
        Infer jobs for all employers in the database.
        """
        all_jobs = []

        for employer in self.employer_db.employers.values():
            jobs = self.infer_jobs_for_employer(employer)
            all_jobs.extend(jobs)

        logger.info(f"Inferred {len(all_jobs)} job archetypes for {len(self.employer_db.employers)} employers")

        return all_jobs

    def to_dataframe(self, jobs: List[InferredJob]) -> pd.DataFrame:
        """Convert inferred jobs to DataFrame."""
        records = []

        for job in jobs:
            records.append({
                'employer_name': job.employer_name,
                'city': job.employer_city,
                'state': job.employer_state,
                'occupation': job.occupation,
                'soc_code': job.soc_code,
                'job_title': job.soc_title,
                'estimated_headcount': job.estimated_headcount,
                'headcount_confidence': job.headcount_confidence,
                'salary_min': job.salary_min,
                'salary_median': job.salary_median,
                'salary_max': job.salary_max,
                'salary_confidence': job.salary_confidence,
                'location_confidence': job.location_confidence,
                'overall_confidence': job.overall_confidence,
                'industry': job.industry,
                'inference_source': job.inference_source,
                'record_type': job.record_type,
            })

        return pd.DataFrame(records)

    def expand_to_individual_jobs(self, jobs: List[InferredJob],
                                  max_per_archetype: int = 100) -> pd.DataFrame:
        """
        Expand job archetypes into individual job records.

        Each archetype represents N positions. This creates N individual records
        with varied salaries within the range.
        """
        individual_records = []

        for job in jobs:
            # Limit expansion to avoid huge datasets
            n_records = min(job.estimated_headcount, max_per_archetype)

            for i in range(n_records):
                # Vary salary within range (normal distribution around median)
                salary_std = (job.salary_max - job.salary_min) / 4
                salary = np.random.normal(job.salary_median, salary_std)
                salary = max(job.salary_min, min(job.salary_max, salary))

                individual_records.append({
                    'employer_name': job.employer_name,
                    'city': job.employer_city,
                    'state': job.employer_state,
                    'job_title': job.soc_title,
                    'soc_code': job.soc_code,
                    'estimated_salary': round(salary, 0),
                    'salary_confidence': job.salary_confidence,
                    'location_confidence': job.location_confidence,
                    'overall_confidence': job.overall_confidence,
                    'industry': job.industry,
                    'record_type': 'inferred_individual',
                    'source': job.inference_source,
                })

        return pd.DataFrame(individual_records)


def run_inference(data_dir: str = None,
                  output_dir: str = './data/ma_jobs') -> Tuple[pd.DataFrame, Dict]:
    """
    Run the full inference pipeline.

    Returns:
        Tuple of (inferred jobs DataFrame, statistics dict)
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("JOB INFERENCE ENGINE")
    print("=" * 70)

    # Build employer database
    print("\n1. Building employer database...")
    employer_db = build_employer_database(data_dir)

    employer_summary = employer_db.summary()
    print(f"   Employers: {employer_summary['total_employers']}")
    print(f"   Total employment: {employer_summary['total_employment']:,}")

    # Load BLS data
    print("\n2. Loading BLS occupation data...")
    bls_path = os.path.join(output_dir, 'bls_oews_boston_msa.csv')

    if os.path.exists(bls_path):
        bls_data = pd.read_csv(bls_path)
        print(f"   Loaded {len(bls_data)} occupation categories")
    else:
        print("   WARNING: BLS data not found, using defaults")
        bls_data = pd.DataFrame()

    # Run inference
    print("\n3. Inferring jobs...")
    engine = JobInferenceEngine(employer_db, bls_data)

    inferred_jobs = engine.infer_all_jobs()
    jobs_df = engine.to_dataframe(inferred_jobs)

    total_inferred = jobs_df['estimated_headcount'].sum()
    print(f"   Job archetypes: {len(jobs_df)}")
    print(f"   Total inferred positions: {total_inferred:,}")

    # Save results
    print("\n4. Saving results...")
    os.makedirs(output_dir, exist_ok=True)

    # Save archetypes
    archetype_path = os.path.join(output_dir, 'inferred_job_archetypes.csv')
    jobs_df.to_csv(archetype_path, index=False)
    print(f"   Saved: {archetype_path}")

    # Save employer database
    employer_path = os.path.join(output_dir, 'employer_database.csv')
    employer_db.to_dataframe().to_csv(employer_path, index=False)
    print(f"   Saved: {employer_path}")

    # Statistics
    stats = {
        'total_employers': employer_summary['total_employers'],
        'total_employer_headcount': employer_summary['total_employment'],
        'job_archetypes': len(jobs_df),
        'total_inferred_jobs': int(total_inferred),
        'avg_confidence': round(jobs_df['overall_confidence'].mean(), 3),
        'by_industry': jobs_df.groupby('industry')['estimated_headcount'].sum().to_dict(),
        'by_city': jobs_df.groupby('city')['estimated_headcount'].sum().to_dict(),
    }

    # Summary
    print("\n" + "=" * 70)
    print("INFERENCE SUMMARY")
    print("=" * 70)

    print(f"\nTotal Inferred Jobs: {stats['total_inferred_jobs']:,}")
    print(f"Average Confidence: {stats['avg_confidence']:.1%}")

    print("\nBy Industry:")
    for industry, count in sorted(stats['by_industry'].items(), key=lambda x: -x[1]):
        print(f"  {industry:<25} {count:>10,}")

    print("\nTop Cities:")
    for city, count in sorted(stats['by_city'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {city:<25} {count:>10,}")

    print("\nSample Inferred Jobs:")
    print("-" * 70)
    sample = jobs_df.sample(min(10, len(jobs_df)))
    for _, row in sample.iterrows():
        print(f"  {row['estimated_headcount']:>5} {row['job_title'][:30]:<30} @ {row['employer_name'][:25]:<25}")
        print(f"        Salary: ${row['salary_min']:,.0f} - ${row['salary_max']:,.0f}  |  Confidence: {row['overall_confidence']:.0%}")

    return jobs_df, stats


def demo():
    """Demo the inference engine."""
    jobs_df, stats = run_inference()

    print("\n" + "=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print(f"\nGenerated {stats['total_inferred_jobs']:,} inferred job positions")
    print(f"Output saved to ./data/ma_jobs/")


if __name__ == "__main__":
    demo()
