#!/usr/bin/env python3
"""
Combine All Data Sources into Comprehensive MA Jobs Database
=============================================================

Combines three tiers of job data:
1. OBSERVED (high confidence 0.85-0.95): Actual payroll records, NPI providers, visa filings
2. KNOWN EMPLOYER INFERRED (medium confidence 0.20-0.45): Jobs at known companies
3. CBP SYNTHETIC (low confidence 0.10-0.20): Statistical estimates for unknown establishments

Handles overlap by:
- Subtracting observed counts from CBP totals by industry
- Subtracting known employer headcounts from CBP totals
- Maintaining clear record_type labeling for confidence tracking

Author: ShortList.ai
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)


def load_observed_data(data_dir: str = "./data/ma_jobs") -> pd.DataFrame:
    """Load observed jobs from various sources."""
    observed_files = [
        "ma_state_payroll.csv",
        "boston_city_payroll.csv",
        "cambridge_city_payroll.csv",
        "federal_employees_ma.csv",
        "npi_healthcare_ma.csv",
        "h1b_ma_2024.csv",
        "perm_ma.csv",
    ]

    all_observed = []

    for filename in observed_files:
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df['source_file'] = filename

            # Ensure required columns exist
            if 'record_type' not in df.columns:
                df['record_type'] = 'observed'
            if 'overall_confidence' not in df.columns:
                df['overall_confidence'] = 0.90

            all_observed.append(df)
            logger.info(f"Loaded {len(df)} records from {filename}")

    if all_observed:
        return pd.concat(all_observed, ignore_index=True)
    return pd.DataFrame()


def load_known_employer_inferred(data_dir: str = "./data/ma_jobs") -> pd.DataFrame:
    """Load inferred jobs from known employer database."""
    filepath = os.path.join(data_dir, "inferred_job_archetypes_expanded.csv")

    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        if 'record_type' not in df.columns:
            df['record_type'] = 'known_employer_inferred'
        logger.info(f"Loaded {len(df)} known employer archetypes")
        return df

    return pd.DataFrame()


def load_cbp_synthetic(data_dir: str = "./data/ma_jobs") -> pd.DataFrame:
    """Load CBP-based synthetic job archetypes."""
    filepath = os.path.join(data_dir, "cbp_synthetic_archetypes.csv")

    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} CBP synthetic archetypes")
        return df

    return pd.DataFrame()


def adjust_cbp_for_overlap(cbp_df: pd.DataFrame,
                            observed_df: pd.DataFrame,
                            known_employer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust CBP synthetic data to avoid double-counting with observed and known employer data.

    Strategy:
    - Calculate observed + known employer headcount by industry
    - Reduce CBP synthetic headcount proportionally
    """
    if cbp_df.empty:
        return cbp_df

    # Calculate known employment by industry
    known_by_industry = {}

    # From observed data
    if not observed_df.empty and 'industry' in observed_df.columns:
        obs_counts = observed_df.groupby('industry').size()
        for ind, count in obs_counts.items():
            known_by_industry[ind] = known_by_industry.get(ind, 0) + count

    # From known employer inferred
    if not known_employer_df.empty and 'industry' in known_employer_df.columns:
        if 'estimated_headcount' in known_employer_df.columns:
            emp_counts = known_employer_df.groupby('industry')['estimated_headcount'].sum()
        else:
            emp_counts = known_employer_df.groupby('industry').size()
        for ind, count in emp_counts.items():
            known_by_industry[ind] = known_by_industry.get(ind, 0) + count

    # Calculate CBP totals by industry
    cbp_by_industry = cbp_df.groupby('industry')['estimated_headcount'].sum()

    # Adjust CBP data
    adjusted_df = cbp_df.copy()

    for idx, row in adjusted_df.iterrows():
        industry = row['industry']
        known_count = known_by_industry.get(industry, 0)
        cbp_total = cbp_by_industry.get(industry, 0)

        if cbp_total > 0:
            # Calculate remaining percentage (what's NOT already accounted for)
            remaining_pct = max(0, 1 - (known_count / cbp_total))

            # Adjust headcount
            adjusted_df.at[idx, 'estimated_headcount'] = int(row['estimated_headcount'] * remaining_pct)

            # Also reduce confidence slightly since we're taking the "remainder"
            adjusted_df.at[idx, 'overall_confidence'] = row['overall_confidence'] * 0.9

    # Remove rows with 0 headcount
    adjusted_df = adjusted_df[adjusted_df['estimated_headcount'] > 0]

    return adjusted_df


def standardize_columns(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    """Standardize column names across different data sources."""
    # Create a copy
    result = df.copy()

    # Standard columns we want
    standard_cols = [
        'employer_name', 'city', 'state', 'job_title', 'soc_code',
        'estimated_salary', 'salary_min', 'salary_median', 'salary_max',
        'estimated_headcount', 'overall_confidence', 'salary_confidence',
        'location_confidence', 'industry', 'record_type', 'source'
    ]

    # Map common column names
    column_map = {
        'name': 'employer_name',
        'employer': 'employer_name',
        'title': 'job_title',
        'occupation_title': 'job_title',
        'occ_title': 'job_title',
        'salary': 'estimated_salary',
        'base_salary': 'estimated_salary',
        'mean_wage': 'estimated_salary',
        'annual_salary': 'estimated_salary',
        'location': 'city',
        'work_city': 'city',
        'confidence': 'overall_confidence',
    }

    result = result.rename(columns=column_map)

    # Add missing columns with defaults
    if 'state' not in result.columns:
        result['state'] = 'MA'
    if 'record_type' not in result.columns:
        result['record_type'] = source_type
    if 'estimated_headcount' not in result.columns:
        result['estimated_headcount'] = 1
    if 'overall_confidence' not in result.columns:
        if source_type == 'observed':
            result['overall_confidence'] = 0.90
        elif source_type == 'known_employer_inferred':
            result['overall_confidence'] = 0.35
        else:
            result['overall_confidence'] = 0.12

    return result


def combine_all_sources(output_dir: str = "./data/ma_jobs") -> Tuple[pd.DataFrame, Dict]:
    """
    Combine all data sources into a comprehensive MA jobs database.

    Returns:
        Tuple of (combined DataFrame, statistics dict)
    """
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("COMPREHENSIVE MA JOBS DATABASE")
    print("Combining All Data Sources")
    print("=" * 70)

    # 1. Load all data sources
    print("\n1. Loading data sources...")

    observed_df = load_observed_data(output_dir)
    print(f"   Observed records: {len(observed_df):,}")

    known_employer_df = load_known_employer_inferred(output_dir)
    print(f"   Known employer archetypes: {len(known_employer_df):,}")
    if not known_employer_df.empty and 'estimated_headcount' in known_employer_df.columns:
        print(f"   Known employer positions: {known_employer_df['estimated_headcount'].sum():,}")

    cbp_df = load_cbp_synthetic(output_dir)
    print(f"   CBP synthetic archetypes: {len(cbp_df):,}")
    if not cbp_df.empty and 'estimated_headcount' in cbp_df.columns:
        print(f"   CBP synthetic positions (raw): {cbp_df['estimated_headcount'].sum():,}")

    # 2. Adjust CBP for overlap
    print("\n2. Adjusting for overlap...")
    cbp_adjusted = adjust_cbp_for_overlap(cbp_df, observed_df, known_employer_df)
    if not cbp_adjusted.empty:
        print(f"   CBP synthetic positions (adjusted): {cbp_adjusted['estimated_headcount'].sum():,}")

    # 3. Standardize columns
    print("\n3. Standardizing columns...")

    if not observed_df.empty:
        observed_df = standardize_columns(observed_df, 'observed')
    if not known_employer_df.empty:
        known_employer_df = standardize_columns(known_employer_df, 'known_employer_inferred')
    if not cbp_adjusted.empty:
        cbp_adjusted = standardize_columns(cbp_adjusted, 'cbp_synthetic')

    # 4. Combine all sources
    print("\n4. Combining all sources...")

    all_dfs = []
    if not observed_df.empty:
        all_dfs.append(observed_df)
    if not known_employer_df.empty:
        all_dfs.append(known_employer_df)
    if not cbp_adjusted.empty:
        all_dfs.append(cbp_adjusted)

    if not all_dfs:
        print("ERROR: No data to combine!")
        return pd.DataFrame(), {}

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"   Combined records: {len(combined):,}")

    # 5. Calculate totals
    if 'estimated_headcount' in combined.columns:
        total_positions = combined['estimated_headcount'].sum()
    else:
        total_positions = len(combined)

    print(f"   Total positions: {total_positions:,}")

    # 6. Save combined data
    print("\n5. Saving combined database...")

    # Save archetypes (for analysis)
    archetype_path = os.path.join(output_dir, "ma_jobs_comprehensive.csv")
    combined.to_csv(archetype_path, index=False)
    print(f"   Saved: {archetype_path}")

    # 7. Statistics
    stats = {
        'observed_records': len(observed_df) if not observed_df.empty else 0,
        'known_employer_archetypes': len(known_employer_df) if not known_employer_df.empty else 0,
        'cbp_archetypes': len(cbp_adjusted) if not cbp_adjusted.empty else 0,
        'total_archetypes': len(combined),
        'total_positions': int(total_positions),
        'cbp_benchmark': 3487228,  # Census CBP MA total
    }

    # By record type
    if 'record_type' in combined.columns and 'estimated_headcount' in combined.columns:
        stats['by_record_type'] = combined.groupby('record_type')['estimated_headcount'].sum().to_dict()

    # By industry
    if 'industry' in combined.columns and 'estimated_headcount' in combined.columns:
        stats['by_industry'] = combined.groupby('industry')['estimated_headcount'].sum().to_dict()

    # Coverage percentage
    stats['coverage_pct'] = round(stats['total_positions'] / stats['cbp_benchmark'] * 100, 1)

    # Summary
    print("\n" + "=" * 70)
    print("COMPREHENSIVE DATABASE SUMMARY")
    print("=" * 70)

    print(f"\nCBP Benchmark (MA private sector): {stats['cbp_benchmark']:,}")
    print(f"Total Positions in Database: {stats['total_positions']:,}")
    print(f"Coverage: {stats['coverage_pct']}%")

    print("\nBy Record Type:")
    if 'by_record_type' in stats:
        for rtype, count in sorted(stats['by_record_type'].items(), key=lambda x: -x[1]):
            pct = count / stats['total_positions'] * 100
            print(f"  {rtype:<30} {count:>12,} ({pct:>5.1f}%)")

    print("\nBy Industry (top 10):")
    if 'by_industry' in stats:
        for industry, count in sorted(stats['by_industry'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {industry:<25} {count:>12,}")

    # Confidence distribution
    if 'overall_confidence' in combined.columns:
        print("\nConfidence Distribution:")
        high_conf = combined[combined['overall_confidence'] >= 0.70]['estimated_headcount'].sum()
        med_conf = combined[(combined['overall_confidence'] >= 0.20) & (combined['overall_confidence'] < 0.70)]['estimated_headcount'].sum()
        low_conf = combined[combined['overall_confidence'] < 0.20]['estimated_headcount'].sum()
        print(f"  High (>=0.70):   {high_conf:>12,} ({high_conf/total_positions*100:>5.1f}%)")
        print(f"  Medium (0.20-0.70): {med_conf:>12,} ({med_conf/total_positions*100:>5.1f}%)")
        print(f"  Low (<0.20):     {low_conf:>12,} ({low_conf/total_positions*100:>5.1f}%)")

    return combined, stats


def create_summary_csv(combined: pd.DataFrame, output_dir: str = "./data/ma_jobs"):
    """Create a summary CSV with aggregated statistics."""
    if combined.empty:
        return

    # Aggregate by industry and record type
    summary = combined.groupby(['industry', 'record_type']).agg({
        'estimated_headcount': 'sum',
        'overall_confidence': 'mean',
    }).reset_index()

    summary_path = os.path.join(output_dir, "ma_jobs_summary.csv")
    summary.to_csv(summary_path, index=False)
    print(f"\nSaved summary: {summary_path}")


if __name__ == "__main__":
    combined, stats = combine_all_sources()

    if not combined.empty:
        create_summary_csv(combined)

        print("\n" + "=" * 70)
        print("DATABASE COMPLETE")
        print("=" * 70)
        print(f"\nTotal positions covered: {stats['total_positions']:,}")
        print(f"Coverage of MA private sector: {stats['coverage_pct']}%")
