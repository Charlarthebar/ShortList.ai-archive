#!/usr/bin/env python3
"""
Deduplication Module for MA Employment Data
=============================================

This module handles deduplication across multiple data sources to ensure
we're counting unique individuals, not duplicate records.

Strategy:
1. Within-source: Each source has unique IDs (NPI number, employee ID, etc.)
2. Cross-source: Match by normalized name + city to find overlaps
3. When duplicates found: Keep record with most information (salary > no salary)

Key insight: A state hospital doctor may appear in BOTH:
- MA State Payroll (with salary)
- NPI Registry (with license info)
These are the SAME person, so we merge/dedupe them.

Author: ShortList.ai
"""

import re
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """
    Normalize a name for matching purposes.

    - Lowercase
    - Remove middle initials
    - Remove suffixes (Jr, Sr, III, MD, RN, etc.)
    - Remove extra whitespace
    - Handle "Last, First" vs "First Last" formats
    """
    if pd.isna(name) or not name:
        return ""

    name = str(name).lower().strip()

    # Remove common suffixes
    suffixes = [
        r'\s+(jr\.?|sr\.?|ii|iii|iv|v)$',
        r',?\s+(md|do|rn|lpn|np|pa|phd|jd|esq|cpa|pe|dds|dmd|od|dpm|dc|pharmd)\.?$',
        r',?\s+(m\.?d\.?|d\.?o\.?|r\.?n\.?|ph\.?d\.?)$',
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)

    # Handle "Last, First" format
    if ',' in name:
        parts = name.split(',', 1)
        if len(parts) == 2:
            name = f"{parts[1].strip()} {parts[0].strip()}"

    # Remove middle initials (single letters followed by period or space)
    name = re.sub(r'\s+[a-z]\.?\s+', ' ', name)
    name = re.sub(r'\s+[a-z]\.?$', '', name)

    # Remove extra whitespace
    name = ' '.join(name.split())

    return name


def normalize_city(city: str) -> str:
    """Normalize city name for matching."""
    if pd.isna(city) or not city:
        return ""

    city = str(city).lower().strip()

    # Common abbreviations
    city = city.replace('st.', 'saint')
    city = city.replace('mt.', 'mount')

    return city


def extract_job_category(title: str) -> str:
    """
    Extract broad job category for matching.

    This helps match "Registered Nurse" with "Staff Nurse" etc.
    """
    if pd.isna(title) or not title:
        return "other"

    title = str(title).lower()

    # Healthcare
    if any(x in title for x in ['nurse', 'rn', 'lpn', 'nursing']):
        return 'nurse'
    if any(x in title for x in ['physician', 'doctor', 'md', 'do', 'surgeon']):
        return 'physician'
    if any(x in title for x in ['therapist', 'therapy']):
        return 'therapist'
    if any(x in title for x in ['pharmacist', 'pharmacy']):
        return 'pharmacist'
    if any(x in title for x in ['dentist', 'dental']):
        return 'dentist'
    if any(x in title for x in ['psycholog']):
        return 'psychologist'

    # Education
    if any(x in title for x in ['teacher', 'professor', 'instructor', 'educator']):
        return 'teacher'
    if 'principal' in title:
        return 'principal'

    # Public Safety
    if any(x in title for x in ['police', 'officer', 'detective', 'sergeant']):
        return 'police'
    if any(x in title for x in ['fire', 'firefighter', 'emt', 'paramedic']):
        return 'fire'

    # Legal
    if any(x in title for x in ['attorney', 'lawyer', 'counsel']):
        return 'attorney'

    # Administrative
    if any(x in title for x in ['clerk', 'secretary', 'admin', 'assistant']):
        return 'admin'

    return 'other'


def create_dedup_key(row: pd.Series, name_col: str = 'employee_name',
                     city_col: str = 'city') -> str:
    """
    Create a deduplication key from a record.

    Key format: normalized_name|normalized_city
    """
    name = normalize_name(row.get(name_col, ''))
    city = normalize_city(row.get(city_col, ''))

    if not name:
        return None

    return f"{name}|{city}"


def score_record(row: pd.Series) -> float:
    """
    Score a record by information completeness.

    Higher score = more valuable record to keep.
    """
    score = 0.0

    # Has salary data (most valuable)
    salary_cols = ['total_pay', 'salary', 'raw_salary_min', 'compensation']
    for col in salary_cols:
        if col in row and pd.notna(row[col]) and row[col] > 0:
            score += 10.0
            break

    # Has job title
    title_cols = ['job_title', 'raw_title', 'position_title']
    for col in title_cols:
        if col in row and pd.notna(row[col]) and len(str(row[col])) > 2:
            score += 3.0
            break

    # Has employer/department
    emp_cols = ['department', 'employer_name', 'organization_name', 'raw_company']
    for col in emp_cols:
        if col in row and pd.notna(row[col]) and len(str(row[col])) > 2:
            score += 2.0
            break

    # Has unique identifier (NPI, license number)
    id_cols = ['npi', 'npi_number', 'license_number', 'bbo_number']
    for col in id_cols:
        if col in row and pd.notna(row[col]):
            score += 1.0
            break

    # Source reliability bonus
    source = str(row.get('source', row.get('source_name', ''))).lower()
    if 'payroll' in source:
        score += 2.0  # Payroll has verified salary
    elif 'npi' in source:
        score += 1.0  # NPI has verified license

    return score


def deduplicate_dataframe(df: pd.DataFrame,
                          name_col: str = 'employee_name',
                          city_col: str = 'city',
                          source_col: str = 'source') -> Tuple[pd.DataFrame, Dict]:
    """
    Deduplicate a DataFrame of job records.

    Args:
        df: DataFrame with job records
        name_col: Column containing employee name
        city_col: Column containing city
        source_col: Column containing source name

    Returns:
        Tuple of (deduplicated DataFrame, stats dictionary)
    """
    logger.info(f"Starting deduplication of {len(df)} records...")

    original_count = len(df)

    # Add dedup key and score columns
    df = df.copy()
    df['_dedup_key'] = df.apply(
        lambda row: create_dedup_key(row, name_col, city_col),
        axis=1
    )
    df['_score'] = df.apply(score_record, axis=1)

    # Records without valid dedup key (no name) - keep all
    no_key = df[df['_dedup_key'].isna()].copy()
    has_key = df[df['_dedup_key'].notna()].copy()

    logger.info(f"  Records with dedup key: {len(has_key)}")
    logger.info(f"  Records without name (keeping all): {len(no_key)}")

    # Group by dedup key and keep highest-scored record
    # But also track what sources were merged
    deduped_records = []
    merge_stats = defaultdict(int)

    for key, group in has_key.groupby('_dedup_key'):
        if len(group) == 1:
            # No duplicate, keep as-is
            best = group.iloc[0].to_dict()
            best['_merged_sources'] = [best.get(source_col, 'unknown')]
            deduped_records.append(best)
        else:
            # Multiple records with same key - keep best, track sources
            group_sorted = group.sort_values('_score', ascending=False)
            best = group_sorted.iloc[0].to_dict()

            # Track all sources that were merged
            sources = group[source_col].unique().tolist() if source_col in group.columns else ['unknown']
            best['_merged_sources'] = sources
            best['_duplicate_count'] = len(group)

            # Track merge statistics
            source_combo = ' + '.join(sorted(set(str(s) for s in sources)))
            merge_stats[source_combo] += 1

            deduped_records.append(best)

    # Combine deduped records with no-key records
    deduped_df = pd.DataFrame(deduped_records)

    if len(no_key) > 0:
        no_key['_merged_sources'] = no_key[source_col].apply(lambda x: [x]) if source_col in no_key.columns else [['unknown']]
        deduped_df = pd.concat([deduped_df, no_key], ignore_index=True)

    # Clean up temp columns
    for col in ['_dedup_key', '_score']:
        if col in deduped_df.columns:
            deduped_df = deduped_df.drop(columns=[col])

    # Calculate stats
    duplicates_removed = original_count - len(deduped_df)

    stats = {
        'original_count': original_count,
        'deduped_count': len(deduped_df),
        'duplicates_removed': duplicates_removed,
        'dedup_rate': round(duplicates_removed / original_count * 100, 2) if original_count > 0 else 0,
        'merge_stats': dict(merge_stats),
    }

    logger.info(f"  Deduplication complete: {original_count} -> {len(deduped_df)} ({duplicates_removed} duplicates removed)")

    return deduped_df, stats


def deduplicate_sources(sources: Dict[str, pd.DataFrame],
                        name_cols: Dict[str, str] = None,
                        city_cols: Dict[str, str] = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Deduplicate across multiple data sources.

    Args:
        sources: Dictionary of source_name -> DataFrame
        name_cols: Dictionary of source_name -> name column name
        city_cols: Dictionary of source_name -> city column name

    Returns:
        Tuple of (combined deduplicated DataFrame, stats dictionary)
    """
    # Default column mappings
    default_name_cols = {
        'ma_state_payroll': 'employee_name',
        'boston_payroll': 'employee_name',
        'cambridge_payroll': 'employee_name',
        'npi_healthcare': 'provider_name',
        'h1b_visa': 'employee_name',
        'perm_visa': 'employee_name',
    }

    default_city_cols = {
        'ma_state_payroll': 'city',
        'boston_payroll': 'city',
        'cambridge_payroll': 'city',
        'npi_healthcare': 'city',
        'h1b_visa': 'city',
        'perm_visa': 'city',
    }

    name_cols = name_cols or default_name_cols
    city_cols = city_cols or default_city_cols

    # Add source column to each DataFrame and standardize columns
    all_dfs = []
    source_stats = {}

    for source_name, df in sources.items():
        df = df.copy()
        df['source'] = source_name

        # Standardize name column
        name_col = name_cols.get(source_name, 'employee_name')
        if name_col in df.columns and name_col != 'employee_name':
            df['employee_name'] = df[name_col]

        # Standardize city column
        city_col = city_cols.get(source_name, 'city')
        if city_col in df.columns and city_col != 'city':
            df['city'] = df[city_col]

        source_stats[source_name] = len(df)
        all_dfs.append(df)

    # Combine all sources
    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined {len(sources)} sources: {len(combined)} total records")

    # Deduplicate
    deduped, stats = deduplicate_dataframe(combined, 'employee_name', 'city', 'source')

    stats['source_counts'] = source_stats

    return deduped, stats


def analyze_duplicates(df: pd.DataFrame,
                       name_col: str = 'employee_name',
                       city_col: str = 'city',
                       source_col: str = 'source') -> pd.DataFrame:
    """
    Analyze potential duplicates without removing them.

    Returns a DataFrame showing duplicate groups.
    """
    df = df.copy()
    df['_dedup_key'] = df.apply(
        lambda row: create_dedup_key(row, name_col, city_col),
        axis=1
    )

    # Find keys that appear more than once
    key_counts = df['_dedup_key'].value_counts()
    duplicate_keys = key_counts[key_counts > 1].index.tolist()

    # Get all records with duplicate keys
    duplicates = df[df['_dedup_key'].isin(duplicate_keys)].copy()
    duplicates = duplicates.sort_values(['_dedup_key', source_col])

    return duplicates


# ============================================================================
# MAIN DEMO
# ============================================================================

def demo():
    """Demo the deduplication module."""
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("DEDUPLICATION MODULE DEMO")
    print("=" * 70)

    # Create sample data with intentional duplicates
    sample_data = [
        # Same person in two sources
        {'employee_name': 'John Smith', 'city': 'Boston', 'job_title': 'Registered Nurse',
         'total_pay': 85000, 'source': 'ma_state_payroll'},
        {'employee_name': 'SMITH, JOHN A', 'city': 'Boston', 'job_title': 'RN',
         'npi': '1234567890', 'source': 'npi_healthcare'},

        # Different people with same name
        {'employee_name': 'Mary Johnson', 'city': 'Boston', 'job_title': 'Teacher',
         'total_pay': 72000, 'source': 'boston_payroll'},
        {'employee_name': 'Mary Johnson', 'city': 'Cambridge', 'job_title': 'Teacher',
         'total_pay': 75000, 'source': 'cambridge_payroll'},

        # Unique person
        {'employee_name': 'Robert Lee', 'city': 'Worcester', 'job_title': 'Police Officer',
         'total_pay': 92000, 'source': 'ma_state_payroll'},
    ]

    df = pd.DataFrame(sample_data)

    print(f"\nOriginal records: {len(df)}")
    print(df[['employee_name', 'city', 'source']].to_string())

    # Test name normalization
    print("\n" + "-" * 70)
    print("NAME NORMALIZATION TESTS:")
    print("-" * 70)

    test_names = [
        "John Smith",
        "SMITH, JOHN A",
        "Smith, John A.",
        "Dr. John Smith MD",
        "John A. Smith Jr.",
    ]

    for name in test_names:
        print(f"  '{name}' -> '{normalize_name(name)}'")

    # Run deduplication
    print("\n" + "-" * 70)
    print("DEDUPLICATION RESULTS:")
    print("-" * 70)

    deduped_df, stats = deduplicate_dataframe(df)

    print(f"\nOriginal: {stats['original_count']} records")
    print(f"After dedup: {stats['deduped_count']} records")
    print(f"Duplicates removed: {stats['duplicates_removed']}")
    print(f"Dedup rate: {stats['dedup_rate']}%")

    print("\nMerge statistics (which sources were combined):")
    for combo, count in stats['merge_stats'].items():
        print(f"  {combo}: {count} merges")

    print("\nDeduped records:")
    print(deduped_df[['employee_name', 'city', 'source', '_merged_sources']].to_string())


if __name__ == "__main__":
    demo()
