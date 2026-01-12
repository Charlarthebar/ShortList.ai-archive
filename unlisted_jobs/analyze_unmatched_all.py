#!/usr/bin/env python3
"""
Analyze Unmatched Job Titles Across All Sources
================================================

Analyzes which job titles didn't match any canonical roles across H-1B, PERM, and MA Payroll.

Usage:
    python analyze_unmatched_all.py --source h1b_visa --limit 10000
    python analyze_unmatched_all.py --source perm_visa --limit 10000
    python analyze_unmatched_all.py --source ma_state_payroll --limit 10000
    python analyze_unmatched_all.py --all  # Analyze all sources

Author: ShortList.ai
"""

import os
import sys
from collections import Counter
from typing import Dict, List, Tuple
import re

from sources.h1b_visa import H1BVisaConnector
from sources.perm_visa import PERMVisaConnector
from sources.ma_state_payroll import MAStatePayrollConnector
from title_normalizer import TitleNormalizer
from database import DatabaseManager, Config


def analyze_source(source_name: str, limit: int = None):
    """
    Analyze unmatched titles for a specific source.

    Args:
        source_name: 'h1b_visa', 'perm_visa', or 'ma_state_payroll'
        limit: Optional limit on records to analyze
    """
    # Initialize
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    normalizer = TitleNormalizer(db)

    print("="*70)
    print(f"UNMATCHED TITLE ANALYSIS: {source_name.upper()}")
    print("="*70)
    print(f"\nCanonical roles in database: {len(normalizer.role_id_cache)}")

    # Fetch data based on source
    print(f"\nFetching {source_name} data (limit={limit or 'all'})...")

    if source_name == 'h1b_visa':
        connector = H1BVisaConnector()
        df = connector.fetch_year(year=2024, limit=limit)
        records = connector.to_standard_format(df)
    elif source_name == 'perm_visa':
        connector = PERMVisaConnector()
        df = connector.fetch_year(year=2024, limit=limit)
        records = connector.to_standard_format(df)
    elif source_name == 'ma_state_payroll':
        connector = MAStatePayrollConnector()
        # For MA payroll, we need to load from CSV or use sample data
        try:
            df = connector.load_from_csv('data/ma_payroll_2024.csv', limit=limit)
        except:
            df = connector._load_sample_data(limit=limit or 1000)
        records = connector.to_standard_format(df)
    else:
        print(f"ERROR: Unknown source: {source_name}")
        print("Valid sources: h1b_visa, perm_visa, ma_state_payroll")
        db.close_all_connections()
        return

    print(f"Total records: {len(records)}")

    # Analyze matches
    matched = []
    unmatched = []

    for record in records:
        title = record['raw_title']
        result = normalizer.parse_title(title)

        if result.canonical_role_id is None:
            unmatched.append(title)
        else:
            matched.append((title, result.canonical_role_name))

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print('='*70)
    print(f"Matched:   {len(matched):,} ({len(matched)/len(records)*100:.1f}%)")
    print(f"Unmatched: {len(unmatched):,} ({len(unmatched)/len(records)*100:.1f}%)")

    # Show matched role distribution
    print(f"\n{'='*70}")
    print("TOP 20 MATCHED ROLES")
    print('='*70)
    role_counts = Counter([role_name for _, role_name in matched])
    for role_name, count in role_counts.most_common(20):
        pct = count / len(matched) * 100 if matched else 0
        print(f"{count:6,}  ({pct:5.1f}%)  {role_name}")

    # Show most common unmatched titles
    print(f"\n{'='*70}")
    print("TOP 50 MOST COMMON UNMATCHED TITLES")
    print('='*70)
    print(f"{'Count':<8} {'Title':<50} {'Suggested Role'}")
    print('-'*70)

    title_counts = Counter(unmatched)
    for title, count in title_counts.most_common(50):
        # Find suggestion for this title
        suggestion = suggest_role(title.lower())
        title_display = title[:47] + "..." if len(title) > 50 else title
        suggestion_display = suggestion[:30] if suggestion else "NEW ROLE NEEDED"
        print(f"{count:<8} {title_display:<50} {suggestion_display}")

    # Categorize unmatched by pattern
    print(f"\n{'='*70}")
    print("UNMATCHED PATTERNS BY CATEGORY")
    print('='*70)

    categories = categorize_unmatched(unmatched)
    for category, titles in sorted(categories.items(), key=lambda x: len(x[1]), reverse=True):
        if titles:
            print(f"\n{category} ({len(titles)} titles):")
            # Show top 5 examples
            examples = Counter(titles).most_common(5)
            for title, count in examples:
                print(f"  {count:4,}× {title[:60]}")

    # Recommendations
    print(f"\n{'='*70}")
    print("RECOMMENDATIONS")
    print('='*70)

    # Get top unmatched by volume
    top_unmatched = title_counts.most_common(20)
    total_unmatched_count = sum(title_counts.values())

    print("\n1. HIGH-PRIORITY ROLES TO ADD (would capture most jobs):")
    print("-" * 70)

    role_suggestions = {}
    for title, count in top_unmatched:
        suggested = suggest_role(title.lower())
        if suggested and suggested not in normalizer.role_id_cache:
            if suggested not in role_suggestions:
                role_suggestions[suggested] = 0
            role_suggestions[suggested] += count

    for role, count in sorted(role_suggestions.items(), key=lambda x: x[1], reverse=True)[:15]:
        pct = count / total_unmatched_count * 100 if total_unmatched_count > 0 else 0
        print(f"  {count:6,} jobs ({pct:5.1f}% of unmatched) → {role}")

    db.close_all_connections()


def suggest_role(title_lower: str) -> str:
    """Suggest what role a title should match to."""

    # Engineering variants
    if any(x in title_lower for x in ['software', 'programmer', 'developer', 'sde', 'coding']):
        if 'test' in title_lower or 'qa' in title_lower:
            return "QA Engineer"
        if 'devops' in title_lower or 'dev ops' in title_lower:
            return "DevOps Engineer"
        return "Software Engineer"

    if 'systems' in title_lower and 'engineer' in title_lower:
        return "Systems Engineer"

    if 'network' in title_lower and 'engineer' in title_lower:
        return "Network Engineer"

    if 'security' in title_lower and ('engineer' in title_lower or 'analyst' in title_lower):
        return "Security Engineer"

    if 'hardware' in title_lower or 'electrical' in title_lower:
        return "Hardware Engineer"

    if 'mechanical' in title_lower:
        return "Mechanical Engineer"

    if 'civil' in title_lower:
        return "Civil Engineer"

    if 'industrial' in title_lower and 'engineer' in title_lower:
        return "Industrial Engineer"

    # Data roles
    if 'data scientist' in title_lower or 'machine learning' in title_lower or 'ml engineer' in title_lower:
        return "Data Scientist"

    if 'data engineer' in title_lower:
        return "Data Engineer"

    if 'data analyst' in title_lower or 'business intelligence' in title_lower:
        return "Data Analyst"

    # Management
    if 'project manager' in title_lower or 'project lead' in title_lower:
        return "Project Manager"

    if 'product manager' in title_lower:
        return "Product Manager"

    if 'program manager' in title_lower:
        return "Program Manager"

    # Analysis
    if 'systems analyst' in title_lower or 'system analyst' in title_lower:
        return "Systems Analyst"

    if 'business analyst' in title_lower:
        return "Business Analyst"

    if 'financial analyst' in title_lower:
        return "Financial Analyst"

    # Healthcare
    if any(x in title_lower for x in ['physician', 'doctor', 'cardiologist', 'psychiatrist']):
        return "Physician"

    if 'nurse' in title_lower:
        return "Registered Nurse"

    if 'pharmacist' in title_lower:
        return "Pharmacist"

    if 'physical therapist' in title_lower or 'pt ' in title_lower:
        return "Physical Therapist"

    if 'occupational therapist' in title_lower:
        return "Occupational Therapist"

    if 'medical technologist' in title_lower or 'med tech' in title_lower:
        return "Medical Technologist"

    # Education
    if 'teacher' in title_lower or 'educator' in title_lower:
        return "Teacher"

    if 'professor' in title_lower or 'instructor' in title_lower:
        return "Professor"

    # Government specific
    if 'police' in title_lower or 'officer' in title_lower or 'law enforcement' in title_lower:
        return "Police Officer"

    if 'firefighter' in title_lower or 'fire fighter' in title_lower:
        return "Firefighter"

    if 'social worker' in title_lower:
        return "Social Worker"

    if 'librarian' in title_lower:
        return "Librarian"

    if 'paralegal' in title_lower or 'legal assistant' in title_lower:
        return "Paralegal"

    # Accounting
    if 'accountant' in title_lower or 'auditor' in title_lower:
        return "Accountant"

    # Architecture
    if 'architect' in title_lower:
        if 'solution' in title_lower or 'cloud' in title_lower or 'enterprise' in title_lower:
            return "Solutions Architect"
        return "Architect"

    # Consultant
    if 'consultant' in title_lower:
        return "Consultant"

    # Technical Lead / Specialist
    if 'technical lead' in title_lower or 'tech lead' in title_lower:
        return "Technical Lead"

    if 'specialist' in title_lower:
        return "Technical Specialist"

    # Research
    if 'research' in title_lower and 'scientist' in title_lower:
        return "Research Scientist"

    if 'postdoc' in title_lower or 'post doc' in title_lower or 'postdoctoral' in title_lower:
        return "Postdoctoral Researcher"

    if 'economist' in title_lower:
        return "Economist"

    # Executive
    if any(x in title_lower for x in ['ceo', 'cto', 'cfo', 'cmo', 'chief']):
        return "Executive"

    # Designer
    if 'designer' in title_lower:
        if 'ux' in title_lower or 'ui' in title_lower or 'user experience' in title_lower:
            return "UX Designer"
        return "Designer"

    # Writer/Content
    if 'writer' in title_lower or 'content' in title_lower:
        return "Writer"

    return None


def categorize_unmatched(titles: List[str]) -> Dict[str, List[str]]:
    """Categorize unmatched titles by type."""
    categories = {
        'Engineering': [],
        'Data & Analytics': [],
        'Management': [],
        'Healthcare': [],
        'Education': [],
        'Government/Public Service': [],
        'Business & Finance': [],
        'Consulting': [],
        'Research': [],
        'Other': []
    }

    for title in titles:
        title_lower = title.lower()

        if any(x in title_lower for x in ['engineer', 'developer', 'programmer', 'software', 'hardware', 'devops']):
            categories['Engineering'].append(title)
        elif any(x in title_lower for x in ['data', 'analyst', 'analytics', 'scientist']):
            categories['Data & Analytics'].append(title)
        elif any(x in title_lower for x in ['manager', 'director', 'lead', 'coordinator', 'supervisor']):
            categories['Management'].append(title)
        elif any(x in title_lower for x in ['physician', 'doctor', 'nurse', 'medical', 'clinical', 'therapist']):
            categories['Healthcare'].append(title)
        elif any(x in title_lower for x in ['teacher', 'professor', 'instructor', 'educator']):
            categories['Education'].append(title)
        elif any(x in title_lower for x in ['police', 'firefighter', 'social worker', 'officer', 'librarian']):
            categories['Government/Public Service'].append(title)
        elif any(x in title_lower for x in ['accountant', 'financial', 'finance', 'economist', 'auditor']):
            categories['Business & Finance'].append(title)
        elif 'consultant' in title_lower:
            categories['Consulting'].append(title)
        elif 'research' in title_lower or 'postdoc' in title_lower:
            categories['Research'].append(title)
        else:
            categories['Other'].append(title)

    return categories


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyze unmatched job titles")
    parser.add_argument('--source', type=str, choices=['h1b_visa', 'perm_visa', 'ma_state_payroll'],
                       help='Data source to analyze')
    parser.add_argument('--all', action='store_true', help='Analyze all sources')
    parser.add_argument('--limit', type=int, help='Limit analysis to N records')

    args = parser.parse_args()

    # Set DB_USER if not set
    if 'DB_USER' not in os.environ:
        os.environ['DB_USER'] = 'noahhopkins'

    if args.all:
        # Analyze all sources
        for source in ['h1b_visa', 'perm_visa', 'ma_state_payroll']:
            print("\n\n")
            analyze_source(source, limit=args.limit)
    elif args.source:
        analyze_source(args.source, limit=args.limit)
    else:
        print("ERROR: Must specify --source or --all")
        print("Usage: python analyze_unmatched_all.py --source h1b_visa --limit 10000")
        print("   or: python analyze_unmatched_all.py --all")
        sys.exit(1)
