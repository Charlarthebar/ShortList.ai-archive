#!/usr/bin/env python3
"""
Analyze Unmatched H-1B Job Titles
==================================

After ingesting H-1B data, this script analyzes which job titles didn't match
any canonical roles and shows the most common unmatched patterns.

This helps identify:
1. Which new roles to add (high-frequency unmatched titles)
2. Which regex patterns to improve
3. Coverage gaps in our canonical role taxonomy

Usage:
    python analyze_unmatched.py

Author: ShortList.ai
"""

import os
import sys
from collections import Counter
from typing import Dict, List, Tuple
import re

from sources.h1b_visa import H1BVisaConnector
from title_normalizer import TitleNormalizer
from database import DatabaseManager, Config

def analyze_unmatched_titles(limit: int = None):
    """
    Analyze which H-1B titles didn't match canonical roles.

    Args:
        limit: Optional limit on records to analyze (None = all)
    """
    # Initialize
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    normalizer = TitleNormalizer(db)
    connector = H1BVisaConnector()

    print("="*70)
    print("UNMATCHED H-1B TITLE ANALYSIS")
    print("="*70)
    print(f"\nCanonical roles in database: {len(normalizer.role_id_cache)}")

    # Fetch H-1B data
    print(f"\nFetching H-1B data (limit={limit or 'all'})...")
    df = connector.fetch_year(year=2024, limit=limit)
    records = connector.to_standard_format(df)
    print(f"Total certified records: {len(records)}")

    # Analyze matches
    matched = []
    unmatched = []
    unmatched_details = []  # (title, would_match_with_role)

    for record in records:
        title = record['raw_title']
        result = normalizer.parse_title(title)

        if result.canonical_role_id is None:
            unmatched.append(title)
            # Try to suggest what role it should match
            suggestion = suggest_role(title.lower())
            unmatched_details.append((title, suggestion))
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
        pct = count / len(matched) * 100
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

    for role, count in sorted(role_suggestions.items(), key=lambda x: x[1], reverse=True)[:10]:
        pct = count / total_unmatched_count * 100
        print(f"  {count:6,} jobs ({pct:5.1f}% of unmatched) → {role}")

    print("\n2. PATTERN IMPROVEMENTS NEEDED:")
    print("-" * 70)
    print("  - Add more variants for existing roles (e.g., 'Sr.', 'III', 'Lead')")
    print("  - Handle special formatting (job codes, level indicators)")
    print("  - Add industry-specific titles (e.g., 'Oracle Developer' → Software Engineer)")

    db.close_all_connections()


def suggest_role(title_lower: str) -> str:
    """Suggest what role a title should match to."""

    # Engineering variants
    if any(x in title_lower for x in ['software', 'programmer', 'developer', 'sde', 'coding']):
        if 'test' in title_lower or 'qa' in title_lower:
            return "QA Engineer"
        return "Software Engineer"

    if 'systems' in title_lower and 'engineer' in title_lower:
        return "Systems Engineer"

    if 'network' in title_lower:
        return "Network Engineer"

    if 'hardware' in title_lower or 'electrical' in title_lower:
        return "Hardware Engineer"

    if 'mechanical' in title_lower:
        return "Mechanical Engineer"

    if 'civil' in title_lower:
        return "Civil Engineer"

    # Data roles
    if 'data scientist' in title_lower or 'machine learning' in title_lower:
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
    if 'research' in title_lower:
        return "Research Scientist"

    if 'economist' in title_lower:
        return "Economist"

    # Executive
    if any(x in title_lower for x in ['ceo', 'cto', 'cfo', 'cmo', 'chief']):
        return "Executive"

    return None


def categorize_unmatched(titles: List[str]) -> Dict[str, List[str]]:
    """Categorize unmatched titles by type."""
    categories = {
        'Engineering': [],
        'Data & Analytics': [],
        'Management': [],
        'Healthcare': [],
        'Business & Finance': [],
        'Consulting': [],
        'Research': [],
        'Other': []
    }

    for title in titles:
        title_lower = title.lower()

        if any(x in title_lower for x in ['engineer', 'developer', 'programmer', 'software', 'hardware']):
            categories['Engineering'].append(title)
        elif any(x in title_lower for x in ['data', 'analyst', 'analytics', 'scientist']):
            categories['Data & Analytics'].append(title)
        elif any(x in title_lower for x in ['manager', 'director', 'lead', 'coordinator']):
            categories['Management'].append(title)
        elif any(x in title_lower for x in ['physician', 'doctor', 'nurse', 'medical', 'clinical']):
            categories['Healthcare'].append(title)
        elif any(x in title_lower for x in ['accountant', 'financial', 'finance', 'economist']):
            categories['Business & Finance'].append(title)
        elif 'consultant' in title_lower:
            categories['Consulting'].append(title)
        elif 'research' in title_lower:
            categories['Research'].append(title)
        else:
            categories['Other'].append(title)

    return categories


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyze unmatched H-1B job titles")
    parser.add_argument('--limit', type=int, help='Limit analysis to N records')

    args = parser.parse_args()

    # Set DB_USER if not set
    if 'DB_USER' not in os.environ:
        os.environ['DB_USER'] = 'noahhopkins'

    analyze_unmatched_titles(limit=args.limit)
