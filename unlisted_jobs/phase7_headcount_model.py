#!/usr/bin/env python3
"""
Phase 7: Headcount Distribution Model
======================================

Infers employee counts across all archetypes (observed and unobserved)
by learning patterns from observed job density.

Approach:
1. Analyze observed job counts by company × role
2. Learn patterns: Company Size × Industry → Role Distribution
3. Apply patterns to infer missing archetypes
4. Generate complete workforce estimates

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

print("="*70)
print("PHASE 7: HEADCOUNT DISTRIBUTION MODEL")
print("="*70)
print()

# ============================================================================
# STEP 1: EXTRACT OBSERVED JOB COUNTS
# ============================================================================

print("Step 1: Extracting observed job counts...")
print("-" * 70)

config = Config()
db = DatabaseManager(config)
db.initialize_pool()
conn = db.get_connection()

# Get all observed jobs with company, role, and metadata
query = """
SELECT
    c.id as company_id,
    c.name as company_name,
    c.size_category,
    c.industry,
    c.is_public,
    cr.id as role_id,
    cr.name as canonical_role,
    cr.role_family,
    cr.category as role_category,
    COUNT(*) as job_count,
    AVG(COALESCE(oj.salary_point, (oj.salary_min + oj.salary_max) / 2)) as avg_salary
FROM observed_jobs oj
JOIN companies c ON oj.company_id = c.id
JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
WHERE oj.canonical_role_id IS NOT NULL
GROUP BY c.id, c.name, c.size_category, c.industry, c.is_public,
         cr.id, cr.name, cr.role_family, cr.category
ORDER BY c.name, job_count DESC
"""

print("Executing query...")
df_jobs = pd.read_sql(query, conn)

# Also get total company stats
company_query = """
SELECT
    c.id as company_id,
    c.name as company_name,
    c.size_category,
    c.industry,
    c.is_public,
    COUNT(DISTINCT oj.canonical_role_id) as unique_roles,
    COUNT(*) as total_jobs
FROM companies c
JOIN observed_jobs oj ON c.id = oj.company_id
WHERE oj.canonical_role_id IS NOT NULL
GROUP BY c.id, c.name, c.size_category, c.industry, c.is_public
"""

df_companies = pd.read_sql(company_query, conn)

db.release_connection(conn)
db.close_all_connections()

print(f"✓ Extracted {len(df_jobs):,} company × role combinations")
print(f"✓ Covering {len(df_companies):,} companies")
print(f"✓ Across {df_jobs['canonical_role'].nunique()} roles")
print()

# ============================================================================
# STEP 2: ANALYZE JOB DENSITY PATTERNS
# ============================================================================

print("Step 2: Analyzing job density patterns...")
print("-" * 70)

print("\n📊 Overall Statistics:")
print(f"  Companies with observations: {len(df_companies):,}")
print(f"  Unique roles observed: {df_jobs['canonical_role'].nunique()}")
print(f"  Total job observations: {df_jobs['job_count'].sum():,}")
print(f"  Avg jobs per company: {df_companies['total_jobs'].mean():.1f}")
print(f"  Avg roles per company: {df_companies['unique_roles'].mean():.1f}")

print("\n🏢 Top 10 Companies by Job Count:")
top_companies = df_companies.nlargest(10, 'total_jobs')
for i, row in enumerate(top_companies.itertuples(), 1):
    print(f"  {i:2d}. {row.company_name:50s} {row.total_jobs:>6,} jobs, {row.unique_roles:>3} roles")

print("\n🎯 Top 10 Most Common Roles:")
role_totals = df_jobs.groupby('canonical_role')['job_count'].sum().sort_values(ascending=False)
for i, (role, count) in enumerate(role_totals.head(10).items(), 1):
    companies_with_role = len(df_jobs[df_jobs['canonical_role'] == role])
    print(f"  {i:2d}. {role:40s} {count:>7,} jobs at {companies_with_role:>5,} companies")

print("\n📈 Role Distribution Patterns:")
# Analyze how many roles each company typically has
role_dist = df_companies['unique_roles'].describe()
print(f"  Min roles per company:     {role_dist['min']:>6.0f}")
print(f"  25th percentile:           {role_dist['25%']:>6.0f}")
print(f"  Median:                    {role_dist['50%']:>6.0f}")
print(f"  75th percentile:           {role_dist['75%']:>6.0f}")
print(f"  Max roles per company:     {role_dist['max']:>6.0f}")

print()

# ============================================================================
# STEP 3: COMPUTE ROLE DENSITY METRICS
# ============================================================================

print("Step 3: Computing role density metrics...")
print("-" * 70)

# For each company, compute what % of their workforce is in each role
df_jobs_with_pct = df_jobs.merge(
    df_companies[['company_id', 'total_jobs']],
    on='company_id',
    how='left'
)
df_jobs_with_pct['role_percentage'] = (df_jobs_with_pct['job_count'] / df_jobs_with_pct['total_jobs']) * 100

print("\n🔍 Role Concentration Analysis:")
print("  (What % of a company's workforce is typically in each role?)")

# Get median percentage for each role across all companies
role_percentages = df_jobs_with_pct.groupby('canonical_role').agg({
    'role_percentage': ['median', 'mean', 'std', 'count']
}).round(2)
role_percentages.columns = ['median_pct', 'mean_pct', 'std_pct', 'companies']
role_percentages = role_percentages.sort_values('median_pct', ascending=False)

print("\nTop 10 Roles by Median Workforce %:")
for i, (role, row) in enumerate(role_percentages.head(10).iterrows(), 1):
    print(f"  {i:2d}. {role:40s} {row['median_pct']:>6.2f}% (±{row['std_pct']:>6.2f}%), n={int(row['companies']):>5,}")

print()

# ============================================================================
# STEP 4: INDUSTRY & SIZE PATTERNS
# ============================================================================

print("Step 4: Analyzing industry and size patterns...")
print("-" * 70)

# Clean up industry data
df_companies['industry_clean'] = df_companies['industry'].fillna('Unknown')
df_jobs_with_pct['industry_clean'] = df_jobs_with_pct['industry'].fillna('Unknown')

print("\n🏭 Industries Represented:")
industry_counts = df_companies['industry_clean'].value_counts()
print(f"  Total industries: {len(industry_counts)}")
for i, (industry, count) in enumerate(industry_counts.head(10).items(), 1):
    pct = count / len(df_companies) * 100
    print(f"  {i:2d}. {industry:40s} {count:>6,} companies ({pct:>5.1f}%)")

# Analyze role patterns by industry
print("\n🎯 Role Distribution by Industry:")
print("  (Top role in each major industry)")

top_industries = industry_counts.head(5).index
for industry in top_industries:
    industry_jobs = df_jobs_with_pct[df_jobs_with_pct['industry_clean'] == industry]
    if len(industry_jobs) > 0:
        top_role = industry_jobs.groupby('canonical_role')['job_count'].sum().idxmax()
        top_count = industry_jobs.groupby('canonical_role')['job_count'].sum().max()
        total = industry_jobs['job_count'].sum()
        pct = (top_count / total) * 100
        print(f"  {industry:40s} → {top_role:30s} ({pct:>5.1f}%)")

print()

# ============================================================================
# STEP 5: BUILD ROLE DISTRIBUTION TEMPLATES
# ============================================================================

print("Step 5: Building role distribution templates...")
print("-" * 70)

# For each industry, compute the "typical" role distribution
print("\nComputing industry-specific role distributions...")

industry_templates = {}
for industry in df_jobs_with_pct['industry_clean'].unique():
    industry_data = df_jobs_with_pct[df_jobs_with_pct['industry_clean'] == industry]

    if len(industry_data) < 10:  # Skip industries with too few data points
        continue

    # Compute median percentage for each role in this industry
    role_dist = industry_data.groupby('canonical_role')['role_percentage'].median().to_dict()

    # Normalize to sum to 100%
    total_pct = sum(role_dist.values())
    if total_pct > 0:
        role_dist = {k: (v / total_pct) * 100 for k, v in role_dist.items()}

    industry_templates[industry] = {
        'role_distribution': role_dist,
        'sample_size': len(industry_data),
        'companies': industry_data['company_id'].nunique()
    }

print(f"✓ Built {len(industry_templates)} industry templates")

# Build a default template for unknown industries
all_role_pcts = df_jobs_with_pct.groupby('canonical_role')['role_percentage'].median().to_dict()
total_pct = sum(all_role_pcts.values())
if total_pct > 0:
    all_role_pcts = {k: (v / total_pct) * 100 for k, v in all_role_pcts.items()}

default_template = {
    'role_distribution': all_role_pcts,
    'sample_size': len(df_jobs_with_pct),
    'companies': df_companies['company_id'].nunique()
}

print(f"✓ Built default template with {len(all_role_pcts)} roles")

print()

# ============================================================================
# STEP 6: INFER MISSING ARCHETYPES
# ============================================================================

print("Step 6: Inferring headcount for unobserved archetypes...")
print("-" * 70)

print("\nStrategy:")
print("  1. For each company, identify observed roles")
print("  2. Use industry template to infer missing roles")
print("  3. Scale by company's total observed jobs")
print("  4. Generate headcount estimates")

# Create complete company × role matrix
all_roles = sorted(df_jobs['canonical_role'].unique())
all_companies = df_companies['company_id'].unique()

print(f"\nMatrix dimensions:")
print(f"  Companies: {len(all_companies):,}")
print(f"  Roles: {len(all_roles)}")
print(f"  Total cells: {len(all_companies) * len(all_roles):,}")

# Count how many are observed vs need inference
total_possible = len(all_companies) * len(all_roles)
observed_cells = len(df_jobs)
missing_cells = total_possible - observed_cells

print(f"\nCoverage:")
print(f"  Observed: {observed_cells:>8,} ({observed_cells/total_possible*100:>5.1f}%)")
print(f"  Missing:  {missing_cells:>8,} ({missing_cells/total_possible*100:>5.1f}%)")

print("\nInferring headcount for missing cells...")

# Build inference results
inference_results = []

for company_id in all_companies:
    company_info = df_companies[df_companies['company_id'] == company_id].iloc[0]
    company_name = company_info['company_name']
    industry = company_info['industry_clean'] if pd.notna(company_info['industry']) else 'Unknown'
    total_observed_jobs = company_info['total_jobs']

    # Get observed roles for this company
    observed_roles = df_jobs[df_jobs['company_id'] == company_id]
    observed_role_names = set(observed_roles['canonical_role'].values)

    # Get appropriate template
    template = industry_templates.get(industry, default_template)
    role_dist = template['role_distribution']

    # For each role that exists in template but not observed
    for role_name in all_roles:
        if role_name in observed_role_names:
            # Use observed count
            observed_count = observed_roles[observed_roles['canonical_role'] == role_name]['job_count'].values[0]
            inference_results.append({
                'company_id': company_id,
                'company_name': company_name,
                'canonical_role': role_name,
                'headcount': observed_count,
                'is_observed': True,
                'confidence': 'high'
            })
        else:
            # Infer from template
            if role_name in role_dist:
                # Use template percentage × total observed jobs
                expected_pct = role_dist[role_name] / 100
                inferred_count = int(total_observed_jobs * expected_pct)

                # Only include if count > 0
                if inferred_count > 0:
                    inference_results.append({
                        'company_id': company_id,
                        'company_name': company_name,
                        'canonical_role': role_name,
                        'headcount': inferred_count,
                        'is_observed': False,
                        'confidence': 'medium' if template['sample_size'] > 50 else 'low'
                    })

df_inference = pd.DataFrame(inference_results)

print(f"\n✓ Generated {len(df_inference):,} headcount estimates")
print(f"  Observed: {df_inference['is_observed'].sum():>8,}")
print(f"  Inferred: {(~df_inference['is_observed']).sum():>8,}")

print()

# ============================================================================
# STEP 7: ANALYZE RESULTS
# ============================================================================

print("Step 7: Analyzing inference results...")
print("-" * 70)

# Total workforce estimates
total_observed_headcount = df_inference[df_inference['is_observed']]['headcount'].sum()
total_inferred_headcount = df_inference[~df_inference['is_observed']]['headcount'].sum()
total_headcount = df_inference['headcount'].sum()

print(f"\n📊 Total Workforce Estimates:")
print(f"  Observed headcount:   {total_observed_headcount:>12,}")
print(f"  Inferred headcount:   {total_inferred_headcount:>12,}")
print(f"  Total headcount:      {total_headcount:>12,}")
print(f"  Inferred %:           {total_inferred_headcount/total_headcount*100:>11.1f}%")

# By role
print(f"\n🎯 Top 10 Roles by Total Headcount:")
role_headcounts = df_inference.groupby('canonical_role').agg({
    'headcount': 'sum',
    'is_observed': lambda x: x.sum()
}).rename(columns={'is_observed': 'observed_companies'})
role_headcounts = role_headcounts.sort_values('headcount', ascending=False)

for i, (role, row) in enumerate(role_headcounts.head(10).iterrows(), 1):
    pct = row['headcount'] / total_headcount * 100
    print(f"  {i:2d}. {role:40s} {int(row['headcount']):>8,} ({pct:>5.1f}%), obs={int(row['observed_companies']):>5,}")

# By company
print(f"\n🏢 Top 10 Companies by Total Estimated Workforce:")
company_headcounts = df_inference.groupby('company_name').agg({
    'headcount': 'sum',
    'is_observed': lambda x: x.sum()
}).rename(columns={'is_observed': 'observed_roles'})
company_headcounts = company_headcounts.sort_values('headcount', ascending=False)

for i, (company, row) in enumerate(company_headcounts.head(10).iterrows(), 1):
    print(f"  {i:2d}. {company:50s} {int(row['headcount']):>8,}, {int(row['observed_roles']):>3} roles obs")

print()

# ============================================================================
# STEP 8: SAVE RESULTS
# ============================================================================

print("Step 8: Saving results...")
print("-" * 70)

# Save inference results
df_inference.to_csv('headcount_estimates.csv', index=False)
print("✓ Saved headcount_estimates.csv")

# Save templates
joblib.dump(industry_templates, 'industry_role_templates.pkl')
print("✓ Saved industry_role_templates.pkl")

joblib.dump(default_template, 'default_role_template.pkl')
print("✓ Saved default_role_template.pkl")

# Save summary stats
summary_stats = {
    'total_companies': len(all_companies),
    'total_roles': len(all_roles),
    'observed_cells': observed_cells,
    'inferred_cells': len(df_inference) - observed_cells,
    'total_observed_headcount': int(total_observed_headcount),
    'total_inferred_headcount': int(total_inferred_headcount),
    'total_headcount': int(total_headcount),
    'role_headcounts': role_headcounts.to_dict('index'),
    'company_headcounts': company_headcounts.head(100).to_dict('index')
}

joblib.dump(summary_stats, 'headcount_summary.pkl')
print("✓ Saved headcount_summary.pkl")

print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*70)
print("PHASE 7 COMPLETE!")
print("="*70)

print(f"\n📊 Headcount Modeling Results:")
print(f"  Companies analyzed:       {len(all_companies):>8,}")
print(f"  Roles covered:            {len(all_roles):>8}")
print(f"  Observed job counts:      {observed_cells:>8,}")
print(f"  Inferred job counts:      {len(df_inference) - observed_cells:>8,}")
print(f"  Total headcount estimate: {total_headcount:>8,}")

print(f"\n📁 Files Created:")
print(f"  - headcount_estimates.csv ({len(df_inference):,} rows)")
print(f"  - industry_role_templates.pkl ({len(industry_templates)} templates)")
print(f"  - default_role_template.pkl")
print(f"  - headcount_summary.pkl")

print(f"\n🚀 Next Steps:")
print(f"  1. Review headcount_estimates.csv for workforce compositions")
print(f"  2. Use estimates for market sizing and benchmarking")
print(f"  3. Proceed to Phase 8: Archetype Inference")

print("\n" + "="*70)
