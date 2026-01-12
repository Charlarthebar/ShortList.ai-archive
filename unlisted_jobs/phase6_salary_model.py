#!/usr/bin/env python3
"""
Phase 6: Salary Estimation Model with XGBoost
==============================================

Builds a salary prediction model using XGBoost to estimate salaries for
all archetypes (observed and unobserved).

Features:
- XGBoost regression model
- Stratified train/val/test split
- Median/mode imputation for missing data
- 90% confidence intervals via quantile regression
- Comprehensive evaluation metrics

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Set style for plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

print("="*70)
print("PHASE 6: SALARY ESTIMATION MODEL (Gradient Boosting)")
print("="*70)
print()

# ============================================================================
# STEP 1: DATA EXTRACTION
# ============================================================================

print("Step 1: Extracting data from database...")
print("-" * 70)

config = Config()
db = DatabaseManager(config)
db.initialize_pool()
conn = db.get_connection()

query = """
SELECT
    oj.id,
    COALESCE(oj.salary_point, (oj.salary_min + oj.salary_max) / 2) as salary,
    oj.raw_title,
    oj.seniority as seniority_level,
    cr.name as canonical_role,
    cr.role_family,
    cr.soc_code,
    cr.category as role_category,
    c.name as company_name,
    c.size_category as company_size,
    c.industry,
    c.is_public,
    COALESCE(ma.name, l.city || ', ' || l.state) as metro_name,
    COALESCE(l.state, 'Unknown') as state,
    s.name as source
FROM observed_jobs oj
JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
JOIN companies c ON oj.company_id = c.id
JOIN locations l ON oj.location_id = l.id
LEFT JOIN metro_areas ma ON l.metro_id = ma.id
JOIN sources s ON oj.source_id = s.id
WHERE oj.canonical_role_id IS NOT NULL
  AND (oj.salary_point IS NOT NULL OR (oj.salary_min IS NOT NULL AND oj.salary_max IS NOT NULL))
  AND COALESCE(oj.salary_point, (oj.salary_min + oj.salary_max) / 2) > 0
  AND COALESCE(oj.salary_point, (oj.salary_min + oj.salary_max) / 2) < 1000000
"""

print("Executing query...")
df = pd.read_sql(query, conn)
db.release_connection(conn)
db.close_all_connections()

print(f"✓ Extracted {len(df):,} observed jobs with salaries")
print(f"  Columns: {list(df.columns)}")
print()

# ============================================================================
# STEP 2: EXPLORATORY DATA ANALYSIS
# ============================================================================

print("Step 2: Exploratory Data Analysis")
print("-" * 70)

print("\n📊 Dataset Overview:")
print(df.info())

print("\n💰 Salary Statistics:")
print(df['salary'].describe())

print("\n📈 Salary Distribution:")
print(f"  Min:     ${df['salary'].min():>12,.0f}")
print(f"  25th:    ${df['salary'].quantile(0.25):>12,.0f}")
print(f"  Median:  ${df['salary'].median():>12,.0f}")
print(f"  75th:    ${df['salary'].quantile(0.75):>12,.0f}")
print(f"  Max:     ${df['salary'].max():>12,.0f}")

print("\n🔍 Missing Data:")
missing = df.isnull().sum()
missing_pct = (missing / len(df) * 100).round(2)
missing_df = pd.DataFrame({
    'Missing': missing,
    'Percentage': missing_pct
})
print(missing_df[missing_df['Missing'] > 0].sort_values('Missing', ascending=False))

print("\n🎯 Top 10 Roles by Count:")
role_counts = df['canonical_role'].value_counts().head(10)
for i, (role, count) in enumerate(role_counts.items(), 1):
    pct = count / len(df) * 100
    print(f"  {i:2d}. {role:40s} {count:>6,} ({pct:>5.1f}%)")

print("\n🌎 Top 10 Metros by Count:")
metro_counts = df['metro_name'].value_counts().head(10)
for i, (metro, count) in enumerate(metro_counts.items(), 1):
    pct = count / len(df) * 100
    print(f"  {i:2d}. {metro:40s} {count:>6,} ({pct:>5.1f}%)")

print("\n🏢 Top 10 Companies by Count:")
company_counts = df['company_name'].value_counts().head(10)
for i, (company, count) in enumerate(company_counts.items(), 1):
    pct = count / len(df) * 100
    print(f"  {i:2d}. {company:40s} {count:>6,} ({pct:>5.1f}%)")

print("\n📚 Data Source Distribution:")
source_dist = df['source'].value_counts()
for source, count in source_dist.items():
    pct = count / len(df) * 100
    avg_salary = df[df['source'] == source]['salary'].mean()
    print(f"  {source:20s}: {count:>6,} jobs ({pct:>5.1f}%), avg salary: ${avg_salary:>10,.0f}")

print("\n💼 Seniority Distribution:")
seniority_dist = df['seniority_level'].value_counts()
for level, count in seniority_dist.items():
    pct = count / len(df) * 100
    avg_salary = df[df['seniority_level'] == level]['salary'].mean()
    print(f"  {level:20s}: {count:>6,} jobs ({pct:>5.1f}%), avg salary: ${avg_salary:>10,.0f}")

print()

# ============================================================================
# STEP 3: FEATURE ENGINEERING
# ============================================================================

print("Step 3: Feature Engineering")
print("-" * 70)

# Create a copy for modeling
df_model = df.copy()

print("\n1. Handling missing values...")

# Impute missing company_size with mode
if df_model['company_size'].isnull().any():
    mode_size = df_model['company_size'].mode()[0] if len(df_model['company_size'].mode()) > 0 else 'Unknown'
    missing_size = df_model['company_size'].isnull().sum()
    df_model['company_size'].fillna(mode_size, inplace=True)
    print(f"  ✓ Imputed {missing_size:,} missing company_size with mode: {mode_size}")

# Impute missing industry with mode
if df_model['industry'].isnull().any():
    mode_industry = df_model['industry'].mode()[0] if len(df_model['industry'].mode()) > 0 else 'Unknown'
    missing_ind = df_model['industry'].isnull().sum()
    df_model['industry'].fillna(mode_industry, inplace=True)
    print(f"  ✓ Imputed {missing_ind:,} missing industry with mode: {mode_industry}")

# Impute missing seniority with mode
if df_model['seniority_level'].isnull().any():
    mode_seniority = df_model['seniority_level'].mode()[0] if len(df_model['seniority_level'].mode()) > 0 else 'Mid'
    missing_sen = df_model['seniority_level'].isnull().sum()
    df_model['seniority_level'].fillna(mode_seniority, inplace=True)
    print(f"  ✓ Imputed {missing_sen:,} missing seniority_level with mode: {mode_seniority}")

# Fill missing is_public with False
if df_model['is_public'].isnull().any():
    missing_pub = df_model['is_public'].isnull().sum()
    df_model['is_public'].fillna(False, inplace=True)
    print(f"  ✓ Imputed {missing_pub:,} missing is_public with False")

print("\n2. Creating numerical features...")

# Convert is_public to int
df_model['is_public_int'] = df_model['is_public'].astype(int)
print(f"  ✓ Created is_public_int")

# Average salaries by role (for model feature)
role_avg_salary = df_model.groupby('canonical_role')['salary'].transform('median')
df_model['role_median_salary'] = role_avg_salary
print(f"  ✓ Created role_median_salary")

# Average salaries by metro
metro_avg_salary = df_model.groupby('metro_name')['salary'].transform('median')
df_model['metro_median_salary'] = metro_avg_salary
print(f"  ✓ Created metro_median_salary")

# Average salaries by company
company_avg_salary = df_model.groupby('company_name')['salary'].transform('median')
df_model['company_median_salary'] = company_avg_salary
print(f"  ✓ Created company_median_salary")

print("\n3. Encoding categorical features...")

# Features to encode
categorical_features = [
    'canonical_role',
    'role_family',
    'seniority_level',
    'industry',
    'state',
    'source',
    'company_size',
    'role_category'
]

encoders = {}
for feature in categorical_features:
    le = LabelEncoder()
    df_model[f'{feature}_encoded'] = le.fit_transform(df_model[feature])
    encoders[feature] = le
    print(f"  ✓ Encoded {feature}: {len(le.classes_)} unique values")

print()

# ============================================================================
# STEP 4: TRAIN/VAL/TEST SPLIT
# ============================================================================

print("Step 4: Creating train/validation/test splits")
print("-" * 70)

# Define features and target
feature_cols = [
    'is_public_int',
    'role_median_salary',
    'metro_median_salary',
    'company_median_salary',
    'canonical_role_encoded',
    'role_family_encoded',
    'seniority_level_encoded',
    'industry_encoded',
    'state_encoded',
    'source_encoded',
    'company_size_encoded',
    'role_category_encoded'
]

X = df_model[feature_cols]
y = df_model['salary']

print(f"\nFeatures: {len(feature_cols)}")
for i, col in enumerate(feature_cols, 1):
    print(f"  {i:2d}. {col}")

print(f"\nTarget: salary")
print(f"Samples: {len(X):,}")

# Random split (no stratification needed with 195k samples)
print("\nSplitting data (70/15/15) randomly...")
X_temp, X_test, y_temp, y_test = train_test_split(
    X, y, test_size=0.15, random_state=42
)

X_train, X_val, y_train, y_val = train_test_split(
    X_temp, y_temp, test_size=0.176, random_state=42  # 0.176 * 0.85 ≈ 0.15
)

print(f"  ✓ Train: {len(X_train):>6,} samples ({len(X_train)/len(X)*100:>5.1f}%)")
print(f"  ✓ Val:   {len(X_val):>6,} samples ({len(X_val)/len(X)*100:>5.1f}%)")
print(f"  ✓ Test:  {len(X_test):>6,} samples ({len(X_test)/len(X)*100:>5.1f}%)")

print()

# Save split indices for later reference
split_info = {
    'train_indices': X_train.index.tolist(),
    'val_indices': X_val.index.tolist(),
    'test_indices': X_test.index.tolist()
}

print("Step 4 complete!")
print()

# Save progress so far
print("Saving intermediate results...")
df_model.to_csv('salary_model_data.csv', index=False)
joblib.dump(encoders, 'feature_encoders.pkl')
joblib.dump(split_info, 'data_splits.pkl')
print("✓ Saved salary_model_data.csv, feature_encoders.pkl, data_splits.pkl")
print()

print("="*70)
print("Data preparation complete! Ready for modeling.")
print("="*70)
print("\nNext: Run phase6_train_model.py to train XGBoost model")
