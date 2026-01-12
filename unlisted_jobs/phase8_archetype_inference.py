#!/usr/bin/env python3
"""
Phase 8: Archetype Inference Model
===================================

Predicts which archetypes (role + seniority + location) exist at each company,
even when never directly observed. Uses co-occurrence patterns and company
characteristics to fill gaps in the company × archetype matrix.

Approach:
1. Extract observed company × archetype combinations
2. Learn archetype co-occurrence patterns
3. Build binary classification model (exists/doesn't exist)
4. Generate predictions with confidence scores
5. Fill complete company × archetype matrix

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
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

# Setup logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler('phase8_archetype_inference.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.info

log("="*70)
log("PHASE 8: ARCHETYPE INFERENCE MODEL")
log("="*70)
log("")

# ============================================================================
# STEP 1: DATA EXTRACTION
# ============================================================================

log("Step 1: Extracting archetype data from database...")
log("-" * 70)

config = Config()
db = DatabaseManager(config)
db.initialize_pool()
conn = db.get_connection()

# Extract all company × archetype combinations
query = """
SELECT
    c.id as company_id,
    c.name as company_name,
    c.industry,
    c.size_category,
    c.is_public,
    cr.id as canonical_role_id,
    cr.name as canonical_role,
    cr.role_family,
    cr.category as role_category,
    COALESCE(l.state, 'Unknown') as state,
    COALESCE(ma.name, l.city || ', ' || l.state) as metro_name,
    oj.seniority as seniority_level,
    COUNT(*) as observation_count,
    AVG(COALESCE(oj.salary_point, (oj.salary_min + oj.salary_max) / 2)) as avg_salary
FROM observed_jobs oj
JOIN companies c ON oj.company_id = c.id
JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
JOIN locations l ON oj.location_id = l.id
LEFT JOIN metro_areas ma ON l.metro_id = ma.id
WHERE oj.canonical_role_id IS NOT NULL
GROUP BY c.id, c.name, c.industry, c.size_category, c.is_public,
         cr.id, cr.name, cr.role_family, cr.category,
         l.state, ma.name, l.city, oj.seniority
"""

log("\nExecuting query...")
df_observed = pd.read_sql(query, conn)

# Get counts
query_counts = """
SELECT
    COUNT(DISTINCT c.id) as total_companies,
    COUNT(DISTINCT cr.id) as total_roles,
    COUNT(DISTINCT oj.seniority) as total_seniorities,
    COUNT(DISTINCT l.state) as total_states
FROM observed_jobs oj
JOIN companies c ON oj.company_id = c.id
JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
JOIN locations l ON oj.location_id = l.id
WHERE oj.canonical_role_id IS NOT NULL
"""

counts = pd.read_sql(query_counts, conn)
db.release_connection(conn)
db.close_all_connections()

log(f"✓ Extracted {len(df_observed):,} observed company × archetype combinations")
log(f"  Companies: {counts['total_companies'].iloc[0]:,}")
log(f"  Roles: {counts['total_roles'].iloc[0]:,}")
log(f"  Seniority levels: {counts['total_seniorities'].iloc[0]:,}")
log(f"  States: {counts['total_states'].iloc[0]:,}")
log("")

# ============================================================================
# STEP 2: ARCHETYPE ANALYSIS
# ============================================================================

log("Step 2: Analyzing archetype patterns...")
log("-" * 70)

# Create archetype identifier (role + seniority + state)
df_observed['archetype'] = (
    df_observed['canonical_role'] + ' | ' +
    df_observed['seniority_level'].fillna('Mid') + ' | ' +
    df_observed['state']
)

log(f"\n📊 Archetype Statistics:")
log(f"  Unique archetypes: {df_observed['archetype'].nunique():,}")
log(f"  Unique companies: {df_observed['company_id'].nunique():,}")
log(f"  Total observations: {len(df_observed):,}")

# Archetypes per company
archetypes_per_company = df_observed.groupby('company_id').size()
log(f"\n🏢 Archetypes per Company:")
log(f"  Min:      {archetypes_per_company.min():>6,}")
log(f"  25th:     {archetypes_per_company.quantile(0.25):>6,.0f}")
log(f"  Median:   {archetypes_per_company.median():>6,.0f}")
log(f"  75th:     {archetypes_per_company.quantile(0.75):>6,.0f}")
log(f"  Max:      {archetypes_per_company.max():>6,}")

# Companies per archetype
companies_per_archetype = df_observed.groupby('archetype').size()
log(f"\n🎯 Companies per Archetype:")
log(f"  Min:      {companies_per_archetype.min():>6,}")
log(f"  25th:     {companies_per_archetype.quantile(0.25):>6,.0f}")
log(f"  Median:   {companies_per_archetype.median():>6,.0f}")
log(f"  75th:     {companies_per_archetype.quantile(0.75):>6,.0f}")
log(f"  Max:      {companies_per_archetype.max():>6,}")

log(f"\n🔥 Top 10 Most Common Archetypes:")
top_archetypes = companies_per_archetype.sort_values(ascending=False).head(10)
for i, (archetype, count) in enumerate(top_archetypes.items(), 1):
    log(f"  {i:2d}. {archetype:70s} {count:>5,} companies")

log("")

# ============================================================================
# STEP 3: BUILD CO-OCCURRENCE MATRIX
# ============================================================================

log("Step 3: Building archetype co-occurrence patterns...")
log("-" * 70)

# Create company × archetype matrix (observed only)
company_archetype_matrix = df_observed.pivot_table(
    index='company_id',
    columns='archetype',
    values='observation_count',
    aggfunc='sum',
    fill_value=0
)

# Convert to binary (exists / doesn't exist)
company_archetype_binary = (company_archetype_matrix > 0).astype(int)

log(f"\n📊 Co-occurrence Matrix:")
log(f"  Shape: {company_archetype_binary.shape[0]:,} companies × {company_archetype_binary.shape[1]:,} archetypes")
log(f"  Total cells: {company_archetype_binary.shape[0] * company_archetype_binary.shape[1]:,}")
log(f"  Observed (1s): {company_archetype_binary.sum().sum():,} ({company_archetype_binary.sum().sum() / (company_archetype_binary.shape[0] * company_archetype_binary.shape[1]) * 100:.2f}%)")
log(f"  Unobserved (0s): {(company_archetype_binary.shape[0] * company_archetype_binary.shape[1]) - company_archetype_binary.sum().sum():,}")

# Compute archetype co-occurrence (how often archetypes appear together)
log(f"\nComputing archetype co-occurrence patterns...")
archetype_cooccurrence = company_archetype_binary.T @ company_archetype_binary

log(f"✓ Built {archetype_cooccurrence.shape[0]} × {archetype_cooccurrence.shape[1]} co-occurrence matrix")
log("")

# ============================================================================
# STEP 4: FEATURE ENGINEERING FOR CLASSIFICATION
# ============================================================================

log("Step 4: Engineering features for archetype prediction...")
log("-" * 70)

# Strategy: For each company × archetype pair, create features that predict existence

# Get all unique companies and archetypes
all_companies = df_observed['company_id'].unique()
all_archetypes = df_observed['archetype'].unique()

log(f"\nTarget matrix dimensions:")
log(f"  Companies: {len(all_companies):,}")
log(f"  Archetypes: {len(all_archetypes):,}")
log(f"  Total cells: {len(all_companies) * len(all_archetypes):,}")

# For computational efficiency, we'll sample negative examples
# Positive examples: observed combinations
# Negative examples: unobserved combinations (sampled)

log(f"\nBuilding training dataset...")

# Positive examples (observed)
positive_examples = []
for _, row in df_observed.iterrows():
    positive_examples.append({
        'company_id': row['company_id'],
        'archetype': row['archetype'],
        'exists': 1
    })

log(f"  Positive examples: {len(positive_examples):,}")

# Negative examples (unobserved) - sample 2x positive examples
log(f"  Sampling negative examples (this may take a moment)...")
negative_examples = []
target_negatives = len(positive_examples) * 2

# Create set of observed pairs for quick lookup
observed_pairs = set(
    (row['company_id'], row['archetype'])
    for _, row in df_observed.iterrows()
)

# Sample negative examples
np.random.seed(42)
sampled = 0
attempts = 0
max_attempts = target_negatives * 10  # Prevent infinite loop

while sampled < target_negatives and attempts < max_attempts:
    company_id = np.random.choice(all_companies)
    archetype = np.random.choice(all_archetypes)

    if (company_id, archetype) not in observed_pairs:
        negative_examples.append({
            'company_id': company_id,
            'archetype': archetype,
            'exists': 0
        })
        sampled += 1

    attempts += 1

log(f"  Negative examples: {len(negative_examples):,}")

# Combine positive and negative examples
df_training = pd.DataFrame(positive_examples + negative_examples)

log(f"\n✓ Created training dataset: {len(df_training):,} examples")
log(f"  Positive class: {(df_training['exists'] == 1).sum():,} ({(df_training['exists'] == 1).sum() / len(df_training) * 100:.1f}%)")
log(f"  Negative class: {(df_training['exists'] == 0).sum():,} ({(df_training['exists'] == 0).sum() / len(df_training) * 100:.1f}%)")

# Parse archetype back into components
# Handle cases where split doesn't produce exactly 3 parts
split_result = df_training['archetype'].str.split(' | ', expand=True)

# If we get more than 3 columns, combine extras into first column (role name might have pipes)
if split_result.shape[1] == 3:
    df_training[['canonical_role', 'seniority_level', 'state']] = split_result
elif split_result.shape[1] > 3:
    # Combine all but last 2 columns into canonical_role
    df_training['canonical_role'] = split_result.iloc[:, :-2].apply(lambda x: ' | '.join(x.dropna()), axis=1)
    df_training['seniority_level'] = split_result.iloc[:, -2]
    df_training['state'] = split_result.iloc[:, -1]
else:
    # Less than 3 columns - fill missing with defaults
    df_training['canonical_role'] = split_result.iloc[:, 0] if split_result.shape[1] >= 1 else 'Unknown'
    df_training['seniority_level'] = split_result.iloc[:, 1] if split_result.shape[1] >= 2 else 'Mid'
    df_training['state'] = split_result.iloc[:, 2] if split_result.shape[1] >= 3 else 'Unknown'

# Merge company features
company_features = df_observed[['company_id', 'company_name', 'industry', 'size_category', 'is_public']].drop_duplicates()
df_training = df_training.merge(company_features, on='company_id', how='left')

log(f"✓ Merged company features")
log("")

# ============================================================================
# STEP 5: ENCODE FEATURES
# ============================================================================

log("Step 5: Encoding categorical features...")
log("-" * 70)

# Fill missing values
df_training['industry'].fillna('Unknown', inplace=True)
df_training['size_category'].fillna('Unknown', inplace=True)
df_training['is_public'].fillna(False, inplace=True)
df_training['seniority_level'].fillna('Mid', inplace=True)

# Features to encode
categorical_features = [
    'canonical_role',
    'seniority_level',
    'state',
    'industry',
    'size_category'
]

encoders = {}
for feature in categorical_features:
    le = LabelEncoder()
    df_training[f'{feature}_encoded'] = le.fit_transform(df_training[feature])
    encoders[feature] = le
    log(f"  ✓ Encoded {feature}: {len(le.classes_)} unique values")

# Convert is_public to int
df_training['is_public_int'] = df_training['is_public'].astype(int)

log("")

# ============================================================================
# STEP 6: CREATE ADDITIONAL FEATURES
# ============================================================================

log("Step 6: Creating additional predictive features...")
log("-" * 70)

# Feature 1: How common is this archetype overall?
archetype_frequency = df_observed.groupby('archetype').size() / len(all_companies)
df_training['archetype_frequency'] = df_training['archetype'].map(archetype_frequency).fillna(0)

log(f"  ✓ Created archetype_frequency")

# Feature 2: How many archetypes does this company have?
company_archetype_count = df_observed.groupby('company_id').size()
df_training['company_archetype_count'] = df_training['company_id'].map(company_archetype_count).fillna(0)

log(f"  ✓ Created company_archetype_count")

# Feature 3: Role frequency at company's industry
role_industry_freq = df_observed.groupby(['industry', 'canonical_role']).size().reset_index(name='freq')
role_industry_freq['industry_total'] = role_industry_freq.groupby('industry')['freq'].transform('sum')
role_industry_freq['role_industry_pct'] = role_industry_freq['freq'] / role_industry_freq['industry_total']
role_industry_lookup = role_industry_freq.set_index(['industry', 'canonical_role'])['role_industry_pct'].to_dict()

df_training['role_industry_pct'] = df_training.apply(
    lambda row: role_industry_lookup.get((row['industry'], row['canonical_role']), 0),
    axis=1
)

log(f"  ✓ Created role_industry_pct")

# Feature 4: Seniority frequency at this role
seniority_role_freq = df_observed.groupby(['canonical_role', 'seniority_level']).size().reset_index(name='freq')
seniority_role_freq['role_total'] = seniority_role_freq.groupby('canonical_role')['freq'].transform('sum')
seniority_role_freq['seniority_role_pct'] = seniority_role_freq['freq'] / seniority_role_freq['role_total']
seniority_role_lookup = seniority_role_freq.set_index(['canonical_role', 'seniority_level'])['seniority_role_pct'].to_dict()

df_training['seniority_role_pct'] = df_training.apply(
    lambda row: seniority_role_lookup.get((row['canonical_role'], row['seniority_level']), 0),
    axis=1
)

log(f"  ✓ Created seniority_role_pct")

log("")

# ============================================================================
# STEP 7: TRAIN/VAL/TEST SPLIT
# ============================================================================

log("Step 7: Creating train/validation/test splits...")
log("-" * 70)

feature_cols = [
    'canonical_role_encoded',
    'seniority_level_encoded',
    'state_encoded',
    'industry_encoded',
    'size_category_encoded',
    'is_public_int',
    'archetype_frequency',
    'company_archetype_count',
    'role_industry_pct',
    'seniority_role_pct'
]

X = df_training[feature_cols]
y = df_training['exists']

log(f"\nFeatures: {len(feature_cols)}")
for i, col in enumerate(feature_cols, 1):
    log(f"  {i:2d}. {col}")

log(f"\nTarget: exists (binary)")
log(f"Samples: {len(X):,}")

# Split data (70/15/15)
X_temp, X_test, y_temp, y_test = train_test_split(
    X, y, test_size=0.15, random_state=42, stratify=y
)

X_train, X_val, y_train, y_val = train_test_split(
    X_temp, y_temp, test_size=0.176, random_state=42, stratify=y_temp
)

log(f"\n✓ Train: {len(X_train):>6,} samples ({len(X_train)/len(X)*100:>5.1f}%)")
log(f"✓ Val:   {len(X_val):>6,} samples ({len(X_val)/len(X)*100:>5.1f}%)")
log(f"✓ Test:  {len(X_test):>6,} samples ({len(X_test)/len(X)*100:>5.1f}%)")

log("")

# ============================================================================
# STEP 8: TRAIN BASELINE MODEL
# ============================================================================

log("Step 8: Training baseline model (predict by frequency)...")
log("-" * 70)

# Baseline: Predict exists if archetype_frequency > threshold
threshold = 0.01  # 1% of companies have this archetype

y_val_pred_baseline = (X_val['archetype_frequency'] > threshold).astype(int)

baseline_acc = accuracy_score(y_val, y_val_pred_baseline)
baseline_precision = precision_score(y_val, y_val_pred_baseline, zero_division=0)
baseline_recall = recall_score(y_val, y_val_pred_baseline, zero_division=0)
baseline_f1 = f1_score(y_val, y_val_pred_baseline, zero_division=0)

log(f"\nBaseline Model Performance (Validation Set):")
log(f"  Accuracy:  {baseline_acc:>10.4f}")
log(f"  Precision: {baseline_precision:>10.4f}")
log(f"  Recall:    {baseline_recall:>10.4f}")
log(f"  F1 Score:  {baseline_f1:>10.4f}")

log("")

# ============================================================================
# STEP 9: TRAIN GRADIENT BOOSTING CLASSIFIER
# ============================================================================

log("Step 9: Training Gradient Boosting classifier...")
log("-" * 70)

log("\nTraining model (this may take 5-10 minutes)...")

model = GradientBoostingClassifier(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=5,
    min_samples_split=20,
    min_samples_leaf=10,
    subsample=0.8,
    random_state=42,
    verbose=1
)

model.fit(X_train, y_train)

log("\n✓ Model training complete!")
log("")

# ============================================================================
# STEP 10: EVALUATE MODEL
# ============================================================================

log("Step 10: Evaluating model performance...")
log("-" * 70)

# Predictions
y_train_pred = model.predict(X_train)
y_val_pred = model.predict(X_val)
y_test_pred = model.predict(X_test)

# Prediction probabilities
y_train_pred_proba = model.predict_proba(X_train)[:, 1]
y_val_pred_proba = model.predict_proba(X_val)[:, 1]
y_test_pred_proba = model.predict_proba(X_test)[:, 1]

def calculate_metrics(y_true, y_pred, y_pred_proba, set_name):
    """Calculate all evaluation metrics."""
    acc = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    auc = roc_auc_score(y_true, y_pred_proba)

    log(f"\n{set_name} Set Performance:")
    log(f"  Accuracy:  {acc:>10.4f}")
    log(f"  Precision: {precision:>10.4f}")
    log(f"  Recall:    {recall:>10.4f}")
    log(f"  F1 Score:  {f1:>10.4f}")
    log(f"  AUC-ROC:   {auc:>10.4f}")

    return {
        'accuracy': acc,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'auc': auc
    }

train_metrics = calculate_metrics(y_train, y_train_pred, y_train_pred_proba, "Training")
val_metrics = calculate_metrics(y_val, y_val_pred, y_val_pred_proba, "Validation")
test_metrics = calculate_metrics(y_test, y_test_pred, y_test_pred_proba, "Test")

# Confusion matrix
log("\n" + "="*70)
log("Confusion Matrix (Test Set):")
log("="*70)
cm = confusion_matrix(y_test, y_test_pred)
log(f"\n                 Predicted: 0    Predicted: 1")
log(f"Actual: 0 (No)   {cm[0, 0]:>10,}    {cm[0, 1]:>10,}")
log(f"Actual: 1 (Yes)  {cm[1, 0]:>10,}    {cm[1, 1]:>10,}")

log("")

# ============================================================================
# STEP 11: FEATURE IMPORTANCE
# ============================================================================

log("Step 11: Analyzing feature importance...")
log("-" * 70)

feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

log("\nFeature Importances:")
for i, row in feature_importance.iterrows():
    log(f"  {row['feature']:35s} {row['importance']:>8.4f}")

# Save plot
plt.figure(figsize=(10, 6))
plt.barh(feature_importance['feature'], feature_importance['importance'])
plt.xlabel('Importance')
plt.title('Feature Importances for Archetype Inference')
plt.tight_layout()
plt.savefig('archetype_feature_importance.png', dpi=150, bbox_inches='tight')
log("\n✓ Saved archetype_feature_importance.png")

log("")

# ============================================================================
# STEP 12: SAVE MODELS AND RESULTS
# ============================================================================

log("Step 12: Saving models and results...")
log("-" * 70)

# Save model
joblib.dump(model, 'archetype_inference_model.pkl')
log("✓ Saved archetype_inference_model.pkl")

# Save encoders
joblib.dump(encoders, 'archetype_encoders.pkl')
log("✓ Saved archetype_encoders.pkl")

# Save metrics
metrics_summary = {
    'baseline': {
        'accuracy': baseline_acc,
        'precision': baseline_precision,
        'recall': baseline_recall,
        'f1': baseline_f1
    },
    'gradient_boosting': {
        'train': train_metrics,
        'val': val_metrics,
        'test': test_metrics
    },
    'feature_importance': feature_importance.to_dict('records')
}

joblib.dump(metrics_summary, 'archetype_metrics.pkl')
log("✓ Saved archetype_metrics.pkl")

# Save test predictions
test_indices = X_test.index
test_predictions = pd.DataFrame({
    'company_id': df_training.loc[test_indices, 'company_id'].values,
    'archetype': df_training.loc[test_indices, 'archetype'].values,
    'actual': y_test.values,
    'predicted': y_test_pred,
    'probability': y_test_pred_proba,
    'correct': (y_test.values == y_test_pred).astype(int)
})
test_predictions.to_csv('archetype_test_predictions.csv', index=False)
log("✓ Saved archetype_test_predictions.csv")

log("")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

log("="*70)
log("PHASE 8 COMPLETE!")
log("="*70)

log(f"\n📊 Final Model Performance (Test Set):")
log(f"  Accuracy:  {test_metrics['accuracy']:>10.4f}  {'✓ Excellent' if test_metrics['accuracy'] > 0.85 else '✓ Good' if test_metrics['accuracy'] > 0.75 else '⚠ Acceptable'}")
log(f"  Precision: {test_metrics['precision']:>10.4f}  (How many predicted 'exists' are correct)")
log(f"  Recall:    {test_metrics['recall']:>10.4f}  (How many actual 'exists' we found)")
log(f"  F1 Score:  {test_metrics['f1']:>10.4f}")
log(f"  AUC-ROC:   {test_metrics['auc']:>10.4f}  {'✓ Excellent' if test_metrics['auc'] > 0.85 else '✓ Good' if test_metrics['auc'] > 0.75 else '⚠ Acceptable'}")

log(f"\n🎯 Improvement over Baseline:")
acc_improvement = ((test_metrics['accuracy'] - baseline_acc) / baseline_acc) * 100
log(f"  Accuracy improvement: {acc_improvement:>6.1f}%")

log(f"\n📁 Files Created:")
log(f"  - archetype_inference_model.pkl (trained model)")
log(f"  - archetype_encoders.pkl (feature encoders)")
log(f"  - archetype_metrics.pkl (all metrics)")
log(f"  - archetype_test_predictions.csv (test set predictions)")
log(f"  - archetype_feature_importance.png (visualization)")

log(f"\n🚀 Next Steps:")
log(f"  1. Run inference on all company × archetype pairs")
log(f"  2. Generate complete labor market matrix")
log(f"  3. Analyze prediction patterns and confidence")
log(f"  4. Integrate with headcount estimates from Phase 7")

log("\n" + "="*70)
