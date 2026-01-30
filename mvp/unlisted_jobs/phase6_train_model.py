#!/usr/bin/env python3
"""
Phase 6: Train Salary Estimation Model
=======================================

Trains baseline and Gradient Boosting models, evaluates performance,
and generates salary predictions with confidence intervals.

Author: ShortList.ai
Date: 2026-01-12
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

print("="*70)
print("PHASE 6: MODEL TRAINING & EVALUATION")
print("="*70)
print()

# ============================================================================
# STEP 1: LOAD PREPARED DATA
# ============================================================================

print("Step 1: Loading prepared data...")
print("-" * 70)

df_model = pd.read_csv('salary_model_data.csv')
encoders = joblib.load('feature_encoders.pkl')
split_info = joblib.load('data_splits.pkl')

print(f"✓ Loaded {len(df_model):,} samples")
print(f"✓ Loaded {len(encoders)} encoders")

# Recreate train/val/test splits
train_idx = split_info['train_indices']
val_idx = split_info['val_indices']
test_idx = split_info['test_indices']

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

X_train = df_model.loc[train_idx, feature_cols]
y_train = df_model.loc[train_idx, 'salary']

X_val = df_model.loc[val_idx, feature_cols]
y_val = df_model.loc[val_idx, 'salary']

X_test = df_model.loc[test_idx, feature_cols]
y_test = df_model.loc[test_idx, 'salary']

print(f"✓ Train: {len(X_train):>6,} samples")
print(f"✓ Val:   {len(X_val):>6,} samples")
print(f"✓ Test:  {len(X_test):>6,} samples")
print()

# ============================================================================
# STEP 2: BASELINE MODEL (Median by Role)
# ============================================================================

print("Step 2: Training baseline model (median by role)...")
print("-" * 70)

# Calculate median salary by role for baseline
role_medians = df_model.loc[train_idx].groupby('canonical_role')['salary'].median()

# Predict using role median
def predict_baseline(df_subset):
    """Predict salary using median salary for each role."""
    predictions = []
    for idx in df_subset.index:
        role = df_model.loc[idx, 'canonical_role']
        pred = role_medians.get(role, role_medians.median())
        predictions.append(pred)
    return np.array(predictions)

# Evaluate baseline on validation set
y_val_pred_baseline = predict_baseline(df_model.loc[val_idx])

baseline_mae = mean_absolute_error(y_val, y_val_pred_baseline)
baseline_rmse = np.sqrt(mean_squared_error(y_val, y_val_pred_baseline))
baseline_r2 = r2_score(y_val, y_val_pred_baseline)
baseline_mape = np.mean(np.abs((y_val - y_val_pred_baseline) / y_val)) * 100

print(f"\nBaseline Model Performance (Validation Set):")
print(f"  MAE:   ${baseline_mae:>12,.0f}")
print(f"  RMSE:  ${baseline_rmse:>12,.0f}")
print(f"  R²:    {baseline_r2:>12.4f}")
print(f"  MAPE:  {baseline_mape:>12.1f}%")
print()

# ============================================================================
# STEP 3: GRADIENT BOOSTING MODEL
# ============================================================================

print("Step 3: Training Gradient Boosting model...")
print("-" * 70)

# Train model with good hyperparameters
print("\nTraining model (this may take 5-10 minutes)...")
model = GradientBoostingRegressor(
    n_estimators=200,          # Number of boosting stages
    learning_rate=0.1,         # Shrinks contribution of each tree
    max_depth=5,               # Maximum depth of trees
    min_samples_split=20,      # Minimum samples to split node
    min_samples_leaf=10,       # Minimum samples in leaf
    subsample=0.8,             # Fraction of samples for each tree
    random_state=42,
    verbose=1                  # Show progress
)

model.fit(X_train, y_train)

print("\n✓ Model training complete!")
print()

# ============================================================================
# STEP 4: EVALUATE MODEL
# ============================================================================

print("Step 4: Evaluating model performance...")
print("-" * 70)

# Predictions
y_train_pred = model.predict(X_train)
y_val_pred = model.predict(X_val)
y_test_pred = model.predict(X_test)

def calculate_metrics(y_true, y_pred, set_name):
    """Calculate all evaluation metrics."""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100

    print(f"\n{set_name} Set Performance:")
    print(f"  MAE:   ${mae:>12,.0f}")
    print(f"  RMSE:  ${rmse:>12,.0f}")
    print(f"  R²:    {r2:>12.4f}")
    print(f"  MAPE:  {mape:>12.1f}%")

    return {'mae': mae, 'rmse': rmse, 'r2': r2, 'mape': mape}

train_metrics = calculate_metrics(y_train, y_train_pred, "Training")
val_metrics = calculate_metrics(y_val, y_val_pred, "Validation")
test_metrics = calculate_metrics(y_test, y_test_pred, "Test")

# Check for overfitting
print("\n" + "="*70)
print("Overfitting Check:")
print("="*70)
train_val_diff = train_metrics['mae'] - val_metrics['mae']
if abs(train_val_diff) < 5000:
    print(f"✓ No significant overfitting (MAE diff: ${abs(train_val_diff):,.0f})")
elif abs(train_val_diff) < 10000:
    print(f"⚠ Minor overfitting detected (MAE diff: ${abs(train_val_diff):,.0f})")
else:
    print(f"⚠ Significant overfitting detected (MAE diff: ${abs(train_val_diff):,.0f})")

print()

# ============================================================================
# STEP 5: FEATURE IMPORTANCE
# ============================================================================

print("Step 5: Analyzing feature importance...")
print("-" * 70)

feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nTop 10 Most Important Features:")
for i, row in feature_importance.head(10).iterrows():
    print(f"  {row['feature']:30s} {row['importance']:>8.4f}")

# Save feature importance plot
plt.figure(figsize=(10, 8))
plt.barh(feature_importance['feature'].head(10), feature_importance['importance'].head(10))
plt.xlabel('Importance')
plt.title('Top 10 Feature Importances')
plt.tight_layout()
plt.savefig('feature_importance.png', dpi=150, bbox_inches='tight')
print("\n✓ Saved feature_importance.png")

print()

# ============================================================================
# STEP 6: PREDICTION ANALYSIS
# ============================================================================

print("Step 6: Analyzing predictions...")
print("-" * 70)

# Analyze predictions by role
test_results = pd.DataFrame({
    'actual': y_test,
    'predicted': y_test_pred,
    'error': y_test_pred - y_test,
    'abs_error': np.abs(y_test_pred - y_test),
    'pct_error': np.abs((y_test_pred - y_test) / y_test) * 100,
    'canonical_role': df_model.loc[test_idx, 'canonical_role'].values
})

print("\nPrediction Accuracy by Role (Top 10 roles by count):")
role_performance = test_results.groupby('canonical_role').agg({
    'actual': 'count',
    'abs_error': 'mean',
    'pct_error': 'mean'
}).rename(columns={'actual': 'count', 'abs_error': 'mae', 'pct_error': 'mape'})
role_performance = role_performance.sort_values('count', ascending=False)

for i, (role, row) in enumerate(role_performance.head(10).iterrows(), 1):
    print(f"  {i:2d}. {role:40s} MAE: ${row['mae']:>10,.0f}, MAPE: {row['mape']:>6.1f}%, n={int(row['count']):>5,}")

print()

# ============================================================================
# STEP 7: GENERATE CONFIDENCE INTERVALS
# ============================================================================

print("Step 7: Generating confidence intervals...")
print("-" * 70)

# Train quantile models for 10th and 90th percentiles
print("\nTraining quantile regressors for confidence intervals...")

# Lower bound (10th percentile)
model_lower = GradientBoostingRegressor(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=4,
    min_samples_split=20,
    loss='quantile',
    alpha=0.1,  # 10th percentile
    random_state=42,
    verbose=0
)
model_lower.fit(X_train, y_train)
print("✓ Trained lower bound model (10th percentile)")

# Upper bound (90th percentile)
model_upper = GradientBoostingRegressor(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=4,
    min_samples_split=20,
    loss='quantile',
    alpha=0.9,  # 90th percentile
    random_state=42,
    verbose=0
)
model_upper.fit(X_train, y_train)
print("✓ Trained upper bound model (90th percentile)")

# Generate predictions with intervals on test set
y_test_lower = model_lower.predict(X_test)
y_test_upper = model_upper.predict(X_test)

# Calculate coverage (what % of actuals fall within the interval)
coverage = np.mean((y_test >= y_test_lower) & (y_test <= y_test_upper)) * 100
avg_interval_width = np.mean(y_test_upper - y_test_lower)

print(f"\nConfidence Interval Performance:")
print(f"  Coverage:              {coverage:>6.1f}% (target: 80%)")
print(f"  Avg interval width:    ${avg_interval_width:>10,.0f}")

print()

# ============================================================================
# STEP 8: VISUALIZATIONS
# ============================================================================

print("Step 8: Creating visualizations...")
print("-" * 70)

# 1. Predicted vs Actual
plt.figure(figsize=(10, 10))
plt.scatter(y_test, y_test_pred, alpha=0.3, s=10)
min_val = min(y_test.min(), y_test_pred.min())
max_val = max(y_test.max(), y_test_pred.max())
plt.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Perfect Prediction')
plt.xlabel('Actual Salary ($)')
plt.ylabel('Predicted Salary ($)')
plt.title(f'Predicted vs Actual Salaries (Test Set)\nR² = {test_metrics["r2"]:.4f}, MAE = ${test_metrics["mae"]:,.0f}')
plt.legend()
plt.tight_layout()
plt.savefig('predicted_vs_actual.png', dpi=150, bbox_inches='tight')
print("✓ Saved predicted_vs_actual.png")

# 2. Residual plot
plt.figure(figsize=(12, 6))
residuals = y_test - y_test_pred
plt.scatter(y_test_pred, residuals, alpha=0.3, s=10)
plt.axhline(y=0, color='r', linestyle='--', linewidth=2)
plt.xlabel('Predicted Salary ($)')
plt.ylabel('Residual ($)')
plt.title('Residual Plot')
plt.tight_layout()
plt.savefig('residuals.png', dpi=150, bbox_inches='tight')
print("✓ Saved residuals.png")

# 3. Error distribution
plt.figure(figsize=(12, 6))
plt.hist(test_results['abs_error'], bins=50, edgecolor='black')
plt.xlabel('Absolute Error ($)')
plt.ylabel('Frequency')
plt.title(f'Distribution of Prediction Errors (Test Set)\nMedian Error: ${test_results["abs_error"].median():,.0f}')
plt.axvline(test_results['abs_error'].median(), color='r', linestyle='--', linewidth=2, label='Median')
plt.legend()
plt.tight_layout()
plt.savefig('error_distribution.png', dpi=150, bbox_inches='tight')
print("✓ Saved error_distribution.png")

print()

# ============================================================================
# STEP 9: SAVE MODELS AND RESULTS
# ============================================================================

print("Step 9: Saving models and results...")
print("-" * 70)

# Save models
joblib.dump(model, 'salary_model_main.pkl')
print("✓ Saved salary_model_main.pkl")

joblib.dump(model_lower, 'salary_model_lower.pkl')
print("✓ Saved salary_model_lower.pkl")

joblib.dump(model_upper, 'salary_model_upper.pkl')
print("✓ Saved salary_model_upper.pkl")

# Save metrics
metrics_summary = {
    'baseline': {
        'mae': baseline_mae,
        'rmse': baseline_rmse,
        'r2': baseline_r2,
        'mape': baseline_mape
    },
    'gradient_boosting': {
        'train': train_metrics,
        'val': val_metrics,
        'test': test_metrics
    },
    'confidence_intervals': {
        'coverage': coverage,
        'avg_width': avg_interval_width
    },
    'feature_importance': feature_importance.to_dict('records')
}

joblib.dump(metrics_summary, 'model_metrics.pkl')
print("✓ Saved model_metrics.pkl")

# Save test predictions with intervals
test_predictions = pd.DataFrame({
    'id': df_model.loc[test_idx, 'id'].values,
    'canonical_role': df_model.loc[test_idx, 'canonical_role'].values,
    'actual_salary': y_test.values,
    'predicted_salary': y_test_pred,
    'lower_bound': y_test_lower,
    'upper_bound': y_test_upper,
    'error': y_test_pred - y_test.values,
    'abs_error': np.abs(y_test_pred - y_test.values),
    'pct_error': np.abs((y_test_pred - y_test.values) / y_test.values) * 100
})
test_predictions.to_csv('test_predictions.csv', index=False)
print("✓ Saved test_predictions.csv")

print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*70)
print("MODEL TRAINING COMPLETE!")
print("="*70)

print(f"\n📊 Final Model Performance (Test Set):")
print(f"  MAE:   ${test_metrics['mae']:>12,.0f}  {'✓ Excellent' if test_metrics['mae'] < 20000 else '✓ Good' if test_metrics['mae'] < 30000 else '⚠ Acceptable'}")
print(f"  RMSE:  ${test_metrics['rmse']:>12,.0f}")
print(f"  R²:    {test_metrics['r2']:>12.4f}  {'✓ Excellent' if test_metrics['r2'] > 0.80 else '✓ Good' if test_metrics['r2'] > 0.70 else '⚠ Acceptable'}")
print(f"  MAPE:  {test_metrics['mape']:>12.1f}%  {'✓ Excellent' if test_metrics['mape'] < 15 else '✓ Good' if test_metrics['mape'] < 25 else '⚠ Acceptable'}")

print(f"\n🎯 Improvement over Baseline:")
improvement = ((baseline_mae - test_metrics['mae']) / baseline_mae) * 100
print(f"  MAE reduction: {improvement:>6.1f}%")

print(f"\n📁 Files Created:")
print(f"  - salary_model_main.pkl (main prediction model)")
print(f"  - salary_model_lower.pkl (lower bound)")
print(f"  - salary_model_upper.pkl (upper bound)")
print(f"  - model_metrics.pkl (all metrics)")
print(f"  - test_predictions.csv (test set predictions)")
print(f"  - feature_importance.png")
print(f"  - predicted_vs_actual.png")
print(f"  - residuals.png")
print(f"  - error_distribution.png")

print(f"\n🚀 Next Steps:")
print(f"  1. Review visualizations to understand model behavior")
print(f"  2. Run inference on all archetypes (phase6_inference.py)")
print(f"  3. Generate salary predictions for unobserved job combinations")

print("\n" + "="*70)
