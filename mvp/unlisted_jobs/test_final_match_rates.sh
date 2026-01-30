#!/bin/bash
echo "==================================================================="
echo "FINAL MATCH RATES TEST (65 Canonical Roles)"
echo "==================================================================="
echo ""

for source in h1b_visa perm_visa ma_state_payroll; do
    echo "Testing $source..."
    DB_USER=noahhopkins python3 analyze_unmatched_all.py --source $source --limit 10000 2>&1 | grep -A 1 "SUMMARY" | tail -2
    echo ""
done
