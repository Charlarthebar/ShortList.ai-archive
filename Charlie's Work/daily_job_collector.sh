#!/bin/bash
# Daily Nationwide Job Collector
# 1. Collects new jobs from all sources
# 2. Updates job statuses (marks removed listings as "filled")
# 3. Cleans HTML and estimates salaries
# 4. Exports updated CSV

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/nationwide_daily_$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG_FILE"
echo "Nationwide Jobs Daily Collection" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

cd "$SCRIPT_DIR"

# Step 1: Collect new jobs
echo "" >> "$LOG_FILE"
echo "Step 1: Collecting jobs from all sources..." >> "$LOG_FILE"
python -u nationwide_jobs_collector.py >> "$LOG_FILE" 2>&1

# Step 2: Update salaries, clean HTML, mark filled jobs
echo "" >> "$LOG_FILE"
echo "Step 2: Updating database..." >> "$LOG_FILE"
python -u update_nationwide_jobs.py >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Completed: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Final stats
echo "" >> "$LOG_FILE"
sqlite3 nationwide_jobs.db "
SELECT 'Total jobs:', COUNT(*) FROM jobs;
SELECT 'Open jobs:', COUNT(*) FROM jobs WHERE status = 'active';
SELECT 'Filled jobs:', COUNT(*) FROM jobs WHERE status = 'filled';
" >> "$LOG_FILE"

find "$LOG_DIR" -name "nationwide_daily_*.log" -mtime +30 -delete 2>/dev/null
