#!/bin/bash
# Daily Cambridge Jobs Collector
# 1. Collects new jobs from all sources
# 2. Updates job statuses (marks removed listings as "filled")
# 3. Exports updated CSV

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/cambridge_daily_$TIMESTAMP.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log start
echo "========================================" >> "$LOG_FILE"
echo "Cambridge Jobs Daily Collection" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Change to script directory
cd "$SCRIPT_DIR"

# Step 1: Collect new jobs
echo "" >> "$LOG_FILE"
echo "Step 1: Collecting jobs from all sources..." >> "$LOG_FILE"
python -u cambridge_jobs_collector.py >> "$LOG_FILE" 2>&1

# Step 2: Update salaries and statuses
echo "" >> "$LOG_FILE"
echo "Step 2: Updating salaries and job statuses..." >> "$LOG_FILE"
python -u update_cambridge_jobs.py >> "$LOG_FILE" 2>&1

# Log completion
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Collection Completed: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Show final stats
echo "" >> "$LOG_FILE"
echo "Final Database Stats:" >> "$LOG_FILE"
sqlite3 cambridge_jobs.db "
SELECT 'Total jobs:', COUNT(*) FROM jobs;
SELECT 'Open jobs:', COUNT(*) FROM jobs WHERE status = 'active';
SELECT 'Filled jobs:', COUNT(*) FROM jobs WHERE status = 'filled';
SELECT 'With salary:', COUNT(*) FROM jobs WHERE salary_min IS NOT NULL AND salary_min > 0;
" >> "$LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "cambridge_daily_*.log" -mtime +30 -delete 2>/dev/null

echo "Log saved to: $LOG_FILE"
