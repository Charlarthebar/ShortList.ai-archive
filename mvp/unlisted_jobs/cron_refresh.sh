#!/bin/bash
#
# Cron job script for refreshing job postings
#
# Add to crontab with:
#   crontab -e
#   0 */6 * * * /Users/noahhopkins/ShortList.ai/unlisted_jobs/cron_refresh.sh >> /var/log/shortlist_refresh.log 2>&1
#
# Or for local development:
#   0 */6 * * * /Users/noahhopkins/ShortList.ai/unlisted_jobs/cron_refresh.sh >> ~/Library/Logs/shortlist_refresh.log 2>&1

cd /Users/noahhopkins/ShortList.ai/unlisted_jobs

echo "========================================"
echo "Starting refresh at $(date)"
echo "========================================"

# Refresh all ATS types
python3 refresh_postings.py --ats greenhouse
python3 refresh_postings.py --ats lever
python3 refresh_postings.py --ats smartrecruiters

# Detect new openings and record events
python3 detect_openings.py --hours 7

# Send notifications for new openings
python3 send_notifications.py

echo "========================================"
echo "Refresh complete at $(date)"
echo "========================================"
