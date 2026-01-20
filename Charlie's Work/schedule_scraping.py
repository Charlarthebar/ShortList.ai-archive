#!/usr/bin/env python3
"""
Job Scraping Scheduler
======================

This script sets up scheduled scraping for job data collection:
- DAILY: Active job listings from 6 sources (both Cambridge area AND nationwide)
  1. Cambridge Area Collector -> cambridge_jobs.db (filtered to 10-mile radius + remote)
  2. Nationwide Collector -> nationwide_jobs.db (all US jobs)

  Sources: Adzuna, USAJOBS, The Muse, Coresignal, RemoteOK, Remotive

- WEEKLY: Unlisted/filled jobs from BLS, Form 990, state payroll, and historical tracking

Scheduling Methods:
1. cron (Linux/macOS) - Traditional, runs even when logged out
2. launchd (macOS) - Native macOS scheduler
3. Built-in loop (fallback) - Runs in foreground

Usage:
    python schedule_scraping.py --setup-cron     # Set up cron jobs
    python schedule_scraping.py --setup-launchd  # Set up macOS launchd
    python schedule_scraping.py --run-daily      # Run daily job scraper now
    python schedule_scraping.py --run-weekly     # Run weekly unlisted collector now
    python schedule_scraping.py --run-loop       # Run in continuous loop mode
    python schedule_scraping.py --status         # Check schedule status
"""

import os
import sys
import subprocess
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('schedule.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent.absolute()
# Use the actual Python path, not the shim (fixes pyenv compatibility)
PYTHON_PATH = os.path.realpath(sys.executable)
# Daily scripts: Cambridge collector first, then nationwide
CAMBRIDGE_SCRIPT = SCRIPT_DIR / "cambridge_jobs_collector.py"
NATIONWIDE_SCRIPT = SCRIPT_DIR / "nationwide_jobs_collector.py"
WEEKLY_SCRIPT = SCRIPT_DIR / "unlisted_jobs_collector.py"
DB_PATH = SCRIPT_DIR / "cambridge_jobs.db"
LOG_DIR = SCRIPT_DIR / "logs"


def ensure_log_dir():
    """Create logs directory if it doesn't exist."""
    LOG_DIR.mkdir(exist_ok=True)


def run_daily_scraper():
    """
    Run the daily job scrapers.

    Runs BOTH collectors sequentially:
    1. Cambridge Area Jobs Collector (6 sources, filtered to Cambridge area + remote)
    2. Nationwide Jobs Collector (6 sources, all US jobs)

    Sources:
    - Adzuna API
    - USAJOBS API
    - The Muse API
    - Coresignal API
    - RemoteOK API
    - Remotive API
    """
    logger.info("="*60)
    logger.info("DAILY SCRAPER - Collecting active job listings")
    logger.info("="*60)

    ensure_log_dir()
    log_file = LOG_DIR / f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    overall_success = True
    log_content = []
    log_content.append(f"=== Daily Scraper Run: {datetime.now()} ===\n")

    # =========================================================
    # 1. CAMBRIDGE AREA JOBS COLLECTOR
    # =========================================================
    logger.info("Step 1/2: Running Cambridge Area Jobs Collector...")
    log_content.append("\n" + "="*60)
    log_content.append("STEP 1: CAMBRIDGE AREA JOBS COLLECTOR")
    log_content.append("="*60 + "\n")

    try:
        if CAMBRIDGE_SCRIPT.exists():
            result = subprocess.run(
                [PYTHON_PATH, str(CAMBRIDGE_SCRIPT)],
                cwd=str(SCRIPT_DIR),
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )

            log_content.append("STDOUT:\n")
            log_content.append(result.stdout)
            log_content.append("\nSTDERR:\n")
            log_content.append(result.stderr)

            if result.returncode == 0:
                logger.info("Cambridge collector completed successfully")
            else:
                logger.error(f"Cambridge collector failed with code {result.returncode}")
                overall_success = False
        else:
            logger.error(f"Cambridge script not found: {CAMBRIDGE_SCRIPT}")
            log_content.append(f"ERROR: Cambridge script not found: {CAMBRIDGE_SCRIPT}\n")
            overall_success = False

    except subprocess.TimeoutExpired:
        logger.error("Cambridge collector timed out after 30 minutes")
        log_content.append("ERROR: Cambridge collector timed out after 30 minutes\n")
        overall_success = False
    except Exception as e:
        logger.error(f"Error running Cambridge collector: {e}")
        log_content.append(f"ERROR: {e}\n")
        overall_success = False

    # =========================================================
    # 2. NATIONWIDE JOBS COLLECTOR
    # =========================================================
    logger.info("Step 2/2: Running Nationwide Jobs Collector...")
    log_content.append("\n" + "="*60)
    log_content.append("STEP 2: NATIONWIDE JOBS COLLECTOR")
    log_content.append("="*60 + "\n")

    try:
        if NATIONWIDE_SCRIPT.exists():
            result = subprocess.run(
                [PYTHON_PATH, str(NATIONWIDE_SCRIPT)],
                cwd=str(SCRIPT_DIR),
                capture_output=True,
                text=True,
                timeout=3600  # 60 minute timeout (nationwide takes longer)
            )

            log_content.append("STDOUT:\n")
            log_content.append(result.stdout)
            log_content.append("\nSTDERR:\n")
            log_content.append(result.stderr)

            if result.returncode == 0:
                logger.info("Nationwide collector completed successfully")
            else:
                logger.error(f"Nationwide collector failed with code {result.returncode}")
                overall_success = False
        else:
            logger.error(f"Nationwide script not found: {NATIONWIDE_SCRIPT}")
            log_content.append(f"ERROR: Nationwide script not found: {NATIONWIDE_SCRIPT}\n")
            overall_success = False

    except subprocess.TimeoutExpired:
        logger.error("Nationwide collector timed out after 60 minutes")
        log_content.append("ERROR: Nationwide collector timed out after 60 minutes\n")
        overall_success = False
    except Exception as e:
        logger.error(f"Error running Nationwide collector: {e}")
        log_content.append(f"ERROR: {e}\n")
        overall_success = False

    # Write combined log
    with open(log_file, 'w') as f:
        f.write('\n'.join(log_content))

    if overall_success:
        logger.info(f"Daily scraper completed successfully. Log: {log_file}")
    else:
        logger.error(f"Daily scraper completed with errors. Log: {log_file}")

    return overall_success


def run_weekly_collector():
    """
    Run the weekly unlisted jobs collector.

    This collects data about FILLED/UNLISTED jobs from:
    - BLS OEWS (employment by occupation)
    - IRS Form 990 (nonprofit employees)
    - MA State Payroll (government jobs)
    - Historical tracking (mark stale jobs as filled)
    """
    logger.info("="*60)
    logger.info("WEEKLY COLLECTOR - Collecting unlisted/filled jobs data")
    logger.info("="*60)

    ensure_log_dir()
    log_file = LOG_DIR / f"weekly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    try:
        if WEEKLY_SCRIPT.exists():
            result = subprocess.run(
                [PYTHON_PATH, str(WEEKLY_SCRIPT)],
                cwd=str(SCRIPT_DIR),
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )

            # Log output
            with open(log_file, 'w') as f:
                f.write(f"=== Weekly Collector Run: {datetime.now()} ===\n\n")
                f.write("STDOUT:\n")
                f.write(result.stdout)
                f.write("\n\nSTDERR:\n")
                f.write(result.stderr)

            if result.returncode == 0:
                logger.info(f"Weekly collector completed successfully. Log: {log_file}")
            else:
                logger.error(f"Weekly collector failed with code {result.returncode}")

            return result.returncode == 0
        else:
            logger.error(f"Weekly script not found: {WEEKLY_SCRIPT}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("Weekly collector timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"Error running weekly collector: {e}")
        return False


def setup_cron():
    """
    Set up cron jobs for scheduled scraping.

    Schedule:
    - Daily scraper: Every day at 6 AM
    - Weekly collector: Every Sunday at 3 AM
    """
    logger.info("Setting up cron jobs...")

    # Generate cron entries
    daily_cron = f"0 6 * * * cd {SCRIPT_DIR} && {PYTHON_PATH} {SCRIPT_DIR}/schedule_scraping.py --run-daily >> {LOG_DIR}/cron_daily.log 2>&1"
    weekly_cron = f"0 3 * * 0 cd {SCRIPT_DIR} && {PYTHON_PATH} {SCRIPT_DIR}/schedule_scraping.py --run-weekly >> {LOG_DIR}/cron_weekly.log 2>&1"

    cron_content = f"""# ShortList.ai Job Scraping Schedule
# Added by schedule_scraping.py on {datetime.now()}
#
# Daily scraper (6 AM every day) - Collects active job listings
{daily_cron}
#
# Weekly collector (3 AM every Sunday) - Collects unlisted/filled jobs
{weekly_cron}
"""

    print("\n" + "="*60)
    print("CRON SETUP INSTRUCTIONS")
    print("="*60)
    print("\nAdd these lines to your crontab:")
    print("  1. Run: crontab -e")
    print("  2. Add these lines:\n")
    print(cron_content)
    print("\nAlternatively, run this command:")
    print(f"  (crontab -l 2>/dev/null; echo '{daily_cron}'; echo '{weekly_cron}') | crontab -")
    print("\nTo verify, run: crontab -l")
    print("="*60)

    # Save cron content to file for reference
    cron_file = SCRIPT_DIR / "cron_jobs.txt"
    with open(cron_file, 'w') as f:
        f.write(cron_content)
    logger.info(f"Cron configuration saved to: {cron_file}")


def setup_launchd():
    """
    Set up macOS launchd jobs for scheduled scraping.

    Creates plist files in ~/Library/LaunchAgents/
    """
    logger.info("Setting up macOS launchd jobs...")

    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    launch_agents_dir.mkdir(parents=True, exist_ok=True)

    # Daily scraper plist
    daily_plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.shortlist.daily-scraper</string>

    <key>ProgramArguments</key>
    <array>
        <string>{PYTHON_PATH}</string>
        <string>{SCRIPT_DIR}/schedule_scraping.py</string>
        <string>--run-daily</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{SCRIPT_DIR}</string>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>{LOG_DIR}/launchd_daily.log</string>

    <key>StandardErrorPath</key>
    <string>{LOG_DIR}/launchd_daily_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
"""

    # Weekly collector plist
    weekly_plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.shortlist.weekly-collector</string>

    <key>ProgramArguments</key>
    <array>
        <string>{PYTHON_PATH}</string>
        <string>{SCRIPT_DIR}/schedule_scraping.py</string>
        <string>--run-weekly</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{SCRIPT_DIR}</string>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key>
        <integer>0</integer>
        <key>Hour</key>
        <integer>3</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>{LOG_DIR}/launchd_weekly.log</string>

    <key>StandardErrorPath</key>
    <string>{LOG_DIR}/launchd_weekly_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
"""

    ensure_log_dir()

    # Write plist files
    daily_plist_path = launch_agents_dir / "com.shortlist.daily-scraper.plist"
    weekly_plist_path = launch_agents_dir / "com.shortlist.weekly-collector.plist"

    with open(daily_plist_path, 'w') as f:
        f.write(daily_plist)

    with open(weekly_plist_path, 'w') as f:
        f.write(weekly_plist)

    print("\n" + "="*60)
    print("LAUNCHD SETUP COMPLETE")
    print("="*60)
    print(f"\nCreated plist files:")
    print(f"  - {daily_plist_path}")
    print(f"  - {weekly_plist_path}")
    print("\nTo activate the schedules, run:")
    print(f"  launchctl load {daily_plist_path}")
    print(f"  launchctl load {weekly_plist_path}")
    print("\nTo check status:")
    print("  launchctl list | grep shortlist")
    print("\nTo unload:")
    print(f"  launchctl unload {daily_plist_path}")
    print(f"  launchctl unload {weekly_plist_path}")
    print("\nSchedule:")
    print("  - Daily scraper: Every day at 6:00 AM")
    print("  - Weekly collector: Every Sunday at 3:00 AM")
    print("="*60)

    logger.info("launchd configuration complete")


def run_loop():
    """
    Run in continuous loop mode (fallback for systems without cron/launchd).

    This keeps running and executes scrapers on schedule.
    """
    logger.info("Starting continuous loop scheduler...")
    print("\n" + "="*60)
    print("CONTINUOUS LOOP SCHEDULER")
    print("="*60)
    print("Running scrapers on schedule. Press Ctrl+C to stop.")
    print("Schedule:")
    print("  - Daily scraper: Every 24 hours")
    print("  - Weekly collector: Every 7 days")
    print("="*60 + "\n")

    last_daily = None
    last_weekly = None

    # State file to persist run times across restarts
    state_file = SCRIPT_DIR / ".scheduler_state.json"

    # Load previous state
    if state_file.exists():
        try:
            with open(state_file) as f:
                state = json.load(f)
                if state.get('last_daily'):
                    last_daily = datetime.fromisoformat(state['last_daily'])
                if state.get('last_weekly'):
                    last_weekly = datetime.fromisoformat(state['last_weekly'])
            logger.info(f"Loaded state: daily={last_daily}, weekly={last_weekly}")
        except Exception as e:
            logger.warning(f"Could not load state: {e}")

    def save_state():
        with open(state_file, 'w') as f:
            json.dump({
                'last_daily': last_daily.isoformat() if last_daily else None,
                'last_weekly': last_weekly.isoformat() if last_weekly else None,
            }, f)

    try:
        while True:
            now = datetime.now()

            # Check if daily scraper should run
            should_run_daily = (
                last_daily is None or
                (now - last_daily) >= timedelta(hours=24)
            )

            # Check if weekly collector should run
            should_run_weekly = (
                last_weekly is None or
                (now - last_weekly) >= timedelta(days=7)
            )

            if should_run_daily:
                logger.info("Running daily scraper...")
                if run_daily_scraper():
                    last_daily = now
                    save_state()

            if should_run_weekly:
                logger.info("Running weekly collector...")
                if run_weekly_collector():
                    last_weekly = now
                    save_state()

            # Calculate time until next check (every hour)
            next_check = now + timedelta(hours=1)
            sleep_seconds = (next_check - datetime.now()).total_seconds()

            logger.info(f"Next check in {sleep_seconds/60:.0f} minutes. "
                       f"Daily last ran: {last_daily}, Weekly last ran: {last_weekly}")

            time.sleep(max(60, sleep_seconds))  # At least 1 minute

    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        save_state()


def check_status():
    """Check the status of scheduled jobs and recent runs."""
    print("\n" + "="*60)
    print("SCRAPING SCHEDULE STATUS")
    print("="*60)

    # Check for cron jobs
    print("\n--- CRON JOBS ---")
    try:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        if 'shortlist' in result.stdout.lower() or 'schedule_scraping' in result.stdout:
            print("Found ShortList cron jobs:")
            for line in result.stdout.split('\n'):
                if 'shortlist' in line.lower() or 'schedule_scraping' in line:
                    print(f"  {line}")
        else:
            print("No ShortList cron jobs found")
    except:
        print("Could not check cron")

    # Check for launchd jobs
    print("\n--- LAUNCHD JOBS ---")
    try:
        result = subprocess.run(['launchctl', 'list'], capture_output=True, text=True)
        if 'shortlist' in result.stdout.lower():
            print("Found ShortList launchd jobs:")
            for line in result.stdout.split('\n'):
                if 'shortlist' in line.lower():
                    print(f"  {line}")
        else:
            print("No ShortList launchd jobs loaded")
    except:
        print("Could not check launchd")

    # Check recent logs
    print("\n--- RECENT LOGS ---")
    if LOG_DIR.exists():
        logs = sorted(LOG_DIR.glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True)[:5]
        if logs:
            for log in logs:
                mtime = datetime.fromtimestamp(log.stat().st_mtime)
                print(f"  {log.name}: {mtime}")
        else:
            print("No log files found")
    else:
        print("Log directory does not exist")

    # Check database status
    print("\n--- DATABASE STATUS ---")

    # Cambridge database
    cambridge_db = SCRIPT_DIR / "cambridge_jobs.db"
    print("\nCambridge Jobs Database:")
    if cambridge_db.exists():
        import sqlite3
        conn = sqlite3.connect(cambridge_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='active'")
        active = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_remote=1")
        remote = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(last_seen) FROM jobs")
        last_seen = cursor.fetchone()[0]

        print(f"  Active jobs: {active}")
        print(f"  Remote jobs: {remote}")
        print(f"  Last update: {last_seen}")

        conn.close()
    else:
        print("  Database not found")

    # Nationwide database
    nationwide_db = SCRIPT_DIR / "nationwide_jobs.db"
    print("\nNationwide Jobs Database:")
    if nationwide_db.exists():
        import sqlite3
        conn = sqlite3.connect(nationwide_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='active'")
        active = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE status='filled'")
        filled = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(last_seen) FROM jobs")
        last_seen = cursor.fetchone()[0]

        print(f"  Active jobs: {active}")
        print(f"  Filled jobs: {filled}")
        print(f"  Last update: {last_seen}")

        conn.close()
    else:
        print("  Database not found")

    print("\n" + "="*60)


def main():
    parser = argparse.ArgumentParser(description="Job Scraping Scheduler")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--setup-cron', action='store_true',
                       help='Show cron setup instructions')
    group.add_argument('--setup-launchd', action='store_true',
                       help='Set up macOS launchd jobs')
    group.add_argument('--run-daily', action='store_true',
                       help='Run daily job scraper now')
    group.add_argument('--run-weekly', action='store_true',
                       help='Run weekly unlisted collector now')
    group.add_argument('--run-loop', action='store_true',
                       help='Run in continuous loop mode')
    group.add_argument('--status', action='store_true',
                       help='Check schedule status')

    args = parser.parse_args()

    if args.setup_cron:
        setup_cron()
    elif args.setup_launchd:
        setup_launchd()
    elif args.run_daily:
        success = run_daily_scraper()
        sys.exit(0 if success else 1)
    elif args.run_weekly:
        success = run_weekly_collector()
        sys.exit(0 if success else 1)
    elif args.run_loop:
        run_loop()
    elif args.status:
        check_status()


if __name__ == "__main__":
    main()
