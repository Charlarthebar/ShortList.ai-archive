#!/usr/bin/env python3
"""
Job Database Scheduler
======================

Scheduled job to keep the job database up-to-date by periodically
scraping all sources and updating the database.
"""

import os
import sys
import json
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from job_database_builder import JobDatabaseBuilder, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.json") -> Config:
    """Load configuration from file."""
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        return Config(**config_dict)
    return Config()


def update_job_database():
    """Update the job database by scraping all sources."""
    logger.info("Starting scheduled job database update")
    start_time = datetime.now()

    try:
        config = load_config()
        builder = JobDatabaseBuilder(config)
        builder.run()

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Job database update completed in {elapsed:.2f} seconds")
    except Exception as e:
        logger.error(f"Error during scheduled update: {e}", exc_info=True)


def main():
    """Run the scheduler."""
    config = load_config()

    scheduler = BlockingScheduler()

    # Schedule daily updates at 2 AM
    scheduler.add_job(
        update_job_database,
        trigger=CronTrigger(hour=2, minute=0),
        id='daily_job_update',
        name='Daily Job Database Update',
        replace_existing=True
    )

    # Schedule incremental updates every 6 hours
    scheduler.add_job(
        update_job_database,
        trigger=CronTrigger(hour='*/6'),
        id='incremental_update',
        name='Incremental Job Database Update',
        replace_existing=True
    )

    logger.info("Scheduler started. Jobs will run daily at 2 AM and every 6 hours.")
    logger.info("Press Ctrl+C to exit.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
