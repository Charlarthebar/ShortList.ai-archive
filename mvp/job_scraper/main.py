#!/usr/bin/env python3
"""
Multi-State Job Scraper - Main Orchestrator

Systematically scrapes job listings across multiple platforms
using geographic and industry-based iteration for 90%+ coverage.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from core import (
    Job, SearchQuery, ScrapeResult, StateResult,
    GroupIterator, load_state_config, list_available_states,
    JobDeduplicator
)
from scrapers import ScraperRegistry

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobScrapeOrchestrator:
    """Orchestrates job scraping across platforms and search groups."""

    def __init__(
        self,
        platforms: Optional[list[str]] = None,
        max_workers: int = 2,
        output_dir: Path = None
    ):
        self.platforms = platforms or ["indeed", "ziprecruiter"]
        self.max_workers = max_workers
        self.output_dir = output_dir or Path(__file__).parent / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize scrapers
        self.scrapers = {}
        for platform in self.platforms:
            scraper = ScraperRegistry.create(platform)
            if scraper:
                self.scrapers[platform] = scraper
            else:
                logger.warning(f"Unknown platform: {platform}")

    def scrape_state(self, state_name: str, progress_callback=None) -> StateResult:
        """Scrape all jobs for a single state."""
        start_time = time.time()
        logger.info(f"Starting scrape for {state_name}")

        # Load state config
        try:
            config = load_state_config(state_name)
        except FileNotFoundError as e:
            logger.error(str(e))
            return StateResult(
                state=state_name,
                state_abbrev="",
                total_jobs=0,
                unique_jobs=0,
                duplicates_removed=0,
                coverage_estimate=0.0,
                jobs=[],
                scrape_results=[],
                errors=[str(e)],
                duration_seconds=0
            )

        iterator = GroupIterator(config)
        deduplicator = JobDeduplicator()

        all_jobs: list[Job] = []
        all_results: list[ScrapeResult] = []
        all_errors: list[str] = []
        total_jobs_before_dedup = 0

        # Get all queries
        queries = list(iterator.iterate_all())
        total_queries = len(queries)
        logger.info(f"Total queries to execute: {total_queries}")

        # Process queries
        for i, query in enumerate(queries, 1):
            if progress_callback:
                progress_callback(i, total_queries, query)

            logger.info(f"[{i}/{total_queries}] Processing: {query.group_name} - {query.location}")

            # Execute on each platform
            for platform_name, scraper in self.scrapers.items():
                try:
                    result = scraper.search(query)
                    all_results.append(result)

                    if result.errors:
                        all_errors.extend(result.errors)

                    # Deduplicate jobs
                    for job in result.jobs:
                        total_jobs_before_dedup += 1
                        if deduplicator.add_job(job):
                            all_jobs.append(job)

                    logger.info(
                        f"  {platform_name}: {len(result.jobs)} jobs found, "
                        f"{deduplicator.get_stats()['total_unique_ids']} unique total"
                    )

                except Exception as e:
                    error_msg = f"Error with {platform_name}: {str(e)}"
                    logger.error(error_msg)
                    all_errors.append(error_msg)

        duration = time.time() - start_time
        duplicates_removed = total_jobs_before_dedup - len(all_jobs)

        # Estimate coverage
        estimated_total = config.config.get("estimated_total_jobs", 0)
        coverage = len(all_jobs) / estimated_total if estimated_total > 0 else 0

        result = StateResult(
            state=config.state,
            state_abbrev=config.state_abbrev,
            total_jobs=total_jobs_before_dedup,
            unique_jobs=len(all_jobs),
            duplicates_removed=duplicates_removed,
            coverage_estimate=coverage,
            jobs=all_jobs,
            scrape_results=all_results,
            errors=all_errors,
            duration_seconds=duration
        )

        # Save results
        self._save_results(result)

        return result

    def scrape_multiple_states(self, state_names: list[str]) -> dict[str, StateResult]:
        """Scrape multiple states sequentially."""
        results = {}
        for state_name in state_names:
            results[state_name] = self.scrape_state(state_name)
        return results

    def _save_results(self, result: StateResult):
        """Save scrape results to files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_slug = result.state.lower().replace(" ", "_")

        # Save jobs as JSON
        jobs_file = self.output_dir / f"{state_slug}_jobs_{timestamp}.json"
        jobs_data = {
            "state": result.state,
            "state_abbrev": result.state_abbrev,
            "scraped_at": result.scraped_at.isoformat(),
            "total_jobs": result.total_jobs,
            "unique_jobs": result.unique_jobs,
            "duplicates_removed": result.duplicates_removed,
            "coverage_estimate": result.coverage_estimate,
            "duration_seconds": result.duration_seconds,
            "jobs": [job.to_dict() for job in result.jobs]
        }

        with open(jobs_file, 'w') as f:
            json.dump(jobs_data, f, indent=2)

        logger.info(f"Saved {len(result.jobs)} jobs to {jobs_file}")

        # Save CSV for easy viewing
        csv_file = self.output_dir / f"{state_slug}_jobs_{timestamp}.csv"
        self._save_csv(result.jobs, csv_file)

        # Save summary
        summary_file = self.output_dir / f"{state_slug}_summary_{timestamp}.json"
        summary = {
            "state": result.state,
            "state_abbrev": result.state_abbrev,
            "scraped_at": result.scraped_at.isoformat(),
            "total_jobs_found": result.total_jobs,
            "unique_jobs": result.unique_jobs,
            "duplicates_removed": result.duplicates_removed,
            "coverage_estimate": f"{result.coverage_estimate:.1%}",
            "duration_seconds": result.duration_seconds,
            "errors_count": len(result.errors),
            "errors": result.errors[:20]  # First 20 errors
        }

        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

    def _save_csv(self, jobs: list[Job], filepath: Path):
        """Save jobs to CSV format."""
        import csv

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'title', 'company', 'location', 'platform', 'url',
                'salary_min', 'salary_max', 'salary_type', 'job_type',
                'remote', 'posted_date', 'search_group'
            ])

            for job in jobs:
                writer.writerow([
                    job.title,
                    job.company,
                    job.location,
                    job.platform,
                    job.url,
                    job.salary_min,
                    job.salary_max,
                    job.salary_type,
                    job.job_type,
                    job.remote,
                    job.posted_date.isoformat() if job.posted_date else '',
                    job.search_group
                ])

        logger.info(f"Saved CSV to {filepath}")


def print_progress(current: int, total: int, query: SearchQuery):
    """Print progress to console."""
    pct = (current / total) * 100
    print(f"\r[{current}/{total}] ({pct:.1f}%) {query.group_name}: {query.location}", end="", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape job listings across multiple platforms and states"
    )
    parser.add_argument(
        "--state", "-s",
        action="append",
        dest="states",
        help="State to scrape (can be specified multiple times)"
    )
    parser.add_argument(
        "--all-states",
        action="store_true",
        help="Scrape all configured states"
    )
    parser.add_argument(
        "--list-states",
        action="store_true",
        help="List available state configurations"
    )
    parser.add_argument(
        "--platforms", "-p",
        nargs="+",
        default=["activejobsdb", "usajobs"],
        help="Platforms to scrape (default: activejobsdb usajobs). "
             "Available: activejobsdb (RapidAPI), indeed, ziprecruiter, linkedin, glassdoor, "
             "simplyhired, usajobs, govtjobs, dice, higheredjobs. "
             "Note: activejobsdb requires RAPIDAPI_KEY in .env"
    )
    parser.add_argument(
        "--all-platforms",
        action="store_true",
        help="Scrape all available platforms"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output directory for results"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # List available states
    if args.list_states:
        states = list_available_states()
        print("Available state configurations:")
        for state in states:
            print(f"  - {state}")
        return 0

    # Determine which states to scrape
    if args.all_states:
        states = list_available_states()
    elif args.states:
        states = args.states
    else:
        parser.print_help()
        print("\nError: Please specify --state or --all-states")
        return 1

    # Determine which platforms to use
    ALL_PLATFORMS = [
        # Aggregator APIs (most reliable, requires RapidAPI subscription)
        "activejobsdb",
        # General job boards (may get blocked)
        "indeed", "ziprecruiter", "linkedin", "glassdoor", "simplyhired",
        # Government
        "usajobs", "govtjobs",
        # Specialized boards
        "dice", "higheredjobs",
        # Major NC employers
        "bofa", "wellsfargo", "lowes", "atrium", "truist", "fidelity",
        # Healthcare/Education
        "duke", "unchealth", "healthecareers", "nursingjobs",
        # Tech companies
        "redhat", "epicgames", "pendo", "bandwidth", "sas",
    ]
    if args.all_platforms:
        platforms = ALL_PLATFORMS
    else:
        platforms = args.platforms

    # Create orchestrator
    orchestrator = JobScrapeOrchestrator(
        platforms=platforms,
        output_dir=args.output
    )

    # Run scraping
    print(f"\nScraping jobs for {len(states)} state(s): {', '.join(states)}")
    print(f"Platforms: {', '.join(platforms)}")
    print("-" * 50)

    total_jobs = 0
    for state in states:
        print(f"\n{'='*50}")
        print(f"Processing: {state}")
        print("="*50)

        result = orchestrator.scrape_state(state, progress_callback=print_progress)
        print()  # New line after progress

        print(f"\nResults for {state}:")
        print(f"  Total jobs found: {result.total_jobs:,}")
        print(f"  Unique jobs: {result.unique_jobs:,}")
        print(f"  Duplicates removed: {result.duplicates_removed:,}")
        print(f"  Coverage estimate: {result.coverage_estimate:.1%}")
        print(f"  Duration: {result.duration_seconds:.1f}s")
        print(f"  Errors: {len(result.errors)}")

        total_jobs += result.unique_jobs

    print("\n" + "="*50)
    print(f"TOTAL UNIQUE JOBS COLLECTED: {total_jobs:,}")
    print("="*50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
