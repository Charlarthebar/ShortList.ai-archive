#!/usr/bin/env python3
"""
Rippling ATS Connector
======================

Fetches job postings from Rippling-powered career sites.

Rippling provides a public JSON API at:
https://api.rippling.com/platform/api/ats/v1/board/{company_slug}/jobs

Author: ShortList.ai
Date: 2026-01-16
"""

import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class RipplingConnector(BaseATSConnector):
    """
    Connector for Rippling ATS.

    Rippling exposes a public API for job boards:
    - List jobs: GET /platform/api/ats/v1/board/{company_slug}/jobs

    No authentication required for public job board data.
    """

    ATS_TYPE = "rippling"
    API_BASE = "https://api.rippling.com/platform/api/ats/v1/board"

    def __init__(self, company_id: str, company_name: str = None):
        """
        Initialize Rippling connector.

        Args:
            company_id: The company's Rippling board slug (appears in URL)
                        e.g., "rippling" for ats.rippling.com/rippling
            company_name: Human-readable company name (optional)
        """
        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.replace("-", " ").title(),
            base_url=f"{self.API_BASE}/{company_id}"
        )
        self.careers_url = f"https://ats.rippling.com/{company_id}"

    def _get_default_base_url(self) -> str:
        return f"{self.API_BASE}/{self.company_id}"

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from Rippling.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        try:
            url = f"{self.base_url}/jobs"
            logger.info(f"Fetching jobs from: {url}")

            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()

            # Rippling returns a list directly
            if isinstance(data, list):
                raw_jobs = data
            else:
                raw_jobs = data.get("results", data.get("jobs", []))

            logger.info(f"Found {len(raw_jobs)} jobs at {self.company_name}")

            for raw_job in raw_jobs:
                try:
                    posting = self.parse_job(raw_job)
                    if posting:
                        jobs.append(posting)
                except Exception as e:
                    logger.warning(f"Error parsing job {raw_job.get('uuid')}: {e}")
                    continue

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Company '{self.company_id}' not found on Rippling")
            else:
                logger.error(f"HTTP error fetching Rippling jobs: {e}")
        except Exception as e:
            logger.error(f"Error fetching Rippling jobs: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw Rippling job data into JobPosting format.

        Rippling job structure:
        {
            "uuid": "88d5c37a-49de-4e5f-ae52-12cb961a14ba",
            "name": "Head of Sales",
            "department": {
                "id": "Sales",
                "label": "Sales"
            },
            "url": "https://ats.rippling.com/framework/jobs/88d5c37a-49de-4e5f-ae52-12cb961a14ba",
            "workLocation": {
                "label": "Remote (United States)",
                "id": "Remote (United States)"
            }
        }
        """
        if not raw_job:
            return None

        job_id = raw_job.get("uuid", "")
        if not job_id:
            return None

        # Extract location
        work_location = raw_job.get("workLocation", {})
        location_name = work_location.get("label", "") if isinstance(work_location, dict) else str(work_location)
        location_info = self._extract_location(location_name)

        # Check for remote in location
        is_remote = location_info.get("is_remote", False) or "remote" in location_name.lower()

        # Parse department
        department_data = raw_job.get("department", {})
        department = department_data.get("label") if isinstance(department_data, dict) else None

        # Get URL
        job_url = raw_job.get("url") or self.get_job_url(job_id)

        return JobPosting(
            external_id=f"rippling_{self.company_id}_{job_id}",
            title=raw_job.get("name", "Unknown Title"),
            company_name=self.company_name,
            location_raw=location_name,
            city=location_info.get("city"),
            state=location_info.get("state"),
            is_remote=is_remote,
            description=None,  # Rippling basic API doesn't include description
            department=department,
            posted_date=None,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def get_job_url(self, job_id: str) -> str:
        """Generate public URL for a Rippling job."""
        return f"{self.careers_url}/jobs/{job_id}"

    @classmethod
    def discover_company(cls, company_id: str) -> bool:
        """
        Check if a company uses Rippling.

        Returns:
            True if company has a Rippling board, False otherwise
        """
        try:
            url = f"{cls.API_BASE}/{company_id}/jobs"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Check if it's a non-empty list or has jobs
                if isinstance(data, list):
                    return len(data) > 0
                return bool(data.get("results") or data.get("jobs"))
            return False
        except:
            return False


def test_rippling():
    """Test the Rippling connector with a known company."""
    logging.basicConfig(level=logging.INFO)

    # Test with Rippling itself
    connector = RipplingConnector("rippling", "Rippling")
    print(f"\nTesting {connector}")

    jobs = connector.fetch_jobs()
    print(f"\nFetched {len(jobs)} jobs from Rippling")

    if jobs:
        print("\nSample job:")
        sample = jobs[0]
        print(f"  ID: {sample.external_id}")
        print(f"  Title: {sample.title}")
        print(f"  Location: {sample.location_raw}")
        print(f"  City: {sample.city}, State: {sample.state}")
        print(f"  Remote: {sample.is_remote}")
        print(f"  Department: {sample.department}")
        print(f"  URL: {sample.url}")


if __name__ == "__main__":
    test_rippling()
