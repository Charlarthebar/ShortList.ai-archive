#!/usr/bin/env python3
"""
Greenhouse ATS Connector
========================

Fetches job postings from Greenhouse-powered career sites.

Greenhouse provides a public JSON API at:
https://boards-api.greenhouse.io/v1/boards/{company}/jobs

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
import time

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class GreenhouseConnector(BaseATSConnector):
    """
    Connector for Greenhouse ATS.

    Greenhouse exposes a public API for job boards:
    - List jobs: GET /v1/boards/{company}/jobs
    - Job details: GET /v1/boards/{company}/jobs/{id}

    Documentation: https://developers.greenhouse.io/job-board.html
    """

    ATS_TYPE = "greenhouse"
    API_BASE = "https://boards-api.greenhouse.io/v1/boards"

    def __init__(self, company_id: str, company_name: str = None):
        """
        Initialize Greenhouse connector.

        Args:
            company_id: The company's Greenhouse board token (appears in URL)
                        e.g., "stripe" for boards.greenhouse.io/stripe
            company_name: Human-readable company name (optional, defaults to company_id)
        """
        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.title(),
            base_url=f"{self.API_BASE}/{company_id}"
        )
        self.careers_url = f"https://boards.greenhouse.io/{company_id}"

    def _get_default_base_url(self) -> str:
        return f"{self.API_BASE}/{self.company_id}"

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from Greenhouse.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        try:
            # Fetch job list with content (descriptions)
            url = f"{self.base_url}/jobs?content=true"
            logger.info(f"Fetching jobs from: {url}")

            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            raw_jobs = data.get("jobs", [])

            logger.info(f"Found {len(raw_jobs)} jobs at {self.company_name}")

            for raw_job in raw_jobs:
                try:
                    posting = self.parse_job(raw_job)
                    if posting:
                        jobs.append(posting)
                except Exception as e:
                    logger.warning(f"Error parsing job {raw_job.get('id')}: {e}")
                    continue

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Company '{self.company_id}' not found on Greenhouse")
            else:
                logger.error(f"HTTP error fetching Greenhouse jobs: {e}")
        except Exception as e:
            logger.error(f"Error fetching Greenhouse jobs: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw Greenhouse job data into JobPosting format.

        Greenhouse job structure:
        {
            "id": 123456,
            "title": "Software Engineer",
            "location": {"name": "San Francisco, CA"},
            "content": "<p>Job description HTML...</p>",
            "departments": [{"name": "Engineering"}],
            "offices": [{"name": "San Francisco", "location": "CA, USA"}],
            "metadata": [...],
            "updated_at": "2024-01-15T12:00:00Z",
            "absolute_url": "https://boards.greenhouse.io/..."
        }
        """
        if not raw_job:
            return None

        job_id = str(raw_job.get("id", ""))
        if not job_id:
            return None

        # Extract location
        location_data = raw_job.get("location", {})
        location_name = location_data.get("name", "") if isinstance(location_data, dict) else str(location_data)
        location_info = self._extract_location(location_name)

        # Parse department
        departments = raw_job.get("departments", [])
        department = departments[0].get("name") if departments else None

        # Parse description (HTML)
        description_html = raw_job.get("content", "")
        description_text = self._clean_html(description_html)

        # Parse posted/updated date
        updated_at = raw_job.get("updated_at")
        posted_date = None
        if updated_at:
            try:
                posted_date = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            except ValueError:
                pass

        # Get URL
        job_url = raw_job.get("absolute_url", self.get_job_url(job_id))

        # Check for salary in metadata (some companies include it)
        salary_info = self._extract_salary_from_metadata(raw_job.get("metadata", []))

        return JobPosting(
            external_id=f"gh_{self.company_id}_{job_id}",
            title=raw_job.get("title", "Unknown Title"),
            company_name=self.company_name,
            location_raw=location_name,
            city=location_info.get("city"),
            state=location_info.get("state"),
            is_remote=location_info.get("is_remote", False),
            description=description_text,
            department=department,
            salary_min=salary_info.get("min"),
            salary_max=salary_info.get("max"),
            salary_currency=salary_info.get("currency", "USD"),
            salary_period=salary_info.get("period", "annual"),
            posted_date=posted_date,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def _extract_salary_from_metadata(self, metadata: List[Dict]) -> Dict[str, Any]:
        """
        Extract salary from Greenhouse metadata field.

        Some companies put salary info in custom metadata fields.
        """
        salary_info = {"min": None, "max": None, "currency": "USD", "period": "annual"}

        if not metadata:
            return salary_info

        for item in metadata:
            name = item.get("name", "").lower()
            value = item.get("value", "")

            if "salary" in name or "compensation" in name or "pay" in name:
                parsed = self._parse_salary(str(value))
                if parsed["min"]:
                    salary_info = parsed
                    break

        return salary_info

    def get_job_url(self, job_id: str) -> str:
        """Generate public URL for a Greenhouse job."""
        return f"{self.careers_url}/jobs/{job_id}"

    @classmethod
    def discover_company(cls, company_id: str) -> bool:
        """
        Check if a company uses Greenhouse.

        Returns:
            True if company has a Greenhouse board, False otherwise
        """
        try:
            url = f"{cls.API_BASE}/{company_id}/jobs"
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False


# ============================================================================
# EXAMPLE COMPANIES USING GREENHOUSE
# ============================================================================

# Major tech companies known to use Greenhouse:
GREENHOUSE_COMPANIES = {
    # Company ID (board token): Company Name
    "stripe": "Stripe",
    "airbnb": "Airbnb",
    "figma": "Figma",
    "notion": "Notion",
    "doordash": "DoorDash",
    "instacart": "Instacart",
    "slack": "Slack",
    "dropbox": "Dropbox",
    "coinbase": "Coinbase",
    "square": "Square",
    "robinhood": "Robinhood",
    "plaid": "Plaid",
    "mongodb": "MongoDB",
    "twilio": "Twilio",
    "hashicorp": "HashiCorp",
    "gitlab": "GitLab",
    "airtable": "Airtable",
    "confluent": "Confluent",
    "datadog": "Datadog",
    "snowflake": "Snowflake",
}


def test_greenhouse():
    """Test the Greenhouse connector with a known company."""
    logging.basicConfig(level=logging.INFO)

    # Test with Stripe (well-known Greenhouse user)
    connector = GreenhouseConnector("stripe", "Stripe")
    print(f"\nTesting {connector}")

    jobs = connector.fetch_jobs()
    print(f"\nFetched {len(jobs)} jobs from Stripe")

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
        if sample.posted_date:
            print(f"  Posted: {sample.posted_date}")
        if sample.salary_min:
            print(f"  Salary: ${sample.salary_min:,.0f} - ${sample.salary_max:,.0f}")


if __name__ == "__main__":
    test_greenhouse()
