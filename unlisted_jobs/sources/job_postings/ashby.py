#!/usr/bin/env python3
"""
Ashby ATS Connector
===================

Fetches job postings from Ashby-powered career sites.

Ashby provides a public JSON API at:
https://api.ashbyhq.com/posting-api/job-board/{company_slug}

Author: ShortList.ai
Date: 2026-01-16
"""

import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class AshbyConnector(BaseATSConnector):
    """
    Connector for Ashby ATS.

    Ashby exposes a public API for job boards:
    - List jobs: GET /posting-api/job-board/{company_slug}
    - With compensation: GET /posting-api/job-board/{company_slug}?includeCompensation=true

    No authentication required for public job board data.
    """

    ATS_TYPE = "ashby"
    API_BASE = "https://api.ashbyhq.com/posting-api/job-board"

    def __init__(self, company_id: str, company_name: str = None):
        """
        Initialize Ashby connector.

        Args:
            company_id: The company's Ashby board slug (appears in URL)
                        e.g., "openai" for jobs.ashbyhq.com/openai
            company_name: Human-readable company name (optional)
        """
        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.title(),
            base_url=f"{self.API_BASE}/{company_id}"
        )
        self.careers_url = f"https://jobs.ashbyhq.com/{company_id}"

    def _get_default_base_url(self) -> str:
        return f"{self.API_BASE}/{self.company_id}"

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from Ashby.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        try:
            # Fetch job list with compensation data
            url = f"{self.base_url}?includeCompensation=true"
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
                logger.error(f"Company '{self.company_id}' not found on Ashby")
            else:
                logger.error(f"HTTP error fetching Ashby jobs: {e}")
        except Exception as e:
            logger.error(f"Error fetching Ashby jobs: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw Ashby job data into JobPosting format.

        Ashby job structure:
        {
            "id": "abc123",
            "title": "Software Engineer",
            "location": "San Francisco, CA",
            "department": "Engineering",
            "team": "Backend",
            "isRemote": true,
            "compensation": {
                "compensationTierSummary": "$150,000 - $200,000",
                "summaryComponents": [...]
            },
            "employmentType": "FullTime",
            "publishedDate": "2024-01-15T12:00:00Z",
            "descriptionPlain": "Job description text...",
            "descriptionHtml": "<p>Job description HTML...</p>",
            "jobUrl": "https://jobs.ashbyhq.com/company/abc123"
        }
        """
        if not raw_job:
            return None

        job_id = raw_job.get("id", "")
        if not job_id:
            return None

        # Extract location
        location_name = raw_job.get("location", "")
        location_info = self._extract_location(location_name)

        # Ashby has explicit isRemote field
        is_remote = raw_job.get("isRemote", False) or location_info.get("is_remote", False)

        # Parse department/team
        department = raw_job.get("department")
        team = raw_job.get("team")
        if team and department:
            department = f"{department} - {team}"
        elif team:
            department = team

        # Parse description
        description_text = raw_job.get("descriptionPlain") or self._clean_html(raw_job.get("descriptionHtml", ""))

        # Parse posted date
        published_date = raw_job.get("publishedDate")
        posted_date = None
        if published_date:
            try:
                posted_date = datetime.fromisoformat(published_date.replace("Z", "+00:00"))
            except ValueError:
                pass

        # Get URL
        job_url = raw_job.get("jobUrl") or self.get_job_url(job_id)

        # Parse compensation
        salary_info = self._extract_compensation(raw_job.get("compensation"))

        # Parse employment type
        employment_type = raw_job.get("employmentType", "")
        employment_type = self._normalize_employment_type(employment_type)

        return JobPosting(
            external_id=f"ashby_{self.company_id}_{job_id}",
            title=raw_job.get("title", "Unknown Title"),
            company_name=self.company_name,
            location_raw=location_name,
            city=location_info.get("city"),
            state=location_info.get("state"),
            is_remote=is_remote,
            description=description_text,
            department=department,
            employment_type=employment_type,
            salary_min=salary_info.get("min"),
            salary_max=salary_info.get("max"),
            salary_currency=salary_info.get("currency", "USD"),
            salary_period=salary_info.get("period", "annual"),
            posted_date=posted_date,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def _extract_compensation(self, compensation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract salary from Ashby compensation field.

        Ashby provides compensation in a structured format:
        {
            "compensationTierSummary": "$150,000 - $200,000 USD",
            "summaryComponents": [
                {"summary": "$150,000 - $200,000", "type": "Salary"}
            ]
        }
        """
        salary_info = {"min": None, "max": None, "currency": "USD", "period": "annual"}

        if not compensation:
            return salary_info

        # Try the summary first
        summary = compensation.get("compensationTierSummary", "")
        if summary:
            parsed = self._parse_salary(summary)
            if parsed["min"]:
                return parsed

        # Try summary components
        components = compensation.get("summaryComponents", [])
        for component in components:
            comp_type = component.get("type", "").lower()
            if comp_type in ["salary", "base", "base salary"]:
                summary = component.get("summary", "")
                parsed = self._parse_salary(summary)
                if parsed["min"]:
                    return parsed

        return salary_info

    def _normalize_employment_type(self, employment_type: str) -> str:
        """Normalize Ashby employment type to standard format."""
        if not employment_type:
            return None

        type_map = {
            "fulltime": "full-time",
            "full_time": "full-time",
            "parttime": "part-time",
            "part_time": "part-time",
            "contract": "contract",
            "contractor": "contract",
            "intern": "internship",
            "internship": "internship",
            "temporary": "temporary",
            "temp": "temporary",
        }

        return type_map.get(employment_type.lower(), employment_type.lower())

    def get_job_url(self, job_id: str) -> str:
        """Generate public URL for an Ashby job."""
        return f"{self.careers_url}/{job_id}"

    @classmethod
    def discover_company(cls, company_id: str) -> bool:
        """
        Check if a company uses Ashby.

        Returns:
            True if company has an Ashby board, False otherwise
        """
        try:
            url = f"{cls.API_BASE}/{company_id}"
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False


def test_ashby():
    """Test the Ashby connector with a known company."""
    logging.basicConfig(level=logging.INFO)

    # Test with OpenAI (well-known Ashby user)
    connector = AshbyConnector("openai", "OpenAI")
    print(f"\nTesting {connector}")

    jobs = connector.fetch_jobs()
    print(f"\nFetched {len(jobs)} jobs from OpenAI")

    if jobs:
        print("\nSample job:")
        sample = jobs[0]
        print(f"  ID: {sample.external_id}")
        print(f"  Title: {sample.title}")
        print(f"  Location: {sample.location_raw}")
        print(f"  City: {sample.city}, State: {sample.state}")
        print(f"  Remote: {sample.is_remote}")
        print(f"  Department: {sample.department}")
        print(f"  Employment Type: {sample.employment_type}")
        print(f"  URL: {sample.url}")
        if sample.posted_date:
            print(f"  Posted: {sample.posted_date}")
        if sample.salary_min:
            print(f"  Salary: ${sample.salary_min:,.0f} - ${sample.salary_max:,.0f}")


if __name__ == "__main__":
    test_ashby()
