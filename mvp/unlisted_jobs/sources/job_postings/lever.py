#!/usr/bin/env python3
"""
Lever ATS Connector
===================

Fetches job postings from Lever-powered career sites.

Lever provides a public JSON API at:
https://api.lever.co/v0/postings/{company}

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


class LeverConnector(BaseATSConnector):
    """
    Connector for Lever ATS.

    Lever exposes a public API for job boards:
    - List jobs: GET /v0/postings/{company}
    - Job details included in list response

    Documentation: https://github.com/lever/postings-api
    """

    ATS_TYPE = "lever"
    API_BASE = "https://api.lever.co/v0/postings"

    def __init__(self, company_id: str, company_name: str = None):
        """
        Initialize Lever connector.

        Args:
            company_id: The company's Lever board ID (appears in URL)
                        e.g., "netflix" for jobs.lever.co/netflix
            company_name: Human-readable company name (optional)
        """
        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.title(),
            base_url=f"{self.API_BASE}/{company_id}"
        )
        self.careers_url = f"https://jobs.lever.co/{company_id}"

    def _get_default_base_url(self) -> str:
        return f"{self.API_BASE}/{self.company_id}"

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from Lever.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        try:
            # Lever API returns all postings in a single request
            url = f"{self.base_url}?mode=json"
            logger.info(f"Fetching jobs from: {url}")

            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()

            raw_jobs = response.json()

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
                logger.error(f"Company '{self.company_id}' not found on Lever")
            else:
                logger.error(f"HTTP error fetching Lever jobs: {e}")
        except Exception as e:
            logger.error(f"Error fetching Lever jobs: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw Lever job data into JobPosting format.

        Lever job structure:
        {
            "id": "abc123",
            "text": "Software Engineer",
            "hostedUrl": "https://jobs.lever.co/company/abc123",
            "applyUrl": "https://jobs.lever.co/company/abc123/apply",
            "categories": {
                "team": "Engineering",
                "department": "Engineering",
                "location": "San Francisco, CA",
                "commitment": "Full-time"
            },
            "description": "Job description text...",
            "descriptionPlain": "Plain text description",
            "lists": [
                {
                    "text": "Requirements",
                    "content": "<li>5+ years experience</li>..."
                }
            ],
            "additional": "Additional info...",
            "createdAt": 1673808000000
        }
        """
        if not raw_job:
            return None

        job_id = raw_job.get("id", "")
        if not job_id:
            return None

        # Extract categories
        categories = raw_job.get("categories", {})
        location_name = categories.get("location", "")
        department = categories.get("department") or categories.get("team")
        employment_type = categories.get("commitment")

        # Parse location
        location_info = self._extract_location(location_name)

        # Get description
        description = raw_job.get("descriptionPlain") or raw_job.get("description", "")
        description_text = self._clean_html(description) if description else None

        # Extract requirements from lists
        requirements = self._extract_requirements(raw_job.get("lists", []))

        # Parse posted date (Lever uses millisecond timestamps)
        created_at = raw_job.get("createdAt")
        posted_date = None
        if created_at:
            try:
                posted_date = datetime.fromtimestamp(created_at / 1000)
            except (ValueError, OSError):
                pass

        # Get URL
        job_url = raw_job.get("hostedUrl", self.get_job_url(job_id))

        # Look for salary in additional info or description
        salary_info = self._extract_salary_from_content(
            raw_job.get("additional", ""),
            description
        )

        return JobPosting(
            external_id=f"lever_{self.company_id}_{job_id}",
            title=raw_job.get("text", "Unknown Title"),
            company_name=self.company_name,
            location_raw=location_name,
            city=location_info.get("city"),
            state=location_info.get("state"),
            is_remote=location_info.get("is_remote", False),
            description=description_text,
            requirements=requirements,
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

    def _extract_requirements(self, lists: List[Dict]) -> Optional[str]:
        """
        Extract requirements from Lever lists field.

        Lever often has separate sections for requirements, responsibilities, etc.
        """
        if not lists:
            return None

        requirements_text = []
        for lst in lists:
            text = lst.get("text", "").lower()
            if "requirement" in text or "qualification" in text or "you have" in text:
                content = lst.get("content", "")
                clean_content = self._clean_html(content)
                if clean_content:
                    requirements_text.append(clean_content)

        return "\n".join(requirements_text) if requirements_text else None

    def _extract_salary_from_content(self, additional: str, description: str) -> Dict[str, Any]:
        """
        Extract salary from additional info or description.

        Some companies include salary ranges in the job description.
        """
        salary_info = {"min": None, "max": None, "currency": "USD", "period": "annual"}

        content = f"{additional or ''} {description or ''}"
        if not content:
            return salary_info

        import re

        # Look for salary patterns
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*[-–to]\s*\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # $100,000 - $150,000
            r'(\d{2,3})k\s*[-–to]\s*(\d{2,3})k',  # 100k - 150k
            r'salary[:\s]+\$?(\d{1,3}(?:,\d{3})*)',  # salary: $100,000
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) >= 2:
                    min_val = float(groups[0].replace(",", ""))
                    max_val = float(groups[1].replace(",", ""))

                    # Handle 'k' notation
                    if min_val < 1000:
                        min_val *= 1000
                        max_val *= 1000

                    salary_info["min"] = min_val
                    salary_info["max"] = max_val
                    break
                elif len(groups) == 1:
                    val = float(groups[0].replace(",", ""))
                    salary_info["min"] = val
                    salary_info["max"] = val
                    break

        return salary_info

    def get_job_url(self, job_id: str) -> str:
        """Generate public URL for a Lever job."""
        return f"{self.careers_url}/{job_id}"

    @classmethod
    def discover_company(cls, company_id: str) -> bool:
        """
        Check if a company uses Lever.

        Returns:
            True if company has a Lever board, False otherwise
        """
        try:
            url = f"{cls.API_BASE}/{company_id}?mode=json"
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False


# ============================================================================
# EXAMPLE COMPANIES USING LEVER
# ============================================================================

# Major tech companies known to use Lever:
LEVER_COMPANIES = {
    # Company ID: Company Name
    "netflix": "Netflix",
    "reddit": "Reddit",
    "spotify": "Spotify",
    "github": "GitHub",
    "twitch": "Twitch",
    "lyft": "Lyft",
    "postmates": "Postmates",
    "thumbtack": "Thumbtack",
    "opendoor": "Opendoor",
    "gusto": "Gusto",
    "asana": "Asana",
    "flexport": "Flexport",
    "rippling": "Rippling",
    "brex": "Brex",
    "affirm": "Affirm",
    "scale": "Scale AI",
    "anduril": "Anduril",
    "ramp": "Ramp",
}


def test_lever():
    """Test the Lever connector with a known company."""
    logging.basicConfig(level=logging.INFO)

    # Test with Netflix (well-known Lever user)
    connector = LeverConnector("netflix", "Netflix")
    print(f"\nTesting {connector}")

    jobs = connector.fetch_jobs()
    print(f"\nFetched {len(jobs)} jobs from Netflix")

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
    test_lever()
