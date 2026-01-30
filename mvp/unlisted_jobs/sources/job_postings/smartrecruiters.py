#!/usr/bin/env python3
"""
SmartRecruiters ATS Connector
=============================

Fetches job postings from SmartRecruiters-powered career sites.

SmartRecruiters provides a public API at:
https://api.smartrecruiters.com/v1/companies/{company}/postings

Career pages are hosted at:
https://careers.smartrecruiters.com/{company}

Author: ShortList.ai
Date: 2026-01-13
"""

import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
import time

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class SmartRecruitersConnector(BaseATSConnector):
    """
    Connector for SmartRecruiters ATS.

    SmartRecruiters exposes a public API for job postings:
    - List jobs: GET /v1/companies/{company}/postings
    - Job details: GET /v1/companies/{company}/postings/{id}

    Documentation: https://dev.smartrecruiters.com/customer-api/live-docs/postings-api/
    """

    ATS_TYPE = "smartrecruiters"
    API_BASE = "https://api.smartrecruiters.com/v1/companies"

    def __init__(self, company_id: str, company_name: str = None):
        """
        Initialize SmartRecruiters connector.

        Args:
            company_id: The company's SmartRecruiters identifier (appears in URL)
                        e.g., "BOSCH" for careers.smartrecruiters.com/BOSCH
            company_name: Human-readable company name (optional)
        """
        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.title(),
            base_url=f"{self.API_BASE}/{company_id}"
        )
        self.careers_url = f"https://careers.smartrecruiters.com/{company_id}"

    def _get_default_base_url(self) -> str:
        return f"{self.API_BASE}/{self.company_id}"

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from SmartRecruiters.

        The API supports pagination with offset/limit parameters.

        Returns:
            List of JobPosting objects
        """
        jobs = []
        offset = 0
        limit = 100  # Max per request

        try:
            while True:
                url = f"{self.base_url}/postings"
                params = {
                    'offset': offset,
                    'limit': limit
                }

                logger.info(f"Fetching jobs from: {url} (offset={offset})")

                response = requests.get(
                    url,
                    params=params,
                    timeout=self.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()
                raw_jobs = data.get('content', [])

                if not raw_jobs:
                    break

                logger.info(f"Got {len(raw_jobs)} jobs in this batch")

                for raw_job in raw_jobs:
                    try:
                        posting = self.parse_job(raw_job)
                        if posting:
                            jobs.append(posting)
                    except Exception as e:
                        logger.warning(f"Error parsing job {raw_job.get('id')}: {e}")
                        continue

                # Check if more pages
                total_found = data.get('totalFound', len(raw_jobs))
                offset += limit

                if offset >= total_found:
                    break

                # Rate limiting
                time.sleep(0.5)

            logger.info(f"Total jobs found at {self.company_name}: {len(jobs)}")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Company '{self.company_id}' not found on SmartRecruiters")
            else:
                logger.error(f"HTTP error fetching SmartRecruiters jobs: {e}")
        except Exception as e:
            logger.error(f"Error fetching SmartRecruiters jobs: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw SmartRecruiters job data into JobPosting format.

        SmartRecruiters job structure:
        {
            "id": "743999...",
            "uuid": "...",
            "name": "Software Engineer",
            "refNumber": "REQ-12345",
            "releasedDate": "2024-01-15T12:00:00.000Z",
            "location": {
                "city": "San Francisco",
                "region": "CA",
                "country": "us",
                "remote": false
            },
            "industry": {"id": "...", "label": "Technology"},
            "department": {"id": "...", "label": "Engineering"},
            "function": {"id": "...", "label": "Software Development"},
            "experienceLevel": {"id": "...", "label": "Mid-Senior level"},
            "typeOfEmployment": {"id": "...", "label": "Full-time"},
            "company": {"name": "Company Name", "identifier": "..."},
            "ref": "https://..."
        }
        """
        if not raw_job:
            return None

        job_id = raw_job.get('id') or raw_job.get('uuid', '')
        if not job_id:
            return None

        # Extract location
        location_data = raw_job.get('location', {})
        city = location_data.get('city')
        state = location_data.get('region')
        country = location_data.get('country', 'us').upper()
        is_remote = location_data.get('remote', False)

        # Build location string
        location_parts = [p for p in [city, state] if p]
        location_raw = ', '.join(location_parts) if location_parts else 'Remote' if is_remote else ''

        # Normalize state
        if state:
            state = self._normalize_state(state)

        # Map country codes to full names
        country_map = {
            'US': 'United States',
            'USA': 'United States',
            'CA': 'Canada',
            'GB': 'United Kingdom',
            'UK': 'United Kingdom',
            'DE': 'Germany',
            'FR': 'France',
        }
        country = country_map.get(country, country)

        # Extract department
        department_data = raw_job.get('department', {})
        department = department_data.get('label') if isinstance(department_data, dict) else None

        # Extract employment type
        employment_data = raw_job.get('typeOfEmployment', {})
        employment_type = employment_data.get('label') if isinstance(employment_data, dict) else None

        # Parse posted date
        released_date = raw_job.get('releasedDate')
        posted_date = None
        if released_date:
            try:
                # Handle milliseconds
                released_date = released_date.replace('.000Z', 'Z')
                posted_date = datetime.fromisoformat(released_date.replace('Z', '+00:00'))
            except ValueError:
                pass

        # Get job URL
        job_url = raw_job.get('ref', self.get_job_url(job_id))

        # Get company name
        company_data = raw_job.get('company', {})
        company_name = company_data.get('name', self.company_name) if isinstance(company_data, dict) else self.company_name

        return JobPosting(
            external_id=f"sr_{self.company_id}_{job_id}",
            title=raw_job.get('name', 'Unknown Title'),
            company_name=company_name,
            location_raw=location_raw,
            city=city,
            state=state,
            country=country,
            is_remote=is_remote,
            department=department,
            employment_type=employment_type,
            posted_date=posted_date,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def fetch_job_details(self, job_id: str) -> Optional[Dict]:
        """
        Fetch detailed job posting including description.

        The listing API doesn't include full description,
        so we need a separate call for details.
        """
        try:
            url = f"{self.base_url}/postings/{job_id}"
            response = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.debug(f"Could not fetch details for job {job_id}: {e}")
            return None

    def get_job_url(self, job_id: str) -> str:
        """Generate public URL for a SmartRecruiters job."""
        return f"{self.careers_url}/job/{job_id}"

    @classmethod
    def discover_company(cls, company_id: str) -> bool:
        """
        Check if a company uses SmartRecruiters.

        Returns:
            True if company has a SmartRecruiters board, False otherwise
        """
        try:
            url = f"{cls.API_BASE}/{company_id}/postings?limit=1"
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False


# ============================================================================
# EXAMPLE COMPANIES USING SMARTRECRUITERS
# ============================================================================

# Major companies known to use SmartRecruiters:
SMARTRECRUITERS_COMPANIES = {
    # Company ID: Company Name
    "BOSCH": "Bosch",
    "Visa": "Visa",
    "LinkedIn": "LinkedIn",
    "McDonalds": "McDonald's",
    "IKEA": "IKEA",
    "Adidas": "Adidas",
    "Spotify": "Spotify",
    "Square1": "Square",
    "Wayfair": "Wayfair",
    "eBay": "eBay",
    "Capgemini": "Capgemini",
    "Infosys": "Infosys",
    "TCS": "Tata Consultancy Services",
    "PTC": "PTC",
    "Equinix": "Equinix",
}


def test_smartrecruiters():
    """Test the SmartRecruiters connector with a known company."""
    logging.basicConfig(level=logging.INFO)

    # Test with Bosch (well-known SmartRecruiters user)
    connector = SmartRecruitersConnector("BOSCH", "Bosch")
    print(f"\nTesting {connector}")

    # First check if company exists
    exists = SmartRecruitersConnector.discover_company("BOSCH")
    print(f"Company exists: {exists}")

    if exists:
        jobs = connector.fetch_jobs()
        print(f"\nFetched {len(jobs)} jobs from Bosch")

        if jobs:
            print("\nSample job:")
            sample = jobs[0]
            print(f"  ID: {sample.external_id}")
            print(f"  Title: {sample.title}")
            print(f"  Location: {sample.location_raw}")
            print(f"  City: {sample.city}, State: {sample.state}")
            print(f"  Remote: {sample.is_remote}")
            print(f"  Department: {sample.department}")
            print(f"  Employment: {sample.employment_type}")
            print(f"  URL: {sample.url}")
            if sample.posted_date:
                print(f"  Posted: {sample.posted_date}")


if __name__ == "__main__":
    test_smartrecruiters()
