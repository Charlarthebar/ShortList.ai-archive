#!/usr/bin/env python3
"""
Workday ATS Connector (Basic)
=============================

Fetches job postings from Workday-powered career sites.

Workday career sites typically have URLs like:
https://{company}.wd5.myworkdayjobs.com/en-US/{tenant}/jobs

The jobs are loaded via a GraphQL-like API that returns JSON.

Author: ShortList.ai
Date: 2026-01-13
"""

import json
import logging
import re
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import time

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class WorkdayConnector(BaseATSConnector):
    """
    Connector for Workday ATS.

    Workday uses a custom API for job listings. The API endpoint pattern is:
    https://{company}.{workday_host}/wday/cxs/{company}/{tenant}/jobs

    Common workday_hosts: wd1, wd3, wd5.myworkdayjobs.com

    Note: Workday sites can be complex with different configurations.
    This connector handles the most common patterns.
    """

    ATS_TYPE = "workday"

    # Common Workday hosts
    WORKDAY_HOSTS = [
        "wd1.myworkdayjobs.com",
        "wd3.myworkdayjobs.com",
        "wd5.myworkdayjobs.com",
        "wd12.myworkdayjobs.com",
    ]

    def __init__(self, company_id: str, company_name: str = None, workday_host: str = None, tenant: str = None):
        """
        Initialize Workday connector.

        Args:
            company_id: The company's Workday identifier (appears in URL)
                        e.g., "amazon" for amazon.wd5.myworkdayjobs.com
            company_name: Human-readable company name (optional)
            workday_host: The Workday host (e.g., "wd5.myworkdayjobs.com")
            tenant: The tenant/career site ID (often "en-US" followed by site name)
        """
        self.workday_host = workday_host
        self.tenant = tenant

        super().__init__(
            company_id=company_id,
            company_name=company_name or company_id.title(),
            base_url=None  # Will be set after discovery
        )

        # Try to discover the correct host if not provided
        if not self.workday_host:
            self._discover_workday_config()

    def _get_default_base_url(self) -> str:
        if self.workday_host and self.tenant:
            return f"https://{self.company_id}.{self.workday_host}/wday/cxs/{self.company_id}/{self.tenant}"
        return None

    def _discover_workday_config(self):
        """
        Try to discover the correct Workday host and tenant.

        Workday URLs follow patterns like:
        - https://{company}.{host}/wday/cxs/{company}/{tenant}/jobs
        - https://{company}.{host}/en-US/{tenant}/
        """
        for host in self.WORKDAY_HOSTS:
            # Try the main careers page
            careers_url = f"https://{self.company_id}.{host}/"

            try:
                response = requests.get(careers_url, timeout=10, allow_redirects=True)
                if response.status_code == 200:
                    # Look for tenant in the response
                    tenant = self._extract_tenant(response.text, response.url)
                    if tenant:
                        self.workday_host = host
                        self.tenant = tenant
                        self.base_url = self._get_default_base_url()
                        self.careers_url = f"https://{self.company_id}.{host}/en-US/{tenant}/"
                        logger.info(f"Discovered Workday config: host={host}, tenant={tenant}")
                        return
            except Exception as e:
                logger.debug(f"Could not connect to {host}: {e}")
                continue

        logger.warning(f"Could not auto-discover Workday config for {self.company_id}")

    def _extract_tenant(self, html: str, url: str) -> Optional[str]:
        """
        Extract tenant identifier from Workday page.

        The tenant is usually in the URL or embedded in JavaScript.
        """
        # Try to extract from URL
        # Pattern: /en-US/{tenant}/ or /{tenant}/
        url_match = re.search(r'/en-US/([^/]+)/', url)
        if url_match:
            return url_match.group(1)

        # Try to find in HTML/JavaScript
        # Look for patterns like: "siteId":"tenant_name"
        patterns = [
            r'"siteId"\s*:\s*"([^"]+)"',
            r'"careerSiteId"\s*:\s*"([^"]+)"',
            r'wday/cxs/[^/]+/([^/]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                return match.group(1)

        return None

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from Workday.

        Uses the Workday CXS API which returns paginated results.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        if not self.base_url:
            logger.error(f"No Workday configuration found for {self.company_id}")
            return jobs

        offset = 0
        limit = 20  # Workday typically returns 20 per page

        try:
            while True:
                # Workday uses POST with JSON body for searches
                search_url = f"{self.base_url}/jobs"
                payload = {
                    "appliedFacets": {},
                    "limit": limit,
                    "offset": offset,
                    "searchText": ""
                }

                headers = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                }

                logger.info(f"Fetching jobs from Workday (offset={offset})")

                response = requests.post(
                    search_url,
                    json=payload,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()
                raw_jobs = data.get('jobPostings', [])

                if not raw_jobs:
                    break

                logger.info(f"Got {len(raw_jobs)} jobs in this batch")

                for raw_job in raw_jobs:
                    try:
                        posting = self.parse_job(raw_job)
                        if posting:
                            jobs.append(posting)
                    except Exception as e:
                        logger.warning(f"Error parsing job: {e}")
                        continue

                # Check if more pages
                total = data.get('total', len(raw_jobs))
                offset += limit

                if offset >= total or len(raw_jobs) < limit:
                    break

                # Rate limiting
                time.sleep(0.5)

            logger.info(f"Total jobs found at {self.company_name}: {len(jobs)}")

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching Workday jobs: {e}")
            # Try alternative API structure
            jobs = self._fetch_jobs_alternative()
        except Exception as e:
            logger.error(f"Error fetching Workday jobs: {e}")

        return jobs

    def _fetch_jobs_alternative(self) -> List[JobPosting]:
        """
        Alternative method to fetch jobs if primary API fails.

        Some Workday sites use a different API structure.
        """
        jobs = []

        try:
            # Try the facetSearch endpoint
            if self.base_url:
                search_url = self.base_url.replace('/jobs', '/facetSearch')
                payload = {"facets": []}

                headers = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }

                response = requests.post(
                    search_url,
                    json=payload,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    data = response.json()
                    # Process faceted results
                    for item in data.get('facets', []):
                        if item.get('type') == 'JOB_POSTING':
                            for value in item.get('values', []):
                                jobs.append(self._parse_facet_job(value))

        except Exception as e:
            logger.debug(f"Alternative fetch also failed: {e}")

        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse raw Workday job data into JobPosting format.

        Workday job structure varies but typically includes:
        {
            "bulletFields": ["Full time", "San Francisco, CA"],
            "title": "Software Engineer",
            "locationsText": "San Francisco, CA",
            "postedOn": "Posted 5 Days Ago",
            "externalPath": "/en-US/External/job/...",
            "timeType": "Full time"
        }
        """
        if not raw_job:
            return None

        # Extract ID from path
        external_path = raw_job.get('externalPath', '')
        job_id = external_path.split('/')[-1] if external_path else str(hash(str(raw_job)))[:16]

        # Extract title
        title = raw_job.get('title', 'Unknown Title')

        # Extract location
        location_raw = raw_job.get('locationsText', '')
        if not location_raw:
            # Try bullet fields
            bullet_fields = raw_job.get('bulletFields', [])
            for field in bullet_fields:
                if ',' in str(field) and any(state in str(field).upper() for state in ['CA', 'NY', 'TX', 'WA', 'MA']):
                    location_raw = field
                    break

        location_info = self._extract_location(location_raw)

        # Check for remote
        is_remote = 'remote' in str(raw_job).lower()

        # Extract employment type
        employment_type = raw_job.get('timeType', '')
        if not employment_type:
            bullet_fields = raw_job.get('bulletFields', [])
            for field in bullet_fields:
                if any(t in str(field).lower() for t in ['full time', 'part time', 'contract']):
                    employment_type = field
                    break

        # Build URL
        job_url = None
        if external_path and self.careers_url:
            job_url = urljoin(self.careers_url, external_path)
        elif external_path and self.workday_host:
            job_url = f"https://{self.company_id}.{self.workday_host}{external_path}"

        # Parse posted date (Workday uses relative dates like "Posted 5 Days Ago")
        posted_on = raw_job.get('postedOn', '')
        posted_date = self._parse_relative_date(posted_on)

        return JobPosting(
            external_id=f"wd_{self.company_id}_{job_id}",
            title=title,
            company_name=self.company_name,
            location_raw=location_raw,
            city=location_info.get('city'),
            state=location_info.get('state'),
            is_remote=is_remote or location_info.get('is_remote', False),
            employment_type=employment_type,
            posted_date=posted_date,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def _parse_facet_job(self, facet_value: Dict) -> Optional[JobPosting]:
        """Parse job from facet search results."""
        return JobPosting(
            external_id=f"wd_{self.company_id}_{facet_value.get('id', '')}",
            title=facet_value.get('label', 'Unknown Title'),
            company_name=self.company_name,
            ats_type=self.ATS_TYPE,
            raw_data=facet_value
        )

    def _parse_relative_date(self, date_text: str) -> Optional[datetime]:
        """
        Parse Workday relative date strings.

        Examples: "Posted 5 Days Ago", "Posted Today", "Posted 30+ Days Ago"
        """
        if not date_text:
            return None

        date_text = date_text.lower()
        now = datetime.now()

        if 'today' in date_text:
            return now
        elif 'yesterday' in date_text:
            return now.replace(day=now.day - 1)

        # Extract number of days
        days_match = re.search(r'(\d+)\+?\s*days?', date_text)
        if days_match:
            days = int(days_match.group(1))
            from datetime import timedelta
            return now - timedelta(days=days)

        return None

    @classmethod
    def discover_company(cls, company_id: str) -> Dict[str, Any]:
        """
        Check if a company uses Workday and return configuration.

        Returns:
            Dict with 'found', 'host', 'tenant' keys
        """
        result = {'found': False, 'host': None, 'tenant': None}

        for host in cls.WORKDAY_HOSTS:
            careers_url = f"https://{company_id}.{host}/"

            try:
                response = requests.get(careers_url, timeout=10, allow_redirects=True)
                if response.status_code == 200:
                    result['found'] = True
                    result['host'] = host

                    # Try to find tenant
                    tenant_match = re.search(r'/en-US/([^/]+)/', response.url)
                    if tenant_match:
                        result['tenant'] = tenant_match.group(1)

                    return result
            except:
                continue

        return result


# ============================================================================
# EXAMPLE COMPANIES USING WORKDAY
# ============================================================================

# Major companies known to use Workday:
WORKDAY_COMPANIES = {
    # (company_id, host, tenant): Company Name
    ("amazon", "wd5.myworkdayjobs.com", "External"): "Amazon",
    ("netflix", "wd5.myworkdayjobs.com", "External"): "Netflix",
    ("salesforce", "wd12.myworkdayjobs.com", "External"): "Salesforce",
    ("google", "wd5.myworkdayjobs.com", None): "Google",
    ("meta", "wd5.myworkdayjobs.com", "External"): "Meta",
    ("microsoft", "wd5.myworkdayjobs.com", "External"): "Microsoft",
    ("apple", "wd3.myworkdayjobs.com", "External"): "Apple",
    ("nvidia", "wd5.myworkdayjobs.com", "NVIDIAExternalCareerSite"): "NVIDIA",
    ("intel", "wd1.myworkdayjobs.com", "External"): "Intel",
    ("ibm", "wd3.myworkdayjobs.com", "External"): "IBM",
    ("oracle", "wd1.myworkdayjobs.com", "External"): "Oracle",
    ("walmart", "wd5.myworkdayjobs.com", "External"): "Walmart",
    ("target", "wd5.myworkdayjobs.com", "External"): "Target",
}


def test_workday():
    """Test the Workday connector."""
    logging.basicConfig(level=logging.INFO)

    # Test with discovery
    print("\n" + "="*60)
    print("Testing Workday Discovery")
    print("="*60)

    test_company = "nvidia"
    result = WorkdayConnector.discover_company(test_company)
    print(f"\nDiscovery result for {test_company}: {result}")

    if result['found']:
        connector = WorkdayConnector(
            company_id=test_company,
            company_name="NVIDIA",
            workday_host=result['host'],
            tenant=result.get('tenant')
        )
        print(f"\nTesting {connector}")

        jobs = connector.fetch_jobs()
        print(f"\nFetched {len(jobs)} jobs")

        if jobs:
            print("\nSample jobs:")
            for job in jobs[:3]:
                print(f"  - {job.title}")
                print(f"    Location: {job.location_raw}")
                print(f"    URL: {job.url}")
                print()


if __name__ == "__main__":
    test_workday()
