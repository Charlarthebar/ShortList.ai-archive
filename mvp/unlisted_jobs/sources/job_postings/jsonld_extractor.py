#!/usr/bin/env python3
"""
JSON-LD JobPosting Extractor
============================

Extracts job postings from web pages using schema.org JSON-LD markup.

Many company career pages embed structured job data using the JobPosting schema:
https://schema.org/JobPosting

This extractor can pull job data from any page with proper JSON-LD markup,
making it a versatile fallback for companies not using a standard ATS.

Author: ShortList.ai
Date: 2026-01-13
"""

import json
import logging
import re
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from .base_connector import BaseATSConnector, JobPosting

logger = logging.getLogger(__name__)


class JSONLDExtractor(BaseATSConnector):
    """
    Extracts job postings from web pages with schema.org JSON-LD markup.

    The schema.org JobPosting format includes:
    - title, description, datePosted
    - hiringOrganization (company info)
    - jobLocation (address)
    - baseSalary (compensation)
    - employmentType (full-time, part-time, etc.)

    Reference: https://schema.org/JobPosting
    """

    ATS_TYPE = "jsonld"

    def __init__(self, company_name: str, careers_url: str, company_id: str = None):
        """
        Initialize JSON-LD extractor.

        Args:
            company_name: Human-readable company name
            careers_url: URL to the company's careers/jobs page
            company_id: Optional unique identifier (defaults to domain)
        """
        parsed = urlparse(careers_url)
        domain = parsed.netloc.replace("www.", "")

        super().__init__(
            company_id=company_id or domain,
            company_name=company_name,
            base_url=careers_url
        )
        self.careers_url = careers_url
        self.domain = domain

    def _get_default_base_url(self) -> str:
        return self.careers_url

    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch job postings by extracting JSON-LD from the careers page.

        Returns:
            List of JobPosting objects
        """
        jobs = []

        try:
            # Fetch the careers page
            logger.info(f"Fetching careers page: {self.careers_url}")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            response = requests.get(
                self.careers_url,
                timeout=self.REQUEST_TIMEOUT,
                headers=headers
            )
            response.raise_for_status()

            # Extract JSON-LD from page
            jsonld_data = self._extract_jsonld(response.text)

            if not jsonld_data:
                logger.warning(f"No JSON-LD found on {self.careers_url}")
                return jobs

            # Find JobPosting entries
            job_postings = self._find_job_postings(jsonld_data)
            logger.info(f"Found {len(job_postings)} JobPosting entries")

            for raw_job in job_postings:
                try:
                    posting = self.parse_job(raw_job)
                    if posting:
                        jobs.append(posting)
                except Exception as e:
                    logger.warning(f"Error parsing job: {e}")
                    continue

            # If no direct JobPostings found, try to find job links and scrape each
            if not jobs:
                job_links = self._find_job_links(response.text)
                logger.info(f"Found {len(job_links)} job links to scrape")

                for link in job_links[:50]:  # Limit to 50 jobs
                    try:
                        job_page = requests.get(
                            link,
                            timeout=self.REQUEST_TIMEOUT,
                            headers=headers
                        )
                        job_jsonld = self._extract_jsonld(job_page.text)
                        job_postings = self._find_job_postings(job_jsonld)

                        for raw_job in job_postings:
                            posting = self.parse_job(raw_job)
                            if posting:
                                posting.url = link
                                jobs.append(posting)
                                break
                    except Exception as e:
                        logger.debug(f"Error scraping {link}: {e}")
                        continue

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching careers page: {e}")
        except Exception as e:
            logger.error(f"Error fetching jobs: {e}")

        return jobs

    def _extract_jsonld(self, html: str) -> List[Dict]:
        """
        Extract all JSON-LD blocks from HTML.

        Returns:
            List of parsed JSON-LD objects
        """
        jsonld_data = []

        soup = BeautifulSoup(html, 'html.parser')

        # Find all JSON-LD script tags
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                content = script.string
                if content:
                    data = json.loads(content)
                    if isinstance(data, list):
                        jsonld_data.extend(data)
                    else:
                        jsonld_data.append(data)
            except json.JSONDecodeError as e:
                logger.debug(f"Invalid JSON-LD: {e}")
                continue

        return jsonld_data

    def _find_job_postings(self, jsonld_data: List[Dict]) -> List[Dict]:
        """
        Find JobPosting objects in JSON-LD data.

        Handles various structures:
        - Direct JobPosting objects
        - @graph arrays containing JobPostings
        - ItemList containing JobPostings
        """
        job_postings = []

        for item in jsonld_data:
            if not isinstance(item, dict):
                continue

            item_type = item.get('@type', '')

            # Direct JobPosting
            if item_type == 'JobPosting' or (isinstance(item_type, list) and 'JobPosting' in item_type):
                job_postings.append(item)

            # @graph array
            elif '@graph' in item:
                for graph_item in item['@graph']:
                    if isinstance(graph_item, dict):
                        graph_type = graph_item.get('@type', '')
                        if graph_type == 'JobPosting' or (isinstance(graph_type, list) and 'JobPosting' in graph_type):
                            job_postings.append(graph_item)

            # ItemList with JobPostings
            elif item_type == 'ItemList':
                for list_item in item.get('itemListElement', []):
                    if isinstance(list_item, dict):
                        list_item_type = list_item.get('@type', '')
                        if list_item_type == 'JobPosting':
                            job_postings.append(list_item)
                        elif 'item' in list_item:
                            inner = list_item['item']
                            if isinstance(inner, dict) and inner.get('@type') == 'JobPosting':
                                job_postings.append(inner)

        return job_postings

    def _find_job_links(self, html: str) -> List[str]:
        """
        Find links to individual job postings on the page.

        Returns:
            List of absolute URLs to job pages
        """
        soup = BeautifulSoup(html, 'html.parser')
        job_links = []

        # Common patterns for job links
        job_patterns = [
            r'/jobs?/',
            r'/careers?/',
            r'/positions?/',
            r'/openings?/',
            r'/opportunities?/',
        ]

        for link in soup.find_all('a', href=True):
            href = link['href']

            # Check if URL looks like a job posting
            for pattern in job_patterns:
                if re.search(pattern, href, re.IGNORECASE):
                    # Make absolute URL
                    absolute_url = urljoin(self.careers_url, href)

                    # Only include URLs from same domain
                    if urlparse(absolute_url).netloc == urlparse(self.careers_url).netloc:
                        if absolute_url not in job_links:
                            job_links.append(absolute_url)
                    break

        return job_links

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[JobPosting]:
        """
        Parse schema.org JobPosting into our JobPosting format.

        Reference: https://schema.org/JobPosting
        """
        if not raw_job:
            return None

        # Generate external ID
        identifier = raw_job.get('identifier') or raw_job.get('@id') or raw_job.get('url', '')
        if isinstance(identifier, dict):
            identifier = identifier.get('value', str(hash(json.dumps(raw_job, sort_keys=True))))
        job_id = str(identifier) if identifier else str(hash(json.dumps(raw_job, sort_keys=True)))[:16]

        # Extract title
        title = raw_job.get('title', 'Unknown Title')

        # Extract company name
        company_name = self.company_name
        hiring_org = raw_job.get('hiringOrganization', {})
        if isinstance(hiring_org, dict):
            company_name = hiring_org.get('name', company_name)
        elif isinstance(hiring_org, str):
            company_name = hiring_org

        # Extract location
        location_info = self._parse_location(raw_job.get('jobLocation'))

        # Extract description
        description = raw_job.get('description', '')
        if description:
            description = self._clean_html(description)

        # Extract employment type
        employment_type = raw_job.get('employmentType', '')
        if isinstance(employment_type, list):
            employment_type = ', '.join(employment_type)

        # Extract salary
        salary_info = self._parse_salary_schema(raw_job.get('baseSalary'))

        # Parse date
        date_posted = raw_job.get('datePosted')
        posted_date = None
        if date_posted:
            try:
                posted_date = datetime.fromisoformat(date_posted.replace('Z', '+00:00'))
            except ValueError:
                pass

        # Get URL
        job_url = raw_job.get('url', '')
        if job_url and not job_url.startswith('http'):
            job_url = urljoin(self.careers_url, job_url)

        return JobPosting(
            external_id=f"jsonld_{self.company_id}_{job_id}",
            title=title,
            company_name=company_name,
            location_raw=location_info.get('raw', ''),
            city=location_info.get('city'),
            state=location_info.get('state'),
            country=location_info.get('country', 'United States'),
            is_remote=location_info.get('is_remote', False),
            description=description,
            employment_type=employment_type,
            salary_min=salary_info.get('min'),
            salary_max=salary_info.get('max'),
            salary_currency=salary_info.get('currency', 'USD'),
            salary_period=salary_info.get('period', 'annual'),
            posted_date=posted_date,
            url=job_url,
            ats_type=self.ATS_TYPE,
            raw_data=raw_job
        )

    def _parse_location(self, job_location) -> Dict[str, Any]:
        """
        Parse schema.org Place/PostalAddress location.

        jobLocation can be:
        - A Place object with address property
        - A PostalAddress directly
        - A string
        - A list of locations
        """
        result = {
            'raw': '',
            'city': None,
            'state': None,
            'country': 'United States',
            'is_remote': False
        }

        if not job_location:
            return result

        # Handle list of locations (take first)
        if isinstance(job_location, list):
            job_location = job_location[0] if job_location else None

        if isinstance(job_location, str):
            result['raw'] = job_location
            location_info = self._extract_location(job_location)
            result.update(location_info)
            return result

        if isinstance(job_location, dict):
            # Check for Place type
            address = job_location.get('address', job_location)

            if isinstance(address, dict):
                city = address.get('addressLocality', '')
                state = address.get('addressRegion', '')
                country = address.get('addressCountry', 'United States')

                # Handle country as object
                if isinstance(country, dict):
                    country = country.get('name', 'United States')

                result['city'] = city if city else None
                result['state'] = self._normalize_state(state) if state else None
                result['country'] = country

                # Build raw location string
                parts = [p for p in [city, state, country] if p]
                result['raw'] = ', '.join(parts)

            elif isinstance(address, str):
                result['raw'] = address
                location_info = self._extract_location(address)
                result.update(location_info)

        # Check for remote work
        job_location_type = job_location.get('jobLocationType', '') if isinstance(job_location, dict) else ''
        if 'remote' in str(job_location_type).lower() or 'remote' in result['raw'].lower():
            result['is_remote'] = True

        return result

    def _parse_salary_schema(self, base_salary) -> Dict[str, Any]:
        """
        Parse schema.org MonetaryAmount salary.

        baseSalary can be:
        - A MonetaryAmount object
        - A QuantitativeValue with value/minValue/maxValue
        - A number
        - A string
        """
        result = {
            'min': None,
            'max': None,
            'currency': 'USD',
            'period': 'annual'
        }

        if not base_salary:
            return result

        if isinstance(base_salary, (int, float)):
            result['min'] = float(base_salary)
            result['max'] = float(base_salary)
            return result

        if isinstance(base_salary, str):
            return self._parse_salary(base_salary)

        if isinstance(base_salary, dict):
            # Get currency
            result['currency'] = base_salary.get('currency', 'USD')

            # Get period (unitText)
            unit = base_salary.get('unitText', '').lower()
            if 'hour' in unit:
                result['period'] = 'hourly'
            elif 'month' in unit:
                result['period'] = 'monthly'
            elif 'week' in unit:
                result['period'] = 'weekly'

            # Get value (can be nested in 'value' property)
            value = base_salary.get('value', base_salary)

            if isinstance(value, (int, float)):
                result['min'] = float(value)
                result['max'] = float(value)
            elif isinstance(value, dict):
                # QuantitativeValue
                result['min'] = value.get('minValue') or value.get('value')
                result['max'] = value.get('maxValue') or value.get('value')

                if result['min']:
                    result['min'] = float(result['min'])
                if result['max']:
                    result['max'] = float(result['max'])

        return result

    @classmethod
    def detect_jsonld_jobs(cls, url: str) -> bool:
        """
        Check if a URL has JSON-LD JobPosting markup.

        Returns:
            True if JSON-LD job postings are found
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            response = requests.get(url, timeout=10, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    content = script.string
                    if content and 'JobPosting' in content:
                        return True
                except:
                    continue

            return False
        except:
            return False


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

def test_jsonld_extractor():
    """Test the JSON-LD extractor."""
    logging.basicConfig(level=logging.INFO)

    # Test with a company that uses JSON-LD
    # (Note: Many companies use this format)
    test_urls = [
        ("https://careers.microsoft.com/", "Microsoft"),
        ("https://www.apple.com/careers/us/", "Apple"),
    ]

    for url, company in test_urls:
        print(f"\n{'='*60}")
        print(f"Testing: {company}")
        print(f"{'='*60}")

        # First check if JSON-LD is present
        has_jsonld = JSONLDExtractor.detect_jsonld_jobs(url)
        print(f"Has JSON-LD jobs: {has_jsonld}")

        if has_jsonld:
            extractor = JSONLDExtractor(company, url)
            jobs = extractor.fetch_jobs()
            print(f"Found {len(jobs)} jobs")

            if jobs:
                sample = jobs[0]
                print(f"\nSample job:")
                print(f"  Title: {sample.title}")
                print(f"  Location: {sample.location_raw}")
                print(f"  URL: {sample.url}")


if __name__ == "__main__":
    test_jsonld_extractor()
