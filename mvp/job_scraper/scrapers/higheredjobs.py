"""HigherEdJobs.com scraper for education/university jobs.

Important for states with significant higher education presence.
North Carolina has Duke, UNC, NC State, Wake Forest, and many community colleges.
"""

import logging
import time
import re
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class HigherEdJobsScraper(BaseScraper):
    """Scraper for HigherEdJobs.com - education sector jobs."""

    platform_name = "higheredjobs"
    base_url = "https://www.higheredjobs.com"
    max_results_per_search = 100
    results_per_page = 25
    rate_limit_seconds = 2.5

    # State code mapping for HigherEdJobs
    STATE_CODES = {
        "NC": "37", "TX": "48", "CA": "06", "FL": "12", "NY": "36",
        "VA": "51", "GA": "13", "PA": "42", "OH": "39", "MI": "26",
        "IL": "17", "MA": "25", "NJ": "34", "WA": "53", "CO": "08",
        "AZ": "04", "TN": "47", "MD": "24", "MN": "27", "WI": "55",
    }

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)
        self.state_abbrev = config.get("state_abbrev", "NC") if config else "NC"
        self.state_code = self.STATE_CODES.get(self.state_abbrev, "37")

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build HigherEdJobs search URL."""
        params = {
            "State": self.state_code,
            "SortBy": "1",  # Sort by date
            "NumJobs": self.results_per_page,
            "StartRow": (page - 1) * self.results_per_page,
        }

        if query.keywords:
            params["Keyword"] = " ".join(query.keywords)

        # Add location city if specified
        if query.location and "," in query.location:
            city = query.location.split(",")[0].strip()
            params["City"] = city

        return f"{self.base_url}/search/results.cfm?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on HigherEdJobs."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            page = 1
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, page)
                logger.info(f"Fetching HigherEdJobs: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed on page {page}: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings
                job_rows = soup.find_all('div', class_='row-fluid record-container')
                if not job_rows:
                    job_rows = soup.find_all('div', class_='job-listing')
                if not job_rows:
                    # Try table format
                    job_rows = soup.find_all('tr', class_='record')

                if not job_rows:
                    logger.info(f"No jobs found on page {page}")
                    break

                # Get total count
                if pages_scraped == 0:
                    count_elem = soup.find('span', class_='results-count')
                    if not count_elem:
                        count_elem = soup.find('div', class_='search-results-count')
                    if count_elem:
                        match = re.search(r'([\d,]+)', count_elem.get_text())
                        if match:
                            total_found = int(match.group(1).replace(',', ''))

                for row in job_rows:
                    job = self._parse_job_row(row, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(job_rows) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping HigherEdJobs: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=total_found or len(jobs),
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job_row(self, row, query: SearchQuery) -> Optional[Job]:
        """Parse a job listing row."""
        try:
            # Title
            title_elem = row.find('a', class_='job-title')
            if not title_elem:
                title_elem = row.find('a', href=lambda h: h and '/details.cfm' in h)
            if not title_elem:
                title_elem = row.find('h3')
                if title_elem:
                    title_elem = title_elem.find('a')

            title = title_elem.get_text(strip=True) if title_elem else None

            # URL
            url = ""
            if title_elem and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                elif not href.startswith('http'):
                    url = f"{self.base_url}/{href}"
                else:
                    url = href

            if not title or not url:
                return None

            # Institution (company)
            company_elem = row.find('a', class_='employer-name')
            if not company_elem:
                company_elem = row.find('span', class_='institution')
            if not company_elem:
                # Try to find link to institution
                company_elem = row.find('a', href=lambda h: h and '/institution' in str(h))

            company = company_elem.get_text(strip=True) if company_elem else "Unknown Institution"

            # Location
            location_elem = row.find('span', class_='location')
            if not location_elem:
                location_elem = row.find('div', class_='job-location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # Clean up location
            if location:
                location = re.sub(r'\s+', ' ', location).strip()

            # Posted date
            posted_date = None
            date_elem = row.find('span', class_='posted-date')
            if not date_elem:
                date_elem = row.find('td', class_='date')
            if date_elem:
                posted_date = self._parse_date(date_elem.get_text(strip=True))

            # Job type - check for keywords
            job_type = None
            text = row.get_text().lower()
            if 'full-time' in text or 'full time' in text:
                job_type = 'full-time'
            elif 'part-time' in text or 'part time' in text:
                job_type = 'part-time'
            elif 'adjunct' in text:
                job_type = 'part-time'

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                job_type=job_type,
                posted_date=posted_date,
                industry="Education",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing HigherEdJobs row: {e}")
            return None

    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Parse date text."""
        if not date_text:
            return None

        text_lower = date_text.lower().strip()
        now = datetime.now()

        # Try "today" / "yesterday"
        if 'today' in text_lower:
            return now
        elif 'yesterday' in text_lower:
            return now - timedelta(days=1)

        # Try "X days ago"
        match = re.search(r'(\d+)\s*day', text_lower)
        if match:
            return now - timedelta(days=int(match.group(1)))

        # Try various date formats
        date_formats = [
            "%m/%d/%Y",
            "%m/%d/%y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%d",
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_text.strip(), fmt)
            except ValueError:
                continue

        return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
