"""Indeed job scraper."""

import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote_plus

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class IndeedScraper(BaseScraper):
    """Scraper for Indeed.com job listings."""

    platform_name = "indeed"
    base_url = "https://www.indeed.com"
    max_results_per_search = 100
    results_per_page = 15
    rate_limit_seconds = 3.0

    def _build_search_url(self, query: SearchQuery, start: int = 0) -> str:
        """Build Indeed search URL."""
        params = {
            "l": query.location,
            "start": start,
            "sort": "date",
        }

        if query.keywords:
            params["q"] = " ".join(query.keywords)

        if query.radius_miles and query.radius_miles > 0:
            params["radius"] = query.radius_miles

        if query.job_type:
            jt_map = {
                "fulltime": "fulltime",
                "parttime": "parttime",
                "contract": "contract",
                "temporary": "temporary",
                "internship": "internship",
            }
            if query.job_type.lower() in jt_map:
                params["jt"] = jt_map[query.job_type.lower()]

        if query.remote_only:
            params["remotejob"] = "032b3046-06a3-4876-8dfd-474eb5e7ed11"

        return f"{self.base_url}/jobs?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Indeed."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            start = 0
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, start)
                logger.info(f"Fetching: {url}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed on page {pages_scraped + 1}: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code} on page {pages_scraped + 1}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('div', class_='job_seen_beacon')

                if not job_cards:
                    # Try alternative selector
                    job_cards = soup.find_all('div', {'data-jk': True})

                if not job_cards:
                    logger.info(f"No more jobs found after {pages_scraped} pages")
                    break

                # Get total count from first page
                if pages_scraped == 0:
                    count_elem = soup.find('div', class_='jobsearch-JobCountAndSortPane-jobCount')
                    if count_elem:
                        count_text = count_elem.get_text()
                        # Extract number from "X jobs"
                        import re
                        match = re.search(r'([\d,]+)', count_text)
                        if match:
                            total_found = int(match.group(1).replace(',', ''))

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                start += self.results_per_page

                # Stop if we got fewer than a full page
                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping Indeed: {e}")
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

    def _parse_job_card(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse a job card HTML element."""
        try:
            # Title
            title_elem = card.find('h2', class_='jobTitle')
            if not title_elem:
                title_elem = card.find('a', {'data-jk': True})
            title = title_elem.get_text(strip=True) if title_elem else None

            # Company
            company_elem = card.find('span', {'data-testid': 'company-name'})
            if not company_elem:
                company_elem = card.find('span', class_='companyName')
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"

            # Location
            location_elem = card.find('div', {'data-testid': 'text-location'})
            if not location_elem:
                location_elem = card.find('div', class_='companyLocation')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # URL
            link_elem = card.find('a', href=True)
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href
            else:
                # Try to get job key
                jk = card.get('data-jk')
                url = f"{self.base_url}/viewjob?jk={jk}" if jk else ""

            if not title or not url:
                return None

            # Salary (if present)
            salary_min, salary_max, salary_type = None, None, None
            salary_elem = card.find('div', class_='salary-snippet-container')
            if salary_elem:
                salary_min, salary_max, salary_type = self._parse_salary(
                    salary_elem.get_text(strip=True)
                )

            # Job type
            job_type = None
            metadata_elem = card.find('div', class_='metadata')
            if metadata_elem:
                text = metadata_elem.get_text().lower()
                if 'full-time' in text:
                    job_type = 'full-time'
                elif 'part-time' in text:
                    job_type = 'part-time'
                elif 'contract' in text:
                    job_type = 'contract'

            # Remote detection
            remote = False
            if 'remote' in location.lower():
                remote = True

            # Posted date
            posted_date = None
            date_elem = card.find('span', class_='date')
            if date_elem:
                posted_date = self._parse_date(date_elem.get_text(strip=True))

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                job_type=job_type,
                remote=remote,
                posted_date=posted_date,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary text into components."""
        import re

        salary_min, salary_max, salary_type = None, None, None

        if not salary_text:
            return salary_min, salary_max, salary_type

        # Determine type
        text_lower = salary_text.lower()
        if 'hour' in text_lower:
            salary_type = 'hourly'
        elif 'year' in text_lower or 'annual' in text_lower:
            salary_type = 'yearly'
        elif 'month' in text_lower:
            salary_type = 'monthly'
        elif 'week' in text_lower:
            salary_type = 'weekly'

        # Extract numbers
        numbers = re.findall(r'[\$]?([\d,]+(?:\.\d+)?)', salary_text)
        numbers = [float(n.replace(',', '')) for n in numbers]

        if len(numbers) >= 2:
            salary_min = min(numbers)
            salary_max = max(numbers)
        elif len(numbers) == 1:
            salary_min = salary_max = numbers[0]

        return salary_min, salary_max, salary_type

    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Parse relative date text to datetime."""
        import re

        text_lower = date_text.lower()
        now = datetime.now()

        if 'just posted' in text_lower or 'today' in text_lower:
            return now
        elif 'yesterday' in text_lower:
            return now - timedelta(days=1)

        # "X days ago"
        match = re.search(r'(\d+)\s*day', text_lower)
        if match:
            return now - timedelta(days=int(match.group(1)))

        # "X+ days ago"
        match = re.search(r'(\d+)\+\s*day', text_lower)
        if match:
            return now - timedelta(days=int(match.group(1)))

        return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        """Parse raw job data (for API responses)."""
        # This would be used if Indeed had a JSON API
        # For now, we use _parse_job_card for HTML parsing
        pass
