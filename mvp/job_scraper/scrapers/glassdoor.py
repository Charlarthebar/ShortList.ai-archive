"""Glassdoor job scraper.

NOTE: Glassdoor requires authentication for full access and has
anti-scraping measures. Consider using their API partner program
for production applications.
"""

import logging
import time
from typing import Optional
from datetime import datetime
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class GlassdoorScraper(BaseScraper):
    """Scraper for Glassdoor job listings."""

    platform_name = "glassdoor"
    base_url = "https://www.glassdoor.com"
    max_results_per_search = 90
    results_per_page = 30
    rate_limit_seconds = 4.0

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build Glassdoor search URL."""
        # Glassdoor uses a different URL structure
        location_slug = query.location.lower().replace(", ", "-").replace(" ", "-")

        base_path = f"/Job/{location_slug}-jobs-SRCH_IL.0,{len(location_slug)}"

        if query.keywords:
            keywords_slug = "-".join(query.keywords).lower()
            base_path += f"_KO{len(location_slug)+1},{len(location_slug)+1+len(keywords_slug)}_{keywords_slug}"

        if page > 1:
            base_path += f"_IP{page}"

        base_path += ".htm"

        return f"{self.base_url}{base_path}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Glassdoor."""
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
                logger.info(f"Fetching: {url}")

                try:
                    response = self._make_request(url, allow_redirects=True)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code == 403:
                    errors.append("Access forbidden - may need authentication")
                    break
                elif response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('li', class_='react-job-listing')

                if not job_cards:
                    # Alternative selector
                    job_cards = soup.find_all('div', {'data-test': 'job-link'})

                if not job_cards:
                    logger.info(f"No jobs found on page {page}")
                    break

                # Get total count
                if pages_scraped == 0:
                    count_elem = soup.find('div', {'data-test': 'job-search-count'})
                    if count_elem:
                        import re
                        match = re.search(r'([\d,]+)', count_elem.get_text())
                        if match:
                            total_found = int(match.group(1).replace(',', ''))

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping Glassdoor: {e}")
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
        """Parse a Glassdoor job card."""
        try:
            # Title
            title_elem = card.find('a', {'data-test': 'job-link'})
            if not title_elem:
                title_elem = card.find('a', class_='job-title')
            title = title_elem.get_text(strip=True) if title_elem else None

            # URL
            url = ""
            if title_elem and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            # Company
            company_elem = card.find('div', {'data-test': 'employer-name'})
            if not company_elem:
                company_elem = card.find('span', class_='job-company')
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"
            # Remove rating from company name
            if company:
                import re
                company = re.sub(r'\d+\.\d+$', '', company).strip()

            # Location
            location_elem = card.find('span', {'data-test': 'location'})
            if not location_elem:
                location_elem = card.find('span', class_='job-location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            if not title or not url:
                return None

            # Salary
            salary_min, salary_max, salary_type = None, None, None
            salary_elem = card.find('span', {'data-test': 'detailSalary'})
            if salary_elem:
                salary_min, salary_max, salary_type = self._parse_salary(
                    salary_elem.get_text(strip=True)
                )

            # Remote
            remote = False
            if location and 'remote' in location.lower():
                remote = True

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                remote=remote,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Glassdoor job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse Glassdoor salary estimate."""
        import re

        salary_min, salary_max, salary_type = None, None, None

        if not salary_text:
            return salary_min, salary_max, salary_type

        text_lower = salary_text.lower()
        if 'hour' in text_lower or '/hr' in text_lower:
            salary_type = 'hourly'
        elif 'year' in text_lower or '/yr' in text_lower or 'k' in text_lower:
            salary_type = 'yearly'

        # Handle "90K - 120K" format
        numbers = re.findall(r'([\d,]+)k?', salary_text, re.IGNORECASE)
        if numbers:
            parsed = []
            for n in numbers:
                val = float(n.replace(',', ''))
                # If "K" was present, multiply
                if 'k' in salary_text.lower() and val < 1000:
                    val *= 1000
                parsed.append(val)

            if len(parsed) >= 2:
                salary_min = min(parsed)
                salary_max = max(parsed)
            elif len(parsed) == 1:
                salary_min = salary_max = parsed[0]

        return salary_min, salary_max, salary_type

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
