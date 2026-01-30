"""Healthcare job board scrapers.

Healthcare is a major industry in NC with systems like Duke Health,
UNC Health, Atrium Health, Novant Health, and many others.
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
class HealthECareersScraper(BaseScraper):
    """Scraper for Health eCareers - major healthcare job board."""

    platform_name = "healthecareers"
    base_url = "https://www.healthecareers.com"
    max_results_per_search = 100
    results_per_page = 25
    rate_limit_seconds = 2.0

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build Health eCareers search URL."""
        params = {
            "location": query.location or "North Carolina",
            "page": page,
            "sort": "date",
        }

        if query.keywords:
            params["q"] = " ".join(query.keywords)

        if query.radius_miles:
            params["radius"] = query.radius_miles

        return f"{self.base_url}/jobs?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute search on Health eCareers."""
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
                logger.info(f"Fetching Health eCareers: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('div', class_='job-card')
                if not job_cards:
                    job_cards = soup.find_all('article', class_='job')
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job-listing')

                if not job_cards:
                    break

                # Get total count
                if pages_scraped == 0:
                    count_elem = soup.find('span', class_='results-count')
                    if count_elem:
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
            logger.error(f"Error scraping Health eCareers: {e}")
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
        """Parse Health eCareers job card."""
        try:
            title_elem = card.find('a', class_='job-title')
            if not title_elem:
                title_elem = card.find('h2') or card.find('h3')
                if title_elem:
                    title_elem = title_elem.find('a') or title_elem

            title = title_elem.get_text(strip=True) if title_elem else None

            url = ""
            if title_elem and hasattr(title_elem, 'get') and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            if not title or not url:
                return None

            company_elem = card.find('span', class_='company') or card.find('div', class_='employer')
            company = company_elem.get_text(strip=True) if company_elem else "Healthcare Employer"

            loc_elem = card.find('span', class_='location')
            location = loc_elem.get_text(strip=True) if loc_elem else query.location

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                industry="Healthcare",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Health eCareers job: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass


@ScraperRegistry.register
class NursingJobsScraper(BaseScraper):
    """Scraper for nursing-specific job boards."""

    platform_name = "nursingjobs"
    base_url = "https://www.nursingjobs.com"
    max_results_per_search = 100
    results_per_page = 20
    rate_limit_seconds = 2.0

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute search on NursingJobs."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            params = {
                "location": "North Carolina",
                "sort": "date",
            }
            if query.keywords:
                params["q"] = " ".join(query.keywords)

            url = f"{self.base_url}/jobs?{urlencode(params)}"
            logger.info(f"Fetching NursingJobs")

            try:
                response = self._make_request(url)
            except Exception as e:
                errors.append(f"Request failed: {e}")
                return ScrapeResult(
                    query=query,
                    platform=self.platform_name,
                    jobs=[],
                    total_found=0,
                    pages_scraped=0,
                    errors=errors,
                    duration_seconds=time.time() - start_time
                )

            if response.status_code != 200:
                errors.append(f"HTTP {response.status_code}")
            else:
                soup = BeautifulSoup(response.text, 'html.parser')

                job_cards = soup.find_all('div', class_='job-listing')
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job')

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped = 1

        except Exception as e:
            logger.error(f"Error scraping NursingJobs: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=len(jobs),
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job_card(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse nursing job card."""
        try:
            title_elem = card.find('a', class_='job-title') or card.find('a')
            title = title_elem.get_text(strip=True) if title_elem else None

            url = ""
            if title_elem and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            if not title or not url:
                return None

            company_elem = card.find('span', class_='company')
            company = company_elem.get_text(strip=True) if company_elem else "Healthcare Facility"

            loc_elem = card.find('span', class_='location')
            location = loc_elem.get_text(strip=True) if loc_elem else query.location

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                industry="Healthcare/Nursing",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing NursingJobs: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
