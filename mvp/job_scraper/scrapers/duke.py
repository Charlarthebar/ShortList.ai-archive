"""Duke University and Duke Health careers scraper.

Duke is one of NC's largest employers with Duke University, Duke Health System,
and Duke University Health System combined employing over 40,000 people.
"""

import logging
import time
import re
from typing import Optional
from datetime import datetime
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class DukeScraper(BaseScraper):
    """Scraper for Duke University and Duke Health careers."""

    platform_name = "duke"
    base_url = "https://careers.duke.edu"
    api_url = "https://careers.duke.edu/api/jobs"
    max_results_per_search = 500
    results_per_page = 25
    rate_limit_seconds = 1.5

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build Duke careers search URL."""
        params = {
            "page": page,
            "per_page": self.results_per_page,
            "sort_by": "posted_date",
            "sort_order": "desc",
        }

        if query.keywords:
            params["q"] = " ".join(query.keywords)

        return f"{self.api_url}?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Duke careers."""
        # Try API first, fall back to HTML
        result = self._search_api(query)

        if not result.jobs and not result.errors:
            result = self._search_html(query)

        return result

    def _search_api(self, query: SearchQuery) -> ScrapeResult:
        """Search using Duke's API if available."""
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
                logger.info(f"Fetching Duke careers API: page {page}")

                try:
                    headers = self._get_headers()
                    headers["Accept"] = "application/json"
                    response = self._make_request(url, headers=headers)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    # API might not be available
                    break

                try:
                    data = response.json()
                except Exception:
                    break

                # Handle different response structures
                if isinstance(data, dict):
                    total_found = data.get("total", data.get("count", 0))
                    items = data.get("jobs", data.get("results", data.get("data", [])))
                elif isinstance(data, list):
                    items = data
                    if pages_scraped == 0:
                        total_found = len(items)
                else:
                    break

                if not items:
                    break

                for item in items:
                    job = self._parse_api_job(item, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(items) < self.results_per_page:
                    break

        except Exception as e:
            logger.debug(f"Duke API error: {e}")

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

    def _search_html(self, query: SearchQuery) -> ScrapeResult:
        """Fallback HTML scraping for Duke careers."""
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
                params = {"page": page}
                if query.keywords:
                    params["q"] = " ".join(query.keywords)

                url = f"{self.base_url}/search?{urlencode(params)}"
                logger.info(f"Fetching Duke careers HTML: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings - try various selectors
                job_cards = soup.find_all('div', class_='job-listing')
                if not job_cards:
                    job_cards = soup.find_all('tr', class_='job-row')
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job-item')
                if not job_cards:
                    job_cards = soup.find_all('a', class_='job-link')

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
                    job = self._parse_html_job(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping Duke HTML: {e}")
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

    def _parse_api_job(self, item: dict, query: SearchQuery) -> Optional[Job]:
        """Parse a job from API response."""
        try:
            title = item.get("title", item.get("job_title", ""))
            if not title:
                return None

            # Company/Department
            company = item.get("department", item.get("organization", "Duke"))
            if "health" in company.lower() or "hospital" in company.lower():
                company = f"Duke Health - {company}"
            elif "university" not in company.lower() and "duke" not in company.lower():
                company = f"Duke University - {company}"

            # Location
            location = item.get("location", item.get("city", "Durham, NC"))
            if location and "NC" not in location and "North Carolina" not in location:
                location = f"{location}, NC"

            # URL
            url = item.get("url", item.get("apply_url", ""))
            if not url:
                job_id = item.get("id", item.get("job_id", item.get("requisition_id", "")))
                if job_id:
                    url = f"{self.base_url}/job/{job_id}"

            if not url:
                return None

            # Posted date
            posted_date = None
            date_str = item.get("posted_date", item.get("posting_date", ""))
            if date_str:
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"]:
                    try:
                        posted_date = datetime.strptime(date_str[:10], fmt[:len(date_str[:10])])
                        break
                    except ValueError:
                        continue

            # Job type
            job_type = item.get("job_type", item.get("employment_type", ""))
            if job_type:
                job_type = job_type.lower()
                if "full" in job_type:
                    job_type = "full-time"
                elif "part" in job_type:
                    job_type = "part-time"

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                job_type=job_type,
                posted_date=posted_date,
                industry="Healthcare/Education",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Duke API job: {e}")
            return None

    def _parse_html_job(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse a job from HTML."""
        try:
            # Title
            title_elem = card.find('a', class_='job-title')
            if not title_elem:
                title_elem = card.find('h3') or card.find('h4')
                if title_elem:
                    title_elem = title_elem.find('a') or title_elem

            if isinstance(card, str) or card.name == 'a':
                title_elem = card

            title = title_elem.get_text(strip=True) if title_elem else None

            # URL
            url = ""
            if title_elem and hasattr(title_elem, 'get') and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                elif href.startswith('http'):
                    url = href
                else:
                    url = f"{self.base_url}/{href}"

            if not title or not url:
                return None

            # Department
            dept_elem = card.find('span', class_='department')
            company = dept_elem.get_text(strip=True) if dept_elem else "Duke"

            # Location
            loc_elem = card.find('span', class_='location')
            location = loc_elem.get_text(strip=True) if loc_elem else "Durham, NC"

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                industry="Healthcare/Education",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Duke HTML job: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        return self._parse_api_job(raw_data, query)


@ScraperRegistry.register
class UNCHealthScraper(BaseScraper):
    """Scraper for UNC Health careers."""

    platform_name = "unchealth"
    base_url = "https://jobs.unchealthcare.org"
    max_results_per_search = 300
    results_per_page = 25
    rate_limit_seconds = 2.0

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on UNC Health careers."""
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
                params = {
                    "page": page,
                    "sort": "date",
                }
                if query.keywords:
                    params["q"] = " ".join(query.keywords)

                url = f"{self.base_url}/search?{urlencode(params)}"
                logger.info(f"Fetching UNC Health: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings
                job_cards = soup.find_all('div', class_='job')
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job-listing')
                if not job_cards:
                    job_cards = soup.find_all('tr', class_='job-row')

                if not job_cards:
                    break

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping UNC Health: {e}")
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
        """Parse a UNC Health job card."""
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

            dept_elem = card.find('span', class_='department')
            company = f"UNC Health - {dept_elem.get_text(strip=True)}" if dept_elem else "UNC Health"

            loc_elem = card.find('span', class_='location')
            location = loc_elem.get_text(strip=True) if loc_elem else "Chapel Hill, NC"

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
            logger.debug(f"Error parsing UNC Health job: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
