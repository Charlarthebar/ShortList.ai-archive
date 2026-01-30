"""Government Jobs scraper (GovernmentJobs.com / NeoGov).

Supports scraping state and local government jobs from governmentjobs.com,
which powers job listings for many state governments including North Carolina.
"""

import logging
import time
import re
from typing import Optional
from datetime import datetime
from urllib.parse import urlencode, quote

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


# State employer mappings for governmentjobs.com
STATE_EMPLOYERS = {
    "NC": "northcarolina",
    "TX": "texas",
    "CA": "ca",
    "FL": "florida",
    "NY": "ny",
    "VA": "virginia",
    "GA": "georgia",
    "PA": "pa",
    "OH": "ohio",
    "MI": "michigan",
    "WA": "washington",
    "OR": "oregon",
    "CO": "colorado",
    "AZ": "az",
    "MA": "massachusetts",
}


@ScraperRegistry.register
class GovernmentJobsScraper(BaseScraper):
    """Scraper for governmentjobs.com (NeoGov platform)."""

    platform_name = "govtjobs"
    base_url = "https://www.governmentjobs.com"
    max_results_per_search = 200
    results_per_page = 25
    rate_limit_seconds = 2.0

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)
        # Default to NC but can be overridden
        self.state_abbrev = config.get("state_abbrev", "NC") if config else "NC"
        self.employer = config.get("employer") if config else None

        if not self.employer:
            self.employer = STATE_EMPLOYERS.get(self.state_abbrev, "northcarolina")

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build governmentjobs.com search URL."""
        # Use the JSON API endpoint
        params = {
            "page": page,
            "pagesize": self.results_per_page,
            "sort": "posting_date",
            "sortdir": "desc",
        }

        if query.keywords:
            params["keyword"] = " ".join(query.keywords)

        # Location filtering - governmentjobs uses different structure
        if query.location:
            params["location"] = query.location

        base_path = f"/api/jobsearchv2/jobs/{self.employer}"
        return f"{self.base_url}{base_path}?{urlencode(params)}"

    def _build_html_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build HTML fallback search URL."""
        params = {}
        if query.keywords:
            params["keyword"] = " ".join(query.keywords)
        if page > 1:
            params["page"] = page

        base_path = f"/careers/{self.employer}"
        if params:
            return f"{self.base_url}{base_path}?{urlencode(params)}"
        return f"{self.base_url}{base_path}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on governmentjobs.com."""
        # Try API first, fall back to HTML scraping
        result = self._search_api(query)

        if not result.jobs and not result.errors:
            logger.info("API returned no results, trying HTML scraping")
            result = self._search_html(query)

        return result

    def _search_api(self, query: SearchQuery) -> ScrapeResult:
        """Search using the JSON API."""
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
                logger.info(f"Fetching GovtJobs API: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed on page {page}: {e}")
                    break

                if response.status_code != 200:
                    # API might not be available, return empty for fallback
                    logger.debug(f"API returned {response.status_code}")
                    break

                try:
                    data = response.json()
                except Exception:
                    break

                # Parse response structure
                if isinstance(data, dict):
                    total_found = data.get("TotalCount", data.get("totalCount", 0))
                    items = data.get("Jobs", data.get("jobs", []))
                elif isinstance(data, list):
                    items = data
                    total_found = len(items) if pages_scraped == 0 else total_found
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
            logger.error(f"Error querying GovtJobs API: {e}")
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

    def _search_html(self, query: SearchQuery) -> ScrapeResult:
        """Fallback HTML scraping."""
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
                url = self._build_html_search_url(query, page)
                logger.info(f"Fetching GovtJobs HTML: {url}")

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
                job_cards = soup.find_all('div', class_='job-item')
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job-listing')
                if not job_cards:
                    job_cards = soup.find_all('tr', class_='job-row')

                if not job_cards:
                    logger.info(f"No jobs found on page {page}")
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
            logger.error(f"Error scraping GovtJobs HTML: {e}")
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
            title = item.get("Title", item.get("title", ""))
            if not title:
                return None

            # Department/Agency as company
            company = item.get("Department", item.get("department", ""))
            if not company:
                company = item.get("Agency", item.get("agency", f"{self.state_abbrev} State Government"))

            # Location
            location = item.get("Location", item.get("location", ""))
            if not location:
                city = item.get("City", item.get("city", ""))
                state = item.get("State", item.get("state", self.state_abbrev))
                location = f"{city}, {state}".strip(", ") if city else query.location

            # URL
            job_id = item.get("JobId", item.get("jobId", item.get("id", "")))
            url = item.get("Url", item.get("url", ""))
            if not url and job_id:
                url = f"{self.base_url}/careers/{self.employer}/jobs/{job_id}"

            if not url:
                return None

            # Salary
            salary_min, salary_max = None, None
            salary_str = item.get("Salary", item.get("salary", ""))
            if salary_str:
                salary_min, salary_max, _ = self._parse_salary(salary_str)

            # Also check explicit min/max fields
            if not salary_min:
                salary_min = item.get("SalaryMin", item.get("salaryMin"))
                salary_max = item.get("SalaryMax", item.get("salaryMax"))

            # Job type
            job_type = item.get("JobType", item.get("jobType", ""))
            if job_type:
                job_type = job_type.lower()
                if "full" in job_type:
                    job_type = "full-time"
                elif "part" in job_type:
                    job_type = "part-time"

            # Posted date
            posted_date = None
            date_str = item.get("PostingDate", item.get("postingDate", item.get("OpenDate", "")))
            if date_str:
                try:
                    if 'T' in str(date_str):
                        posted_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
                    else:
                        posted_date = datetime.strptime(str(date_str), "%m/%d/%Y")
                except ValueError:
                    pass

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type="yearly",
                job_type=job_type,
                remote=False,
                posted_date=posted_date,
                industry="Government",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing GovtJobs API item: {e}")
            return None

    def _parse_html_job(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse a job from HTML."""
        try:
            # Title
            title_elem = card.find('a', class_='job-title') or card.find('h3')
            title = title_elem.get_text(strip=True) if title_elem else None

            if not title:
                return None

            # URL
            url = ""
            if title_elem and title_elem.name == 'a':
                href = title_elem.get('href', '')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            if not url:
                return None

            # Company/Department
            company_elem = card.find('span', class_='department') or card.find('div', class_='agency')
            company = company_elem.get_text(strip=True) if company_elem else f"{self.state_abbrev} State Government"

            # Location
            location_elem = card.find('span', class_='location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # Salary
            salary_min, salary_max = None, None
            salary_elem = card.find('span', class_='salary')
            if salary_elem:
                salary_min, salary_max, _ = self._parse_salary(salary_elem.get_text(strip=True))

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type="yearly",
                industry="Government",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing GovtJobs HTML: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary text."""
        salary_min, salary_max, salary_type = None, None, "yearly"

        if not salary_text:
            return salary_min, salary_max, salary_type

        text_lower = salary_text.lower()
        if 'hour' in text_lower or '/hr' in text_lower:
            salary_type = 'hourly'

        # Extract numbers
        numbers = re.findall(r'[\$]?([\d,]+(?:\.\d+)?)', salary_text)
        numbers = [float(n.replace(',', '')) for n in numbers]

        if len(numbers) >= 2:
            salary_min = min(numbers)
            salary_max = max(numbers)
        elif len(numbers) == 1:
            salary_min = salary_max = numbers[0]

        return salary_min, salary_max, salary_type

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        """Required abstract method implementation."""
        return self._parse_api_job(raw_data, query)
