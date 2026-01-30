"""USAJobs.gov scraper using the official API.

This scraper uses the official USAJobs API, which is free and reliable.
To get API credentials:
1. Go to https://developer.usajobs.gov/APIRequest/Index
2. Request an API key (instant approval)
3. Set the credentials in your config or environment variables:
   - USAJOBS_API_KEY (your email)
   - USAJOBS_AUTH_KEY (the authorization key provided)
"""

import logging
import os
import time
from pathlib import Path

# Load .env file if present
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass  # python-dotenv not installed, rely on environment variables
from typing import Optional
from datetime import datetime
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class USAJobsScraper(BaseScraper):
    """Scraper for USAJobs.gov using the official API."""

    platform_name = "usajobs"
    base_url = "https://data.usajobs.gov/api"
    max_results_per_search = 500
    results_per_page = 100
    rate_limit_seconds = 1.0  # API is generous with rate limits

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)
        # API credentials - from config or environment
        self.api_key = (
            config.get("api_key") if config else None
        ) or os.environ.get("USAJOBS_API_KEY", "")
        self.auth_key = (
            config.get("auth_key") if config else None
        ) or os.environ.get("USAJOBS_AUTH_KEY", "")

        if not self.api_key or not self.auth_key:
            logger.warning(
                "USAJobs API credentials not configured. "
                "Set USAJOBS_API_KEY and USAJOBS_AUTH_KEY environment variables "
                "or pass api_key/auth_key in config."
            )

    def _get_headers(self) -> dict:
        """Get headers for USAJobs API requests."""
        return {
            "Host": "data.usajobs.gov",
            "User-Agent": self.api_key,
            "Authorization-Key": self.auth_key,
            "Accept": "application/json",
        }

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build USAJobs API search URL."""
        params = {
            "LocationName": query.location,
            "ResultsPerPage": self.results_per_page,
            "Page": page,
            "SortField": "DatePosted",
            "SortDirection": "Desc",
        }

        if query.keywords:
            params["Keyword"] = " ".join(query.keywords)

        if query.radius_miles and query.radius_miles > 0:
            params["Radius"] = query.radius_miles

        if query.remote_only:
            params["RemoteIndicator"] = "True"

        return f"{self.base_url}/Search?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on USAJobs API."""
        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        if not self.api_key or not self.auth_key:
            return ScrapeResult(
                query=query,
                platform=self.platform_name,
                jobs=[],
                total_found=0,
                pages_scraped=0,
                errors=["USAJobs API credentials not configured"],
                duration_seconds=0
            )

        try:
            page = 1
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, page)
                logger.info(f"Fetching USAJobs API: page {page}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed on page {page}: {e}")
                    break

                if response.status_code == 401:
                    errors.append("Invalid API credentials")
                    break
                elif response.status_code == 429:
                    errors.append("Rate limited - waiting and retrying")
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                try:
                    data = response.json()
                except Exception as e:
                    errors.append(f"Failed to parse JSON: {e}")
                    break

                search_result = data.get("SearchResult", {})
                result_count = search_result.get("SearchResultCount", 0)

                # Get total on first page
                if pages_scraped == 0:
                    total_found = search_result.get("SearchResultCountAll", result_count)
                    logger.info(f"USAJobs: Found {total_found} total jobs")

                # Parse jobs
                items = search_result.get("SearchResultItems", [])
                if not items:
                    break

                for item in items:
                    job = self._parse_job(item, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                # Stop if we've got all results
                if len(jobs) >= total_found or len(items) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error querying USAJobs: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=total_found,
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job(self, item: dict, query: SearchQuery) -> Optional[Job]:
        """Parse a USAJobs API result item."""
        try:
            matched_object = item.get("MatchedObjectDescriptor", {})

            title = matched_object.get("PositionTitle", "")
            if not title:
                return None

            # Organization/Agency
            org = matched_object.get("OrganizationName", "")
            dept = matched_object.get("DepartmentName", "")
            company = org or dept or "U.S. Government"

            # Location - can be multiple
            location_data = matched_object.get("PositionLocation", [])
            if location_data and len(location_data) > 0:
                loc = location_data[0]
                location = f"{loc.get('CityName', '')}, {loc.get('CountrySubDivisionCode', '')}"
                location = location.strip(", ")
            else:
                location = query.location

            # URL
            url = matched_object.get("PositionURI", "")
            if not url:
                job_id = matched_object.get("PositionID", "")
                if job_id:
                    url = f"https://www.usajobs.gov/job/{job_id}"

            if not url:
                return None

            # Salary
            salary_min, salary_max = None, None
            remuneration = matched_object.get("PositionRemuneration", [])
            if remuneration and len(remuneration) > 0:
                pay = remuneration[0]
                try:
                    salary_min = float(pay.get("MinimumRange", 0))
                    salary_max = float(pay.get("MaximumRange", 0))
                except (ValueError, TypeError):
                    pass

            salary_type = "yearly"  # Federal jobs are typically annual

            # Job type
            schedule = matched_object.get("PositionSchedule", [])
            job_type = None
            if schedule:
                sched = schedule[0] if isinstance(schedule, list) else schedule
                sched_name = sched.get("Name", "").lower() if isinstance(sched, dict) else str(sched).lower()
                if "full" in sched_name:
                    job_type = "full-time"
                elif "part" in sched_name:
                    job_type = "part-time"

            # Remote
            remote = False
            telework = matched_object.get("TeleworkEligible", False)
            if telework or "remote" in location.lower():
                remote = True

            # Posted date
            posted_date = None
            pub_date = matched_object.get("PublicationStartDate", "")
            if pub_date:
                try:
                    posted_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                except ValueError:
                    pass

            # Description snippet
            description = matched_object.get("UserArea", {}).get("Details", {}).get("MajorDuties", [])
            if description:
                description = " ".join(description[:3]) if isinstance(description, list) else str(description)
            else:
                description = None

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                description=description,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                job_type=job_type,
                remote=remote,
                posted_date=posted_date,
                industry="Government",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing USAJobs item: {e}")
            return None
