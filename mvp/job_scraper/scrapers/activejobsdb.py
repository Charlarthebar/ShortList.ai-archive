"""Active Jobs DB API scraper via RapidAPI.

This scraper uses the Active Jobs DB API from RapidAPI which aggregates
jobs from 130k+ career sites and ATS platforms (Greenhouse, Workday, etc.)

To use:
1. Subscribe to the API at: https://rapidapi.com/fantastic-jobs-fantastic-jobs-default/api/active-jobs-db
2. Get your RapidAPI key from your dashboard
3. Set the environment variable: RAPIDAPI_KEY=your_key_here
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
    pass

from typing import Optional
from datetime import datetime
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class ActiveJobsDBScraper(BaseScraper):
    """Scraper for Active Jobs DB API via RapidAPI.

    This API provides access to jobs from 130k+ career sites and ATS platforms.
    Much more reliable than scraping individual job boards.

    Endpoints available:
    - /active-ats-7d: Jobs posted in last 7 days
    - /active-ats-24h: Jobs indexed in last 24 hours
    - /active-ats-6m: Backfill - all active jobs from past 6 months (Ultra/Mega only)
    - /active-ats-1h: Hourly firehose (Ultra/Mega only)
    """

    platform_name = "activejobsdb"
    base_url = "https://active-jobs-db.p.rapidapi.com"
    max_results_per_search = 10000  # Increased for better coverage
    results_per_page = 100  # API max is 100
    rate_limit_seconds = 0.2  # 5 req/sec for Ultra plan

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)
        # API key from config or environment
        self.api_key = (
            config.get("rapidapi_key") if config else None
        ) or os.environ.get("RAPIDAPI_KEY", "")

        if not self.api_key:
            logger.warning(
                "RapidAPI key not configured. "
                "Set RAPIDAPI_KEY environment variable or pass rapidapi_key in config. "
                "Get your key at: https://rapidapi.com/fantastic-jobs-fantastic-jobs-default/api/active-jobs-db"
            )

    def _get_headers(self) -> dict:
        """Get headers for RapidAPI requests."""
        return {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "active-jobs-db.p.rapidapi.com",
            "Accept": "application/json",
        }

    def _build_search_url(
        self,
        query: SearchQuery,
        offset: int = 0,
        endpoint: str = "/active-ats-6m"  # Default to 6-month backfill for max coverage
    ) -> str:
        """Build Active Jobs DB API search URL.

        Available endpoints:
        - /active-ats-6m: 6-month backfill (most data, Ultra/Mega only)
        - /active-ats-7d: Last 7 days
        - /active-ats-24h: Last 24 hours
        - /active-ats-1h: Last hour (Ultra/Mega only)
        """
        params = {
            "limit": self.results_per_page,
            "offset": offset,
        }

        # Location filter - keep full location string for better matching
        # API supports city, state, country - "North Carolina" or "Charlotte, NC" both work
        if query.location:
            params["location_filter"] = query.location

        # Title/keyword filter with Boolean support
        # API supports: OR, AND, exact phrases in quotes
        if query.keywords:
            # Join with OR for broader matching
            params["title_filter"] = " OR ".join(query.keywords)

        # Description filter for additional keyword matching
        if hasattr(query, 'description_keywords') and query.description_keywords:
            params["description_filter"] = " OR ".join(query.description_keywords)

        # Remote filter
        if query.remote_only:
            params["remote"] = "true"

        # Employment type filter (FULL_TIME, PART_TIME, CONTRACTOR, INTERN, etc.)
        if hasattr(query, 'employment_type') and query.employment_type:
            params["ai_employment_type_filter"] = query.employment_type

        # Work arrangement filter (On-site, Hybrid, Remote OK, Remote Solely)
        if hasattr(query, 'work_arrangement') and query.work_arrangement:
            params["ai_work_arrangement_filter"] = query.work_arrangement

        return f"{self.base_url}{endpoint}?{urlencode(params)}"

    def search(self, query: SearchQuery, endpoint: str = None) -> ScrapeResult:
        """Execute a search on Active Jobs DB API.

        Args:
            query: Search query with location, keywords, etc.
            endpoint: API endpoint to use. If None, tries 6-month backfill first,
                     falls back to 7-day if backfill not available.
        """
        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        if not self.api_key:
            return ScrapeResult(
                query=query,
                platform=self.platform_name,
                jobs=[],
                total_found=0,
                pages_scraped=0,
                errors=["RapidAPI key not configured. Set RAPIDAPI_KEY environment variable."],
                duration_seconds=0
            )

        # Determine endpoint - try backfill first for max data
        endpoints_to_try = [endpoint] if endpoint else ["/active-ats-6m", "/active-ats-7d"]
        current_endpoint = endpoints_to_try[0]

        try:
            offset = 0
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, offset, current_endpoint)
                logger.info(f"Fetching Active Jobs DB API ({current_endpoint}): offset {offset}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed at offset {offset}: {e}")
                    break

                if response.status_code == 401:
                    errors.append("Invalid RapidAPI key")
                    break
                elif response.status_code == 403:
                    # Backfill endpoint requires Ultra/Mega - try fallback
                    if current_endpoint == "/active-ats-6m" and len(endpoints_to_try) > 1:
                        logger.warning("6-month backfill requires Ultra/Mega plan, falling back to 7-day")
                        current_endpoint = "/active-ats-7d"
                        continue
                    errors.append("API access forbidden - check your subscription tier")
                    break
                elif response.status_code == 429:
                    logger.warning("Rate limited - waiting 5s and retrying")
                    time.sleep(5)
                    continue
                elif response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}: {response.text[:200]}")
                    break

                try:
                    data = response.json()
                except Exception as e:
                    errors.append(f"Failed to parse JSON: {e}")
                    break

                # Handle response format
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = data.get("jobs", data.get("data", []))
                    if not items and "error" in data:
                        errors.append(f"API error: {data['error']}")
                        break
                else:
                    items = []

                if not items:
                    logger.info(f"No more jobs found after {pages_scraped} pages ({len(jobs)} total)")
                    break

                # Parse jobs
                for item in items:
                    job = self._parse_job(item, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                offset += self.results_per_page

                logger.info(f"Active Jobs DB: Retrieved {len(jobs)} jobs so far (page {pages_scraped})")

                # Stop if we got fewer than a full page
                if len(items) < self.results_per_page:
                    logger.info(f"Got {len(items)} items (less than {self.results_per_page}), done paginating")
                    break

                # Rate limiting
                time.sleep(self.rate_limit_seconds)

        except Exception as e:
            logger.error(f"Error querying Active Jobs DB: {e}")
            errors.append(str(e))

        duration = time.time() - start_time
        total_found = len(jobs)

        logger.info(f"Active Jobs DB search complete: {total_found} jobs in {duration:.1f}s")

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=total_found,
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def search_state(self, state: str, limit: int = 10000) -> ScrapeResult:
        """Search for all jobs in a state (convenience method).

        This bypasses the normal query system for bulk state-wide searches.
        No keyword filtering = maximum job retrieval.

        Args:
            state: State name (e.g., "North Carolina") or abbreviation (e.g., "NC")
            limit: Maximum jobs to retrieve (default 10000)
        """
        query = SearchQuery(
            location=state,
            group_name="state_search",
            keywords=[]  # No keywords = get ALL jobs in location
        )

        # Temporarily increase max results
        original_max = self.max_results_per_search
        self.max_results_per_search = limit

        result = self.search(query)

        self.max_results_per_search = original_max
        return result

    def search_bulk(
        self,
        location: str = None,
        title_filter: str = None,
        limit: int = 10000,
        endpoint: str = "/active-ats-6m"
    ) -> ScrapeResult:
        """Bulk search for maximum job retrieval.

        This method is optimized for getting as many jobs as possible
        with minimal filtering. Use this for comprehensive data collection.

        Args:
            location: Optional location filter (state, city, or country)
            title_filter: Optional title filter with Boolean support (e.g., "engineer OR developer")
            limit: Maximum jobs to retrieve
            endpoint: API endpoint (/active-ats-6m for 6 months, /active-ats-7d for 7 days)
        """
        query = SearchQuery(
            location=location or "",
            group_name="bulk_search",
            keywords=[]
        )

        # Temporarily increase max results
        original_max = self.max_results_per_search
        self.max_results_per_search = limit

        # Build URL directly for more control
        jobs = []
        errors = []
        pages_scraped = 0
        start_time = time.time()

        try:
            offset = 0
            max_pages = limit // self.results_per_page

            while pages_scraped < max_pages:
                params = {
                    "limit": self.results_per_page,
                    "offset": offset,
                }
                if location:
                    params["location_filter"] = location
                if title_filter:
                    params["title_filter"] = title_filter

                url = f"{self.base_url}{endpoint}?{urlencode(params)}"
                logger.info(f"Bulk fetch ({endpoint}): offset {offset}, location={location}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    if response.status_code == 403 and endpoint == "/active-ats-6m":
                        logger.warning("Backfill requires Ultra/Mega, trying 7-day endpoint")
                        endpoint = "/active-ats-7d"
                        continue
                    errors.append(f"HTTP {response.status_code}")
                    break

                data = response.json()
                items = data if isinstance(data, list) else data.get("jobs", data.get("data", []))

                if not items:
                    break

                for item in items:
                    job = self._parse_job(item, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                offset += self.results_per_page

                logger.info(f"Bulk search: {len(jobs)} jobs retrieved (page {pages_scraped})")

                if len(items) < self.results_per_page:
                    break

                time.sleep(self.rate_limit_seconds)

        except Exception as e:
            errors.append(str(e))

        self.max_results_per_search = original_max
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

    def _parse_job(self, item: dict, query: SearchQuery) -> Optional[Job]:
        """Parse an Active Jobs DB API result item."""
        try:
            # Required fields
            title = item.get("title", "")
            if not title:
                return None

            # Company/Organization
            company = item.get("organization", item.get("company", "Unknown"))

            # Location - API provides structured location data
            location = self._parse_location(item)

            # URL
            url = item.get("url", item.get("apply_url", ""))
            if not url:
                return None

            # Salary - check both raw and AI-enriched fields
            salary_min, salary_max, salary_type = self._parse_salary(item)

            # Job type / Employment type
            job_type = self._parse_job_type(item)

            # Remote detection
            remote = self._parse_remote(item, location)

            # Posted date
            posted_date = self._parse_date(item)

            # Description
            description = item.get("description_text", item.get("description", ""))
            if len(description) > 500:
                description = description[:500] + "..."

            # Additional metadata from AI enrichment
            experience_level = item.get("ai_experience_level", "")
            industry = item.get("industry", "")

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                description=description if description else None,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                job_type=job_type,
                remote=remote,
                posted_date=posted_date,
                industry=industry if industry else None,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Active Jobs DB item: {e}")
            return None

    def _parse_location(self, item: dict) -> str:
        """Parse location from API response."""
        # Try structured location first
        locations = item.get("locations_derived", item.get("locations", []))

        if locations and isinstance(locations, list) and len(locations) > 0:
            loc = locations[0]
            if isinstance(loc, dict):
                city = loc.get("city", "")
                state = loc.get("state", loc.get("region", ""))
                country = loc.get("country", "")

                parts = [p for p in [city, state] if p]
                if parts:
                    return ", ".join(parts)
                if country:
                    return country

        # Fallback to location string
        location = item.get("location", item.get("location_raw", ""))
        if location:
            return location

        return "Unknown"

    def _parse_salary(self, item: dict) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary information from API response."""
        salary_min, salary_max, salary_type = None, None, None

        # Try AI-enriched salary first
        ai_salary = item.get("ai_salary", {})
        if ai_salary and isinstance(ai_salary, dict):
            salary_min = ai_salary.get("min")
            salary_max = ai_salary.get("max")
            salary_type = ai_salary.get("type", "yearly")

        # Fallback to raw salary
        if not salary_min:
            salary_raw = item.get("salary_raw", item.get("salary", ""))
            if salary_raw:
                salary_min, salary_max, salary_type = self._parse_salary_text(salary_raw)

        return salary_min, salary_max, salary_type

    def _parse_salary_text(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary from text string."""
        import re

        salary_min, salary_max, salary_type = None, None, None

        if not salary_text:
            return salary_min, salary_max, salary_type

        text_lower = salary_text.lower()

        # Determine type
        if 'hour' in text_lower or '/hr' in text_lower:
            salary_type = 'hourly'
        elif 'year' in text_lower or 'annual' in text_lower or '/yr' in text_lower:
            salary_type = 'yearly'
        elif 'month' in text_lower:
            salary_type = 'monthly'
        else:
            salary_type = 'yearly'  # Default assumption

        # Extract numbers
        numbers = re.findall(r'[\$]?([\d,]+(?:\.\d+)?)', salary_text)
        numbers = [float(n.replace(',', '')) for n in numbers if n]

        if len(numbers) >= 2:
            salary_min = min(numbers)
            salary_max = max(numbers)
        elif len(numbers) == 1:
            salary_min = salary_max = numbers[0]

        return salary_min, salary_max, salary_type

    def _parse_job_type(self, item: dict) -> Optional[str]:
        """Parse employment type from API response."""
        emp_type = item.get("employment_type", item.get("job_type", ""))

        if not emp_type:
            # Try AI-enriched field
            emp_type = item.get("ai_employment_type", "")

        if emp_type:
            emp_lower = emp_type.lower()
            if "full" in emp_lower:
                return "full-time"
            elif "part" in emp_lower:
                return "part-time"
            elif "contract" in emp_lower:
                return "contract"
            elif "intern" in emp_lower:
                return "internship"
            elif "temp" in emp_lower:
                return "temporary"

        return None

    def _parse_remote(self, item: dict, location: str) -> bool:
        """Determine if job is remote."""
        # Check explicit remote field
        remote = item.get("remote", item.get("is_remote", False))
        if remote and str(remote).lower() in ("true", "1", "yes"):
            return True

        # Check AI work arrangement
        work_arr = item.get("ai_work_arrangement", "").lower()
        if "remote" in work_arr:
            return True

        # Check location string
        if "remote" in location.lower():
            return True

        return False

    def _parse_date(self, item: dict) -> Optional[datetime]:
        """Parse posted date from API response."""
        date_str = item.get("date_posted", item.get("posted_date", item.get("created_at", "")))

        if not date_str:
            return None

        # Try different date formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # Try ISO format
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            pass

        return None
