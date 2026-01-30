"""Job deduplication logic."""

from typing import Optional
from collections import defaultdict
import re

from .models import Job


class JobDeduplicator:
    """Removes duplicate job listings across platforms and searches."""

    def __init__(self):
        self.seen_urls: set[str] = set()
        self.seen_ids: set[str] = set()
        self.jobs_by_company: dict[str, list[Job]] = defaultdict(list)

    def _normalize_company(self, company: str) -> str:
        """Normalize company name for comparison."""
        # Remove common suffixes
        suffixes = [
            r'\s+(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|company|co\.?)$',
            r'\s+\(.*\)$',
        ]
        normalized = company.lower().strip()
        for suffix in suffixes:
            normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)
        return normalized.strip()

    def _normalize_title(self, title: str) -> str:
        """Normalize job title for comparison."""
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^\w\s]', ' ', title.lower())
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()

    def _normalize_location(self, location: str) -> str:
        """Normalize location for comparison."""
        # Extract city and state
        normalized = location.lower().strip()
        # Remove zip codes
        normalized = re.sub(r'\d{5}(-\d{4})?', '', normalized)
        # Remove common words
        normalized = re.sub(r'\b(remote|hybrid|onsite|on-site)\b', '', normalized)
        return normalized.strip()

    def is_duplicate(self, job: Job, fuzzy: bool = True) -> bool:
        """Check if a job is a duplicate."""
        # Check URL first (exact match)
        if job.url_hash in self.seen_urls:
            return True

        # Check unique ID (title + company + location hash)
        if job.unique_id in self.seen_ids:
            return True

        if fuzzy:
            # Fuzzy matching for cross-platform duplicates
            normalized_company = self._normalize_company(job.company)
            normalized_title = self._normalize_title(job.title)
            normalized_location = self._normalize_location(job.location)

            # Check against existing jobs from same company
            for existing in self.jobs_by_company.get(normalized_company, []):
                existing_title = self._normalize_title(existing.title)
                existing_location = self._normalize_location(existing.location)

                # Same title and similar location = duplicate
                if normalized_title == existing_title:
                    # Check if locations are similar (same city)
                    if self._locations_match(normalized_location, existing_location):
                        return True

        return False

    def _locations_match(self, loc1: str, loc2: str) -> bool:
        """Check if two locations are the same city."""
        # Extract first word (usually city name)
        city1 = loc1.split(',')[0].split()[0] if loc1 else ''
        city2 = loc2.split(',')[0].split()[0] if loc2 else ''

        if not city1 or not city2:
            return False

        return city1 == city2

    def add_job(self, job: Job) -> bool:
        """
        Add a job to the deduplicator.
        Returns True if the job was added (not a duplicate).
        Returns False if the job was a duplicate.
        """
        if self.is_duplicate(job):
            return False

        # Track this job
        self.seen_urls.add(job.url_hash)
        self.seen_ids.add(job.unique_id)

        normalized_company = self._normalize_company(job.company)
        self.jobs_by_company[normalized_company].append(job)

        return True

    def deduplicate_batch(self, jobs: list[Job]) -> tuple[list[Job], int]:
        """
        Deduplicate a batch of jobs.
        Returns (unique_jobs, duplicate_count).
        """
        unique_jobs = []
        duplicate_count = 0

        for job in jobs:
            if self.add_job(job):
                unique_jobs.append(job)
            else:
                duplicate_count += 1

        return unique_jobs, duplicate_count

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            "total_unique_urls": len(self.seen_urls),
            "total_unique_ids": len(self.seen_ids),
            "companies_seen": len(self.jobs_by_company),
        }

    def reset(self):
        """Reset the deduplicator state."""
        self.seen_urls.clear()
        self.seen_ids.clear()
        self.jobs_by_company.clear()
