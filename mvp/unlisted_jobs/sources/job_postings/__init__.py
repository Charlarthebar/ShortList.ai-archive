"""
Job Posting Connectors
======================

Connectors for ingesting job postings from employer career sites and ATS platforms.

Supported ATS Platforms:
- Greenhouse
- Lever
- SmartRecruiters
- Workday (basic)

Also includes a generic JSON-LD JobPosting extractor for sites with structured data.
"""

from .base_connector import BaseATSConnector, JobPosting
from .greenhouse import GreenhouseConnector
from .lever import LeverConnector
from .smartrecruiters import SmartRecruitersConnector
from .workday import WorkdayConnector
from .jsonld_extractor import JSONLDExtractor
from .company_targets import (
    CompanyTarget,
    get_all_known_targets,
    get_auto_detect_targets,
    get_all_targets,
)

__all__ = [
    'BaseATSConnector',
    'JobPosting',
    'GreenhouseConnector',
    'LeverConnector',
    'SmartRecruitersConnector',
    'WorkdayConnector',
    'JSONLDExtractor',
    'CompanyTarget',
    'get_all_known_targets',
    'get_auto_detect_targets',
    'get_all_targets',
]
