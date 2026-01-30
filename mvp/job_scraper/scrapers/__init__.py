"""Platform-specific job scrapers."""

from .base import BaseScraper, ScraperRegistry

# General job boards
from .indeed import IndeedScraper
from .ziprecruiter import ZipRecruiterScraper
from .linkedin import LinkedInScraper
from .glassdoor import GlassdoorScraper
from .simplyhired import SimplyHiredScraper

# Government job sources
from .usajobs import USAJobsScraper
from .govtjobs import GovernmentJobsScraper

# Aggregator APIs (RapidAPI)
from .activejobsdb import ActiveJobsDBScraper

# Specialized job boards
from .dice import DiceScraper
from .higheredjobs import HigherEdJobsScraper

# Major NC employers - Workday-based
from .workday import (
    WorkdayScraper,
    BankOfAmericaScraper,
    WellsFargoScraper,
    LowesScraper,
    AtriumHealthScraper,
    TruistScraper,
    FidelityScraper,
)

# NC Healthcare/Education employers
from .duke import DukeScraper, UNCHealthScraper

# NC Tech companies - Greenhouse-based
from .greenhouse import (
    GreenhouseScraper,
    RedHatScraper,
    EpicGamesScraper,
    PendoScraper,
    BandwidthScraper,
    SASScraper,
)

# Healthcare job boards
from .healthcare import HealthECareersScraper, NursingJobsScraper

__all__ = [
    # Base
    "BaseScraper",
    "ScraperRegistry",
    # General boards
    "IndeedScraper",
    "ZipRecruiterScraper",
    "LinkedInScraper",
    "GlassdoorScraper",
    "SimplyHiredScraper",
    # Government
    "USAJobsScraper",
    "GovernmentJobsScraper",
    # Aggregator APIs
    "ActiveJobsDBScraper",
    # Specialized boards
    "DiceScraper",
    "HigherEdJobsScraper",
    # Major employers
    "WorkdayScraper",
    "BankOfAmericaScraper",
    "WellsFargoScraper",
    "LowesScraper",
    "AtriumHealthScraper",
    "TruistScraper",
    "FidelityScraper",
    "DukeScraper",
    "UNCHealthScraper",
    # Tech companies
    "GreenhouseScraper",
    "RedHatScraper",
    "EpicGamesScraper",
    "PendoScraper",
    "BandwidthScraper",
    "SASScraper",
    # Healthcare boards
    "HealthECareersScraper",
    "NursingJobsScraper",
]
