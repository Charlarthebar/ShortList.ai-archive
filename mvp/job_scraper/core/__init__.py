"""Core modules for job scraping."""

from .models import Job, SearchQuery, ScrapeResult, StateResult
from .iterator import StateConfig, GroupIterator, load_state_config, list_available_states
from .deduplicator import JobDeduplicator

__all__ = [
    "Job",
    "SearchQuery",
    "ScrapeResult",
    "StateResult",
    "StateConfig",
    "GroupIterator",
    "load_state_config",
    "list_available_states",
    "JobDeduplicator",
]
