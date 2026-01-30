"""Group iteration logic for systematic job scraping."""

import json
from pathlib import Path
from typing import Iterator, Optional
from dataclasses import dataclass

from .models import SearchQuery


@dataclass
class IterationPhase:
    """Represents a phase in the iteration strategy."""
    phase_num: int
    name: str
    groups: list[str]
    expected_coverage: float


class StateConfig:
    """Loads and manages state configuration."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.state = self.config["state"]
        self.state_abbrev = self.config["state_abbrev"]
        self.groups = self.config["groups"]
        self.iteration_strategy = self.config["iteration_strategy"]
        self.platforms = self.config.get("platforms", {})

    @property
    def enabled_platforms(self) -> list[str]:
        """Get list of enabled platforms sorted by priority."""
        enabled = [
            (name, cfg.get("priority", 99))
            for name, cfg in self.platforms.items()
            if cfg.get("enabled", True)
        ]
        return [name for name, _ in sorted(enabled, key=lambda x: x[1])]


class GroupIterator:
    """Iterates through search groups according to strategy."""

    def __init__(self, state_config: StateConfig):
        self.config = state_config

    def get_phases(self) -> list[IterationPhase]:
        """Get all iteration phases."""
        phases = []
        for phase_data in self.config.iteration_strategy["phases"]:
            phases.append(IterationPhase(
                phase_num=phase_data["phase"],
                name=phase_data["name"],
                groups=phase_data["groups"],
                expected_coverage=phase_data["expected_coverage"]
            ))
        return phases

    def iterate_phase(self, phase: IterationPhase) -> Iterator[SearchQuery]:
        """Yield all search queries for a given phase."""
        for group_name in phase.groups:
            if group_name not in self.config.groups:
                continue

            group = self.config.groups[group_name]
            yield from self._iterate_group(group_name, group, phase.phase_num)

    def _iterate_group(self, group_name: str, group: dict, phase_num: int) -> Iterator[SearchQuery]:
        """Yield search queries for a single group."""
        items = group.get("items", [])
        use_for = group.get("use_for", "primary")

        for item in items:
            if use_for == "filter":
                # This is a filter dimension, skip direct iteration
                continue

            if "search_terms" in item:
                # Geographic search
                for search_term in item["search_terms"]:
                    yield SearchQuery(
                        location=search_term,
                        radius_miles=item.get("radius_miles", 25),
                        remote_only=item.get("remote_filter", False),
                        group_name=f"{group_name}:{item['name']}",
                        phase=phase_num
                    )

            elif "keywords" in item:
                # Industry/keyword search (statewide)
                yield SearchQuery(
                    location=f"{self.config.state}",
                    radius_miles=0,  # Statewide
                    keywords=item["keywords"],
                    group_name=f"{group_name}:{item['name']}",
                    phase=phase_num
                )

    def iterate_all(self) -> Iterator[SearchQuery]:
        """Yield all search queries across all phases."""
        for phase in self.get_phases():
            yield from self.iterate_phase(phase)

    def get_query_count(self) -> int:
        """Get total number of queries to execute."""
        return sum(1 for _ in self.iterate_all())

    def estimate_coverage(self) -> float:
        """Get estimated coverage from config."""
        return self.config.iteration_strategy.get("total_expected_coverage", 0.90)


def load_state_config(state_name: str, configs_dir: Path = None) -> StateConfig:
    """Load configuration for a state by name."""
    if configs_dir is None:
        configs_dir = Path(__file__).parent.parent / "configs" / "states"

    # Normalize state name to filename
    filename = state_name.lower().replace(" ", "_") + ".json"
    config_path = configs_dir / filename

    if not config_path.exists():
        raise FileNotFoundError(f"No config found for state: {state_name} (expected {config_path})")

    return StateConfig(config_path)


def list_available_states(configs_dir: Path = None) -> list[str]:
    """List all states with available configurations."""
    if configs_dir is None:
        configs_dir = Path(__file__).parent.parent / "configs" / "states"

    states = []
    if configs_dir.exists():
        for config_file in configs_dir.glob("*.json"):
            state_name = config_file.stem.replace("_", " ").title()
            states.append(state_name)

    return sorted(states)
