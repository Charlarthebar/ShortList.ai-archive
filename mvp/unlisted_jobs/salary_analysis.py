#!/usr/bin/env python3
"""
Salary Analysis Module
======================

Provides salary intelligence queries and analysis functions for the jobs database.

Usage:
    from salary_analysis import SalaryAnalyzer

    analyzer = SalaryAnalyzer()

    # Get salary data for a job title
    analyzer.salary_for_title("Software Engineer")

    # Compare salaries across states
    analyzer.compare_states("Data Scientist")

    # Get salary progression by seniority
    analyzer.seniority_progression("Product Manager")

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config


@dataclass
class SalaryStats:
    """Salary statistics for a query."""
    title: str
    count: int
    avg_salary: float
    median_salary: float
    min_salary: float
    max_salary: float
    p25_salary: float  # 25th percentile
    p75_salary: float  # 75th percentile

    def __str__(self):
        return f"""
{self.title}
{'='*60}
  Sample Size:  {self.count:,} jobs
  Average:      ${self.avg_salary:,.0f}
  Median:       ${self.median_salary:,.0f}
  Range:        ${self.min_salary:,.0f} - ${self.max_salary:,.0f}
  25th-75th:    ${self.p25_salary:,.0f} - ${self.p75_salary:,.0f}
"""


class SalaryAnalyzer:
    """Salary analysis and intelligence queries."""

    def __init__(self):
        config = Config()
        self.db = DatabaseManager(config)

    # Common prefixes that create false positive matches
    FALSE_POSITIVE_PREFIXES = {
        'doctor': ['post', 'postdoctoral', 'post-doctoral', 'post doctoral'],
        'engineer': [],
        'manager': [],
    }

    def _get_title_conditions(self, title: str) -> tuple:
        """
        Returns SQL conditions and params for smart title matching.
        Uses word boundary matching and excludes known false positive patterns.
        """
        title_lower = title.lower()

        # For multi-word queries, simple contains is usually fine
        if ' ' in title:
            return "raw_title ILIKE %s", (f"%{title}%",)

        # For single words, use word boundary matching
        # Match: starts with word, contains " word" (word boundary), or is exactly the word
        base_condition = """(
            raw_title ILIKE %s
            OR raw_title ILIKE %s
            OR raw_title ILIKE %s
        )"""
        base_params = [f"{title}", f"{title} %", f"% {title}%"]

        # Add exclusions for known false positive patterns
        exclusions = []
        exclusion_params = []

        if title_lower in self.FALSE_POSITIVE_PREFIXES:
            for prefix in self.FALSE_POSITIVE_PREFIXES[title_lower]:
                exclusions.append("raw_title NOT ILIKE %s")
                exclusion_params.append(f"%{prefix}%")

        # Also exclude compound words (e.g., "Postdoctoral" for "Doctor")
        exclusions.append("raw_title NOT ILIKE %s")
        exclusion_params.append(f"%{title_lower}al%")  # Exclude -al suffix compounds

        if exclusions:
            full_condition = f"({base_condition} AND {' AND '.join(exclusions)})"
            return full_condition, tuple(base_params + exclusion_params)

        return base_condition, tuple(base_params)

    def salary_for_title(self, title: str, min_samples: int = 10) -> Optional[SalaryStats]:
        """
        Get salary statistics for a job title.
        Uses smart matching that respects word boundaries.
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()

        condition, params = self._get_title_conditions(title)

        # Search for matching titles with word boundary awareness
        cursor.execute(f"""
            SELECT
                COUNT(*) as count,
                ROUND(AVG(salary_point)::numeric, 0) as avg_salary,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as median,
                ROUND(MIN(salary_point)::numeric, 0) as min_salary,
                ROUND(MAX(salary_point)::numeric, 0) as max_salary,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as p25,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as p75
            FROM observed_jobs
            WHERE {condition}
              AND salary_point IS NOT NULL
              AND salary_point BETWEEN 15000 AND 1000000
        """, params)

        row = cursor.fetchone()

        # If no results with strict matching, fall back to contains
        if not row or row[0] < min_samples:
            cursor.execute("""
                SELECT
                    COUNT(*) as count,
                    ROUND(AVG(salary_point)::numeric, 0) as avg_salary,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as median,
                    ROUND(MIN(salary_point)::numeric, 0) as min_salary,
                    ROUND(MAX(salary_point)::numeric, 0) as max_salary,
                    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as p25,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as p75
                FROM observed_jobs
                WHERE raw_title ILIKE %s
                  AND salary_point IS NOT NULL
                  AND salary_point BETWEEN 15000 AND 1000000
            """, (f"%{title}%",))
            row = cursor.fetchone()

        self.db.release_connection(conn)

        if row and row[0] >= min_samples:
            return SalaryStats(
                title=title,
                count=row[0],
                avg_salary=float(row[1]),
                median_salary=float(row[2]),
                min_salary=float(row[3]),
                max_salary=float(row[4]),
                p25_salary=float(row[5]),
                p75_salary=float(row[6])
            )
        return None

    def salary_by_seniority(self, title: str) -> List[Dict[str, Any]]:
        """Get salary breakdown by seniority level for a title."""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        condition, params = self._get_title_conditions(title)

        cursor.execute(f"""
            SELECT
                seniority,
                COUNT(*) as count,
                ROUND(AVG(salary_point)::numeric, 0) as avg_salary,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_point)::numeric, 0) as median
            FROM observed_jobs
            WHERE {condition}
              AND salary_point IS NOT NULL
              AND salary_point BETWEEN 15000 AND 1000000
              AND seniority IS NOT NULL
            GROUP BY seniority
            HAVING COUNT(*) >= 5
            ORDER BY avg_salary DESC
        """, params)

        results = []
        for row in cursor.fetchall():
            results.append({
                'seniority': row[0],
                'count': row[1],
                'avg_salary': float(row[2]),
                'median_salary': float(row[3])
            })

        self.db.release_connection(conn)
        return results

    def compare_states(self, title: str, min_samples: int = 10) -> List[Dict[str, Any]]:
        """Compare salaries for a title across states."""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        condition, params = self._get_title_conditions(title)
        # Replace raw_title with o.raw_title for JOIN query
        condition = condition.replace('raw_title', 'o.raw_title')

        cursor.execute(f"""
            SELECT
                l.state,
                COUNT(*) as count,
                ROUND(AVG(o.salary_point)::numeric, 0) as avg_salary,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.salary_point)::numeric, 0) as median
            FROM observed_jobs o
            JOIN locations l ON o.location_id = l.id
            WHERE {condition}
              AND o.salary_point IS NOT NULL
              AND o.salary_point BETWEEN 15000 AND 1000000
              AND l.state IS NOT NULL
            GROUP BY l.state
            HAVING COUNT(*) >= %s
            ORDER BY avg_salary DESC
        """, params + (min_samples,))

        results = []
        for row in cursor.fetchall():
            results.append({
                'state': row[0],
                'count': row[1],
                'avg_salary': float(row[2]),
                'median_salary': float(row[3])
            })

        self.db.release_connection(conn)
        return results

    def top_paying_employers(self, title: str, limit: int = 20, min_samples: int = 5) -> List[Dict[str, Any]]:
        """Find top paying employers for a job title."""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        condition, params = self._get_title_conditions(title)
        condition = condition.replace('raw_title', 'o.raw_title')

        cursor.execute(f"""
            SELECT
                c.name as company,
                COUNT(*) as count,
                ROUND(AVG(o.salary_point)::numeric, 0) as avg_salary,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.salary_point)::numeric, 0) as median
            FROM observed_jobs o
            JOIN companies c ON o.company_id = c.id
            WHERE {condition}
              AND o.salary_point IS NOT NULL
              AND o.salary_point BETWEEN 15000 AND 1000000
            GROUP BY c.name
            HAVING COUNT(*) >= %s
            ORDER BY avg_salary DESC
            LIMIT %s
        """, params + (min_samples, limit))

        results = []
        for row in cursor.fetchall():
            results.append({
                'company': row[0],
                'count': row[1],
                'avg_salary': float(row[2]),
                'median_salary': float(row[3])
            })

        self.db.release_connection(conn)
        return results

    def similar_titles(self, title: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Find similar job titles and their salaries."""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        condition, params = self._get_title_conditions(title)

        cursor.execute(f"""
            SELECT
                raw_title,
                COUNT(*) as count,
                ROUND(AVG(salary_point)::numeric, 0) as avg_salary
            FROM observed_jobs
            WHERE {condition}
              AND salary_point IS NOT NULL
              AND salary_point BETWEEN 15000 AND 1000000
            GROUP BY raw_title
            HAVING COUNT(*) >= 10
            ORDER BY count DESC
            LIMIT %s
        """, params + (limit,))

        results = []
        for row in cursor.fetchall():
            results.append({
                'title': row[0],
                'count': row[1],
                'avg_salary': float(row[2])
            })

        self.db.release_connection(conn)
        return results

    def seniority_progression(self, title: str) -> None:
        """Print salary progression by seniority for a title."""
        data = self.salary_by_seniority(title)

        if not data:
            print(f"No data found for '{title}'")
            return

        # Define seniority order
        order = ['intern', 'entry', 'mid', 'senior', 'lead', 'manager', 'director', 'exec']
        data_dict = {d['seniority']: d for d in data}

        print(f"\nSalary Progression: {title}")
        print("=" * 60)
        print(f"{'Seniority':<12} {'Count':>10} {'Avg Salary':>15} {'Median':>15}")
        print("-" * 60)

        for level in order:
            if level in data_dict:
                d = data_dict[level]
                print(f"{level:<12} {d['count']:>10,} ${d['avg_salary']:>14,.0f} ${d['median_salary']:>14,.0f}")

    def state_comparison(self, title: str) -> None:
        """Print salary comparison across states for a title."""
        data = self.compare_states(title)

        if not data:
            print(f"No data found for '{title}'")
            return

        print(f"\nSalary by State: {title}")
        print("=" * 60)
        print(f"{'State':<20} {'Count':>10} {'Avg Salary':>15} {'Median':>15}")
        print("-" * 60)

        for d in data[:15]:  # Top 15
            print(f"{d['state']:<20} {d['count']:>10,} ${d['avg_salary']:>14,.0f} ${d['median_salary']:>14,.0f}")

    def employer_comparison(self, title: str) -> None:
        """Print top paying employers for a title."""
        data = self.top_paying_employers(title)

        if not data:
            print(f"No data found for '{title}'")
            return

        print(f"\nTop Paying Employers: {title}")
        print("=" * 70)
        print(f"{'Company':<40} {'Count':>8} {'Avg Salary':>12} {'Median':>12}")
        print("-" * 70)

        for d in data[:15]:
            company = d['company'][:38] if len(d['company']) > 38 else d['company']
            print(f"{company:<40} {d['count']:>8,} ${d['avg_salary']:>11,.0f} ${d['median_salary']:>11,.0f}")

    def full_report(self, title: str) -> None:
        """Generate a full salary report for a job title."""
        stats = self.salary_for_title(title)

        if not stats:
            print(f"Insufficient data for '{title}' (need at least 10 samples)")
            return

        print(stats)

        # Similar titles
        similar = self.similar_titles(title, limit=10)
        if similar:
            print("\nRelated Job Titles")
            print("-" * 60)
            for s in similar:
                print(f"  {s['title']:<40} {s['count']:>6,} jobs  ${s['avg_salary']:>10,.0f}")

        # Seniority breakdown
        self.seniority_progression(title)

        # State comparison
        self.state_comparison(title)

        # Top employers
        self.employer_comparison(title)


def main():
    """Demo the salary analyzer."""
    analyzer = SalaryAnalyzer()

    # Example queries
    print("\n" + "="*70)
    print("SALARY ANALYSIS DEMO")
    print("="*70)

    # Quick lookups
    for title in ["Software Engineer", "Data Scientist", "Nurse", "Teacher"]:
        stats = analyzer.salary_for_title(title)
        if stats:
            print(f"\n{title}: ${stats.median_salary:,.0f} median ({stats.count:,} samples)")

    # Full report for one title
    print("\n")
    analyzer.full_report("Software Engineer")


if __name__ == "__main__":
    main()
