#!/usr/bin/env python3
"""
Employer Database Builder
==========================

Builds a database of known employers with:
- Employee headcounts
- Industry classification
- Location (city, state)
- Source and confidence score

Data sources:
- IRS Form 990 (nonprofits)
- H-1B/PERM filings (tech companies)
- State/city payroll (government)
- Manual additions (major employers)

Author: ShortList.ai
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class Employer:
    """Represents a known employer."""
    name: str
    normalized_name: str
    employee_count: int
    employee_count_confidence: float
    industry: str
    naics_code: Optional[str]
    city: str
    state: str
    source: str
    ein: Optional[str] = None
    is_nonprofit: bool = False
    is_government: bool = False


# Industry classification
INDUSTRY_CATEGORIES = {
    'healthcare': ['hospital', 'medical', 'health', 'clinic', 'physician', 'dental', 'nursing'],
    'higher_education': ['university', 'college', 'institute of technology', 'school of'],
    'k12_education': ['public schools', 'school district', 'elementary', 'high school'],
    'technology': ['software', 'technology', 'tech', 'digital', 'data', 'computing', 'ai', 'cloud'],
    'finance': ['bank', 'financial', 'investment', 'insurance', 'capital', 'asset', 'fidelity', 'state street'],
    'retail': ['retail', 'store', 'shop', 'market', 'mall'],
    'manufacturing': ['manufacturing', 'factory', 'plant', 'production'],
    'government': ['city of', 'state of', 'commonwealth', 'department', 'agency', 'federal'],
    'consulting': ['consulting', 'advisory', 'partners', 'mckinsey', 'bain', 'bcg', 'deloitte'],
    'biotech': ['biotech', 'pharma', 'biogen', 'moderna', 'vertex', 'sarepta', 'therapeutics'],
    'defense': ['raytheon', 'defense', 'military', 'aerospace', 'lockheed', 'general dynamics'],
    'professional_services': ['law', 'legal', 'accounting', 'audit', 'pwc', 'ey', 'kpmg'],
}


# Major MA employers (manually curated with employee counts)
MAJOR_MA_EMPLOYERS = [
    # Healthcare Systems
    Employer("Mass General Brigham", "mass general brigham", 80000, 0.90, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Massachusetts General Hospital", "massachusetts general hospital", 27000, 0.90, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Brigham and Women's Hospital", "brigham and womens hospital", 18000, 0.90, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Boston Children's Hospital", "boston childrens hospital", 12000, 0.90, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Beth Israel Deaconess Medical Center", "beth israel deaconess", 8000, 0.85, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Dana-Farber Cancer Institute", "dana farber", 5000, 0.85, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Tufts Medical Center", "tufts medical center", 4500, 0.85, "healthcare", "622110", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("UMass Memorial Health Care", "umass memorial", 15000, 0.85, "healthcare", "622110", "Worcester", "MA", "990_filing", is_nonprofit=True),
    Employer("Baystate Health", "baystate health", 12000, 0.85, "healthcare", "622110", "Springfield", "MA", "990_filing", is_nonprofit=True),
    Employer("Lahey Hospital & Medical Center", "lahey hospital", 4000, 0.80, "healthcare", "622110", "Burlington", "MA", "990_filing", is_nonprofit=True),
    Employer("Cambridge Health Alliance", "cambridge health alliance", 4000, 0.80, "healthcare", "622110", "Cambridge", "MA", "990_filing", is_nonprofit=True),
    Employer("Mount Auburn Hospital", "mount auburn hospital", 2500, 0.80, "healthcare", "622110", "Cambridge", "MA", "990_filing", is_nonprofit=True),

    # Universities
    Employer("Harvard University", "harvard university", 20000, 0.90, "higher_education", "611310", "Cambridge", "MA", "990_filing", is_nonprofit=True),
    Employer("Massachusetts Institute of Technology", "mit", 13000, 0.90, "higher_education", "611310", "Cambridge", "MA", "990_filing", is_nonprofit=True),
    Employer("Boston University", "boston university", 10000, 0.85, "higher_education", "611310", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Northeastern University", "northeastern university", 6000, 0.85, "higher_education", "611310", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Tufts University", "tufts university", 5500, 0.85, "higher_education", "611310", "Medford", "MA", "990_filing", is_nonprofit=True),
    Employer("Boston College", "boston college", 4000, 0.85, "higher_education", "611310", "Chestnut Hill", "MA", "990_filing", is_nonprofit=True),
    Employer("UMass Amherst", "umass amherst", 6500, 0.85, "higher_education", "611310", "Amherst", "MA", "990_filing", is_nonprofit=True),
    Employer("Worcester Polytechnic Institute", "wpi", 1500, 0.80, "higher_education", "611310", "Worcester", "MA", "990_filing", is_nonprofit=True),
    Employer("Brandeis University", "brandeis university", 2000, 0.80, "higher_education", "611310", "Waltham", "MA", "990_filing", is_nonprofit=True),

    # Technology
    Employer("Amazon", "amazon", 8000, 0.70, "technology", "454110", "Boston", "MA", "h1b_filings"),
    Employer("Google", "google", 5000, 0.65, "technology", "541512", "Cambridge", "MA", "h1b_filings"),
    Employer("Microsoft", "microsoft", 3000, 0.65, "technology", "541512", "Burlington", "MA", "h1b_filings"),
    Employer("Meta", "meta", 2500, 0.60, "technology", "541512", "Boston", "MA", "h1b_filings"),
    Employer("Apple", "apple", 1500, 0.60, "technology", "541512", "Cambridge", "MA", "h1b_filings"),
    Employer("Wayfair", "wayfair", 8000, 0.75, "technology", "454110", "Boston", "MA", "sec_10k"),
    Employer("HubSpot", "hubspot", 3500, 0.75, "technology", "541512", "Cambridge", "MA", "sec_10k"),
    Employer("Toast", "toast", 3000, 0.70, "technology", "541512", "Boston", "MA", "sec_10k"),
    Employer("DraftKings", "draftkings", 4000, 0.70, "technology", "713290", "Boston", "MA", "sec_10k"),
    Employer("Akamai Technologies", "akamai", 3500, 0.75, "technology", "541512", "Cambridge", "MA", "h1b_filings"),
    Employer("The MathWorks", "mathworks", 5000, 0.70, "technology", "541512", "Natick", "MA", "h1b_filings"),
    Employer("Pegasystems", "pegasystems", 3000, 0.70, "technology", "541512", "Cambridge", "MA", "h1b_filings"),

    # Finance
    Employer("Fidelity Investments", "fidelity", 15000, 0.80, "finance", "523920", "Boston", "MA", "news_estimate"),
    Employer("State Street Corporation", "state street", 12000, 0.85, "finance", "522110", "Boston", "MA", "sec_10k"),
    Employer("John Hancock", "john hancock", 5000, 0.75, "finance", "524113", "Boston", "MA", "news_estimate"),
    Employer("Liberty Mutual", "liberty mutual", 8000, 0.75, "finance", "524126", "Boston", "MA", "news_estimate"),
    Employer("Wellington Management", "wellington", 3000, 0.70, "finance", "523920", "Boston", "MA", "news_estimate"),
    Employer("Putnam Investments", "putnam", 2000, 0.70, "finance", "523920", "Boston", "MA", "news_estimate"),

    # Biotech/Pharma
    Employer("Biogen", "biogen", 7500, 0.85, "biotech", "325414", "Cambridge", "MA", "sec_10k"),
    Employer("Moderna", "moderna", 4000, 0.85, "biotech", "325414", "Cambridge", "MA", "sec_10k"),
    Employer("Vertex Pharmaceuticals", "vertex", 4000, 0.85, "biotech", "325414", "Boston", "MA", "sec_10k"),
    Employer("Sarepta Therapeutics", "sarepta", 2500, 0.80, "biotech", "325414", "Cambridge", "MA", "sec_10k"),
    Employer("Novartis", "novartis", 3000, 0.75, "biotech", "325414", "Cambridge", "MA", "news_estimate"),
    Employer("Takeda", "takeda", 2500, 0.75, "biotech", "325414", "Cambridge", "MA", "news_estimate"),
    Employer("Broad Institute", "broad institute", 3500, 0.85, "biotech", "541711", "Cambridge", "MA", "990_filing", is_nonprofit=True),

    # Defense
    Employer("Raytheon", "raytheon", 25000, 0.80, "defense", "336411", "Waltham", "MA", "sec_10k"),
    Employer("General Dynamics", "general dynamics", 5000, 0.70, "defense", "336411", "Quincy", "MA", "sec_10k"),
    Employer("BAE Systems", "bae systems", 4000, 0.70, "defense", "336411", "Burlington", "MA", "news_estimate"),
    Employer("Draper Laboratory", "draper", 2000, 0.80, "defense", "541712", "Cambridge", "MA", "990_filing", is_nonprofit=True),
    Employer("MITRE Corporation", "mitre", 3000, 0.80, "defense", "541712", "Bedford", "MA", "990_filing", is_nonprofit=True),
    Employer("Lincoln Laboratory (MIT)", "lincoln lab", 4000, 0.85, "defense", "541712", "Lexington", "MA", "990_filing", is_nonprofit=True),

    # Consulting
    Employer("Boston Consulting Group", "bcg", 2500, 0.70, "consulting", "541610", "Boston", "MA", "h1b_filings"),
    Employer("Bain & Company", "bain", 2000, 0.70, "consulting", "541610", "Boston", "MA", "h1b_filings"),
    Employer("McKinsey & Company", "mckinsey", 1500, 0.65, "consulting", "541610", "Boston", "MA", "h1b_filings"),
    Employer("Deloitte", "deloitte", 4000, 0.70, "consulting", "541211", "Boston", "MA", "news_estimate"),
    Employer("PwC", "pwc", 3000, 0.70, "consulting", "541211", "Boston", "MA", "news_estimate"),
    Employer("EY", "ey", 2500, 0.70, "consulting", "541211", "Boston", "MA", "news_estimate"),

    # Government
    Employer("Commonwealth of Massachusetts", "commonwealth of massachusetts", 50000, 0.95, "government", "921110", "Boston", "MA", "state_payroll", is_government=True),
    Employer("City of Boston", "city of boston", 20000, 0.95, "government", "921110", "Boston", "MA", "city_payroll", is_government=True),
    Employer("City of Cambridge", "city of cambridge", 4000, 0.90, "government", "921110", "Cambridge", "MA", "city_payroll", is_government=True),
    Employer("City of Worcester", "city of worcester", 8000, 0.85, "government", "921110", "Worcester", "MA", "web_estimate", is_government=True),
    Employer("City of Springfield", "city of springfield", 6700, 0.85, "government", "921110", "Springfield", "MA", "web_estimate", is_government=True),
    Employer("MBTA", "mbta", 6000, 0.80, "government", "485111", "Boston", "MA", "news_estimate", is_government=True),

    # Retail
    Employer("TJX Companies", "tjx", 15000, 0.80, "retail", "452111", "Framingham", "MA", "sec_10k"),
    Employer("BJ's Wholesale", "bjs wholesale", 3000, 0.75, "retail", "452910", "Westborough", "MA", "sec_10k"),
    Employer("Stop & Shop", "stop and shop", 25000, 0.75, "retail", "445110", "Quincy", "MA", "news_estimate"),
    Employer("Market Basket", "market basket", 8000, 0.70, "retail", "445110", "Tewksbury", "MA", "news_estimate"),

    # Insurance
    Employer("Blue Cross Blue Shield of MA", "bcbs ma", 4000, 0.80, "finance", "524114", "Boston", "MA", "990_filing", is_nonprofit=True),
    Employer("Harvard Pilgrim Health Care", "harvard pilgrim", 1500, 0.75, "finance", "524114", "Wellesley", "MA", "990_filing", is_nonprofit=True),
]


def normalize_employer_name(name: str) -> str:
    """Normalize employer name for matching."""
    if not name:
        return ""

    name = name.lower().strip()

    # Remove common suffixes
    suffixes = [', inc.', ', inc', ' inc.', ' inc', ', llc', ' llc',
                ', corp.', ', corp', ' corp.', ' corp', ', ltd', ' ltd',
                ' corporation', ' company', ' co.', ' co']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]

    # Common replacements
    name = name.replace('&', 'and')
    name = name.replace("'s", 's')
    name = name.replace("'", '')

    return name.strip()


def classify_industry(employer_name: str) -> str:
    """Classify employer into industry category."""
    name_lower = employer_name.lower()

    for industry, keywords in INDUSTRY_CATEGORIES.items():
        for keyword in keywords:
            if keyword in name_lower:
                return industry

    return 'other'


class EmployerDatabase:
    """Database of known employers with headcounts."""

    def __init__(self):
        self.employers: Dict[str, Employer] = {}
        self._load_major_employers()

    def _load_major_employers(self):
        """Load manually curated major employers."""
        for emp in MAJOR_MA_EMPLOYERS:
            self.employers[emp.normalized_name] = emp
        logger.info(f"Loaded {len(self.employers)} major employers")

    def add_from_h1b(self, h1b_df: pd.DataFrame,
                     employer_col: str = 'employer_name',
                     city_col: str = 'city',
                     multiplier: float = 15.0):
        """
        Add employers from H-1B filing data.

        H-1B filings represent ~5-10% of tech workforce, so we multiply.
        """
        if employer_col not in h1b_df.columns:
            logger.warning(f"Column {employer_col} not found in H-1B data")
            return

        employer_counts = h1b_df[employer_col].value_counts()

        added = 0
        for employer_name, filing_count in employer_counts.items():
            if pd.isna(employer_name):
                continue

            normalized = normalize_employer_name(employer_name)

            # Skip if already in database
            if normalized in self.employers:
                continue

            # Skip small employers (< 5 filings)
            if filing_count < 5:
                continue

            # Estimate total employees (H-1B is ~5-10% of tech workforce)
            est_employees = int(filing_count * multiplier)

            # Get most common city
            city = 'Boston'  # Default
            if city_col in h1b_df.columns:
                city_counts = h1b_df[h1b_df[employer_col] == employer_name][city_col].value_counts()
                if len(city_counts) > 0:
                    city = city_counts.index[0]

            emp = Employer(
                name=employer_name,
                normalized_name=normalized,
                employee_count=est_employees,
                employee_count_confidence=0.50,  # Lower confidence for estimates
                industry=classify_industry(employer_name),
                naics_code=None,
                city=city if not pd.isna(city) else 'Boston',
                state='MA',
                source='h1b_estimate'
            )

            self.employers[normalized] = emp
            added += 1

        logger.info(f"Added {added} employers from H-1B data")

    def add_from_payroll(self, payroll_df: pd.DataFrame,
                         dept_col: str = 'department',
                         city: str = 'Boston',
                         employer_name: str = 'City of Boston'):
        """
        Add employer from payroll data.

        Payroll gives us exact employee counts.
        """
        normalized = normalize_employer_name(employer_name)

        employee_count = len(payroll_df)

        emp = Employer(
            name=employer_name,
            normalized_name=normalized,
            employee_count=employee_count,
            employee_count_confidence=0.95,  # High confidence from payroll
            industry='government',
            naics_code='921110',
            city=city,
            state='MA',
            source='payroll',
            is_government=True
        )

        self.employers[normalized] = emp
        logger.info(f"Added {employer_name} with {employee_count} employees from payroll")

    def get_employer(self, name: str) -> Optional[Employer]:
        """Look up employer by name."""
        normalized = normalize_employer_name(name)
        return self.employers.get(normalized)

    def get_employers_by_industry(self, industry: str) -> List[Employer]:
        """Get all employers in an industry."""
        return [e for e in self.employers.values() if e.industry == industry]

    def get_total_employment(self) -> int:
        """Get total employment across all known employers."""
        return sum(e.employee_count for e in self.employers.values())

    def to_dataframe(self) -> pd.DataFrame:
        """Export employer database to DataFrame."""
        records = []
        for emp in self.employers.values():
            records.append({
                'employer_name': emp.name,
                'normalized_name': emp.normalized_name,
                'employee_count': emp.employee_count,
                'employee_count_confidence': emp.employee_count_confidence,
                'industry': emp.industry,
                'naics_code': emp.naics_code,
                'city': emp.city,
                'state': emp.state,
                'source': emp.source,
                'is_nonprofit': emp.is_nonprofit,
                'is_government': emp.is_government,
            })

        return pd.DataFrame(records).sort_values('employee_count', ascending=False)

    def summary(self) -> Dict:
        """Get summary statistics."""
        df = self.to_dataframe()

        return {
            'total_employers': len(df),
            'total_employment': df['employee_count'].sum(),
            'by_industry': df.groupby('industry')['employee_count'].sum().to_dict(),
            'by_city': df.groupby('city')['employee_count'].sum().to_dict(),
            'avg_confidence': df['employee_count_confidence'].mean(),
        }


def build_employer_database(data_dir: str = None) -> EmployerDatabase:
    """
    Build comprehensive employer database from all available sources.
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    db = EmployerDatabase()

    # Add from H-1B data
    h1b_path = os.path.join(data_dir, 'h1b_ma_2024.csv')
    if os.path.exists(h1b_path):
        logger.info("Loading H-1B data...")
        h1b_df = pd.read_csv(h1b_path)

        # Find employer column
        emp_cols = [c for c in h1b_df.columns if 'employer' in c.lower() and 'name' in c.lower()]
        city_cols = [c for c in h1b_df.columns if 'city' in c.lower() and 'worksite' in c.lower()]

        if emp_cols:
            db.add_from_h1b(
                h1b_df,
                employer_col=emp_cols[0],
                city_col=city_cols[0] if city_cols else None
            )

    return db


def demo():
    """Demo the employer database."""
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("EMPLOYER DATABASE DEMO")
    print("=" * 70)

    db = build_employer_database()

    summary = db.summary()

    print(f"\nTotal Employers: {summary['total_employers']}")
    print(f"Total Employment: {summary['total_employment']:,}")
    print(f"Average Confidence: {summary['avg_confidence']:.2f}")

    print("\nEmployment by Industry:")
    for industry, count in sorted(summary['by_industry'].items(), key=lambda x: -x[1]):
        print(f"  {industry:<25} {count:>10,}")

    print("\nTop 15 Employers:")
    df = db.to_dataframe()
    for _, row in df.head(15).iterrows():
        print(f"  {row['employee_count']:>8,}  {row['employer_name'][:45]:<45} [{row['industry']}]")


if __name__ == "__main__":
    demo()
