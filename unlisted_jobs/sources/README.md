# Data Source Connectors

This directory contains connectors for various job data sources, organized by reliability tier.

## Available Connectors

### ✅ H-1B Visa Data (`h1b_visa.py`)
**Tier A** - High Reliability (0.85)

- **Source:** US Department of Labor H-1B LCA Disclosure Data
- **What it provides:** Actual job positions with verified salaries, companies, locations
- **Data quality:** Official government data, legally required filings
- **Coverage:** ~500,000+ records per year, focused on skilled/tech positions
- **Update frequency:** Annual (fiscal year)

**Fields available:**
- Employer name
- Job title
- Location (city, state)
- Salary range (prevailing wage or actual wage)
- SOC code (occupation classification)
- Full-time vs part-time
- Case status (certified = approved)

**Usage:**
```python
from sources.h1b_visa import H1BVisaConnector

connector = H1BVisaConnector()
df = connector.fetch_year(year=2024, limit=100)
records = connector.to_standard_format(df)
```

**Integration:**
```bash
# Ingest H-1B data into database
python ingest_h1b.py --year 2024 --limit 1000

# Full load (may take 10-20 minutes)
python ingest_h1b.py --year 2024
```

---

## Planned Connectors

### 🚧 State Payroll (`state_payroll.py`)
**Tier A** - High Reliability (0.95)

- Massachusetts state employee payroll
- Other state payroll systems via open data APIs
- **Status:** Not implemented yet

### 🚧 City Payroll (`city_payroll.py`)
**Tier A** - High Reliability (0.95)

- Cambridge city employee payroll
- Other municipal payroll via Socrata APIs
- **Status:** Not implemented yet

### 🚧 Job Board Scrapers (`job_boards.py`)
**Tier B** - Medium Reliability (0.70)

- Indeed, LinkedIn, Glassdoor (with respect to ToS)
- Observed active postings
- **Status:** Not implemented yet

### 🚧 ATS Feeds (`ats_feeds.py`)
**Tier B** - Medium Reliability (0.75)

- Greenhouse, Lever, and other ATS systems
- Direct API access where available
- **Status:** Not implemented yet

### 🚧 BLS OEWS (`oews.py`)
**Tier C** - Macro Priors (0.40)

- Bureau of Labor Statistics Occupational Employment and Wage Statistics
- Metro-level occupation totals
- **Status:** Not implemented yet

---

## Creating a New Connector

### 1. Create the Connector Class

```python
# sources/my_source.py

class MySourceConnector:
    """
    Connector for [Source Name].

    Tier: [A/B/C]
    Reliability: [0.0-1.0]
    """

    def fetch_data(self):
        """Fetch raw data from source."""
        # Your implementation
        pass

    def to_standard_format(self, raw_data):
        """
        Convert to standard format.

        Returns list of dicts with keys:
        - raw_company
        - raw_location
        - raw_title
        - raw_description
        - raw_salary_min
        - raw_salary_max
        - raw_salary_text
        - source_url
        - source_document_id
        - as_of_date
        - raw_data (dict with extra fields)
        """
        pass
```

### 2. Register the Source

```python
# In database
INSERT INTO sources (name, type, reliability_tier, base_reliability, is_active)
VALUES ('my_source', 'posting', 'B', 0.70, TRUE);
```

### 3. Create Ingestion Script

```python
# ingest_my_source.py

from sources.my_source import MySourceConnector
from database import DatabaseManager

connector = MySourceConnector()
data = connector.fetch_data()
records = connector.to_standard_format(data)

# Process records into database...
```

### 4. Test

```bash
python sources/my_source.py  # Run demo
python ingest_my_source.py --limit 100  # Test ingestion
```

---

## Data Source Standards

### Reliability Tiers

**Tier A (0.85-0.95)** - Official Records
- Government payroll
- Visa filings
- CBAs / Pay tables
- High confidence, verified data

**Tier B (0.60-0.75)** - Observed Postings
- Job board postings we actually saw
- ATS feeds
- Employer career pages
- Medium confidence

**Tier C (0.30-0.50)** - Macro Priors
- BLS OEWS aggregates
- QCEW data
- Industry benchmarks
- Used as priors, not evidence

### Standard Format Fields

All connectors must output records with these fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `raw_company` | str | Yes | Company name as it appears in source |
| `raw_location` | str | Yes | Location string (will be normalized) |
| `raw_title` | str | Yes | Job title as it appears in source |
| `raw_description` | str | No | Full job description if available |
| `raw_salary_min` | float | No | Minimum salary (annual) |
| `raw_salary_max` | float | No | Maximum salary (annual) |
| `raw_salary_text` | str | No | Original salary text for reference |
| `source_url` | str | No | URL to original posting/document |
| `source_document_id` | str | Yes | Unique ID for this source record |
| `as_of_date` | date | Yes | Date this data was observed |
| `raw_data` | dict | No | Any additional source-specific fields |

---

## Testing Connectors

### Unit Test Template

```python
# tests/test_h1b_connector.py

import pytest
from sources.h1b_visa import H1BVisaConnector

def test_fetch_data():
    connector = H1BVisaConnector()
    df = connector.fetch_year(2024, limit=10)
    assert len(df) > 0
    assert 'employer_name' in df.columns

def test_standard_format():
    connector = H1BVisaConnector()
    df = connector.fetch_year(2024, limit=10)
    records = connector.to_standard_format(df)

    assert len(records) > 0
    assert 'raw_company' in records[0]
    assert 'raw_title' in records[0]
    assert 'raw_salary_min' in records[0]
```

Run tests:
```bash
pytest tests/test_h1b_connector.py -v
```

---

## Legal & Compliance Notes

### ⚠️ Important

- **Respect ToS:** Always check Terms of Service before scraping
- **Rate Limiting:** Be respectful with request rates
- **Attribution:** Maintain source attribution in metadata
- **Privacy:** Don't store PII unless legally required and compliant
- **Licensing:** Document data licensing terms in source registry

### Allowed Sources

✅ **Public Government Data** - Generally OK
- H-1B disclosures
- State/city payroll
- OEWS/QCEW aggregates

✅ **Official APIs** - When available
- USAJOBS API
- Socrata open data APIs

⚠️ **Job Boards** - Check ToS carefully
- Some allow scraping with attribution
- Some provide APIs
- Some explicitly forbid scraping

❌ **Never**
- Scrape sites that explicitly forbid it
- Bypass authentication or paywalls
- Store personal contact information
- Resell raw scraped data

---

## Questions?

See main [README.md](../README.md) or contact the team.
