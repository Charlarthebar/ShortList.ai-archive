# Multi-State Job Scraper

A systematic job aggregation tool that captures 90%+ of open jobs by iterating through geographic, industry, and job-type groups across multiple platforms.

## Project Structure

```
job_scraper/
├── configs/
│   └── states/           # Per-state configuration files
│       ├── north_carolina.json
│       ├── texas.json
│       └── ...
├── scrapers/
│   ├── base.py           # Abstract base scraper
│   ├── indeed.py
│   ├── ziprecruiter.py
│   ├── linkedin.py
│   └── glassdoor.py
├── core/
│   ├── iterator.py       # Group iteration logic
│   ├── deduplicator.py   # Job deduplication
│   └── models.py         # Data models
├── output/               # Scraped job data
├── main.py               # Main orchestrator
├── requirements.txt
└── README.md
```

## Usage

```bash
# Scrape all jobs for a single state
python main.py --state north_carolina

# Scrape all jobs for multiple states
python main.py --state north_carolina --state texas --state california

# Scrape all configured states
python main.py --all-states
```

## Configuration

Each state has a JSON config file with groups optimized for 90%+ coverage.
