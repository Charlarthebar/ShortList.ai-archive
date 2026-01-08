# Comprehensive Job Database

A complete system to build and maintain a database of **ALL jobs** in the United States, including:
- Jobs from major job boards (LinkedIn, Indeed, Adzuna, USAJOBS)
- Jobs **NOT listed** on major sites (company career pages, local businesses)
- Historical job data (tracking when jobs open and close)
- Skills and requirements extraction

## Features

### 1. Multi-Source Job Collection
- **Job Boards**: Adzuna API, USAJOBS API, Indeed scraping
- **Company Career Pages**: Direct scraping of employer websites
- **Local Businesses**: Integration points for Google Places, Yelp, business directories

### 2. Job Status Tracking
- **Active**: Currently open positions
- **Closed**: No longer accepting applications
- **Filled**: Position was filled
- **Expired**: Listing expired

### 3. Historical Tracking
Every status change is recorded in the `job_history` table:
- When a job was first seen
- When it was last updated
- When it closed or was filled
- Full audit trail of changes

### 4. Skills Extraction
Automatic extraction of skills from job descriptions:
- 100+ common skills pre-loaded
- Skill categorization (programming, database, cloud, soft skills)
- Skill-to-job linking for advanced searches

### 5. Company Database
Track employers separately to find unlisted jobs:
- Company career page URLs
- Industry and size information
- Automatic career page scraping

## Quick Start

### 1. Set Up Database

```bash
# Create PostgreSQL database
createdb jobs_db

# Or set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=jobs_db
export DB_USER=postgres
export DB_PASSWORD=your_password
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Quick Start (Local Test)

```bash
# Test with Boston area
python quickstart_local.py

# Test with specific city
python quickstart_local.py --city "San Francisco" --state "CA"

# Test with specific ZIP codes
python quickstart_local.py --zips 10001 10002 10003
```

### 4. Run Full Collection

```bash
# Process specific locations
python comprehensive_job_database.py --locations 02139 10001 90210

# Process a city
python comprehensive_job_database.py --city "Boston" --state "MA"

# Full update (all configured sources)
python comprehensive_job_database.py --full
```

## Configuration

### Environment Variables

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=jobs_db
DB_USER=postgres
DB_PASSWORD=your_password

# API Keys (optional but recommended)
ADZUNA_APP_ID=your_id
ADZUNA_APP_KEY=your_key
USAJOBS_API_KEY=your_key
USAJOBS_EMAIL=your_email
GOOGLE_PLACES_API_KEY=your_key  # For local business discovery
YELP_API_KEY=your_key          # For local business discovery

# Settings
MAX_WORKERS=10
REQUEST_DELAY=1.0
RADIUS_MILES=25
```

### Getting API Keys

1. **Adzuna** (Recommended - aggregates many job boards):
   - Sign up at: https://developer.adzuna.com
   - Free tier available

2. **USAJOBS** (Federal government jobs):
   - Sign up at: https://developer.usajobs.gov
   - Free, requires email verification

3. **Google Places** (Local business discovery):
   - Get from Google Cloud Console
   - Pay-per-use pricing

4. **Yelp** (Local business discovery):
   - Sign up at: https://www.yelp.com/developers
   - Free tier available

## Database Schema

### Jobs Table
Main table storing all job postings:
- Basic info: title, employer, description
- Location: city, state, ZIP, lat/lon, remote status
- Compensation: salary range, benefits
- Status: active, closed, filled, expired
- Tracking: first_seen, last_seen, closed_date
- Skills linked via job_skills table

### Companies Table
Track employers for unlisted job discovery:
- Career page URLs
- Industry and size
- Headquarters location
- Last scrape timestamp

### Skills Table
Master list of skills:
- 100+ pre-loaded common skills
- Categories: programming, database, cloud, soft skills
- Aliases for matching variations

### Job History Table
Audit trail of all changes:
- Status changes (active → closed)
- Data updates
- Timestamp of each event

## Finding Unlisted Jobs

The key innovation is **finding jobs not listed on major job boards**:

### 1. Add Companies to Database

```bash
# From JSON file
python comprehensive_job_database.py --add-companies my_companies.json

# JSON format:
{
  "companies": [
    {
      "name": "Local Tech Startup",
      "website": "https://example.com",
      "career_page_url": "https://example.com/careers",
      "industry": "Technology",
      "headquarters_city": "Boston",
      "headquarters_state": "MA",
      "has_career_page": true
    }
  ]
}
```

### 2. Scrape Career Pages

```bash
# Scrape all companies with career pages
python comprehensive_job_database.py --scrape-careers
```

### 3. Sources for Finding Companies

To build a comprehensive company list:

1. **Business Registries**: State secretary of state websites
2. **Chamber of Commerce**: Local business directories
3. **Industry Associations**: Trade group member lists
4. **Google Maps/Places**: Search for businesses by type
5. **Yelp**: Business listings by category
6. **LinkedIn Company Pages**: (manual or with permission)
7. **Crunchbase**: Startup and tech companies
8. **Local News**: Business sections, "companies hiring" articles

## Usage Examples

### Search for Jobs

```bash
# Search by keyword
python comprehensive_job_database.py --search "software engineer" --state "CA"

# Search with filters
python comprehensive_job_database.py --search "data scientist" --city "Boston" --state "MA"
```

### View Statistics

```bash
# Overall stats
python comprehensive_job_database.py --stats

# Location-specific stats
python comprehensive_job_database.py --stats --state "MA" --city "Boston"
```

### Python API

```python
from comprehensive_job_database import ComprehensiveJobDatabase, Config

# Initialize
config = Config()
db = ComprehensiveJobDatabase(config)
db.initialize()

# Add a company
db.add_company({
    "name": "Example Corp",
    "career_page_url": "https://example.com/careers",
    "headquarters_city": "Boston",
    "headquarters_state": "MA",
    "has_career_page": True
})

# Process locations
db.process_location("02139")  # Cambridge, MA

# Scrape career pages
db.process_career_pages(limit=50)

# Search jobs
jobs = db.search_jobs("python developer", {"state": "MA"})

# Get statistics
stats = db.get_statistics(state="MA")

# Cleanup
db.shutdown()
```

## Scaling Strategy

### Start Small (Recommended)
1. Focus on one metro area (e.g., Boston)
2. Add 50-100 local companies
3. Run daily scrapes
4. Validate data quality

### Scale Up
1. Add more metro areas
2. Increase company database
3. Add more job board sources
4. Set up automated scheduling

### Full US Coverage
1. Process all major ZIP codes (~40,000)
2. Build comprehensive company database
3. Use distributed processing
4. Consider cloud deployment

## Scheduled Updates

```python
# scheduler_comprehensive.py
from apscheduler.schedulers.blocking import BlockingScheduler
from comprehensive_job_database import ComprehensiveJobDatabase, Config

scheduler = BlockingScheduler()

@scheduler.scheduled_job('cron', hour=2)  # 2 AM daily
def daily_full_update():
    db = ComprehensiveJobDatabase(Config())
    db.run_full_update()

@scheduler.scheduled_job('interval', hours=6)  # Every 6 hours
def career_page_update():
    db = ComprehensiveJobDatabase(Config())
    db.initialize()
    db.process_career_pages(limit=100)
    db.shutdown()

scheduler.start()
```

## Data Quality

### Deduplication
- Jobs are deduplicated using SHA256 hash of key fields
- Same job from different sources is tracked once
- Cross-source matching prevents duplicates

### Confidence Scores
Each job has a confidence score (0.0 - 1.0):
- 0.9: Government APIs (USAJOBS)
- 0.8: Major job board APIs (Adzuna)
- 0.7: Web scraping (Indeed)
- 0.5-0.6: Career page scraping

### Status Tracking
- Jobs not seen in 30 days → marked as "closed"
- Jobs not seen in 60 days → marked as "expired"
- Configurable via `deduplication_window_days` and `job_expiry_days`

## Troubleshooting

### Database Connection Issues
```bash
# Check PostgreSQL is running
pg_isready

# Create database if missing
createdb jobs_db
```

### No Jobs Found
1. Check API keys are configured
2. Verify location is valid (use ZIP codes for best results)
3. Check logs: `tail -f comprehensive_job_db.log`

### Career Page Scraping Issues
1. Some sites block scraping - increase `request_delay`
2. Add more companies with known career page URLs
3. Check company's `career_page_url` is correct

## Files

| File | Description |
|------|-------------|
| `comprehensive_job_database.py` | Main system with all features |
| `quickstart_local.py` | Quick start script for local testing |
| `sample_companies.json` | Sample companies for career page scraping |
| `job_database_builder.py` | Original simpler version |
| `run_with_sqlite.py` | SQLite wrapper for testing without PostgreSQL |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Job Sources                               │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│   Adzuna    │   USAJOBS   │   Indeed    │  Career Pages    │
│    API      │     API     │   Scraper   │    Scraper       │
└──────┬──────┴──────┬──────┴──────┬──────┴────────┬─────────┘
       │             │             │               │
       └─────────────┴─────────────┴───────────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │    Skills Extractor     │
              │  (100+ common skills)   │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │    Database Manager     │
              │  - Deduplication        │
              │  - Status tracking      │
              │  - History logging      │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │      PostgreSQL         │
              │  - jobs (main table)    │
              │  - companies            │
              │  - skills               │
              │  - job_skills           │
              │  - job_history          │
              └─────────────────────────┘
```

## Contributing

1. Add new job sources by subclassing `JobSource`
2. Improve skill extraction in `SkillsExtractor`
3. Add more companies to `sample_companies.json`
4. Report issues with specific job boards

## License

Part of the ShortList.ai project.
