# US Job Database Builder

A comprehensive system to build and maintain an active database of all jobs in the United States. This system integrates multiple APIs and web scraping sources to create a complete, up-to-date job database.

## Features

- **Multi-source aggregation**: Integrates APIs (Adzuna, USAJOBS) and web scraping (Indeed, Glassdoor, ZipRecruiter, Monster, CareerBuilder)
- **Scalable architecture**: Distributed processing across all US locations
- **PostgreSQL database**: Robust database schema with proper indexing for fast queries
- **Automatic deduplication**: Identifies and merges duplicate job postings across sources
- **Scheduled updates**: Keeps database current with automated periodic scraping
- **Rate limiting**: Respectful scraping with configurable delays and retry logic
- **Error handling**: Comprehensive error handling and logging
- **Monitoring**: Track scraping status and job statistics

## Architecture

```
job_database_builder.py  - Main orchestrator and job sources
├── DatabaseManager      - PostgreSQL operations and schema
├── HTTPClient           - HTTP requests with retry logic
├── GeoHelper            - Geographic utilities
├── JobSource            - Base class for job sources
├── AdzunaSource         - Adzuna API integration
├── USAJobsSource        - USAJOBS API integration
├── IndeedSource         - Indeed.com web scraping
└── JobDatabaseBuilder   - Main orchestrator

scheduler.py             - Scheduled updates
load_us_locations.py     - US location data loader
```

## Installation

### Prerequisites

- Python 3.8+
- PostgreSQL 12+ (with PostGIS extension recommended)
- API keys for:
  - Adzuna (https://developer.adzuna.com/)
  - USAJOBS (https://developer.usajobs.gov/)

### Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL database:**
   ```sql
   CREATE DATABASE jobs_db;
   CREATE USER jobs_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE jobs_db TO jobs_user;
   ```

3. **Configure the system:**
   ```bash
   cp config.example.json config.json
   # Edit config.json with your database and API credentials
   ```

4. **Load US locations (optional):**
   ```bash
   python load_us_locations.py
   # Or download a comprehensive US ZIP code CSV and place it as us_zip_codes.csv
   ```

## Configuration

Edit `config.json` with your settings:

```json
{
  "db_host": "localhost",
  "db_port": 5432,
  "db_name": "jobs_db",
  "db_user": "postgres",
  "db_password": "your_password",
  "adzuna_app_id": "your_app_id",
  "adzuna_app_key": "your_app_key",
  "usajobs_api_key": "your_api_key",
  "usajobs_email": "your_email@example.com",
  "max_workers": 10,
  "request_delay": 1.0,
  "enable_adzuna": true,
  "enable_usajobs": true,
  "enable_indeed": true
}
```

### Environment Variables

You can also set configuration via environment variables:

```bash
export DB_HOST=localhost
export DB_NAME=jobs_db
export ADZUNA_APP_ID=your_app_id
export ADZUNA_APP_KEY=your_app_key
# ... etc
```

## Usage

### Basic Usage

Process specific locations:
```bash
python job_database_builder.py --locations 10001 02139 90210
```

Process all US locations:
```bash
python job_database_builder.py --all
```

Use custom config file:
```bash
python job_database_builder.py --config my_config.json --locations 10001
```

### Scheduled Updates

Run the scheduler to keep the database updated automatically:
```bash
python scheduler.py
```

The scheduler will:
- Run a full update daily at 2 AM
- Run incremental updates every 6 hours

### Database Queries

Example queries on the PostgreSQL database:

```sql
-- Count jobs by source
SELECT source, COUNT(*) as count
FROM jobs
WHERE is_active = TRUE
GROUP BY source;

-- Find jobs in a specific city
SELECT title, employer, location, salary_min, salary_max
FROM jobs
WHERE city = 'Boston' AND state = 'MA' AND is_active = TRUE
ORDER BY posted_date DESC;

-- Jobs posted in last 7 days
SELECT COUNT(*)
FROM jobs
WHERE posted_date >= CURRENT_DATE - INTERVAL '7 days'
AND is_active = TRUE;

-- Scraping status
SELECT source, location, last_scraped, jobs_found, status
FROM scraping_status
ORDER BY last_scraped DESC;
```

## Data Sources

### API Sources

- **Adzuna**: Aggregates job postings from multiple job boards
- **USAJOBS**: Official US federal government job postings

### Web Scraping Sources

- **Indeed**: Largest job board in the US
- **Glassdoor**: Jobs with company reviews
- **ZipRecruiter**: Job aggregator
- **Monster**: Established job board
- **CareerBuilder**: Major job board

**Note**: Web scraping should be done responsibly and in compliance with each site's Terms of Service. Consider using official APIs when available.

## Database Schema

### `jobs` Table

Main table storing all job postings:

- `id`: Primary key
- `job_hash`: Unique hash for deduplication
- `title`, `employer`, `location`: Job details
- `city`, `state`, `zip_code`: Geographic information
- `latitude`, `longitude`: Coordinates
- `remote`: Boolean for remote work
- `salary_min`, `salary_max`: Salary range
- `description`, `requirements`: Job details
- `source`, `source_id`: Source tracking
- `url`: Original job posting URL
- `posted_date`, `expiration_date`: Date information
- `is_active`: Whether job is still active
- `confidence_score`: Data quality score
- `metadata`: Additional JSON data

### `scraping_status` Table

Tracks scraping operations:

- `source`: Job source name
- `location`: Location processed
- `last_scraped`: Timestamp of last scrape
- `jobs_found`, `jobs_new`, `jobs_updated`: Statistics
- `status`: Status (pending, completed, error)

## Performance Considerations

- **Batch processing**: Jobs are inserted in batches for efficiency
- **Indexing**: Database indexes on frequently queried fields
- **Concurrent processing**: Uses ThreadPoolExecutor for parallel scraping
- **Rate limiting**: Configurable delays to respect API limits
- **Connection pooling**: Efficient database connection management

## Scaling to All US Jobs

To scale to all US jobs:

1. **Get comprehensive location data:**
   - Download US ZIP code database (e.g., from simplemaps.com)
   - Or use state-by-state processing

2. **Increase workers:**
   ```json
   {
     "max_workers": 50,
     "request_delay": 0.5
   }
   ```

3. **Run on multiple machines:**
   - Partition locations across machines
   - Use shared PostgreSQL database

4. **Use job queue (optional):**
   - Integrate with Celery or similar
   - Process locations asynchronously

## Monitoring

Check logs:
```bash
tail -f job_scraper.log
```

Query scraping status:
```sql
SELECT * FROM scraping_status
WHERE status = 'error'
ORDER BY updated_at DESC;
```

## Troubleshooting

### Database Connection Issues

```bash
# Test PostgreSQL connection
psql -h localhost -U postgres -d jobs_db
```

### API Rate Limiting

Increase `request_delay` in config:
```json
{
  "request_delay": 2.0
}
```

### Missing Jobs

- Check scraping status table for errors
- Verify API keys are valid
- Check if sources are enabled in config
- Review logs for specific errors

## Legal and Ethical Considerations

- **Respect robots.txt**: Check each site's robots.txt before scraping
- **Rate limiting**: Don't overload servers with requests
- **Terms of Service**: Review and comply with each site's ToS
- **Data usage**: Use scraped data responsibly and ethically
- **Attribution**: Consider attributing sources when displaying data

## Contributing

To add a new job source:

1. Create a new class inheriting from `JobSource`
2. Implement `fetch_jobs()` and `normalize_job()` methods
3. Add source to `JobDatabaseBuilder.__init__()`
4. Update configuration options

Example:
```python
class NewSource(JobSource):
    def fetch_jobs(self, location: str, location_type: str = "zip"):
        # Fetch jobs from new source
        pass

    def normalize_job(self, raw_job: Dict[str, Any]):
        # Normalize to common schema
        pass
```

## License

[Your License Here]

## Support

For issues or questions, please open an issue on the repository.
