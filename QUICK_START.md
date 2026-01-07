# Quick Start Guide - US Job Database Builder

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Set Up PostgreSQL Database

If you don't have PostgreSQL installed:

**macOS:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux:**
```bash
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**Create the database:**
```bash
psql postgres
```

Then in PostgreSQL:
```sql
CREATE DATABASE jobs_db;
CREATE USER jobs_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE jobs_db TO jobs_user;
\q
```

## Step 3: Configure the System

1. Copy the example config:
```bash
cp config.example.json config.json
```

2. Edit `config.json` with your settings:
```json
{
  "db_host": "localhost",
  "db_port": 5432,
  "db_name": "jobs_db",
  "db_user": "jobs_user",
  "db_password": "your_password",
  "adzuna_app_id": "your_adzuna_app_id",
  "adzuna_app_key": "your_adzuna_app_key",
  "usajobs_api_key": "your_usajobs_key",
  "usajobs_email": "your_email@example.com"
}
```

**Get API Keys:**
- **Adzuna**: Sign up at https://developer.adzuna.com/
- **USAJOBS**: Register at https://developer.usajobs.gov/

## Step 4: Run the Job Scraper

### Option A: Process Specific Locations (Recommended for Testing)

```bash
python job_database_builder.py --locations 10001 02139 90210
```

This will scrape jobs for:
- 10001 (New York, NY)
- 02139 (Cambridge, MA)
- 90210 (Beverly Hills, CA)

### Option B: Process All US Locations

```bash
python job_database_builder.py --all
```

**Note**: This will take a long time! Start with specific locations first.

### Option C: Use Custom Config File

```bash
python job_database_builder.py --config my_config.json --locations 10001
```

## Step 5: Check the Results

### View in PostgreSQL:

```bash
psql -U jobs_user -d jobs_db
```

Then run queries:
```sql
-- Count jobs by source
SELECT source, COUNT(*) as count
FROM jobs
WHERE is_active = TRUE
GROUP BY source;

-- View recent jobs
SELECT title, employer, location, salary_min, salary_max, posted_date
FROM jobs
WHERE is_active = TRUE
ORDER BY posted_date DESC
LIMIT 20;

-- Check scraping status
SELECT source, location, last_scraped, jobs_found, status
FROM scraping_status
ORDER BY last_scraped DESC;
```

## Step 6: Set Up Scheduled Updates (Optional)

To automatically update the database:

```bash
python scheduler.py
```

This will:
- Run full updates daily at 2 AM
- Run incremental updates every 6 hours

Press Ctrl+C to stop.

## Troubleshooting

### "Module not found" errors
```bash
pip install -r requirements.txt
```

### Database connection errors
- Check PostgreSQL is running: `psql -U postgres`
- Verify credentials in `config.json`
- Check firewall settings

### API errors
- Verify API keys are correct
- Check API rate limits
- Increase `request_delay` in config if hitting limits

### No jobs found
- Check logs: `tail -f job_scraper.log`
- Verify sources are enabled in config
- Check scraping_status table for errors

## Example Workflow

1. **First time setup:**
   ```bash
   # Install dependencies
   pip install -r requirements.txt

   # Set up database
   createdb jobs_db

   # Configure
   cp config.example.json config.json
   # Edit config.json with your API keys

   # Test with one location
   python job_database_builder.py --locations 10001
   ```

2. **Check results:**
   ```bash
   psql -U jobs_user -d jobs_db -c "SELECT COUNT(*) FROM jobs;"
   ```

3. **Scale up:**
   ```bash
   # Process multiple locations
   python job_database_builder.py --locations 10001 02139 90210 60601

   # Or process all locations (takes hours/days)
   python job_database_builder.py --all
   ```

4. **Set up automation:**
   ```bash
   # Run scheduler in background
   nohup python scheduler.py > scheduler.log 2>&1 &
   ```

## Next Steps

- Read `JOB_DATABASE_README.md` for detailed documentation
- Add more job sources (see README for instructions)
- Scale to all US ZIP codes (download comprehensive ZIP code list)
- Set up monitoring and alerts
