# How to Use the Job Database Builder

## Quick Start (3 Steps)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Up Configuration

**Option A: Use Environment Variables** (Easiest)
```bash
export DB_HOST=localhost
export DB_NAME=jobs_db
export DB_USER=postgres
export DB_PASSWORD=your_password
export ADZUNA_APP_ID=your_app_id
export ADZUNA_APP_KEY=your_app_key
export USAJOBS_API_KEY=your_api_key
export USAJOBS_EMAIL=your_email@example.com
```

**Option B: Use Config File**
```bash
cp config.example.json config.json
# Then edit config.json with your settings
```

### 3. Run the Scraper

**Test with one location:**
```bash
python job_database_builder.py --locations 10001
```

**Process multiple locations:**
```bash
python job_database_builder.py --locations 10001 02139 90210
```

**Process all US locations:**
```bash
python job_database_builder.py --all
```

## Using the File You Have Open

You have `data.scraper.josh.py` open. You can use it in two ways:

### Method 1: Rename it (Recommended)
```bash
# The file has dots in the name which makes imports tricky
# You can use it directly:
python data.scraper.josh.py --locations 10001
```

### Method 2: Import it in Python
```python
import importlib.util
spec = importlib.util.spec_from_file_location("scraper", "data.scraper.josh.py")
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)

# Now use it
config = scraper.Config()
builder = scraper.JobDatabaseBuilder(config)
builder.run(["10001", "02139"])
```

## Common Commands

### Test Your Setup
```bash
python test_scraper.py
```

### Scrape Jobs for Specific ZIP Codes
```bash
python job_database_builder.py --locations 10001 02139 90210
```

### Use Custom Config
```bash
python job_database_builder.py --config my_config.json --locations 10001
```

### Run Scheduled Updates
```bash
python scheduler.py
```

### Check Database
```bash
psql -U postgres -d jobs_db
# Then run:
SELECT COUNT(*) FROM jobs;
SELECT source, COUNT(*) FROM jobs GROUP BY source;
```

## Example: Complete Workflow

```bash
# 1. Install
pip install -r requirements.txt

# 2. Set up database (if not already done)
createdb jobs_db

# 3. Set environment variables
export DB_PASSWORD=your_password
export ADZUNA_APP_ID=your_id
export ADZUNA_APP_KEY=your_key
export USAJOBS_API_KEY=your_key
export USAJOBS_EMAIL=your_email

# 4. Test
python test_scraper.py

# 5. Run scraper
python job_database_builder.py --locations 10001

# 6. Check results
psql -U postgres -d jobs_db -c "SELECT COUNT(*) FROM jobs;"
```

## What Happens When You Run It

1. **Connects to PostgreSQL** - Creates tables if they don't exist
2. **Fetches jobs** - From Adzuna, USAJOBS, and Indeed (if enabled)
3. **Normalizes data** - Converts all jobs to common format
4. **Deduplicates** - Removes duplicate jobs across sources
5. **Saves to database** - Stores all jobs in PostgreSQL
6. **Logs progress** - Writes to `job_scraper.log`

## Viewing Results

### In PostgreSQL:
```sql
-- Count jobs
SELECT COUNT(*) FROM jobs WHERE is_active = TRUE;

-- Jobs by source
SELECT source, COUNT(*) FROM jobs GROUP BY source;

-- Recent jobs
SELECT title, employer, location, posted_date
FROM jobs
ORDER BY posted_date DESC
LIMIT 20;

-- Jobs in a city
SELECT title, employer, salary_min, salary_max
FROM jobs
WHERE city = 'Boston' AND state = 'MA';
```

### In Python:
```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="jobs_db",
    user="postgres",
    password="your_password"
)

df = pd.read_sql("SELECT * FROM jobs WHERE is_active = TRUE", conn)
print(df.head())
```

## Troubleshooting

### "No module named 'psycopg2'"
```bash
pip install psycopg2-binary
```

### "Connection refused" (PostgreSQL)
```bash
# Check if PostgreSQL is running
psql -U postgres

# If not, start it:
# macOS: brew services start postgresql
# Linux: sudo systemctl start postgresql
```

### "No password supplied"
- Set `DB_PASSWORD` environment variable, or
- Edit `config.json` with your password

### "API key invalid"
- Check your API keys are correct
- For Adzuna: https://developer.adzuna.com/
- For USAJOBS: https://developer.usajobs.gov/

### No jobs found
- Check `job_scraper.log` for errors
- Verify API keys are set
- Check if sources are enabled in config

## Next Steps

1. **Start small**: Test with 1-2 ZIP codes first
2. **Scale up**: Process more locations as needed
3. **Automate**: Use `scheduler.py` for regular updates
4. **Monitor**: Check `scraping_status` table for issues

For more details, see `JOB_DATABASE_README.md` or `QUICK_START.md`.
