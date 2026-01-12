# H-1B Data Connector - Quick Guide

## What We Just Built

A complete connector that downloads **real H-1B visa application data** from the US Department of Labor and loads it into your comprehensive job database.

## What is H-1B Data?

When companies hire foreign workers for skilled positions, they must file H-1B visa applications with the government. These filings include:
- **Company name** (e.g., Google, Microsoft, MIT)
- **Job title** (e.g., "Software Engineer", "Data Scientist")
- **Location** (city and state)
- **Salary** (actual wage offered or prevailing wage)
- **SOC code** (official occupation classification)

This is **Tier A data** (0.85 reliability) - official government records.

## Why This is Valuable

- ✅ **Real salaries** for tech/skilled positions
- ✅ **~500,000+ records** per year
- ✅ **Verified companies** and locations
- ✅ **Free and public** data
- ✅ **Updated annually** by the government
- ✅ **Covers roles** like engineers, scientists, doctors, researchers

## How to Use It

### Prerequisites

Make sure you have the database set up:

```bash
cd unlisted_jobs/

# Install dependencies (if not already done)
pip install -r requirements.txt

# Initialize database (if not already done)
python pipeline.py --init-schema --seed-roles
```

### Method 1: Test with Sample Data (2 minutes)

```bash
# This uses built-in sample data (8 records)
python3 sources/h1b_visa.py
```

**What you'll see:**
- Sample of 8 H-1B records (Google, Microsoft, MIT, etc.)
- Salary distribution
- Top employers
- Standard format conversion

### Method 2: Download Real Data - Small Test (5 minutes)

```bash
# Download 1,000 real records from 2024
python3 ingest_h1b.py --year 2024 --limit 1000
```

**What happens:**
1. Downloads H-1B data from DOL website (~100MB file)
2. Caches it locally (future runs are faster)
3. Normalizes companies, locations, titles
4. Creates observed jobs in database
5. Creates compensation observations

**Expected output:**
```
Records fetched:              1,000
Records processed:            800
Observed jobs created:        750
Compensation obs created:     750
Skipped (no role match):      50
Errors:                       0
```

### Method 3: Full Load - All 2024 Data (20 minutes)

```bash
# Download ALL ~500,000 records from 2024
python3 ingest_h1b.py --year 2024
```

**Warning:** This is a large file (~100MB) and will take time to:
- Download (~5 min)
- Process (~15 min)
- Insert into database

**Result:** ~300,000-400,000 jobs in your database!

### Method 4: Load Multiple Years

```bash
# Load 2022, 2023, and 2024
python3 ingest_h1b.py --year 2022
python3 ingest_h1b.py --year 2023
python3 ingest_h1b.py --year 2024
```

---

## Checking Your Data

### In Python

```python
from database import DatabaseManager

db = DatabaseManager()
db.initialize_pool()

# Get all H-1B jobs
conn = db.get_connection()
cursor = conn.cursor()

cursor.execute("""
    SELECT COUNT(*) FROM observed_jobs
    WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa')
""")
count = cursor.fetchone()[0]
print(f"H-1B jobs in database: {count:,}")

# Get salary distribution
cursor.execute("""
    SELECT
        AVG(salary_min) as avg_salary,
        MIN(salary_min) as min_salary,
        MAX(salary_min) as max_salary
    FROM observed_jobs
    WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa')
    AND salary_min IS NOT NULL
""")
stats = cursor.fetchone()
print(f"Average salary: ${stats[0]:,.0f}")
print(f"Salary range: ${stats[1]:,.0f} - ${stats[2]:,.0f}")

db.release_connection(conn)
```

### In PostgreSQL

```bash
psql jobs_comprehensive
```

```sql
-- Count H-1B jobs
SELECT COUNT(*) FROM observed_jobs
WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa');

-- Top companies
SELECT raw_company, COUNT(*) as job_count
FROM observed_jobs
WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa')
GROUP BY raw_company
ORDER BY job_count DESC
LIMIT 10;

-- Top roles
SELECT r.name as role, COUNT(*) as count
FROM observed_jobs o
JOIN canonical_roles r ON o.canonical_role_id = r.id
WHERE o.source_id = (SELECT id FROM sources WHERE name = 'h1b_visa')
GROUP BY r.name
ORDER BY count DESC
LIMIT 10;

-- Salary by role
SELECT
    r.name as role,
    COUNT(*) as count,
    AVG(o.salary_min) as avg_salary,
    MIN(o.salary_min) as min_salary,
    MAX(o.salary_min) as max_salary
FROM observed_jobs o
JOIN canonical_roles r ON o.canonical_role_id = r.id
WHERE o.source_id = (SELECT id FROM sources WHERE name = 'h1b_visa')
AND o.salary_min IS NOT NULL
GROUP BY r.name
ORDER BY count DESC
LIMIT 10;
```

---

## Files Created

| File | Purpose |
|------|---------|
| `sources/h1b_visa.py` | H-1B data connector (downloads & normalizes) |
| `ingest_h1b.py` | Integration script (loads into database) |
| `sources/README.md` | Documentation for all connectors |
| `H1B_CONNECTOR_GUIDE.md` | This file (quick guide) |

---

## What's in the Data?

### Coverage
- **Industries:** Heavy on tech, but also healthcare, finance, education
- **Companies:** Google, Microsoft, Amazon, Meta, universities, hospitals
- **Roles:** Software engineers, data scientists, researchers, doctors, etc.
- **Geography:** All US states (concentrated in CA, WA, NY, MA, TX)

### Salary Ranges (Typical 2024)
- Software Engineer: $120k - $180k
- Senior Software Engineer: $150k - $200k
- Data Scientist: $130k - $170k
- Product Manager: $140k - $190k
- Research Scientist: $100k - $150k

### Data Quality
- ✅ **High accuracy** - legally required filings
- ✅ **Consistent format** - standardized by DOL
- ✅ **Updated annually** - new data each fiscal year
- ⚠️ **Bias:** Skewed toward skilled/tech positions
- ⚠️ **Timing:** Fiscal year data (Oct-Sept), released ~6 months later

---

## Next Steps

### 1. Verify Your Data (Do This Now)
```bash
# Load sample data and check it worked
python3 ingest_h1b.py --year 2024 --limit 100
psql jobs_comprehensive -c "SELECT COUNT(*) FROM observed_jobs WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa');"
```

### 2. Load More Data
```bash
# Load full 2024 dataset
python3 ingest_h1b.py --year 2024
```

### 3. Add More Sources
Next sources to add:
- **State payroll** (Massachusetts) - even higher quality
- **City payroll** (Cambridge) - local government jobs
- **Job boards** (Indeed) - broader coverage

### 4. Run Full Pipeline
```bash
# Process all data through archetype generation
python3 pipeline.py --mode full
```

---

## Troubleshooting

### "Failed to download H-1B data"
- **Solution:** The connector will use sample data automatically
- **Or:** Check your internet connection and try again

### "No role match" for many titles
- **Solution:** Add more canonical roles to `canonical_roles` table
- **Or:** Improve title mapping rules in `title_normalizer.py`

### Database connection error
- **Solution:** Make sure PostgreSQL is running
- **Check:** `pg_isready` or `brew services list` (Mac)

### Slow processing
- **Normal:** Processing 100k+ records takes time (~15 minutes)
- **Speed up:** Use `--limit` flag for testing

---

## Questions?

- Check [README.md](README.md) for full documentation
- Check [sources/README.md](sources/README.md) for connector details
- Check [actual plan](actual plan) for overall roadmap

---

**Congrats! You now have real job data flowing into your comprehensive database!** 🎉
