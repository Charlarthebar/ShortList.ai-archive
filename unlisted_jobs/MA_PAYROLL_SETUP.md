# Massachusetts State Payroll Setup Guide

Quick guide to download and load MA state employee payroll data.

---

## What You'll Get

- **~422,732 state employees** with salaries
- **All state agencies** and departments
- **100% salary coverage** (public records)
- **Perfect complement to H-1B** (government vs tech)

---

## Step 1: Download Data from CTHRU

### Option A: Web Interface (Recommended)

1. **Go to the CTHRU website:**
   - URL: https://cthrupayroll.mass.gov/

2. **Export the data:**
   - Look for "Export" or "Download Data" button
   - Select year: **2024** (or most recent)
   - Format: **CSV**
   - Click download

3. **Save the file:**
   ```bash
   # Save to this location:
   mkdir -p /Users/noahhopkins/ShortList.ai/unlisted_jobs/data
   # Move downloaded file to:
   mv ~/Downloads/payroll_*.csv /Users/noahhopkins/ShortList.ai/unlisted_jobs/data/ma_payroll_2024.csv
   ```

### Option B: Alternative Sources

If CTHRU doesn't work, try these alternatives:

- **MassOpenBooks:** https://massopenbooks.org/ (may have export option)
- **OpenGovPay:** https://opengovpay.com/state/ma (aggregate view)
- **Boston Herald Database:** https://www.bostonherald.com/2025/01/09/2024-massachusetts-state-employee-payroll-master-list-your-tax-dollars-at-work-database-home/

---

## Step 2: Test with Sample Data (Optional)

Before loading real data, test that everything works:

```bash
cd /Users/noahhopkins/ShortList.ai/unlisted_jobs

# Test the connector
python3 sources/ma_state_payroll.py

# Test the ingestion (with sample data - will fail without real CSV, but that's OK)
python3 ingest_ma_payroll.py --file data/sample.csv --limit 10
```

Expected output:
- Sample records loaded
- Salaries normalized
- Ready for database ingestion

---

## Step 3: Load Real Data

Once you have the CSV downloaded:

```bash
cd /Users/noahhopkins/ShortList.ai/unlisted_jobs

# Set database user (if needed)
export DB_USER=noahhopkins

# Test with first 1,000 records
python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv --limit 1000

# If test works, load full dataset (~20-30 minutes)
python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv
```

### What Happens During Ingestion:

1. **Loads CSV** - Reads all employee records
2. **Normalizes** - Maps columns to standard format
3. **Matches titles** - Uses 45 canonical roles to match job titles
4. **Creates jobs** - Inserts observed_jobs records
5. **Adds compensation** - Creates compensation_observations

### Expected Processing Time:

- **1,000 records:** ~30 seconds
- **10,000 records:** ~5 minutes
- **422,732 records (full):** ~20-30 minutes

---

## Step 4: Verify Data Loaded

Check that the data is in the database:

```bash
# Quick status check
python3 check_status.py

# Should show:
# - ma_state_payroll: XXX,XXX jobs
# - Total jobs: ~488k (66k H-1B + 422k MA payroll)
```

Query the database directly:

```python
from database import DatabaseManager, Config
import os
os.environ['DB_USER'] = 'noahhopkins'

config = Config()
db = DatabaseManager(config)
db.initialize_pool()

conn = db.get_connection()
cursor = conn.cursor()

# Count MA payroll jobs
cursor.execute("""
    SELECT COUNT(*) FROM observed_jobs
    WHERE source_id = (SELECT id FROM sources WHERE name = 'ma_state_payroll')
""")
print(f"MA Payroll jobs: {cursor.fetchone()[0]:,}")

# Top departments
cursor.execute("""
    SELECT raw_data->>'department' as dept, COUNT(*) as count
    FROM source_data_raw
    WHERE source_id = (SELECT id FROM sources WHERE name = 'ma_state_payroll')
    GROUP BY dept
    ORDER BY count DESC
    LIMIT 10
""")
print("\nTop 10 Departments:")
for row in cursor.fetchall():
    print(f"  {row[1]:6,}  {row[0]}")

db.release_connection(conn)
db.close_all_connections()
```

---

## Troubleshooting

### Error: "File not found"

Make sure the CSV file is in the right location:
```bash
ls -lh /Users/noahhopkins/ShortList.ai/unlisted_jobs/data/ma_payroll_2024.csv
```

### Error: "Database connection failed"

Set the database user:
```bash
export DB_USER=noahhopkins
```

Or add to `.env` file:
```bash
echo "DB_USER=noahhopkins" >> .env
```

### Error: "Column not found"

The CTHRU CSV format may have changed. Check the actual column names:
```python
import pandas as pd
df = pd.read_csv('data/ma_payroll_2024.csv', nrows=5)
print(df.columns.tolist())
```

Update the column mapping in `sources/ma_state_payroll.py` if needed.

### Low Match Rate (Too Many Skipped)

If many records are skipped due to "no role match", you may need to:
1. Add more canonical roles (see `title_normalizer.py`)
2. Add better pattern matching for government job titles
3. Run `analyze_unmatched.py` to see what titles aren't matching

---

## Expected Results

### Data Coverage:

**Before MA Payroll:**
- H-1B: 66,138 jobs
- Companies: 22,486
- Sectors: Heavily tech-focused

**After MA Payroll:**
- Total: ~488,000 jobs (+635% increase!)
- Companies: ~22,487 (mostly same, + Commonwealth of MA)
- Sectors: Tech + Government (much better diversity)

### Sector Mix:

- **Tech/Skilled:** ~66k (H-1B)
- **Government:** ~422k (MA Payroll)
  - Teachers: ~70-80k
  - Healthcare: ~50-60k
  - Public safety: ~30-40k
  - Administrative: ~100-120k
  - Engineering/Tech: ~10-20k
  - Other: ~150-180k

### Salary Distribution:

- **H-1B median:** ~$140k (tech-heavy)
- **MA Payroll median:** ~$65-75k (broad mix)
- **Combined median:** ~$70-80k (more realistic)

---

## Next Steps After Loading

Once MA payroll is loaded:

1. **Verify data quality:**
   ```bash
   python3 check_status.py
   python3 analyze_unmatched.py --limit 10000
   ```

2. **Load PERM visa data** (see `DATA_SOURCES_GUIDE.md`)

3. **Build inference models** (Phase 6-8 of the plan)

---

## Data Source Information

- **Official source:** https://cthrupayroll.mass.gov/
- **Update frequency:** Bi-weekly
- **Data quality:** Tier A (official government records)
- **Reliability:** 0.90 (very high)
- **Coverage:** All Massachusetts state employees
- **Years available:** 2010-present

---

**Questions?** Check the main guide: `DATA_SOURCES_GUIDE.md`

**Last Updated:** 2026-01-12
