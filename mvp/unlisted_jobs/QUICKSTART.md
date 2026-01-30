# Quick Start Guide

Get the comprehensive job database up and running in 10 minutes.

## Prerequisites

```bash
# 1. Install PostgreSQL
brew install postgresql  # macOS
# or
sudo apt-get install postgresql  # Linux

# 2. Start PostgreSQL
brew services start postgresql  # macOS
# or
sudo service postgresql start  # Linux
```

## Setup (5 minutes)

```bash
# 1. Navigate to unlisted_jobs directory
cd unlisted_jobs/

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create database
createdb jobs_comprehensive

# Optional: Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=jobs_comprehensive
export DB_USER=postgres
export DB_PASSWORD=your_password
```

## Initialize Database (1 minute)

```bash
# Initialize schema and seed canonical roles
python pipeline.py --init-schema --seed-roles
```

This creates:
- All tables (companies, locations, canonical_roles, observed_jobs, job_archetypes, etc.)
- Indexes for performance
- Initial canonical roles (Software Engineer, Data Scientist, etc.)
- Source registry

## Run Sample Pipeline (2 minutes)

```bash
# Run pipeline with sample data
python pipeline.py --mode full
```

This will:
1. Process sample payroll data (MIT, Harvard)
2. Normalize companies and locations
3. Map titles to canonical roles
4. Create observed jobs
5. Generate archetypes
6. Compute quality metrics

## Verify Results

```bash
# Open PostgreSQL
psql jobs_comprehensive

# Check what was created
\dt  -- List all tables

-- View observed jobs
SELECT * FROM observed_jobs;

-- View archetypes
SELECT * FROM job_archetypes;

-- View coverage summary
SELECT * FROM coverage_summary;

-- Check pipeline run log
SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;
```

## Expected Output

After running the pipeline, you should see:

```
============================================================
PHASES 1-4: SOURCE ACQUISITION AND NORMALIZATION
============================================================
Processing sample payroll source...
✓ Processed: Senior Software Engineer at Massachusetts Institute of Technology
✓ Processed: Data Scientist at Harvard University
Observed jobs created: 2

============================================================
PHASES 5-9: INFERENCE AND ARCHETYPE GENERATION
============================================================
Creating sample archetypes from observed data...
✓ Created observed archetype ID 1
✓ Created observed archetype ID 2

============================================================
PHASE 10: QUALITY METRICS
============================================================
Quality Metrics:
{
  "observed_jobs_by_source": {
    "sample_payroll": 2
  },
  "archetypes_by_type": {
    "observed": 2
  },
  "title_mapping": {
    "avg_confidence": 0.85,
    "high_confidence_pct": 100.0
  }
}
```

## Next Steps

### 1. Add Real Data Sources

Edit `pipeline.py` and add connectors for:
- State payroll APIs
- H-1B visa data
- Job board scrapers
- University payroll files

See `sources/` directory (to be created) for examples.

### 2. Run Full Pipeline

```bash
# Process all sources
python pipeline.py --mode full
```

### 3. Query the Database

```python
from database import DatabaseManager

db = DatabaseManager()
db.initialize_pool()

# Get all archetypes for a company
archetypes = db.get_archetypes_by_company(company_id=1)

# Get coverage summary
coverage = db.get_coverage_summary()

print(f"Found {len(archetypes)} archetypes")
```

### 4. Explore the Data

```sql
-- Top companies by estimated headcount
SELECT
    c.name,
    SUM(a.headcount_p50) as total_headcount,
    COUNT(DISTINCT a.canonical_role_id) as unique_roles
FROM job_archetypes a
JOIN companies c ON a.company_id = c.id
GROUP BY c.name
ORDER BY total_headcount DESC
LIMIT 10;

-- Average salary by role and seniority
SELECT
    r.name as role,
    a.seniority,
    AVG(a.salary_p50) as median_salary,
    COUNT(*) as archetype_count
FROM job_archetypes a
JOIN canonical_roles r ON a.canonical_role_id = r.id
WHERE a.salary_p50 IS NOT NULL
GROUP BY r.name, a.seniority
ORDER BY role, seniority;

-- Confidence distribution
SELECT
    CASE
        WHEN composite_confidence >= 0.8 THEN 'High (0.8+)'
        WHEN composite_confidence >= 0.6 THEN 'Medium (0.6-0.8)'
        WHEN composite_confidence >= 0.4 THEN 'Low (0.4-0.6)'
        ELSE 'Very Low (<0.4)'
    END as confidence_tier,
    COUNT(*) as count,
    AVG(composite_confidence) as avg_confidence
FROM job_archetypes
GROUP BY confidence_tier
ORDER BY avg_confidence DESC;
```

## Common Issues

### Issue: "Database does not exist"
```bash
# Solution: Create it
createdb jobs_comprehensive
```

### Issue: "psycopg2 installation failed"
```bash
# Solution: Install PostgreSQL development headers
# macOS:
brew install postgresql

# Ubuntu/Debian:
sudo apt-get install libpq-dev python3-dev

# Then reinstall
pip install psycopg2-binary
```

### Issue: "Permission denied for database"
```bash
# Solution: Grant permissions
psql postgres
postgres=# GRANT ALL PRIVILEGES ON DATABASE jobs_comprehensive TO your_username;
```

### Issue: "Role does not exist"
```bash
# Solution: Create PostgreSQL user
createuser -s postgres
# or
createuser -s your_username
```

## Architecture at a Glance

```
Data Sources        Normalization       Core Tables           Output
─────────────      ───────────────     ─────────────────      ──────────
│ Payroll   │  →   │ Companies   │  →  │ observed_jobs │  →   │ REST API │
│ Job Boards│  →   │ Locations   │  →  │ archetypes    │  →   │ CSV Export│
│ Visa Data │  →   │ Roles       │  →  │ compensation  │  →   │ Analytics│
│ OEWS      │  →   │ Title Mapping│     │               │      │          │
─────────────      ───────────────     ─────────────────      ──────────
```

## File Structure

```
unlisted_jobs/
├── README.md              # Full documentation
├── QUICKSTART.md          # This file
├── requirements.txt       # Python dependencies
├── schema.sql             # Database schema
├── database.py            # Database manager
├── title_normalizer.py    # Title → Role mapping
├── pipeline.py            # Main orchestrator
├── sources/               # Data source connectors (to be added)
│   ├── payroll.py
│   ├── h1b_visa.py
│   └── oews.py
└── models/                # ML models (to be added)
    ├── salary_model.py
    ├── headcount_model.py
    └── description_gen.py
```

## Testing

```bash
# Run title normalizer test
python title_normalizer.py

# Run database connection test
python -c "from database import DatabaseManager; db = DatabaseManager(); db.initialize_pool(); print('✓ Connected')"
```

## Getting Help

- Check [README.md](./README.md) for full documentation
- See examples in `pipeline.py`
- File issues on GitHub
- Contact: ShortList.ai team

## What's Next?

1. **Week 1**: Add more data sources (state payroll, H-1B visa)
2. **Week 2**: Implement salary model (hierarchical Bayesian)
3. **Week 3**: Implement headcount allocation model
4. **Week 4**: Add description generation
5. **Week 5**: Build web dashboard for exploration
6. **Week 6**: Integrate with ShortList.ai MVP

---

Happy building! 🚀
