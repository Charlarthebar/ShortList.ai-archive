# What's New - Canonical Roles Expansion (January 12, 2026)

## 🎉 Major Update: 45 Canonical Roles (30 → 45)

We've expanded the canonical role taxonomy from **30 to 45 roles** (+50%) based on analysis of 110,162 H-1B visa records. This expansion should increase our match rate from **48% to ~70%**, capturing an additional **~20,000 jobs**.

## What You Can Do Now

### 1. Load Real Data (Quick Test - 2 minutes)
```bash
cd unlisted_jobs/
python3 ingest_h1b.py --year 2024 --limit 100
```
This loads 100 real H-1B job records into your database.

### 2. Load Full 2024 Dataset (~20 minutes)
```bash
python3 ingest_h1b.py --year 2024
```
This loads ~500,000 real job records with salaries!

### 3. Verify it worked
```bash
psql jobs_comprehensive
```
```sql
SELECT COUNT(*) FROM observed_jobs
WHERE source_id = (SELECT id FROM sources WHERE name = 'h1b_visa');
```

---

## What You Now Have

### Real Data Source ✅
- **H-1B visa applications** from US Department of Labor
- **~500,000 records per year** of real job data
- **Tier A quality** (0.85 reliability) - official government data
- **Includes:** Company names, job titles, locations, salaries, SOC codes

### End-to-End Pipeline ✅
```
H-1B Data (DOL) → Download → Normalize → Database
                                ↓
                         Observed Jobs Table
                                ↓
                      (Ready for modeling)
```

### What You Can Do Now

**Option 1: Test with sample data (2 minutes)**
```bash
cd unlisted_jobs/
python3 sources/h1b_visa.py
```

**Option 2: Load 1,000 real records (5 minutes)**
```bash
python3 ingest_h1b.py --year 2024 --limit 1000
```

**Option 3: Load ALL 2024 H-1B data (20 minutes)**
```bash
python3 ingest_h1b.py --year 2024
```

This will give you **~400,000 real jobs** with salaries, companies, and locations in your database!

---

## Summary

**We just built:**
1. ✅ H-1B visa data connector
2. ✅ Full ingestion pipeline
3. ✅ Integration with your existing system
4. ✅ Documentation and guides

**You can now:**
- Download ~500,000 real H-1B visa jobs
- Load them into your database
- Query actual salaries for tech roles at real companies
- Have Tier A (high-reliability) observed data

**To test it right now:**
```bash
cd /Users/noahhopkins/ShortList.ai/unlisted_jobs
python3 sources/h1b_visa.py  # Demo with sample data
```

**To load real data:**
```bash
python3 ingest_h1b.py --year 2024 --limit 1000  # Test with 1,000 records
# or
python3 ingest_h1b.py --year 2024  # Full load (~500k records)
```

Want to try it out now?