# State Payroll Data Sources - All 50 States

**Goal:** Add payroll data from all 50 states to achieve comprehensive government employment coverage.

**Current Status:** Massachusetts only (115,753 jobs, 54% match rate)

---

## Data Sources Found

### Multi-State Aggregators

1. **OpenPayrolls.com** - [State Employee Salaries](https://openpayrolls.com/state)
   - Coverage: All 50 states + DC + Guam
   - Records: 100+ million salary records
   - Format: Web interface (may need scraping)
   - Note: 403 error when attempting web fetch (may have anti-scraping protection)

2. **GovSalaries.com** - [Government Salaries Database](https://govsalaries.com/)
   - Coverage: 150+ million records from 60k+ sources
   - Format: Web interface with search
   - Note: May require scraping or API access

3. **U.S. Census Bureau ASPEP** - [Annual Survey of Public Employment & Payroll](https://www.census.gov/programs-surveys/apes.html)
   - Coverage: All state and local governments (2024 data)
   - Format: Aggregate statistics (not individual records)
   - Note: Good for validation but not individual job titles

---

## State-by-State Data Portals

### States with Known Open Data Portals:

**1. New York** - [SeeThroughNY](https://www.seethroughny.net/payrolls)
   - Format: CSV download available
   - Coverage: State + local governments
   - Data: Names, titles, salaries, total earnings

**2. Oklahoma** - [Oklahoma Open Data](https://data.ok.gov/dataset/state-of-oklahoma-payroll-fiscal-year-2024)
   - Format: CSV direct download
   - Coverage: State employees FY 2024
   - Files: Monthly payroll files (Payroll_Public_202401.csv, etc.)

**3. Massachusetts** ✅ (Already ingested)
   - Format: CSV
   - Status: 115,753 jobs ingested

**4. Texas** - [Texas Open Data Portal](https://data.texas.gov/)
   - Likely has state employee data
   - Need to verify exact dataset

**5. California** - [California Open Data Portal](https://data.ca.gov/)
   - Likely has state employee data
   - Need to verify exact dataset

**6. Florida** - [Florida Has a Right to Know](https://floridahasarighttoknow.com/)
   - Public salary database
   - Need to verify download format

---

## Recommended Approach

### Phase 1: High-Priority States (Large populations)
Add the top 10 states by population first:

1. ✅ **Massachusetts** - Already done
2. **California** - 39M population
3. **Texas** - 30M population
4. **Florida** - 22M population
5. **New York** - 19M population
6. **Pennsylvania** - 13M population
7. **Illinois** - 12M population
8. **Ohio** - 12M population
9. **Georgia** - 11M population
10. **North Carolina** - 11M population

**Expected Impact:** These 10 states = 65% of US population

### Phase 2: Remaining 40 States
Add all other states in batches of 10.

---

## Implementation Strategy

### Option A: Manual Downloads (Reliable but Slow)
1. Visit each state's open data portal
2. Find payroll dataset
3. Download CSV/Excel
4. Place in `unlisted_jobs/data/state_payrolls/`
5. Run ingestion script

**Pros:** Reliable, no scraping issues
**Cons:** Time-consuming, manual work
**Time:** ~2-3 hours for 10 states

### Option B: Automated Scraping (Fast but Brittle)
1. Build web scraper for OpenPayrolls.com or GovSalaries.com
2. Bypass 403 errors with headers/selenium
3. Download all 50 states at once

**Pros:** Fast once built, automated
**Cons:** May break if sites change, anti-scraping measures
**Time:** ~3-4 hours to build, then minutes to run

### Option C: Hybrid Approach (Recommended)
1. Start with states that have direct CSV downloads (NY, OK, TX, CA, FL)
2. Build scrapers for states without direct downloads
3. Use OpenPayrolls/GovSalaries as fallback

**Pros:** Best of both worlds
**Cons:** Still requires some manual work
**Time:** ~4-5 hours total

---

## Next Steps

**Immediate (Today):**
1. Build generic state payroll ingestion script
2. Download + ingest New York payroll (SeeThroughNY has easy CSV download)
3. Download + ingest Texas payroll
4. Download + ingest California payroll

**This Week:**
1. Add remaining top 10 states
2. Test match rates and role coverage
3. Adjust canonical roles if needed

**This Month:**
1. Add all 50 states
2. Achieve 80%+ match rate across government jobs
3. Estimate total government workforce

---

## Expected Results

### Current Baseline (MA only):
- 115,753 government jobs
- 54% match rate
- 1 state

### After Top 10 States:
- Estimated: ~1.5 million government jobs
- Expected match rate: 60-70% (more diverse roles)
- 10 states (65% of US population)

### After All 50 States:
- Estimated: ~18 million government jobs
- Expected match rate: 65-75%
- Complete government employment coverage
- Can model complete US labor market

---

## Data Schema Standardization

Each state has different column names. Need to map to our schema:

**Our Schema:**
- employee_name
- job_title
- department
- salary (annual)
- employer (state/city/county)

**Common State Formats:**

**Massachusetts:**
- Name, Title, Department, Regular, Total

**New York:**
- Name, Title, Agency, Base Salary, Total Compensation

**Oklahoma:**
- Employee Name, Position Title, Agency, Annual Salary

**Need to build:** Universal mapper that handles all state formats

---

## Sources

- [OpenPayrolls - State Employee Salaries](https://openpayrolls.com/state)
- [GovSalaries Database](https://govsalaries.com/)
- [U.S. Census Bureau ASPEP](https://www.census.gov/programs-surveys/apes.html)
- [SeeThroughNY Payrolls](https://www.seethroughny.net/payrolls)
- [Oklahoma Open Data Portal](https://data.ok.gov/dataset/state-of-oklahoma-payroll-fiscal-year-2024)

---

**Status:** Research complete, ready to implement
**Next:** Build state payroll ingestion system
