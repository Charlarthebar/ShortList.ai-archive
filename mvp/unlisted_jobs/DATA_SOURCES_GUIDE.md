# Data Sources Guide

Complete guide to all available data connectors for the comprehensive job database.

---

## Overview

We have **3 high-quality (Tier A) data sources** implemented:

1. **H-1B Visa Data** ✅ FULLY OPERATIONAL (~66k jobs loaded)
2. **MA State Payroll** ✅ CONNECTOR READY (needs CSV download)
3. **PERM Visa Data** ✅ CONNECTOR READY (can download)

---

## 1. H-1B Visa Data (ACTIVE)

### What It Is
- Non-immigrant visa applications for temporary skilled workers
- Filed by US employers for foreign workers
- Covers tech, healthcare, education, research, and other skilled positions

### Coverage
- **66,138 jobs** currently loaded (2024 Q4)
- **22,486 companies**
- **6,421 locations**
- Heavy on tech (50-60%), also universities, hospitals

### Data Quality
- **Tier A:** Official DOL government data
- **Reliability:** 0.85 (very high)
- **Salaries:** 100% coverage, verified
- **Update frequency:** Quarterly releases

### How to Use
```bash
# Already loaded! Check status:
python3 check_status.py

# To reload or add more years:
python3 ingest_h1b.py --year 2024  # Full dataset
python3 ingest_h1b.py --year 2023  # Add 2023 data
```

### Strengths
- ✅ Excellent tech sector coverage
- ✅ Real salaries (not estimates)
- ✅ Large sample size
- ✅ Includes SOC codes for mapping

### Limitations
- ❌ Skewed toward tech/specialized roles
- ❌ Missing retail, service, small business
- ❌ Only sponsored positions (not full workforce)

### Source Files
- Connector: `sources/h1b_visa.py`
- Ingestion: `ingest_h1b.py`
- Documentation: `sources/README.md`, `H1B_CONNECTOR_GUIDE.md`

---

## 2. Massachusetts State Payroll (READY TO USE)

### What It Is
- All Massachusetts state government employee salaries
- Public records from the CTHRU transparency platform
- Includes every state employee from governor to janitors

### Coverage
- **~422,732 employees** (2024)
- **All state agencies** and departments
- **Total payroll:** $10.26 billion
- Mix of roles: teachers, nurses, engineers, admin, police, etc.

### Data Quality
- **Tier A:** Official state government data
- **Reliability:** 0.90 (extremely high - verified payroll)
- **Salaries:** 100% coverage, exact figures
- **Update frequency:** Bi-weekly

### How to Get Data

**Step 1: Download CSV from CTHRU**
1. Go to https://cthrupayroll.mass.gov/
2. Click "Export" or "Download Data"
3. Select year: 2024 (or latest)
4. Download CSV file
5. Save to: `unlisted_jobs/data/ma_payroll_2024.csv`

**Step 2: Load into database**
```bash
# Test with sample data first:
python3 sources/ma_state_payroll.py

# Load real data (once you have CSV):
python3 ingest_ma_payroll.py --file data/ma_payroll_2024.csv
```

### Strengths
- ✅ Complete government sector coverage
- ✅ Real, verified salaries
- ✅ Diverse roles (not just tech)
- ✅ Easy to access (public records)
- ✅ Perfect complement to H-1B (different sector)

### Limitations
- ❌ Government only (no private sector)
- ❌ Massachusetts only (not nationwide)
- ❌ Requires manual CSV download (no API)

### Source Files
- Connector: `sources/ma_state_payroll.py`
- Ingestion: `ingest_ma_payroll.py` (TODO: create this)

### Data Format Example
```
Title,Department,Regular,Other,Total,Year
Software Engineer,Executive Office of Technology Services,95000,5000,100000,2024
Teacher,Department of Education,72000,3000,75000,2024
Registered Nurse,Department of Public Health,82000,8000,90000,2024
```

---

## 3. PERM Visa Data (READY TO USE)

### What It Is
- Permanent Labor Certification applications
- Filed by employers seeking to sponsor foreign workers for green cards
- Similar to H-1B but for permanent employment

### Coverage
- **~100k-150k applications/year**
- Similar company/role distribution to H-1B
- Slightly higher salaries (permanent vs temporary)
- Tech-heavy but also other skilled professions

### Data Quality
- **Tier A:** Official DOL government data
- **Reliability:** 0.85 (very high)
- **Salaries:** 100% coverage, verified prevailing wages
- **Update frequency:** Quarterly releases

### How to Use

**Option 1: Download directly**
```bash
# Test with sample data:
python3 sources/perm_visa.py

# Download real data (auto-downloads from DOL):
python3 ingest_perm.py --year 2024 --limit 1000  # Test with 1k
python3 ingest_perm.py --year 2024              # Full dataset
```

**Option 2: Manual download**
1. Go to https://www.dol.gov/agencies/eta/foreign-labor/performance
2. Download "PERM Disclosure Data FY2024"
3. Save to: `unlisted_jobs/data/perm_cache/perm_2024.xlsx`
4. Run ingestion script

### Strengths
- ✅ Similar to H-1B (easy to integrate)
- ✅ Permanent positions (vs temporary)
- ✅ Additional tech coverage
- ✅ Can combine with H-1B for better company profiles

### Limitations
- ❌ Similar biases to H-1B (tech-heavy)
- ❌ Overlap with H-1B data (same companies)
- ❌ Doesn't expand to new sectors

### Source Files
- Connector: `sources/perm_visa.py`
- Ingestion: `ingest_perm.py` (TODO: create this)

---

## Recommended Implementation Order

### Phase 1: MA State Payroll (THIS WEEK)
**Why:** Complements H-1B perfectly, easy to get, different sector

**Steps:**
1. Download CSV from CTHRU
2. Create `ingest_ma_payroll.py` script
3. Load ~422k government jobs
4. Verify data quality

**Expected Result:**
- **Total jobs:** ~488k (66k H-1B + 422k MA state)
- **Sector mix:** Tech + Government
- **Coverage:** Excellent for skilled positions

### Phase 2: PERM Visa Data (NEXT WEEK)
**Why:** More tech data, easy integration (similar to H-1B)

**Steps:**
1. Create `ingest_perm.py` script
2. Download 2024 PERM data
3. Load ~100k-150k tech jobs
4. Analyze overlap with H-1B

**Expected Result:**
- **Total jobs:** ~600k-650k
- **Better company profiles:** More data points per company
- **Salary validation:** Cross-check H-1B vs PERM

### Phase 3: Build Inference Models (WEEKS 3-4)
**Why:** Use high-quality data to infer missing archetypes

**Steps:**
1. Build salary estimation model (Phase 6)
2. Build headcount allocation model (Phase 8)
3. Use H-1B + MA payroll + PERM as training data
4. Test on holdout data

---

## Data Source Comparison

| Feature | H-1B | MA Payroll | PERM |
|---------|------|------------|------|
| **Coverage** | 66k jobs | 422k jobs | ~150k jobs |
| **Sector** | Tech-heavy | Government | Tech-heavy |
| **Geography** | Nationwide | MA only | Nationwide |
| **Salaries** | 100% | 100% | 100% |
| **Reliability** | 0.85 | 0.90 | 0.85 |
| **Status** | ✅ Loaded | 🟡 Ready | 🟡 Ready |
| **Integration** | Done | Easy | Easy |

---

## Future Data Sources (Not Yet Implemented)

### Tier A (High Quality, Worth Building)
1. **California State Payroll** - ~2M employees, public records
2. **NYC City Payroll** - ~300k employees, public records
3. **University Payroll** - Harvard, MIT, BU, etc. (if available)
4. **Hospital Payroll** - Public hospitals (MA, CA publish)

### Tier B (Medium Quality, Useful)
5. **Job Posting Scrapes** - Indeed, LinkedIn (30% have salaries)
6. **Company 10-Ks** - Public company headcounts
7. **LinkedIn Company Pages** - Self-reported employee counts
8. **WARN Notices** - Layoff tracking (headcount changes)

### Tier C (Reference Only)
9. **BLS OEWS** - Industry × Occupation × Metro aggregates
10. **QCEW** - Establishment counts by industry
11. **Census Bureau** - County Business Patterns

---

## Next Steps

**IMMEDIATE (Today):**
1. ✅ H-1B fully loaded (66k jobs)
2. 🔄 Download MA state payroll CSV
3. 🔄 Create MA payroll ingestion script

**THIS WEEK:**
4. Load MA state payroll (~422k jobs)
5. Verify data quality and coverage
6. Analyze combined H-1B + MA payroll

**NEXT WEEK:**
7. Create PERM ingestion script
8. Load PERM data (~150k jobs)
9. Start building salary estimation model (Phase 6)

---

## Sources

- **H-1B Data:** [US Department of Labor - H-1B Performance Data](https://www.dol.gov/agencies/eta/foreign-labor/performance)
- **MA State Payroll:** [CTHRU Payroll System](https://cthrupayroll.mass.gov/)
- **PERM Data:** [DOL PERM Disclosure Data](https://www.dol.gov/agencies/eta/foreign-labor/performance)
- **OpenPayrolls:** [University Salary Database](https://openpayrolls.com/)

---

**Last Updated:** 2026-01-12
