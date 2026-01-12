# Canonical Role Expansion Recommendations

Based on analysis of 29,783 records across 3 data sources (H-1B, PERM, MA Payroll)

**Current Status:**
- **45 canonical roles** in database
- **Match rates:**
  - H-1B: 60.2% (need to get to 70-80%)
  - PERM: 52.5% (need to get to 65-75%)
  - MA Payroll: 13.9% (need to get to 40-50%)

---

## High-Priority Roles to Add (20 roles)

These roles would capture the most unmatched jobs across all three sources:

### Tier 1: Government/Public Service (8 roles)
**Why:** Massively improves MA Payroll match rate (currently 13.9%)

1. **Social Worker** - 467 jobs in MA payroll alone
   - Patterns: `social worker`, `clinical social worker`
   - Example titles: "Social Worker II-DCF", "Clinical Social Worker (D)"

2. **Correction Officer** - 388 jobs in MA payroll
   - Patterns: `correction officer`, `corrections officer`
   - Example titles: "Correction Officer I/II/III"

3. **Police Officer** - Already exists but needs more patterns
   - Add patterns: `state police trooper`, `probation officer`, `law enforcement`, `sheriff`
   - Example titles: "State Police Trooper, 1st Class", "State Police Sergeant"

4. **Licensed Practical Nurse (LPN)** - 86+ jobs
   - Patterns: `licensed practical nurse`, `lpn`, `practical nurse`
   - Example titles: "Licensed Practical Nurse II"

5. **Program Coordinator** - 461 jobs in MA payroll
   - Patterns: `program coordinator`, `program manager assistant`
   - Example titles: "Program Coordinator I/II/III"

6. **Environmental Analyst** - 149+ jobs
   - Patterns: `environmental analyst`, `environmental specialist`
   - Example titles: "Environmental Analyst III/IV/V"

7. **Human Services Coordinator** - 163+ jobs
   - Patterns: `human services coordinator`, `social services coordinator`
   - Example titles: "Human Services Coordinator I/II"

8. **Clerk** - 39+ jobs (typical government role)
   - Patterns: `clerk`, `office clerk`, `administrative clerk`
   - Example titles: "Clerk IV", "Office Clerk"

### Tier 2: Tech/Engineering (4 roles)
**Why:** Improves H-1B and PERM match rates (currently 60% and 52%)

9. **Technical Program Manager** - High frequency in tech
   - Patterns: `technical program manager`, `tpm`, `technical program management`
   - Example titles: "Technical Program Management", "Technical Program Specialist"

10. **Staff Engineer** - Senior engineering role
    - Patterns: `staff engineer`, `principal engineer`, `senior engineer`
    - Example titles: "Staff Engineer", "Staff Scientist"

11. **Applied Scientist** - ML/Research role at tech companies
    - Patterns: `applied scientist`, `research scientist ii`
    - Example titles: "Applied Scientist II", "Applied Research Scientist"

12. **Site Reliability Engineer (SRE)** - Modern DevOps role
    - Patterns: `site reliability engineer`, `sre`, `reliability engineer`
    - Common at Google, Meta, etc.

### Tier 3: Healthcare Expansion (3 roles)
**Why:** Better coverage of healthcare sector

13. **Nursing Assistant/Aide** - 243 jobs in PERM alone
    - Patterns: `nursing assistant`, `nursing aide`, `cna`, `certified nursing assistant`
    - Example titles: "Nursing Assistant", "Nursing Aide", "CNA"

14. **Mental Health Worker** - 101+ jobs in MA payroll
    - Patterns: `mental health worker`, `mental health counselor`, `behavioral health`
    - Example titles: "Mental Health Worker II/III/IV"

15. **Occupational Therapist** - Already exists but low match
    - Improve patterns: `occupational therapist`, `ot `, `otr`
    - Example titles: "Occupational Therapist II"

### Tier 4: Business/Other (5 roles)

16. **Market Research Analyst** - 8+ H-1B jobs
    - Patterns: `market research analyst`, `market analyst`, `marketing research`
    - Example titles: "Market Research Analyst"

17. **Quantitative Analyst** - 17+ PERM jobs (finance)
    - Patterns: `quantitative analyst`, `quant analyst`, `quantitative researcher`
    - Example titles: "Quantitative Analyst", "Quant Researcher"

18. **Tax Specialist** - Accounting adjacent
    - Patterns: `tax specialist`, `tax senior`, `tax accountant`, `tax analyst`
    - Example titles: "Tax Senior", "Tax Specialist"

19. **Attorney/Lawyer** - Legal roles
    - Patterns: `attorney`, `lawyer`, `counsel`, `legal counsel`
    - Example titles: "Associate Attorney", "Counsel II"

20. **Lecturer** - Education role
    - Patterns: `lecturer`, `adjunct`, `instructor`
    - Example titles: "Lecturer", "Instructors/Lecturers/Trainers"

---

## Blue-Collar Roles (Low Priority for Phase 1)

These are common in PERM but not our initial focus:

- **Truck Driver** - 157 jobs in PERM
- **Warehouse Worker** - 35 jobs
- **Housekeeper** - 59 jobs
- **Cook/Chef** - 20 jobs
- **Caregiver** - 53 jobs
- **Factory Worker** - 44 jobs

**Recommendation:** Add these in Phase 2 after we've improved white-collar match rates.

---

## Pattern Improvements for Existing Roles

These roles exist but need better regex patterns:

### Software Engineer
- Add: `java developer`, `dot net developer`, `.net developer`, `salesforce developer`
- Add: `senior software associate`, `software development`, `sde ii/iii`

### Business Analyst
- Add: `lead analyst`, `senior analyst`, `analyst ii/iii`

### Data Analyst
- Add: `business intelligence engineer`, `bi engineer`, `analytics engineer`

### Manager (Generic)
- Add: `associate manager`, `senior manager`, `manager ii/iii`
- Add: `advisory manager`, `consulting manager`

### Technical Specialist
- Add: `solution specialist`, `technical support`, `application support`

---

## Implementation Strategy

### Phase 1: Government Focus (Improve MA Payroll to 40-50%)
**Add first 8 roles (Tier 1)** - Would capture ~1,700 additional MA payroll jobs

Expected improvement:
- MA Payroll: 13.9% → 43% (+29 percentage points!)
- Total new jobs: ~21,000 across all sources

### Phase 2: Tech/Healthcare Focus (Improve H-1B/PERM to 70%+)
**Add roles 9-15 (Tier 2-3)** - Would capture ~500 additional tech jobs

Expected improvement:
- H-1B: 60.2% → 72% (+12 percentage points)
- PERM: 52.5% → 65% (+13 percentage points)
- Total new jobs: ~8,000 across all sources

### Phase 3: Business/Blue-Collar (Optional)
**Add roles 16-20 + blue-collar** - Remaining long-tail coverage

Expected improvement:
- H-1B: 72% → 78%
- PERM: 65% → 75%
- MA Payroll: 43% → 52%

---

## Quick Wins

**If we add just the Top 10 roles:**
1. Social Worker
2. Correction Officer
3. Police Officer (pattern expansion)
4. Program Coordinator
5. Environmental Analyst
6. Human Services Coordinator
7. Licensed Practical Nurse
8. Technical Program Manager
9. Staff Engineer
10. Mental Health Worker

**We would capture an additional ~27,000 jobs** and improve match rates to:
- H-1B: 60% → 67%
- PERM: 52% → 59%
- MA Payroll: 14% → 38%

**Overall database would grow from 136,000 → 163,000 jobs (+20%)**

---

## Next Steps

1. **Review and approve** the top 10-20 roles
2. **Add to title_normalizer.py** with regex patterns
3. **Re-ingest data** or update existing records
4. **Measure improvement** with analyze_unmatched_all.py
5. **Iterate** on pattern matching for remaining unmatched titles

---

**Generated:** 2026-01-12
**Data analyzed:** 29,783 records (10k H-1B, 9.5k PERM, 10k MA Payroll)
