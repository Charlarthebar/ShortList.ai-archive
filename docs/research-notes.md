# Job Database Plan

## Job Database Overview

- Build a job database either fully, or create a representative sample population of jobs for initial product use.
- A complete database can be used for many downstream applications.
- Existing platforms (Indeed, ZipRecruiter, etc.) focus only on **currently open jobs**.
- By incorporating government payrolls, job board data, and other sources, we can infer jobs that may not be publicly listed.
- Use **points of intersection across data sources** to identify jobs that are more likely to represent real roles.
- Support both:
  - Open jobs
  - Filled jobs (not currently open)

---

## Open Jobs (Currently Hiring)

### Data Sources
- **USAJOBS**
- **Adzuna**
- **Jooble**
- **LinkedIn** (potentially)

---

## Filled Jobs (Not Currently Open)

### Data Source #1: BLS OEWS (Bureau of Labor Statistics)

- **What it provides**
  - Aggregate employment counts by occupation for specific metro areas (e.g., Boston–Cambridge).
- **Why it matters**
  - Job boards show roughly **~5% of jobs** (open positions).
  - BLS data captures the **other ~95%** — all filled positions that exist in the labor market.

---

### Data Source #2: IRS Form 990 (Nonprofit Data)

- **What it provides**
  - Employee counts and compensation data for nonprofit organizations, including:
    - Universities
    - Hospitals
    - Charities

---

### Data Source #3: Massachusetts State Payroll

- **What it provides**
  - Real job titles and salaries for Massachusetts state government employees.

---

### Data Source #4: Historical Job Tracking

- **What it does**
  - Automatically marks jobs as `"filled"` when they disappear from job boards.

- **Logic**
  1. Scraper collects jobs and updates a `last_seen` timestamp.
  2. If a job is not seen for **45+ days**, its status is set to `filled`.
  3. Status changes are logged in a `job_status_history` table.

- **Why it matters**
  A job that was posted and then filled tells you:
  - The position exists at that employer.
  - What compensation the employer was willing to offer.
  - Whether the role may reopen in the future.

---
### Data Source #5: Major Cambridge Employers (Estimated)
- 




---
### Data Source #6: Cambridge City Payroll
- 

---
### Data Source #7: Could have users input data when registering
- 

---
### Data Source #8: Could have partnerships with companies in an exchange sort of deal
- In exchange for your company data (open roles, filled roles, titles, etc.), we can provide you access to search for candidates who already have interest in your company, location, etc.

# What We Can Do With the Job Database

## Employees / People

- Users can be categorized as:
  - Looking
  - Just looking
  - Actively looking

- Serve as a **matchmaker** between people and jobs.

### Career Market Maker (Not a Job Board)

#### For Individuals
- Show **all jobs they could plausibly get**, including:
  - Jobs not currently hiring
  - Roles that would hire if approached
  - Adjacent roles the user may not realize exist

- Provide a **recommendation / relevance score**
  - Evolves over time based on:
    - User clicks
    - Time spent
    - Interaction patterns

### Skill ROI Engine

Answers questions such as:
- “If I learn X in 6 weeks, what doors open?”
- “What is the cheapest path to a $40k raise?”
- “Which companies would talk to me if I had one more signal?”

---

## Employers

- Passive Talent Discovery Engine
- Workforce Strategy Simulator

---

## Referral System

