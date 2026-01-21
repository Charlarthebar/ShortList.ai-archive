# ShortList Pivot Plan
## Date: 2026-01-16

This document captures the full pivot plan for the ShortList platform. This replaces the old "role watching" system.

---

## 1) Candidate Flow (must build)

### Where this exists
For every Role Page that represents a specific:
- Company
- Role
- Level (intern / new grad / entry, etc.)
- Location = Boston/Cambridge

Add a single primary call-to-action button: **"Join the Shortlist"**

### What "Join the Shortlist" means (to the user)
This is not a normal job application. It means:
- "I'm interested in this role at this company."
- "I want to be considered quickly if/when the job becomes open."
- "I'm okay sharing my info with the employer if I'm a good fit."

### What the form collects (only these items)
Keep the form short and standardized:

1. **Resume upload** (preferred)
   - If no resume, allow LinkedIn URL as fallback.

2. **Work authorization**
   - e.g., US citizen / permanent resident / F-1 / needs sponsorship, etc.

3. **Grad year OR experience level**
   - enough to tell whether they fit intern/new grad/entry, etc.

4. **Start availability**
   - when they can begin work.

5. **Two short questions**
   - **Project question**: describe a project you built/did (proof of execution).
   - **Fit question**: why you're a fit for this role (role-specific alignment).

### Confirmation message (must show after submit)
Be explicit and consistent:

> "You're on the shortlist. This is not a formal application. If this role opens, we'll notify you, and the employer can review your profile if you meet their requirements."

---

## 2) Screening (must build)

### The screening must be two-step

We are not doing "AI decides who is rejected." We are doing:

### Step A: Must-have gate (objective pass/fail)
Automatically check objective requirements such as:
- work authorization (if the role requires it)
- location / availability constraints
- basic role-level fit (intern vs new grad vs entry)

This produces:
- **Pass / Fail**
- If fail: a simple reason like "requires sponsorship" or "availability doesn't match."

### Step B: AI ranking (only ranks, does not reject)
For everyone who passes must-haves:
- Generate a score from 0 to 100
- Generate:
  - 3 strengths (why they seem promising)
  - 1 concern (risk, gap, or unknown)

**Important rule:**
AI output is used to sort candidates (best first), not to permanently block candidates from being seen.

---

## 3) Employer Controls (must build)

For each Role Page, employers should be able to configure the shortlist settings:

### Must-haves (employer-defined)
The employer can define what counts as a "must-have," such as:
- work authorization requirements
- experience level / graduation window
- a small set of required skills (keep simple)

### Shortlist threshold (employer-defined)
- Employer sets a minimum AI score that counts as "qualified enough to show by default."
- Default suggestion: **70**
- This should filter the default view, not delete candidates.

### Volume cap (optional)
- Employer can cap how many candidates they want to review at a time (example: top 50).

---

## 4) Employer Dashboard (must build)

### Company-level view
Employers should see a list of roles with:
- number of total shortlisted candidates
- number who pass must-haves
- number who meet threshold

### Role-level view (the core experience)
For a specific role, show a ranked list of candidates.

Each candidate entry should show:
- Score
- Strengths + concern (from AI)
- Resume download/link (or LinkedIn)

### Export
Employer can export the shortlist (at least the qualified set) via:
- CSV export, OR
- "download packet" (a bundle of resumes)

---

## 5) Posting Trigger (must build)

When the posting monitor detects that this role has become open (a live posting appears):

### Employer notification
Send a message like:
> "This role just opened, and you already have X qualified shortlisted candidates ready to review."

### Candidate notification
Notify the candidates who joined that role:
> "This role is now open."

### MVP constraint
- Do not auto-submit candidates into the employer's ATS in MVP.
- Just alert + provide the shortlist inside your platform.

---

## 6) Non-refreshable Data Rule (must enforce)

You will have some data that was imported manually or is not reliably refreshable (ex: one-off payroll pulls).

### How that data can be used
It can be used only as:
- historical evidence that a role exists at an employer
- salary anchoring / credibility

### Hard restrictions
That historical data:
- must not trigger notifications
- must be labeled internally as historical with an as-of date
- must not be treated like "live trackable roles"

### Product promise restriction
Only promise alerts ("we'll notify you when it opens") if:
- that employer is in the monitored posting targets list, meaning we actually track their career site.

If we aren't monitoring that employer yet, the UI must not imply we can notify when it opens.

---

## Technical Implementation Notes

### Approach
- **Replacement** of old role-watching system (not alongside)
- **Modify existing tables** where possible (watchable_positions, etc.)
- **AI scoring**: Use OpenAI API

### Key Schema Changes Needed
1. Add `shortlist_applications` table (or repurpose `job_watches`)
2. Add `role_configurations` for employer must-haves/thresholds
3. Add `is_monitored` flag to distinguish trackable vs historical roles
4. Add screening results storage (pass/fail reason, AI score, strengths, concern)

### New Fields for Applications
- resume_url / linkedin_url
- work_authorization (enum)
- grad_year / experience_level
- start_availability (date)
- project_response (text)
- fit_response (text)
- screening_passed (bool)
- screening_fail_reason (text)
- ai_score (0-100)
- ai_strengths (text[])
- ai_concern (text)
