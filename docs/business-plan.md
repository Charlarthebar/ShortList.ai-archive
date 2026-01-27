# ShortList.ai — 6-Page Business Plan

## 1) Executive Summary

**ShortList.ai** is a two-sided job marketplace designed to fix the inefficiency and distrust issues in modern hiring. Traditional job boards treat job search as a one-time transaction: a job opens, candidates apply, employers filter, and the job closes. This creates a volume-driven arms race where job seekers mass-apply and employers are overwhelmed with low-quality applications.

ShortList.ai instead treats hiring as a continuous process. We index both open and filled roles, enabling candidates to “subscribe” to roles over time and receive alerts when roles reopen, when their match improves, or when similar opportunities emerge. We continuously refine candidate profiles using skills and intent signals rather than relying solely on static resumes and keyword matching.

For employers, ShortList.ai provides a new pipeline-building tool: employers can see pre-filtered candidates who are qualified and have subscribed to their roles and proactively recruit qualified passive talent before formally opening requisitions. Our marketplace reduces noise, improves match quality, and creates a retention loop where users stay engaged because the platform keeps working for them.

---

## 2) Problem Statement

The job market is inefficient for both job seekers and employers.

### Job seeker pain
Job seekers experience:
- extremely low response rates despite being qualified
- opaque rejection and filtering processes
- “ghost hiring” and stale job postings
- pressure to apply broadly rather than precisely
- little feedback on why they were rejected or how to improve fit

The results are frustration, wasted time, and declining trust in job boards.

### Employer pain
Employers face the opposite side of the same system:
- massive application volume
- low signal-to-noise ratio (many applicants fail basic criteria)
- difficulty finding high-quality passive candidates
- long hiring cycles and repeated sourcing for similar roles

Employers often spend time filtering instead of engaging strong candidates.

### Root cause
Most job platforms are optimized for:
- listing open jobs
- maximizing applications
- selling visibility

This structure incentivizes volume rather than fit. It also assumes hiring is transactional, when in reality:
- roles recur repeatedly (same job reopened every quarter)
- candidates evolve continuously (skills grow, interests shift)
- hiring pipelines are built over time

---

## 3) Opportunity and Insight

### Core principle
**Hiring is not an event. It’s a continuous matching process.**

The key insight is that many “best matches” happen *before* a job is posted and *before* a candidate actively applies. Traditional platforms miss this because they only index open roles and treat candidates as static profiles.

ShortList.ai creates a marketplace built around:
1) persistent job objects (open + filled roles, role history, recurring openings)
2) persistent candidate objects (skills, intent, trajectory, continuous signals)
3) subscriptions + alerts that create early matching and high retention

This creates value for both sides:
- candidates get early access and clarity on how to improve fit
- employers get warm pipelines and high-intent candidates

---

## 4) Product and Solution

### Product overview
ShortList.ai is a two-sided marketplace with three core layers:

#### (A) Universal Job Index (Open + Filled)
We aggregate and normalize job data across:
- company career pages
- applicant tracking systems (where accessible)
- public postings and feeds
- historical job listings (filled roles)

Each job is stored as a durable object with:
- normalized title and level
- skills and requirements
- compensation (where available)
- location/remote constraints
- job “reopen probability” (based on history)

#### (B) Living Candidate Profile (Skills + Intent + Trajectory)
Candidates onboard by importing LinkedIn or entering basic info. Over time, ShortList.ai builds a richer profile using:
- self-reported skills and interests
- micro-quizzes and lightweight validation
- behavioral signals (roles followed, jobs clicked, skills explored)
- preference signals (location, schedule, work type, industry)

Unlike a resume, this profile updates continuously and improves matching quality over time.

#### (C) Matching + Subscriptions
Candidates can:
- subscribe to specific jobs even if currently filled
  - automated AI interview to determine fit to that specific job
- subscribe to “role archetypes” (e.g., New Grad SWE, ML Engineer)
- receive alerts when:
  - a job reopens
  - a similar job appears
  - their match score improves
  - they unlock a skill that increases eligibility

Employers can:
- claim their job pages
- see the pipeline of subscribed candidates
- invite candidates to apply
- run confidential searches for future roles

### Why this is meaningfully different from LinkedIn / Indeed
LinkedIn and Indeed are optimized for open postings and application flow. ShortList.ai is optimized for:
- role subscriptions (interest capture)
- continuous skill signals
- pipeline formation before openings exist
- matching quality over volume

This changes the system dynamics: instead of candidates spraying applications, the platform creates a structured pathway to high-fit opportunities.

---

## 5) Target Market and Customer Segments

### Initial target user (candidates)
**U.S. knowledge workers**, starting with:
- software engineers
- data analysts / data scientists
- product managers
- technical operations roles

These users are ideal because:
- skills are standardized and measurable
- roles are highly repeatable across companies
- job switching is frequent
- hiring pipelines are competitive and noisy

### Initial target customer (employers)
Employers with recurring needs:
- startups and mid-sized tech companies
- teams hiring for repeated roles (SWE, data, product)
- organizations that struggle with applicant volume and low signal

We will prioritize employers that:
- hire repeatedly for similar roles
- care about candidate quality and speed
- will pay for pipeline access and sourcing tools

---

## 6) Competitive Landscape and Differentiation

### LinkedIn
- Strength: network effects + recruiter workflow
- Weakness: static profiles, limited job intelligence, not built for subscription pipelines  
**ShortList.ai differentiation:** dynamic skill model + “follow jobs” + pipeline-before-openings.

### Indeed
- Strength: massive open-job index and SEO
- Weakness: volume-first marketplace, low candidate quality signal  
**ShortList.ai differentiation:** match quality + intent signals + recurring job tracking.

### Mercor
- Strength: curated talent marketplace and screening
- Weakness: narrower dataset and role coverage  
**ShortList.ai differentiation:** universal index + broader job categories + persistent job objects.

### Why we can compete (defensibility)
ShortList.ai’s defensibility comes from:
- accumulating proprietary “intent + subscription” data
- improving matching quality as profiles evolve
- building a job ontology that gets better with scale
- feedback loops between candidate behavior and employer outreach

---

## 7) Business Model

ShortList.ai monetizes both sides of the marketplace.

### Candidate monetization (freemium → premium)
Free:
- basic profile and job search
- limited subscriptions and alerts

Premium tiers unlock:
- expanded subscriptions + early alerts
- AI interview prep + mock interviews
- resume optimization and tailored narratives
- skill-gap roadmap and learning recommendations
- analytics on match scores and progress tracking

### Employer monetization (SaaS + marketplace)
Employers pay for:
- pipeline access (subscribers and passive matches)
- advanced candidate search
- invite-to-apply workflows
- ATS-lite screening tools
- confidential search mode
- integrations (ATS, HRIS, scheduling)

This is closer to “recruiting SaaS + marketplace access” than a pure job board.

---

## 8) Go-To-Market Strategy

### Phase 1: Build supply + engagement (0 → 10k candidates)
Goal: prove that users subscribe and return.

Actions:
- launch closed beta with knowledge workers
- focus on one job family: SWE + data
- viral loop: “subscribe to your dream job” + alerts
- onboarding via LinkedIn import
- lightweight skill explorer and match scoring

Key success metrics:
- % of users subscribing to filled jobs
- retention (weekly return rate)
- match improvement actions per user
- alerts → clicks conversion

### Phase 2: Employer onboarding (10k → 100k candidates)
Goal: prove employer willingness to pay for pipelines.

Actions:
- recruit 20–50 employers with recurring hiring needs
- allow employers to “claim” roles and view subscribers
- offer invite-to-apply tools and ATS-lite workflow
- charge for premium sourcing features

Key success metrics:
- number of claimed employer pages
- invite-to-apply conversion rate
- time-to-shortlist reduction vs baseline
- paid employer retention

### Phase 3: Scale and partnerships
Goal: expand beyond knowledge work into broader categories.

Actions:
- integrations with ATS providers
- partnerships with learning platforms
- expansion to nontraditional roles and global markets

---

## 9) Execution Plan and Milestones

### 0–3 months
- job ingestion pipeline (career pages + open postings)
- job normalization + ontology v1
- candidate onboarding + profile builder
- subscription + alert system

### 3–6 months
- match scoring v1 with skill extraction
- skill explorer + repeated micro-actions
- analytics dashboards for user engagement
- initial employer claim flow

### 6–12 months
- employer pipeline tools (invite-to-apply)
- improved matching models using behavioral data
- paid tiers + billing
- scalable infra and monitoring

---

## 10) Risks and Mitigation

### Risk 1: Cold start (marketplace liquidity)
**Mitigation:** start candidate-first with a strong “subscription + alert” value prop that works without employers initially.

### Risk 2: Data quality / job ingestion errors
**Mitigation:** normalize titles and skills with human-in-the-loop validation for top roles; focus on a narrow job family first.

### Risk 3: Users won’t engage with skill updates
**Mitigation:** keep micro-actions short and directly tied to visible payoff (match score improvement, alerts, interview readiness).

### Risk 4: Employer adoption friction
**Mitigation:** offer “ATS-lite” tools and pipeline insights that complement existing workflows instead of replacing ATS systems.

---

## 11) Early Success Criteria

ShortList.ai is successful in the early stage if:

### Candidate-side
- users subscribe to filled roles
- users return weekly to check alerts or match score
- users take actions that improve match score

### Employer-side
- employers claim jobs and view subscribers
- employers invite candidates to apply
- employers report higher quality pipelines and faster shortlisting
