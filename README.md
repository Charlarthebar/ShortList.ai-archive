# ShortList.ai

ShortList.ai is a two-sided job marketplace that rethinks hiring as a continuous matching problem, not a one-time application event. Unlike traditional job boards that index only open roles and optimize for volume, ShortList.ai builds a continually updated view of all jobs (open and filled) and matches them to candidates based on skills, interests, and career trajectory.

This repository serves as the system of record for the ShortList.ai project and contains all code, documentation, research, and MVP artifacts in accordance with course requirements

## Team Members
- Charles Lai (Computer Science and Mathematics)
- Joshua Tang (Computer Science, Economics, and Data Science)
- Noah Hopkins (Computer Science and Finance)

## Opportunity Summary
The modern job market is inefficient for both sides.

Candidates face an arms race of automated applications, opaque filtering, and low response rates. Even strong applicants often have no insight into why they were rejected or how to improve their fit.
Employers, meanwhile, are overwhelmed by volume: a majority of applications fail to meet basic qualifications, while qualified passive candidates remain undiscovered.

ShortList.ai addresses this structural mismatch by:

- Indexing open and filled roles, allowing candidates to follow opportunities before they reopen
- Building skill-based profiles that evolve over time rather than static resumes
- Capturing intent and interest signals (subscriptions, skill self-assessment, engagement)
- Enabling employers to discover qualified passive candidates earlier in the hiring cycle

## MVP Scope
The core hypothesis is this: **Users are willing to track jobs over time and engage with skill-based matching even when no immediate opening exists.**

## Candidate-Side MVP
- Account creation (manual input)
- Resume upload or structured skill entry
- Ability to subscribe to a specific job, including filled roles
- Simple skill-based match score (declared skills vs. job requirements)
- Notification when job status changes or match score improves

## Employer-Side MVP
- Ability to post or claim a job
- View anonymized subscribed candidates
- Invite selected candidates to apply

## Explicit Non-Goals for MVP
- Full ATS replacement
- Large-scale resume parsing
- Global job ingestion
- Monetization or payments
- AI interview coaching or career pathing

## Tooling Plan

### Backend & Data
- Python + FastAPI — API development
- PostgreSQL — jobs, users, skills
- pgvector or equivalent — basic skill similarity
- Lightweight LLM usage (API-based) for job and skill normalization

### Frontend
- React + TypeScript
- Web-first, mobile-friendly UI
- Focus on clarity over visual polish

### Infrastructure
- GitHub — single source of truth
- Vercel / Render — deployment
- Supabase or Firebase — authentication (if needed)

### Product & Collaboration
- Slack / Discord — team communication
- In-person meetings/discussions - team communication
