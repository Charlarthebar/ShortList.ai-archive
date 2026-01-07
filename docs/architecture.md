# System Architecture (Initial)

## Overview

The ShortList.ai MVP is a web-based application with a lightweight backend, a simple frontend, and a structured data model focused on jobs, skills, and users.

The architecture prioritizes:
- Fast iteration
- Reproducibility
- Clear separation of concerns

## High-Level Components

### Frontend
- Web client built with React
- Handles user onboarding, skill input, job subscription, and notifications

### Backend API
- Python + FastAPI
- Exposes endpoints for:
  - User creation
  - Job creation / claiming
  - Skill matching
  - Job subscription

### Data Layer
- PostgreSQL database
- Core tables:
  - users
  - jobs
  - skills
  - job_subscriptions
  - user_skills

### Matching Logic
- Rule-based skill overlap scoring (MVP)
- Match score = overlap between user skills and job-required skills
- Designed to be replaced with learned models later

## External Services (Optional for MVP)
- Email service for notifications
- Authentication provider (e.g., Supabase)

## Future Extensions (Out of Scope for MVP)
- Vector-based similarity search
- Automated resume parsing
- Employer ATS integrations
- AI interview coaching

## Architecture Principles

- Everything runnable locally
- No hard dependency on third-party APIs
- Clear upgrade path without rewrite
