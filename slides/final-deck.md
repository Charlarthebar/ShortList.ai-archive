---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
color: #2d3748
style: |
  section {
    font-family: 'Segoe UI', system-ui, sans-serif;
    padding: 40px 60px;
  }
  h1 {
    color: #1a365d;
    font-weight: 700;
    margin-bottom: 0.3em;
  }
  h2 {
    color: #2c5282;
    font-weight: 600;
    border-bottom: 3px solid #4299e1;
    padding-bottom: 10px;
    margin-bottom: 30px;
  }
  h3 {
    color: #4a5568;
    font-weight: 500;
  }
  strong {
    color: #2c5282;
  }
  a {
    color: #3182ce;
  }
  table {
    font-size: 0.85em;
    margin-top: 20px;
  }
  th {
    background-color: #2c5282;
    color: white;
    padding: 12px 16px;
  }
  td {
    padding: 10px 16px;
    border-bottom: 1px solid #e2e8f0;
  }
  tr:nth-child(even) {
    background-color: #f7fafc;
  }
  ul, ol {
    line-height: 1.7;
  }
  li {
    margin-bottom: 8px;
  }
  section.title {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
    background: #1a365d;
    color: white;
  }
  section.title h1 {
    color: white;
    font-size: 3.5em;
    margin-bottom: 0.3em;
  }
  section.title h3 {
    color: #e2e8f0;
    font-size: 1.4em;
    font-weight: 400;
    margin-top: 10px;
  }
  section.title p {
    color: #a0aec0;
    font-style: italic;
    font-size: 1.1em;
    margin-top: 30px;
  }
  section.closing {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
    background: #1a365d;
    color: white;
  }
  section.closing p {
    color: #e2e8f0;
    font-size: 1.4em;
    font-style: italic;
    margin-bottom: 40px;
  }
  section.closing strong {
    color: white;
    font-size: 2.2em;
  }
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 40px;
  }
  .highlight-box {
    background: linear-gradient(135deg, #ebf8ff 0%, #bee3f8 100%);
    padding: 20px;
    border-radius: 8px;
    border-left: 4px solid #3182ce;
    margin-top: 20px;
  }
---

<!-- _class: title -->
<!-- _paginate: false -->
<!-- _backgroundColor: #1a365d -->
<!-- _color: white -->

# ShortList.ai

### Rethinking Hiring as Continuous Matching

*Building talent pipelines before roles are posted*

---

## Hiring is Broken for Everyone

<div class="columns">
<div>

**For Job Seekers:**
- Low response rates despite hundreds of applications
- No visibility into why they weren't selected
- Wasted time on mass applications
- Miss opportunities at dream companies

</div>
<div>

**For Employers:**
- Overwhelmed by application volume (300+ per role)
- Low signal-to-noise ratio
- Repeatedly sourcing for the same roles
- Difficulty finding qualified candidates quickly

</div>
</div>

---

## Today's Job Platforms Are Designed Wrong

Traditional platforms (LinkedIn, Indeed) are optimized for:
- Listing **open** jobs only
- Maximizing application **volume**

This creates an arms race where:
- Candidates spam applications
- Employers get buried in noise
- Quality matches get lost

<div class="highlight-box">
<strong>The result:</strong> Everyone loses.
</div>

---

## ShortList.ai: A New Approach

We track **both open AND filled roles**, enabling a fundamentally different model:

| Traditional Platforms | ShortList.ai |
|----------------------|--------------|
| Apply when job is posted | Subscribe before job opens |
| One-time transaction | Continuous relationship |
| Volume-driven matching | Quality-driven matching |
| Static resumes | Living candidate profiles |

<div class="highlight-box">
<strong>Key insight:</strong> Great hires often come from candidates who were "almost right" for previous roles.
</div>

---

## How It Works — For Job Seekers

**1. Create a Living Profile**
- Skills, experience, and preferences that evolve over time
- Upload resume for automatic parsing
- Set your search status (active, open, exploring)

**2. Subscribe to Roles You Want**
- Follow specific positions—even if they're filled
- Get notified when roles reopen
- See your match score for every opportunity

**3. Join the Shortlist**
- Express interest with a 3-step application
- Stand out before roles are even posted

---

## How It Works — For Hiring Teams

**1. Build Talent Pipelines**
- See who's interested before you post
- Access candidates who subscribed to similar roles
- Discover passive candidates early

**2. Two-Step Smart Screening**
- **Must-Have Gate:** Auto-filter on work authorization, experience, skills
- **AI Ranking:** Score candidates 0-100 with strengths & concerns

**3. Ranked Shortlist Inbox**
- Review pre-qualified candidates
- Set your quality threshold
- Invite top matches to apply

---

## AI-Powered Features

<div class="columns">
<div>

**Match Scoring**
- Skill-based matching algorithms
- Real-time score updates
- Transparent scoring factors

**AI Screening Interviews**
- Competency-based assessment
- Studio-quality experience
- Evidence-based evaluation

</div>
<div>

**Smart Recommendations**
- Resume optimization suggestions
- Skill-gap roadmaps for candidates
- Best-fit role suggestions

**Continuous Learning**
- Matching improves over time
- Personalized insights
- Career trajectory analysis

</div>
</div>

---

## The Continuous Matching Difference

**Traditional Flow:**
Role Opens → Mass Applications → Hire → Role Closed → *Restart from zero*

**ShortList Flow:**
Role Opens → Shortlist Ready → Fast Hire → Role "Filled" → Subscribers Stay → *Instant Pipeline*

<div class="highlight-box">

**Benefits:** Candidates build relationships with target companies • Employers maintain warm pipelines • Better matches happen faster • Data improves over time

</div>

---

## Two-Sided Marketplace

| **For Candidates** | **For Employers** |
|----------------|---------------|
| Living profiles | Pipeline management |
| Role subscriptions | Smart screening |
| Match scores | Ranked inbox |
| AI interview prep | Candidate discovery |
| Skill-gap roadmaps | Shortlist analytics |

<div class="highlight-box">
<strong>Tech Stack:</strong> React • Python/FastAPI • PostgreSQL • AI (Claude & GPT-4)
</div>

---

## Target Users

<div class="columns">
<div>

**Job Seekers**
- Knowledge workers in tech-forward roles
- Software engineers, data scientists, PMs
- Professionals tired of the application black hole

</div>
<div>

**Employers**
- Startups and mid-sized tech companies
- Teams with recurring hiring needs
- Organizations valuing quality over volume

</div>
</div>

<div class="highlight-box">
<strong>Initial Focus:</strong> U.S. tech sector — where skills are standardized, roles are repeatable, and switching is frequent.
</div>

---

## Business Model

<div class="columns">
<div>

**Candidates (Freemium)**

| Free | Premium |
|------|---------|
| Basic profile & search | Unlimited subscriptions |
| Limited subscriptions | AI interview prep |
| Match scores | Resume optimization |
| | Skill-gap roadmaps |

</div>
<div>

**Employers (SaaS)**
- Pipeline access & shortlist browsing
- Advanced candidate search
- Invite-to-apply workflows
- ATS-lite screening tools
- Volume caps & analytics

</div>
</div>

---

## Get Started

<div class="columns">
<div>

**For Job Seekers**

Create your living profile and start subscribing to roles you want—even if they're filled today.

</div>
<div>

**For Employers**

Claim your company page and start building talent pipelines before your next role opens.

</div>
</div>

<div class="highlight-box" style="text-align: center; margin-top: 40px;">
<strong>Stop chasing applications. Start building relationships.</strong>
</div>

---

<!-- _class: closing -->
<!-- _paginate: false -->
<!-- _backgroundColor: #1a365d -->
<!-- _color: white -->

*Stop chasing applications. Start building relationships.*

**Shortlist.ai**
