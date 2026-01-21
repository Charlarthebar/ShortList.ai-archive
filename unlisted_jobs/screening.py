#!/usr/bin/env python3
"""
Shortlist Screening Module
==========================

Two-step screening for shortlist applications:
- Step A: Must-have gate (objective pass/fail)
- Step B: AI ranking (score 0-100 with strengths and concern)

Author: ShortList.ai
Date: 2026-01-16
"""

import os
import sys
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

# OpenAI for AI ranking
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logging.warning("openai not installed - AI ranking disabled")

log = logging.getLogger(__name__)

# Work authorization hierarchy for matching
WORK_AUTH_HIERARCHY = {
    'us_citizen': ['us_citizen'],
    'permanent_resident': ['us_citizen', 'permanent_resident'],
    'f1_opt': ['us_citizen', 'permanent_resident', 'f1_opt', 'f1_cpt'],
    'f1_cpt': ['us_citizen', 'permanent_resident', 'f1_opt', 'f1_cpt'],
    'h1b': ['us_citizen', 'permanent_resident', 'h1b'],
    'needs_sponsorship': ['us_citizen', 'permanent_resident', 'needs_sponsorship'],
    'other': ['us_citizen', 'permanent_resident', 'other'],
}

# Experience level ordering for comparisons
EXPERIENCE_LEVEL_ORDER = ['intern', 'new_grad', 'entry', 'mid', 'senior', 'staff']


class ScreeningResult:
    """Result of screening a candidate."""

    def __init__(
        self,
        passed: bool,
        fail_reason: Optional[str] = None,
        ai_score: Optional[int] = None,
        ai_strengths: Optional[List[str]] = None,
        ai_concern: Optional[str] = None
    ):
        self.passed = passed
        self.fail_reason = fail_reason
        self.ai_score = ai_score
        self.ai_strengths = ai_strengths or []
        self.ai_concern = ai_concern

    def to_dict(self) -> Dict[str, Any]:
        return {
            'screening_passed': self.passed,
            'screening_fail_reason': self.fail_reason,
            'ai_score': self.ai_score,
            'ai_strengths': self.ai_strengths,
            'ai_concern': self.ai_concern,
        }


def screen_application(
    application: Dict[str, Any],
    role_config: Optional[Dict[str, Any]],
    position: Dict[str, Any],
    run_ai_ranking: bool = True
) -> ScreeningResult:
    """
    Screen a shortlist application through both steps.

    Step A: Must-have gate (objective pass/fail)
    Step B: AI ranking (only if Step A passes)

    Args:
        application: The shortlist application data
        role_config: The role configuration (employer-defined must-haves)
        position: The position data
        run_ai_ranking: Whether to run AI ranking (Step B)

    Returns:
        ScreeningResult with pass/fail status and AI scores
    """
    # Step A: Must-have gate
    passed, fail_reason = check_must_haves(application, role_config)

    if not passed:
        return ScreeningResult(passed=False, fail_reason=fail_reason)

    # Step B: AI ranking (only for candidates who pass must-haves)
    if run_ai_ranking:
        try:
            ai_score, ai_strengths, ai_concern = rank_with_ai(
                application, position, role_config
            )
            return ScreeningResult(
                passed=True,
                ai_score=ai_score,
                ai_strengths=ai_strengths,
                ai_concern=ai_concern
            )
        except Exception as e:
            log.error(f"AI ranking failed: {e}")
            # Still mark as passed, just without AI score
            return ScreeningResult(passed=True)

    return ScreeningResult(passed=True)


def check_must_haves(
    application: Dict[str, Any],
    role_config: Optional[Dict[str, Any]]
) -> Tuple[bool, Optional[str]]:
    """
    Step A: Check objective must-have requirements.

    Returns:
        Tuple of (passed: bool, fail_reason: str or None)
    """
    if not role_config:
        # No config means no must-haves, everyone passes
        return True, None

    # Check work authorization
    if role_config.get('require_work_auth'):
        allowed_auth = role_config.get('allowed_work_auth', [])
        candidate_auth = application.get('work_authorization')

        if allowed_auth and candidate_auth:
            # Check if candidate's auth is in the allowed list
            # or if their auth type is compatible with allowed types
            if not is_work_auth_compatible(candidate_auth, allowed_auth):
                return False, f"Work authorization ({candidate_auth}) not accepted for this role"

    # Check experience level
    if role_config.get('require_experience_level'):
        allowed_levels = role_config.get('allowed_experience_levels', [])
        candidate_level = application.get('experience_level')

        if allowed_levels and candidate_level:
            if candidate_level not in allowed_levels:
                return False, f"Experience level ({candidate_level}) doesn't match requirements"

    # Check graduation year
    min_grad_year = role_config.get('min_grad_year')
    max_grad_year = role_config.get('max_grad_year')
    candidate_grad_year = application.get('grad_year')

    if candidate_grad_year:
        if min_grad_year and candidate_grad_year < min_grad_year:
            return False, f"Graduation year ({candidate_grad_year}) is before required minimum ({min_grad_year})"
        if max_grad_year and candidate_grad_year > max_grad_year:
            return False, f"Graduation year ({candidate_grad_year}) is after required maximum ({max_grad_year})"

    # Check start availability (if role has a deadline)
    latest_start = role_config.get('latest_start_date')
    candidate_start = application.get('start_availability')

    if latest_start and candidate_start:
        if isinstance(candidate_start, str):
            candidate_start = datetime.strptime(candidate_start, '%Y-%m-%d').date()
        if isinstance(latest_start, str):
            latest_start = datetime.strptime(latest_start, '%Y-%m-%d').date()

        if candidate_start > latest_start:
            return False, f"Start availability ({candidate_start}) is after required date ({latest_start})"

    # All must-haves passed
    return True, None


def is_work_auth_compatible(candidate_auth: str, allowed_auths: List[str]) -> bool:
    """
    Check if candidate's work authorization is compatible with allowed types.

    A candidate with stronger authorization (e.g., US citizen) is compatible
    with roles that accept weaker authorization (e.g., needs sponsorship).
    """
    # Direct match
    if candidate_auth in allowed_auths:
        return True

    # Check hierarchy - if candidate has "better" auth than required
    # e.g., US citizen can work roles that accept any auth type
    candidate_can_work_for = WORK_AUTH_HIERARCHY.get(candidate_auth, [candidate_auth])
    for allowed in allowed_auths:
        if allowed in candidate_can_work_for:
            return True

    return False


def calculate_fallback_score(
    application: Dict[str, Any],
    position: Dict[str, Any],
    role_config: Optional[Dict[str, Any]]
) -> Tuple[int, List[str], str]:
    """
    Calculate a score based on objective criteria when AI is unavailable.

    This provides a reasonable ranking based on:
    - Application completeness
    - Experience level match
    - Work authorization strength
    - Response quality (length/detail)

    Returns:
        Tuple of (score: int 0-100, strengths: list, concern: str)
    """
    score = 70  # Base score for passing must-haves
    strengths = []
    concerns = []

    # Experience level match (0-10 points)
    candidate_level = application.get('experience_level')
    position_level = position.get('experience_level')
    if candidate_level and position_level:
        if candidate_level == position_level:
            score += 10
            strengths.append(f"Experience level ({candidate_level}) matches role requirements")
        elif candidate_level in EXPERIENCE_LEVEL_ORDER and position_level in EXPERIENCE_LEVEL_ORDER:
            cand_idx = EXPERIENCE_LEVEL_ORDER.index(candidate_level)
            pos_idx = EXPERIENCE_LEVEL_ORDER.index(position_level)
            if cand_idx > pos_idx:
                score += 5  # Over-qualified
                strengths.append(f"Candidate has {candidate_level} experience (above {position_level})")
            else:
                concerns.append(f"Experience level ({candidate_level}) is below target ({position_level})")

    # Work authorization strength (0-5 points)
    work_auth = application.get('work_authorization')
    if work_auth:
        if work_auth == 'us_citizen':
            score += 5
            strengths.append("US citizen - no visa constraints")
        elif work_auth == 'permanent_resident':
            score += 4
            strengths.append("Permanent resident - stable work authorization")
        elif work_auth in ['f1_opt', 'f1_cpt']:
            score += 2
            concerns.append("May require H-1B sponsorship in the future")
        elif work_auth == 'needs_sponsorship':
            concerns.append("Requires visa sponsorship")

    # Project response quality (0-8 points)
    project_response = application.get('project_response', '')
    if project_response:
        word_count = len(project_response.split())
        if word_count >= 150:
            score += 8
            strengths.append("Detailed project description demonstrates execution ability")
        elif word_count >= 75:
            score += 5
            strengths.append("Good project description provided")
        elif word_count >= 25:
            score += 3
        else:
            concerns.append("Brief project response - limited detail")
    else:
        concerns.append("No project response provided")

    # Fit response quality (0-7 points)
    fit_response = application.get('fit_response', '')
    if fit_response:
        word_count = len(fit_response.split())
        if word_count >= 100:
            score += 7
            strengths.append("Thoughtful explanation of role fit")
        elif word_count >= 50:
            score += 4
        elif word_count >= 20:
            score += 2
        else:
            concerns.append("Brief fit response")
    else:
        concerns.append("No fit response provided")

    # Resume/LinkedIn provided (0-5 points)
    if application.get('resume_url') or application.get('resume_text'):
        score += 3
        strengths.append("Resume provided for detailed review")
    if application.get('linkedin_url'):
        score += 2
        strengths.append("LinkedIn profile available")

    # Start availability (0-5 points) - sooner is better
    start_avail = application.get('start_availability')
    if start_avail:
        from datetime import date
        if isinstance(start_avail, str):
            start_avail = datetime.strptime(start_avail, '%Y-%m-%d').date()
        days_until_start = (start_avail - date.today()).days
        if days_until_start <= 14:
            score += 5
            strengths.append("Available to start immediately")
        elif days_until_start <= 30:
            score += 4
        elif days_until_start <= 60:
            score += 2
        else:
            concerns.append(f"Not available for {days_until_start} days")

    # Cap score at 100
    score = min(100, score)

    # Select top 3 strengths and primary concern
    top_strengths = strengths[:3]
    primary_concern = concerns[0] if concerns else "No significant concerns identified"

    return score, top_strengths, primary_concern


def rank_with_ai(
    application: Dict[str, Any],
    position: Dict[str, Any],
    role_config: Optional[Dict[str, Any]]
) -> Tuple[int, List[str], str]:
    """
    Step B: Use AI to rank the candidate.

    Returns:
        Tuple of (score: int 0-100, strengths: list of 3, concern: str)
    """
    if not OPENAI_AVAILABLE:
        log.info("OpenAI not available, using fallback scoring")
        return calculate_fallback_score(application, position, role_config)

    # Get API key from environment
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        log.info("OPENAI_API_KEY not set, using fallback scoring")
        return calculate_fallback_score(application, position, role_config)

    client = openai.OpenAI(api_key=api_key)

    # Build the prompt
    prompt = build_ranking_prompt(application, position, role_config)

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are an expert recruiter evaluating candidates for tech roles.
Your job is to assess candidate fit based on their application.

You must respond with valid JSON in this exact format:
{
    "score": <number 0-100>,
    "strengths": ["<strength 1>", "<strength 2>", "<strength 3>"],
    "concern": "<one key concern or risk>"
}

Scoring guidelines:
- 90-100: Exceptional fit, strong signals of success
- 80-89: Strong fit, likely to succeed
- 70-79: Good fit, meets expectations
- 60-69: Moderate fit, some gaps
- 50-59: Weak fit, significant concerns
- Below 50: Poor fit for this role

Be fair and objective. Focus on demonstrated skills and experience.
Do not penalize for factors like name, school prestige, or company brand."""
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=500,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        score = int(result.get('score', 50))
        score = max(0, min(100, score))  # Clamp to 0-100

        strengths = result.get('strengths', [])[:3]  # Max 3 strengths
        concern = result.get('concern', 'No specific concerns identified')

        return score, strengths, concern

    except Exception as e:
        log.error(f"OpenAI API error: {e}")
        raise


def build_ranking_prompt(
    application: Dict[str, Any],
    position: Dict[str, Any],
    role_config: Optional[Dict[str, Any]]
) -> str:
    """Build the prompt for AI ranking."""

    # Position details
    position_text = f"""
## Role Information
- Title: {position.get('title', 'Unknown')}
- Company: {position.get('company_name', 'Unknown')}
- Location: {position.get('location', 'Unknown')}
- Level: {position.get('experience_level', 'Not specified')}
"""

    if position.get('description'):
        desc = position['description'][:2000]  # Limit description length
        position_text += f"- Description: {desc}\n"

    if role_config:
        if role_config.get('required_skills'):
            skills = ', '.join(role_config['required_skills'])
            position_text += f"- Required skills: {skills}\n"

    # Application details
    application_text = f"""
## Candidate Application
- Work Authorization: {application.get('work_authorization', 'Not specified')}
- Experience Level: {application.get('experience_level', 'Not specified')}
- Graduation Year: {application.get('grad_year', 'Not specified')}
- Start Availability: {application.get('start_availability', 'Not specified')}

### Project Response (proof of execution):
{application.get('project_response', 'No response provided')}

### Fit Response (why they fit this role):
{application.get('fit_response', 'No response provided')}
"""

    # Add resume info if available
    if application.get('resume_text'):
        resume_excerpt = application['resume_text'][:3000]  # Limit resume length
        application_text += f"""
### Resume Summary:
{resume_excerpt}
"""
    elif application.get('linkedin_url'):
        application_text += f"\n- LinkedIn: {application['linkedin_url']}\n"

    return f"""
Please evaluate this candidate for the following role:

{position_text}

{application_text}

Rate this candidate on a scale of 0-100 and provide:
1. Three specific strengths that make them a good fit
2. One key concern or risk factor

Respond with JSON only.
"""


def batch_screen_applications(
    applications: List[Dict[str, Any]],
    role_config: Optional[Dict[str, Any]],
    position: Dict[str, Any],
    run_ai_ranking: bool = True
) -> List[ScreeningResult]:
    """
    Screen multiple applications for a role.

    Args:
        applications: List of application data
        role_config: Role configuration
        position: Position data
        run_ai_ranking: Whether to run AI ranking

    Returns:
        List of ScreeningResult objects
    """
    results = []

    for app in applications:
        try:
            result = screen_application(app, role_config, position, run_ai_ranking)
            results.append(result)
        except Exception as e:
            log.error(f"Error screening application {app.get('id')}: {e}")
            # Mark as passed but without AI score on error
            results.append(ScreeningResult(passed=True))

    return results


def rescreen_application(cursor, application_id: int) -> ScreeningResult:
    """
    Re-screen a single application (fetch data from DB and screen).

    Args:
        cursor: Database cursor
        application_id: The application ID

    Returns:
        ScreeningResult
    """
    # Fetch application
    cursor.execute("""
        SELECT
            sa.id, sa.user_id, sa.position_id,
            sa.resume_url, sa.linkedin_url, sa.work_authorization,
            sa.grad_year, sa.experience_level, sa.start_availability,
            sa.project_response, sa.fit_response,
            wp.title, wp.company_name, wp.location, wp.description
        FROM shortlist_applications sa
        JOIN watchable_positions wp ON sa.position_id = wp.id
        WHERE sa.id = %s
    """, (application_id,))

    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Application {application_id} not found")

    application = {
        'id': row[0],
        'user_id': row[1],
        'position_id': row[2],
        'resume_url': row[3],
        'linkedin_url': row[4],
        'work_authorization': row[5],
        'grad_year': row[6],
        'experience_level': row[7],
        'start_availability': row[8],
        'project_response': row[9],
        'fit_response': row[10],
    }

    position = {
        'title': row[11],
        'company_name': row[12],
        'location': row[13],
        'description': row[14],
    }

    # Fetch role config
    cursor.execute("""
        SELECT
            require_work_auth, allowed_work_auth,
            require_experience_level, allowed_experience_levels,
            min_grad_year, max_grad_year,
            required_skills, score_threshold
        FROM role_configurations
        WHERE position_id = %s
    """, (application['position_id'],))

    config_row = cursor.fetchone()
    role_config = None
    if config_row:
        role_config = {
            'require_work_auth': config_row[0],
            'allowed_work_auth': config_row[1],
            'require_experience_level': config_row[2],
            'allowed_experience_levels': config_row[3],
            'min_grad_year': config_row[4],
            'max_grad_year': config_row[5],
            'required_skills': config_row[6],
            'score_threshold': config_row[7],
        }

    # Screen
    result = screen_application(application, role_config, position)

    # Update database
    cursor.execute("""
        UPDATE shortlist_applications
        SET
            screening_passed = %s,
            screening_fail_reason = %s,
            ai_score = %s,
            ai_strengths = %s,
            ai_concern = %s,
            ai_scored_at = CURRENT_TIMESTAMP,
            status = CASE
                WHEN %s = FALSE THEN 'rejected'
                WHEN %s IS NOT NULL THEN 'qualified'
                ELSE 'screened'
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (
        result.passed,
        result.fail_reason,
        result.ai_score,
        result.ai_strengths if result.ai_strengths else None,
        result.ai_concern,
        result.passed,
        result.ai_score,
        application_id
    ))

    return result
