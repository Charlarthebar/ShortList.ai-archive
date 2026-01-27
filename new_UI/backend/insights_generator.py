#!/usr/bin/env python3
"""
AI-Powered Candidate Insights Generator for ShortList.ai
=========================================================
Uses OpenAI (gpt-4o-mini) to analyze candidates against job requirements
and generate meaningful insights for employers.

Generates:
- Overview: 3-4 sentence fit score justification
- Strengths: 3-5 specific strengths with evidence
- Gaps: 2+ gaps/risks based on role requirements
- Follow-up Questions: Role-specific and candidate-specific questions
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
from typing import Dict, List, Optional, Any

# Load environment variables
load_dotenv()

# Database connection - matches app.py config
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def generate_candidate_insights(
    application_id: int,
    conn=None
) -> Dict[str, Any]:
    """
    Generate AI-powered insights for a candidate application.

    Args:
        application_id: The shortlist_applications.id
        conn: Optional database connection (will create one if not provided)

    Returns:
        Dict with why_this_person, strengths, risks, suggested_questions, etc.
    """
    should_close_conn = False
    if conn is None:
        conn = get_db()
        should_close_conn = True

    try:
        # Gather all candidate data
        candidate_data = _gather_candidate_data(conn, application_id)

        if not candidate_data:
            return {'error': 'Application not found'}

        # Generate insights using OpenAI
        insights = _generate_insights_with_ai(candidate_data)

        # Store insights in database
        _store_insights(conn, application_id, insights)

        return insights

    finally:
        if should_close_conn:
            conn.close()


def _gather_candidate_data(conn, application_id: int) -> Optional[Dict]:
    """Gather all relevant data about the candidate and role."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get application with user and role info
        cur.execute("""
            SELECT
                sa.id as application_id,
                sa.fit_score,
                sa.interview_status,
                sa.interview_transcript,
                sa.interview_evaluation,
                sa.resume_path,
                sa.eligibility_data,
                u.email,
                u.first_name,
                u.last_name,
                sp.resume_text,
                sp.extracted_profile,
                sp.experience_level as candidate_experience_level,
                wp.id as role_id,
                wp.title as role_title,
                wp.company_name,
                wp.description as role_description,
                wp.role_type,
                wp.experience_level as required_experience_level,
                wp.location,
                wp.work_arrangement,
                wp.must_have_skills,
                wp.nice_to_have_skills,
                wp.hard_requirements
            FROM shortlist_applications sa
            JOIN platform_users u ON u.id = sa.user_id
            LEFT JOIN seeker_profiles sp ON sp.user_id = sa.user_id
            JOIN watchable_positions wp ON wp.id = sa.position_id
            WHERE sa.id = %s
        """, (application_id,))

        result = cur.fetchone()
        if not result:
            return None

        data = dict(result)

        # Parse JSON fields
        for field in ['interview_transcript', 'interview_evaluation', 'eligibility_data',
                      'extracted_profile', 'must_have_skills', 'nice_to_have_skills', 'hard_requirements']:
            if data.get(field):
                if isinstance(data[field], str):
                    try:
                        data[field] = json.loads(data[field])
                    except:
                        data[field] = None

        # Get fit responses
        cur.execute("""
            SELECT
                afr.question_id,
                afr.response_value,
                afr.response_text
            FROM application_fit_responses afr
            WHERE afr.application_id = %s
        """, (application_id,))
        data['fit_responses'] = [dict(r) for r in cur.fetchall()]

        return data


def _generate_insights_with_ai(candidate_data: Dict) -> Dict:
    """Use OpenAI to generate candidate insights."""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        # Return placeholder if no API key
        return _generate_placeholder_insights(candidate_data)

    client = OpenAI(api_key=api_key)

    # Build context for the AI
    context = _build_analysis_context(candidate_data)

    prompt = f"""You are an expert recruiter analyzing a job candidate. Based on the information provided, generate insights to help the hiring manager understand this candidate's fit.

ROLE INFORMATION:
- Title: {candidate_data.get('role_title', 'Unknown')}
- Company: {candidate_data.get('company_name', 'Unknown')}
- Description: {candidate_data.get('role_description', 'No description')[:1500]}
- Required Experience Level: {candidate_data.get('required_experience_level', 'Not specified')}
- Location: {candidate_data.get('location', 'Not specified')}
- Work Arrangement: {candidate_data.get('work_arrangement', 'Not specified')}
- Must-Have Skills: {json.dumps(candidate_data.get('must_have_skills', []))}
- Nice-to-Have Skills: {json.dumps(candidate_data.get('nice_to_have_skills', []))}

CANDIDATE INFORMATION:
- Name: {candidate_data.get('first_name', '')} {candidate_data.get('last_name', '')}
- Email: {candidate_data.get('email', '')}
- Current fit score: {candidate_data.get('fit_score', 'Not calculated')}%
- Has Resume: {'Yes' if candidate_data.get('resume_path') else 'No'}
- Interview Status: {candidate_data.get('interview_status', 'pending')}

{context}

Based on this information, provide your analysis in the following JSON format.

CRITICAL: Every insight MUST include evidence_anchors - specific quotes or references that prove the claim. This allows employers to verify every statement.

{{
    "why_this_person": "A compelling 15-20 word summary of why this candidate might be a good fit",

    "overview": "A 3-4 sentence justification of the fit score. Be specific and reference actual data.",

    "overview_evidence": [
        {{
            "source": "resume|interview|fit_responses",
            "quote": "Exact quote or paraphrase that supports the overview",
            "section": "work_experience|education|skills|answer_to_X|transcript"
        }}
    ],

    "strengths": [
        {{
            "text": "Specific strength based on evidence",
            "evidence_anchors": [
                {{
                    "source": "resume",
                    "quote": "Exact text from resume that proves this",
                    "section": "work_experience|education|skills|projects|extracurriculars"
                }},
                {{
                    "source": "interview",
                    "quote": "What they said in interview",
                    "section": "transcript"
                }}
            ]
        }}
    ],

    "risks": [
        {{
            "text": "Specific gap or concern",
            "evidence_anchors": [
                {{
                    "source": "resume|interview|fit_responses|application",
                    "quote": "Evidence or lack thereof that indicates this risk",
                    "section": "relevant section or 'missing'"
                }}
            ]
        }}
    ],

    "suggested_questions": [
        {{
            "question": "Specific follow-up question",
            "rationale": "Why this matters",
            "related_gap": "Which risk/gap this addresses"
        }}
    ],

    "interview_highlights": [
        {{
            "quote": "Notable quote from interview",
            "type": "strength|concern",
            "competency": "What this demonstrates",
            "context": "Brief context of what question prompted this"
        }}
    ]
}}

IMPORTANT RULES FOR EVIDENCE ANCHORS:
1. EVERY strength MUST have at least 1 evidence_anchor with a real quote from their materials
2. EVERY risk MUST have an evidence_anchor explaining what's missing or concerning
3. Quotes should be EXACT text from the resume/interview when possible
4. For resume evidence, specify which section (work_experience, education, skills, projects, extracurriculars)
5. For fit_responses evidence, reference which question they answered
6. For interview evidence, include a brief snippet of what they actually said
7. If evidence comes from absence of information, set quote to describe what's missing

Return ONLY valid JSON, no other text."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert technical recruiter who provides detailed, evidence-based candidate assessments. Always return valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=3000  # Increased for detailed evidence anchors
        )

        response_text = response.choices[0].message.content.strip()

        # Clean up response - remove markdown code blocks if present
        if response_text.startswith('```'):
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
        if response_text.endswith('```'):
            response_text = response_text[:-3]

        insights = json.loads(response_text.strip())
        return insights

    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print(f"Response was: {response_text[:500] if 'response_text' in dir() else 'N/A'}")
        return _generate_placeholder_insights(candidate_data)
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return _generate_placeholder_insights(candidate_data)


def _build_analysis_context(candidate_data: Dict) -> str:
    """Build detailed context string for AI analysis."""
    parts = []

    # Resume/Profile info
    profile = candidate_data.get('extracted_profile') or {}
    if profile:
        parts.append("PARSED RESUME DATA:")
        if profile.get('current_title'):
            parts.append(f"- Current Role: {profile.get('current_title')} at {profile.get('current_company', 'Unknown')}")
        if profile.get('years_experience'):
            parts.append(f"- Years of Experience: {profile.get('years_experience')}")
        if profile.get('skills'):
            skills = profile.get('skills', [])
            if isinstance(skills, list):
                parts.append(f"- Skills: {', '.join(skills[:15])}")
            elif isinstance(skills, str):
                parts.append(f"- Skills: {skills}")
        if profile.get('education'):
            edu = profile.get('education')
            if isinstance(edu, list):
                parts.append(f"- Education: {edu[0] if edu else 'Not specified'}")
            else:
                parts.append(f"- Education: {edu}")
        if profile.get('summary'):
            parts.append(f"- Summary: {profile.get('summary')[:500]}")

    # Raw resume text (truncated)
    if candidate_data.get('resume_text'):
        resume_excerpt = candidate_data['resume_text'][:2000]
        parts.append(f"\nRESUME TEXT EXCERPT:\n{resume_excerpt}")

    # Fit responses
    if candidate_data.get('fit_responses'):
        parts.append("\nFIT QUESTION RESPONSES:")
        for resp in candidate_data['fit_responses']:
            if resp.get('response_text'):
                parts.append(f"- Q: {resp.get('question_id', 'Unknown')}")
                parts.append(f"  A: {resp['response_text'][:300]}")
            elif resp.get('response_value'):
                parts.append(f"- {resp.get('question_id', 'Unknown')}: {resp['response_value']}")

    # Interview data
    if candidate_data.get('interview_status') == 'completed':
        parts.append("\nINTERVIEW STATUS: Completed")

        if candidate_data.get('interview_evaluation'):
            eval_data = candidate_data['interview_evaluation']
            parts.append(f"Interview Evaluation:")
            if isinstance(eval_data, dict):
                if eval_data.get('overall_assessment'):
                    parts.append(f"- Overall: {eval_data['overall_assessment'][:500]}")
                if eval_data.get('competency_scores'):
                    parts.append(f"- Competencies: {json.dumps(eval_data['competency_scores'])}")

        if candidate_data.get('interview_transcript'):
            transcript = candidate_data['interview_transcript']
            if isinstance(transcript, list) and len(transcript) > 0:
                parts.append("\nINTERVIEW TRANSCRIPT EXCERPTS:")
                # Include first few and last few exchanges
                for entry in transcript[:5]:
                    speaker = entry.get('speaker', 'unknown')
                    text = entry.get('text', entry.get('content', ''))[:200]
                    parts.append(f"[{speaker}]: {text}")
                if len(transcript) > 10:
                    parts.append("... (truncated) ...")
                    for entry in transcript[-3:]:
                        speaker = entry.get('speaker', 'unknown')
                        text = entry.get('text', entry.get('content', ''))[:200]
                        parts.append(f"[{speaker}]: {text}")
    else:
        parts.append("\nINTERVIEW STATUS: Not completed")

    return '\n'.join(parts)


def _generate_placeholder_insights(candidate_data: Dict) -> Dict:
    """Generate basic insights without AI (fallback)."""
    profile = candidate_data.get('extracted_profile') or {}
    fit_score = candidate_data.get('fit_score')
    has_resume = bool(candidate_data.get('resume_path'))
    has_interview = candidate_data.get('interview_status') == 'completed'
    has_fit_responses = bool(candidate_data.get('fit_responses'))

    # Build overview based on available data
    overview_parts = []
    if fit_score:
        if fit_score >= 80:
            overview_parts.append(f"Strong fit score of {fit_score}% indicates good alignment with role requirements.")
        elif fit_score >= 60:
            overview_parts.append(f"Moderate fit score of {fit_score}% suggests partial alignment with role requirements.")
        else:
            overview_parts.append(f"Fit score of {fit_score}% indicates some gaps between candidate and role requirements.")

    if has_resume and profile:
        if profile.get('current_title'):
            overview_parts.append(f"Currently working as {profile.get('current_title')}.")
        if profile.get('years_experience'):
            overview_parts.append(f"Has {profile.get('years_experience')} years of experience.")

    if not has_interview:
        overview_parts.append("Interview not yet completed - score may increase after interview.")

    overview = ' '.join(overview_parts) if overview_parts else "Limited data available for assessment."

    # Build strengths
    strengths = []
    if profile.get('skills'):
        strengths.append({
            "text": f"Technical skills include: {', '.join(profile['skills'][:5])}",
            "evidence_source": "resume"
        })
    if profile.get('years_experience') and int(profile.get('years_experience', 0)) >= 3:
        strengths.append({
            "text": f"{profile['years_experience']} years of professional experience",
            "evidence_source": "resume"
        })
    if profile.get('current_title'):
        strengths.append({
            "text": f"Currently employed as {profile['current_title']}",
            "evidence_source": "resume"
        })
    if has_fit_responses:
        strengths.append({
            "text": "Completed all fit assessment questions",
            "evidence_source": "fit_responses"
        })

    # Ensure at least one strength
    if not strengths:
        strengths.append({
            "text": "Applied and expressed interest in the role",
            "evidence_source": "application"
        })

    # Build gaps (always at least 2)
    risks = []
    if not has_interview:
        risks.append({
            "text": "Interview not completed - unable to assess communication skills and cultural fit",
            "evidence_source": "application"
        })
    if not has_resume:
        risks.append({
            "text": "No resume uploaded - limited visibility into background",
            "evidence_source": "application"
        })
    if not has_fit_responses:
        risks.append({
            "text": "Fit questions not answered - unclear on role-specific preferences",
            "evidence_source": "application"
        })

    # Add generic gaps if needed
    if len(risks) < 2:
        risks.append({
            "text": "Experience level alignment with role requirements unclear",
            "evidence_source": "application"
        })
    if len(risks) < 2:
        risks.append({
            "text": "Specific technical depth in required areas not yet verified",
            "evidence_source": "application"
        })

    # Build follow-up questions
    role_title = candidate_data.get('role_title', 'this role')
    questions = [
        {
            "question": f"What specifically attracted you to the {role_title} position?",
            "rationale": "Assess motivation and role understanding"
        },
        {
            "question": "Walk me through a challenging project and how you approached it.",
            "rationale": "Evaluate problem-solving and technical depth"
        },
        {
            "question": "How do you prioritize when facing multiple competing deadlines?",
            "rationale": "Understand work style and organizational skills"
        }
    ]

    return {
        "why_this_person": _build_why_summary(profile, fit_score),
        "overview": overview,
        "strengths": strengths[:5],
        "risks": risks[:4],
        "suggested_questions": questions,
        "interview_highlights": []
    }


def _build_why_summary(profile: Dict, fit_score: Optional[int]) -> str:
    """Build a short summary for why_this_person field."""
    parts = []

    if profile.get('years_experience'):
        parts.append(f"{profile['years_experience']} years experience")

    if profile.get('current_title'):
        parts.append(profile['current_title'].split(' at ')[0])

    if profile.get('skills') and len(profile['skills']) > 0:
        parts.append(profile['skills'][0])

    if fit_score and fit_score >= 70:
        parts.append("strong initial fit")

    if parts:
        return ' + '.join(parts[:4])
    else:
        return "New applicant - review materials to assess fit"


def _store_insights(conn, application_id: int, insights: Dict):
    """Store generated insights in the database."""
    # Combine overview with its evidence for storage
    overview_with_evidence = {
        'text': insights.get('overview', ''),
        'evidence': insights.get('overview_evidence', [])
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO candidate_insights (
                application_id,
                why_this_person,
                overview,
                strengths,
                risks,
                suggested_questions,
                interview_highlights,
                llm_model,
                generation_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (application_id) DO UPDATE SET
                why_this_person = EXCLUDED.why_this_person,
                overview = EXCLUDED.overview,
                strengths = EXCLUDED.strengths,
                risks = EXCLUDED.risks,
                suggested_questions = EXCLUDED.suggested_questions,
                interview_highlights = EXCLUDED.interview_highlights,
                llm_model = EXCLUDED.llm_model,
                generation_version = EXCLUDED.generation_version,
                generated_at = NOW()
        """, (
            application_id,
            insights.get('why_this_person', ''),
            json.dumps(overview_with_evidence),  # Store as JSON with evidence
            json.dumps(insights.get('strengths', [])),
            json.dumps(insights.get('risks', [])),
            json.dumps(insights.get('suggested_questions', [])),
            json.dumps(insights.get('interview_highlights', [])),
            'gpt-4o-mini',
            '2.0'  # Version bump for evidence anchors
        ))
        conn.commit()


def regenerate_insights_for_application(application_id: int) -> Dict:
    """
    Regenerate insights for an application (called when data changes).
    Can be called after interview completion, resume upload, etc.
    """
    return generate_candidate_insights(application_id)


# CLI for testing
if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python insights_generator.py <application_id>")
        sys.exit(1)

    app_id = int(sys.argv[1])
    print(f"Generating insights for application {app_id}...")

    result = generate_candidate_insights(app_id)
    print(json.dumps(result, indent=2))
