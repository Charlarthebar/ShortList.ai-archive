"""
AI Insights Generator for Candidate Profiles
=============================================
Generates employer-facing insights: summaries, strengths/risks, interview questions.
Auto-triggered after interview completes.
"""

from typing import Dict, List, Optional, Any
import json
import os
from openai import OpenAI


class InsightsGenerator:
    """
    Generate AI-powered insights for candidate profiles.

    Generates:
    1. why_this_person - 15-word max one-liner
    2. strengths - Max 4, each with evidence source
    3. risks - Max 3, each with evidence source
    4. suggested_questions - 3 tailored follow-up questions
    5. matched_skill_chips - Top 5 skills matching job requirements
    6. interview_highlights - Key quotes from transcript
    """

    def __init__(self, db_connection):
        self.conn = db_connection
        self.client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

    def generate_insights(self, application_id: int) -> Dict:
        """
        Generate all insights for a candidate application.
        Called after interview completion.
        """
        # Gather all candidate data
        candidate_data = self._gather_candidate_data(application_id)
        job_data = self._get_job_data(candidate_data['position_id'])

        # Generate each insight type
        why_this_person = self._generate_one_liner(candidate_data, job_data)
        strengths_risks = self._generate_strengths_risks(candidate_data, job_data)
        suggested_questions = self._generate_interview_questions(candidate_data, job_data)
        skill_chips = self._generate_skill_chips(candidate_data, job_data)
        highlights = self._extract_highlights(candidate_data)

        # Compile insights
        insights = {
            'why_this_person': why_this_person,
            'strengths': strengths_risks.get('strengths', []),
            'risks': strengths_risks.get('risks', []),
            'suggested_questions': suggested_questions,
            'matched_skill_chips': skill_chips,
            'interview_highlights': highlights
        }

        # Store insights
        self._store_insights(application_id, insights)

        return insights

    def _gather_candidate_data(self, application_id: int) -> Dict:
        """Fetch all candidate data for the application."""
        from psycopg2.extras import RealDictCursor

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get application and profile data
            cur.execute("""
                SELECT
                    sa.id as application_id, sa.user_id, sa.position_id,
                    sa.eligibility_data, sa.interview_transcript, sa.interview_evaluation,
                    sa.fit_score, sa.confidence_level,
                    sp.extracted_profile, sp.resume_text,
                    u.first_name, u.last_name, u.email
                FROM shortlist_applications sa
                JOIN seeker_profiles sp ON sp.user_id = sa.user_id
                JOIN platform_users u ON u.id = sa.user_id
                WHERE sa.id = %s
            """, (application_id,))
            app = cur.fetchone()

            if not app:
                raise ValueError(f"Application {application_id} not found")

            # Get candidate skills
            cur.execute("""
                SELECT os.skill_name as name, cs.confidence
                FROM candidate_skills cs
                JOIN onet_skills os ON os.id = cs.skill_id
                WHERE cs.user_id = %s
                ORDER BY cs.confidence DESC
            """, (app['user_id'],))
            skills = [dict(s) for s in cur.fetchall()]

            # Get fit responses
            cur.execute("""
                SELECT question_id, response_value, response_text
                FROM application_fit_responses
                WHERE application_id = %s
            """, (application_id,))
            fit_responses = [dict(r) for r in cur.fetchall()]

        return {
            'application_id': application_id,
            'position_id': app['position_id'],
            'user_id': app['user_id'],
            'first_name': app['first_name'],
            'last_name': app['last_name'],
            'email': app['email'],
            'extracted_profile': app.get('extracted_profile') or {},
            'resume_text': app.get('resume_text', ''),
            'interview_transcript': app.get('interview_transcript') or [],
            'interview_evaluation': app.get('interview_evaluation') or {},
            'eligibility_data': app.get('eligibility_data') or {},
            'skills': skills,
            'fit_responses': fit_responses,
            'fit_score': app.get('fit_score'),
            'confidence_level': app.get('confidence_level')
        }

    def _get_job_data(self, position_id: int) -> Dict:
        """Fetch job data for context."""
        from psycopg2.extras import RealDictCursor

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, title, company_name, description, role_type,
                       experience_level, hard_requirements,
                       must_have_skills, nice_to_have_skills
                FROM watchable_positions
                WHERE id = %s
            """, (position_id,))
            job = cur.fetchone()

        return dict(job) if job else {}

    def _generate_one_liner(self, candidate: Dict, job: Dict) -> str:
        """
        Generate a concise "why this person" summary.
        Max 15 words, factual, no fluff.
        """
        # Build context
        profile = candidate.get('extracted_profile', {})
        skills = [s['name'] for s in candidate.get('skills', [])[:8]]
        interview_eval = candidate.get('interview_evaluation', {})

        prompt = f"""Based on this candidate profile and job, write ONE sentence (max 15 words)
explaining why this person could be a good fit. Focus on concrete qualifications.

Job: {job.get('title', 'Unknown')} at {job.get('company_name', 'Unknown')}
Required skills: {', '.join((job.get('must_have_skills') or [])[:5])}

Candidate:
- Current/Recent title: {profile.get('current_title', 'Unknown')}
- Skills: {', '.join(skills)}
- Experience: {profile.get('years_experience', 'Unknown')} years
- Interview category: {interview_eval.get('final_screening_category', 'N/A')}

Write a direct, factual one-liner. No fluff. Example format:
"5 years React/Node + startup experience + strong system design skills"
"Senior backend engineer with AWS expertise and fintech background"
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.3
            )
            return response.choices[0].message.content.strip().strip('"')
        except Exception as e:
            print(f"Error generating one-liner: {e}")
            # Fallback to template-based
            title = profile.get('current_title', '')
            years = profile.get('years_experience', '')
            if title and years:
                return f"{years}+ years experience as {title}"
            return "Candidate profile under review"

    def _generate_strengths_risks(self, candidate: Dict, job: Dict) -> Dict:
        """
        Generate strengths and risks with evidence backing.
        Max 4 strengths, max 3 risks.
        """
        interview_eval = candidate.get('interview_evaluation', {})
        profile = candidate.get('extracted_profile', {})
        skills = [s['name'] for s in candidate.get('skills', [])[:10]]

        prompt = f"""Analyze this candidate for the {job.get('title', 'Unknown')} role and identify strengths and risks.

JOB REQUIREMENTS:
- Must-have skills: {', '.join(job.get('must_have_skills') or [])}
- Nice-to-have: {', '.join(job.get('nice_to_have_skills') or [])}
- Experience level: {job.get('experience_level', 'Not specified')}

CANDIDATE DATA:
- Title: {profile.get('current_title', 'Unknown')}
- Years experience: {profile.get('years_experience', 'Unknown')}
- Skills: {', '.join(skills)}
- Interview category: {interview_eval.get('final_screening_category', 'N/A')}
- Key strengths from interview: {json.dumps(interview_eval.get('candidate_summary', {}).get('key_strengths', []))}
- Red flags from interview: {json.dumps(interview_eval.get('red_flags', []))}
- Areas for growth: {json.dumps(interview_eval.get('candidate_summary', {}).get('areas_for_growth', []))}

Return JSON with:
{{
    "strengths": [
        {{"text": "specific strength in plain language", "evidence_source": "resume|interview|fit_responses", "confidence": "high|medium|low"}}
    ],
    "risks": [
        {{"text": "specific concern or gap", "evidence_source": "resume|interview|fit_responses", "confidence": "high|medium|low"}}
    ]
}}

Rules:
- Max 4 strengths, max 3 risks
- Be specific and factual
- Every claim must have evidence source
- Write in plain language employers understand
- If interview data is missing, note limited confidence
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=600,
                temperature=0.2,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            return {
                'strengths': result.get('strengths', [])[:4],
                'risks': result.get('risks', [])[:3]
            }
        except Exception as e:
            print(f"Error generating strengths/risks: {e}")
            # Fallback to interview data if available
            strengths = []
            risks = []

            if interview_eval.get('candidate_summary', {}).get('key_strengths'):
                for s in interview_eval['candidate_summary']['key_strengths'][:4]:
                    strengths.append({
                        'text': s,
                        'evidence_source': 'interview',
                        'confidence': 'medium'
                    })

            if interview_eval.get('red_flags'):
                for flag in interview_eval['red_flags'][:3]:
                    risks.append({
                        'text': flag.get('flag', 'Concern identified'),
                        'evidence_source': 'interview',
                        'confidence': flag.get('severity', 'medium')
                    })

            return {'strengths': strengths, 'risks': risks}

    def _generate_interview_questions(self, candidate: Dict, job: Dict) -> List[Dict]:
        """
        Generate 3 tailored interview questions for gaps.
        """
        interview_eval = candidate.get('interview_evaluation', {})
        skills = [s['name'] for s in candidate.get('skills', [])]
        must_have = job.get('must_have_skills') or []

        # Find missing skills
        candidate_skills_lower = set(s.lower() for s in skills)
        missing_skills = [s for s in must_have if s.lower() not in candidate_skills_lower]

        prompt = f"""Based on this candidate's profile for {job.get('title', 'the role')},
suggest 3 specific interview questions for the hiring manager to ask.

Focus on:
1. Gaps or uncertainties in their background
2. Areas where evidence is weak
3. Red flags that need clarification

Candidate gaps/concerns:
- Interview red flags: {json.dumps(interview_eval.get('red_flags', []))}
- Follow-up questions from AI interview: {json.dumps(interview_eval.get('follow_up_questions_for_human_interviewer', []))}
- Potentially missing skills: {', '.join(missing_skills[:5])}
- Areas for growth: {json.dumps(interview_eval.get('candidate_summary', {}).get('areas_for_growth', []))}

Return a JSON object with a 'questions' array:
{{
    "questions": [
        {{"question": "specific behavioral or technical question", "rationale": "why ask this", "gap_area": "skills|experience|culture|other"}}
    ]
}}

Make questions specific and actionable, not generic.
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.4,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            questions = result.get('questions', [])
            if isinstance(questions, list):
                return questions[:3]
            return []
        except Exception as e:
            print(f"Error generating interview questions: {e}")
            # Fallback to questions from interview if available
            follow_ups = interview_eval.get('follow_up_questions_for_human_interviewer', [])
            return [
                {'question': q, 'rationale': 'From AI interview', 'gap_area': 'experience'}
                for q in follow_ups[:3]
            ]

    def _generate_skill_chips(self, candidate: Dict, job: Dict) -> List[Dict]:
        """
        Generate skill chips for display (matched to job requirements).
        Max 5 chips, must-haves first.
        """
        candidate_skills = set(s['name'].lower() for s in candidate.get('skills', []))
        must_have = job.get('must_have_skills') or []
        nice_to_have = job.get('nice_to_have_skills') or []

        chips = []

        # Must-have matches first
        for skill in must_have:
            if skill.lower() in candidate_skills:
                chips.append({
                    'skill': skill,
                    'is_must_have': True,
                    'matched': True,
                    'source': 'resume'
                })

        # Nice-to-have matches
        for skill in nice_to_have:
            if skill.lower() in candidate_skills and len(chips) < 5:
                chips.append({
                    'skill': skill,
                    'is_must_have': False,
                    'matched': True,
                    'source': 'resume'
                })

        # If still room, add top candidate skills
        if len(chips) < 5:
            for skill_data in candidate.get('skills', []):
                skill = skill_data['name']
                if skill.lower() not in [c['skill'].lower() for c in chips]:
                    chips.append({
                        'skill': skill,
                        'is_must_have': False,
                        'matched': False,
                        'source': 'resume'
                    })
                    if len(chips) >= 5:
                        break

        return chips[:5]

    def _extract_highlights(self, candidate: Dict) -> List[Dict]:
        """Extract notable moments from interview transcript."""
        highlights = []
        interview_eval = candidate.get('interview_evaluation', {})
        transcript = candidate.get('interview_transcript', [])

        if not interview_eval:
            return highlights

        # Use competency evidence as highlights
        for comp in interview_eval.get('competency_scores', []):
            for evidence in comp.get('evidence', [])[:2]:
                if evidence and len(evidence) > 20:  # Only substantial quotes
                    highlights.append({
                        'quote': evidence[:200] + ('...' if len(evidence) > 200 else ''),
                        'competency': comp.get('competency_name'),
                        'type': 'strength' if comp.get('score', 0) >= 4 else 'neutral'
                    })

        # Add red flags as highlights (concerns)
        for flag in interview_eval.get('red_flags', []):
            if flag.get('evidence'):
                highlights.append({
                    'quote': flag['evidence'][:200] + ('...' if len(flag.get('evidence', '')) > 200 else ''),
                    'flag': flag.get('flag'),
                    'type': 'concern',
                    'severity': flag.get('severity', 'medium')
                })

        # Add key quotes from candidate summary
        summary = interview_eval.get('candidate_summary', {})
        for strength in summary.get('key_strengths', [])[:2]:
            if strength and strength not in [h.get('quote', '') for h in highlights]:
                highlights.append({
                    'quote': strength,
                    'type': 'strength',
                    'context': 'Key strength identified'
                })

        return highlights[:10]

    def _store_insights(self, application_id: int, insights: Dict):
        """Store generated insights in database."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO candidate_insights
                (application_id, why_this_person, strengths, risks,
                 suggested_questions, matched_skill_chips, interview_highlights,
                 llm_model, generation_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (application_id) DO UPDATE SET
                    why_this_person = EXCLUDED.why_this_person,
                    strengths = EXCLUDED.strengths,
                    risks = EXCLUDED.risks,
                    suggested_questions = EXCLUDED.suggested_questions,
                    matched_skill_chips = EXCLUDED.matched_skill_chips,
                    interview_highlights = EXCLUDED.interview_highlights,
                    generated_at = NOW(),
                    llm_model = EXCLUDED.llm_model,
                    generation_version = EXCLUDED.generation_version
            """, (
                application_id,
                insights['why_this_person'],
                json.dumps(insights['strengths']),
                json.dumps(insights['risks']),
                json.dumps(insights['suggested_questions']),
                json.dumps(insights['matched_skill_chips']),
                json.dumps(insights['interview_highlights']),
                'gpt-4o-mini',
                '1.0'
            ))
            self.conn.commit()


def generate_and_store_insights(conn, application_id: int) -> Optional[Dict]:
    """
    Convenience function to generate and store insights for an application.
    Called after interview completion.
    """
    try:
        generator = InsightsGenerator(conn)
        insights = generator.generate_insights(application_id)
        return insights
    except Exception as e:
        print(f"Error generating insights for application {application_id}: {e}")
        return None
