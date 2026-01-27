"""
Smart Fit Scoring Engine for ShortList.ai
==========================================
Multi-bucket scoring with hard filters and completeness-based scoring.

Score reflects data completeness directly:
- Resume only: Max ~60% (missing interview + fit responses potential)
- Resume + Fit Responses: Max ~75% (missing interview potential)
- Resume + Fit Responses + Interview: Full 0-100 range

No confidence labels - the score itself reflects completeness.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Tuple, Any
import json


@dataclass
class ScoreBucket:
    name: str
    raw_score: int  # 0-100
    weight: float  # 0.0-1.0
    weighted_score: float
    evidence: List[str] = field(default_factory=list)
    notes: str = ""

    def to_dict(self):
        return {
            'name': self.name,
            'raw_score': self.raw_score,
            'weight': self.weight,
            'weighted_score': self.weighted_score,
            'evidence': self.evidence,
            'notes': self.notes
        }


@dataclass
class FitScoreResult:
    overall_score: int
    hard_filters_passed: bool
    hard_filter_breakdown: Dict[str, bool]
    buckets: List[ScoreBucket]
    deductions: Dict[str, int]
    completeness: Dict[str, bool]  # What data sources are present
    breakdown_explanation: Dict

    def to_dict(self):
        return {
            'overall_score': self.overall_score,
            'hard_filters_passed': self.hard_filters_passed,
            'hard_filter_breakdown': self.hard_filter_breakdown,
            'buckets': [b.to_dict() for b in self.buckets],
            'deductions': self.deductions,
            'completeness': self.completeness,
            'breakdown_explanation': self.breakdown_explanation
        }


class ScoringEngine:
    """
    Multi-bucket scoring engine with hard filter enforcement.

    Scoring Weights (when all data available):
    - Must-have skills: 35%
    - Experience alignment: 25%
    - Interview performance: 20%
    - Fit responses: 10%
    - Nice-to-have skills: 10%

    Deductions:
    - Weak evidence: up to -10
    - Inconsistencies: up to -15
    - Unclear answers: up to -5
    """

    WEIGHTS = {
        'must_have_skills': 0.35,
        'experience_alignment': 0.25,
        'interview_performance': 0.20,
        'fit_responses': 0.10,
        'nice_to_have_skills': 0.10
    }

    def __init__(self, db_connection):
        self.conn = db_connection

    def calculate_fit_score(
        self,
        application_id: int,
        candidate_data: Dict,
        job_data: Dict,
        interview_evaluation: Optional[Dict] = None,
        fit_responses: Optional[List[Dict]] = None
    ) -> FitScoreResult:
        """
        Calculate comprehensive fit score for a candidate.

        Args:
            application_id: The application ID
            candidate_data: Dict with skills, experience, eligibility_data, extracted_profile
            job_data: Dict with must_have_skills, nice_to_have_skills, hard_requirements, role_type
            interview_evaluation: Interview evaluation from AI screening (optional)
            fit_responses: List of fit question responses (optional)
        """
        # Step 1: Evaluate Hard Filters
        hard_filter_result = self._evaluate_hard_filters(
            candidate_data.get('eligibility_data', {}),
            job_data.get('hard_requirements', {})
        )

        # Track what data we have for completeness
        has_resume = bool(candidate_data.get('skills') or candidate_data.get('extracted_profile'))
        has_fit_responses = bool(fit_responses and len(fit_responses) > 0)
        has_interview = bool(interview_evaluation)

        completeness = {
            'resume': has_resume,
            'fit_responses': has_fit_responses,
            'interview': has_interview
        }

        if not hard_filter_result['passed']:
            # Hard filter failed - cap score at 30
            return FitScoreResult(
                overall_score=30,
                hard_filters_passed=False,
                hard_filter_breakdown=hard_filter_result['breakdown'],
                buckets=[],
                deductions={},
                completeness=completeness,
                breakdown_explanation={
                    'failed_hard_filter': hard_filter_result['failure_reason'],
                    'note': 'Score capped at 30 due to hard filter failure'
                }
            )

        # Step 2: Score Each Bucket
        buckets = []
        total_weight = 0

        # Must-have skills
        must_have_result = self._score_must_have_skills(
            candidate_data.get('skills', []),
            job_data.get('must_have_skills', [])
        )
        buckets.append(must_have_result)
        total_weight += must_have_result.weight

        # Experience alignment
        exp_result = self._score_experience_alignment(
            candidate_data.get('extracted_profile', {}),
            job_data
        )
        buckets.append(exp_result)
        total_weight += exp_result.weight

        # Interview performance (if available)
        if interview_evaluation:
            interview_result = self._score_interview(interview_evaluation)
            buckets.append(interview_result)
            total_weight += interview_result.weight
        else:
            # Reduce total weight if no interview
            pass

        # Fit responses (if available)
        if fit_responses and len(fit_responses) > 0:
            fit_result = self._score_fit_responses(
                fit_responses,
                job_data.get('role_type')
            )
            buckets.append(fit_result)
            total_weight += fit_result.weight

        # Nice-to-have skills
        nice_to_have_result = self._score_nice_to_have_skills(
            candidate_data.get('skills', []),
            job_data.get('nice_to_have_skills', [])
        )
        buckets.append(nice_to_have_result)
        total_weight += nice_to_have_result.weight

        # Step 3: Calculate Deductions
        deductions = self._calculate_deductions(
            candidate_data,
            interview_evaluation,
            fit_responses
        )

        # Step 4: Calculate Weighted Score
        if total_weight > 0:
            weighted_sum = sum(b.weighted_score for b in buckets)
            # Normalize to account for missing buckets
            base_score = (weighted_sum / total_weight) * 100
        else:
            base_score = 50  # Default if no data

        # Apply deductions
        total_deductions = sum(deductions.values())
        calculated_score = max(0, min(100, int(base_score - total_deductions)))

        # Step 5: Apply score cap based on completeness
        # Missing data = missing potential. Score reflects what we can verify.
        if has_interview and has_fit_responses:
            # Full data - no cap
            max_score = 100
            missing_penalty = 0
        elif has_fit_responses and not has_interview:
            # Missing interview - cap at 75, they need to interview to prove more
            max_score = 75
            missing_penalty = 15  # Direct penalty for not interviewing
        else:
            # Resume only - cap at 60, very limited data
            max_score = 60
            missing_penalty = 25  # Significant penalty for minimal engagement

        # Apply the penalty and cap
        final_score = max(0, min(max_score, calculated_score - missing_penalty))

        # Add completeness info to deductions for transparency
        if missing_penalty > 0:
            deductions['incomplete_profile'] = missing_penalty

        return FitScoreResult(
            overall_score=final_score,
            hard_filters_passed=True,
            hard_filter_breakdown=hard_filter_result['breakdown'],
            buckets=buckets,
            deductions=deductions,
            completeness=completeness,
            breakdown_explanation=self._build_explanation(buckets, deductions, completeness)
        )

    def _evaluate_hard_filters(
        self,
        eligibility_data: Dict,
        job_requirements: Dict
    ) -> Dict:
        """Evaluate hard pass/fail requirements."""
        breakdown = {
            'work_authorization': True,
            'location': True,
            'start_date': True,
            'seniority': True
        }
        failure_reason = None

        # Work Authorization Check
        if job_requirements.get('requires_authorization'):
            authorized = eligibility_data.get('authorized_us')
            needs_sponsorship = eligibility_data.get('needs_sponsorship')

            if not authorized:
                breakdown['work_authorization'] = False
                failure_reason = 'Not authorized to work in US'
            elif needs_sponsorship and not job_requirements.get('offers_sponsorship'):
                breakdown['work_authorization'] = False
                failure_reason = 'Needs visa sponsorship (not offered)'

        # Location/Hybrid Check
        if job_requirements.get('requires_hybrid'):
            hybrid_onsite = eligibility_data.get('hybrid_onsite')
            if hybrid_onsite == 'no' or hybrid_onsite == 'No, I need fully remote':
                breakdown['location'] = False
                failure_reason = failure_reason or 'Cannot meet hybrid/onsite requirement'

        # Start Date Check
        if job_requirements.get('latest_start_date'):
            candidate_start = eligibility_data.get('start_date')
            start_weeks = {
                'Immediately': 0,
                'Within 2 weeks': 2,
                'Within 1 month': 4,
                '1-3 months': 12,
                '3+ months': 16
            }
            candidate_weeks = start_weeks.get(candidate_start, 8)
            required_weeks = job_requirements.get('max_start_weeks', 8)

            if candidate_weeks > required_weeks:
                breakdown['start_date'] = False
                failure_reason = failure_reason or f'Start date too late'

        # Seniority Check
        if job_requirements.get('min_seniority') or job_requirements.get('max_seniority'):
            candidate_seniority = eligibility_data.get('seniority_band')
            seniority_levels = {'intern': 0, 'entry': 1, 'mid': 2, 'senior': 3, 'lead': 4}

            candidate_level = seniority_levels.get(candidate_seniority, 2)
            min_level = seniority_levels.get(job_requirements.get('min_seniority'), 0)
            max_level = seniority_levels.get(job_requirements.get('max_seniority'), 5)

            if candidate_level < min_level or candidate_level > max_level:
                breakdown['seniority'] = False
                failure_reason = failure_reason or 'Seniority level mismatch'

        return {
            'passed': all(breakdown.values()),
            'breakdown': breakdown,
            'failure_reason': failure_reason
        }

    def _score_must_have_skills(
        self,
        candidate_skills: List[Dict],
        required_skills: List[str]
    ) -> ScoreBucket:
        """Score must-have skill matches."""
        if not required_skills:
            return ScoreBucket(
                name='must_have_skills',
                raw_score=100,
                weight=self.WEIGHTS['must_have_skills'],
                weighted_score=self.WEIGHTS['must_have_skills'] * 1.0,
                evidence=["No must-have skills specified"]
            )

        # Normalize skills for matching
        candidate_skill_names = set()
        for s in candidate_skills:
            if isinstance(s, dict):
                candidate_skill_names.add(s.get('name', '').lower())
            elif isinstance(s, str):
                candidate_skill_names.add(s.lower())

        matched = []
        missing = []

        for skill in required_skills:
            skill_lower = skill.lower()
            if skill_lower in candidate_skill_names or self._fuzzy_skill_match(
                skill_lower, candidate_skill_names
            ):
                matched.append(skill)
            else:
                missing.append(skill)

        if len(required_skills) > 0:
            match_ratio = len(matched) / len(required_skills)
            # Exponential penalty for missing must-haves
            raw_score = int((match_ratio ** 1.5) * 100)
        else:
            raw_score = 100

        return ScoreBucket(
            name='must_have_skills',
            raw_score=raw_score,
            weight=self.WEIGHTS['must_have_skills'],
            weighted_score=self.WEIGHTS['must_have_skills'] * (raw_score / 100),
            evidence=[f"Matched: {', '.join(matched)}"] if matched else ["No matching skills"],
            notes=f"Missing: {', '.join(missing)}" if missing else ""
        )

    def _fuzzy_skill_match(self, skill: str, candidate_skills: set) -> bool:
        """Check for fuzzy skill matches (e.g., 'javascript' matches 'js')."""
        skill_aliases = {
            'javascript': ['js', 'ecmascript'],
            'typescript': ['ts'],
            'python': ['py'],
            'react': ['reactjs', 'react.js'],
            'node': ['nodejs', 'node.js'],
            'postgres': ['postgresql', 'psql'],
            'kubernetes': ['k8s'],
            'amazon web services': ['aws'],
            'google cloud': ['gcp', 'google cloud platform'],
            'machine learning': ['ml'],
            'artificial intelligence': ['ai'],
        }

        # Check if skill is an alias
        for main_skill, aliases in skill_aliases.items():
            if skill == main_skill:
                for alias in aliases:
                    if alias in candidate_skills:
                        return True
            elif skill in aliases:
                if main_skill in candidate_skills:
                    return True
                for alias in aliases:
                    if alias in candidate_skills:
                        return True

        # Substring matching for common patterns
        for candidate_skill in candidate_skills:
            if skill in candidate_skill or candidate_skill in skill:
                return True

        return False

    def _score_experience_alignment(
        self,
        extracted_profile: Dict,
        job_data: Dict
    ) -> ScoreBucket:
        """Score experience alignment with job requirements."""
        raw_score = 50  # Default
        evidence = []

        # Experience level alignment
        candidate_level = (extracted_profile.get('experience_level') or '').lower()
        job_level = (job_data.get('experience_level') or '').lower()

        level_order = ['intern', 'entry', 'junior', 'mid', 'senior', 'lead', 'staff', 'principal']

        if candidate_level and job_level:
            try:
                candidate_idx = next((i for i, l in enumerate(level_order) if l in candidate_level), 3)
                job_idx = next((i for i, l in enumerate(level_order) if l in job_level), 3)

                diff = candidate_idx - job_idx
                if diff == 0:
                    raw_score = 100
                    evidence.append(f"Experience level matches: {candidate_level}")
                elif abs(diff) == 1:
                    raw_score = 85
                    evidence.append(f"Experience level close: {candidate_level} vs {job_level}")
                elif diff > 1:
                    raw_score = 70  # Overqualified
                    evidence.append(f"Overqualified: {candidate_level} for {job_level}")
                else:
                    raw_score = 50  # Underqualified
                    evidence.append(f"May be underqualified: {candidate_level} for {job_level}")
            except (ValueError, StopIteration):
                pass

        # Years of experience
        years_exp = extracted_profile.get('years_experience', 0)
        if years_exp:
            evidence.append(f"{years_exp} years of experience")
            if years_exp >= 5:
                raw_score = min(100, raw_score + 10)

        # Industry alignment
        industries = extracted_profile.get('industries', [])
        job_industry = job_data.get('industry', '')
        if job_industry and any(job_industry.lower() in ind.lower() for ind in industries):
            raw_score = min(100, raw_score + 10)
            evidence.append(f"Industry match: {job_industry}")

        return ScoreBucket(
            name='experience_alignment',
            raw_score=raw_score,
            weight=self.WEIGHTS['experience_alignment'],
            weighted_score=self.WEIGHTS['experience_alignment'] * (raw_score / 100),
            evidence=evidence if evidence else ["Limited experience data available"]
        )

    def _score_interview(self, evaluation: Dict) -> ScoreBucket:
        """Score based on AI interview evaluation."""
        # Map screening category to score
        category_scores = {
            'Strong Proceed': 95,
            'Proceed': 75,
            'Hold': 50,
            'Do Not Proceed': 20
        }

        category = evaluation.get('final_screening_category', 'Hold')
        base_score = category_scores.get(category, 50)

        evidence = []

        # Adjust based on competency scores
        comp_scores = evaluation.get('competency_scores', [])
        if comp_scores:
            must_have_scores = [c.get('score', 3) for c in comp_scores if c.get('is_must_have')]
            if must_have_scores:
                avg_must_have = sum(must_have_scores) / len(must_have_scores)
                # Scale 1-5 to adjustment (-20 to +20)
                adjustment = (avg_must_have - 3) * 10
                base_score = max(0, min(100, base_score + adjustment))

        # Extract strengths as evidence
        candidate_summary = evaluation.get('candidate_summary', {})
        if candidate_summary.get('key_strengths'):
            evidence.extend(candidate_summary['key_strengths'][:3])

        return ScoreBucket(
            name='interview_performance',
            raw_score=int(base_score),
            weight=self.WEIGHTS['interview_performance'],
            weighted_score=self.WEIGHTS['interview_performance'] * (base_score / 100),
            evidence=evidence if evidence else [f"Interview category: {category}"],
            notes=f"Category: {category}"
        )

    def _score_fit_responses(
        self,
        fit_responses: List[Dict],
        role_type: Optional[str]
    ) -> ScoreBucket:
        """Score fit question responses."""
        if not fit_responses:
            return ScoreBucket(
                name='fit_responses',
                raw_score=50,
                weight=self.WEIGHTS['fit_responses'],
                weighted_score=self.WEIGHTS['fit_responses'] * 0.5,
                evidence=["No fit responses provided"]
            )

        # Count completed responses
        mc_responses = [r for r in fit_responses if r.get('response_value')]
        fr_responses = [r for r in fit_responses if r.get('response_text')]

        # Base score on completion
        completion_rate = (len(mc_responses) + len(fr_responses)) / max(len(fit_responses), 1)
        raw_score = int(50 + (completion_rate * 50))  # 50-100 based on completion

        evidence = []
        if mc_responses:
            evidence.append(f"Completed {len(mc_responses)} multiple choice questions")
        if fr_responses:
            evidence.append(f"Provided {len(fr_responses)} free responses")

        return ScoreBucket(
            name='fit_responses',
            raw_score=raw_score,
            weight=self.WEIGHTS['fit_responses'],
            weighted_score=self.WEIGHTS['fit_responses'] * (raw_score / 100),
            evidence=evidence
        )

    def _score_nice_to_have_skills(
        self,
        candidate_skills: List[Dict],
        nice_to_have_skills: List[str]
    ) -> ScoreBucket:
        """Score nice-to-have skill matches (bonus points)."""
        if not nice_to_have_skills:
            return ScoreBucket(
                name='nice_to_have_skills',
                raw_score=50,  # Neutral if no nice-to-haves specified
                weight=self.WEIGHTS['nice_to_have_skills'],
                weighted_score=self.WEIGHTS['nice_to_have_skills'] * 0.5,
                evidence=["No nice-to-have skills specified"]
            )

        # Normalize skills for matching
        candidate_skill_names = set()
        for s in candidate_skills:
            if isinstance(s, dict):
                candidate_skill_names.add(s.get('name', '').lower())
            elif isinstance(s, str):
                candidate_skill_names.add(s.lower())

        matched = []
        for skill in nice_to_have_skills:
            skill_lower = skill.lower()
            if skill_lower in candidate_skill_names or self._fuzzy_skill_match(
                skill_lower, candidate_skill_names
            ):
                matched.append(skill)

        # Nice-to-haves give bonus, not penalty
        match_ratio = len(matched) / len(nice_to_have_skills) if nice_to_have_skills else 0
        raw_score = int(50 + (match_ratio * 50))  # 50-100

        return ScoreBucket(
            name='nice_to_have_skills',
            raw_score=raw_score,
            weight=self.WEIGHTS['nice_to_have_skills'],
            weighted_score=self.WEIGHTS['nice_to_have_skills'] * (raw_score / 100),
            evidence=[f"Bonus skills: {', '.join(matched)}"] if matched else ["No bonus skills matched"]
        )

    def _calculate_deductions(
        self,
        candidate_data: Dict,
        interview_evaluation: Optional[Dict],
        fit_responses: Optional[List[Dict]]
    ) -> Dict[str, int]:
        """Calculate score deductions for weak evidence, inconsistencies, etc."""
        deductions = {
            'weak_evidence': 0,
            'inconsistencies': 0,
            'unclear_answers': 0
        }

        # Weak evidence: Missing key data
        if not candidate_data.get('skills'):
            deductions['weak_evidence'] += 3
        if not candidate_data.get('extracted_profile', {}).get('roles'):
            deductions['weak_evidence'] += 2

        # Check for red flags in interview
        if interview_evaluation:
            red_flags = interview_evaluation.get('red_flags', [])
            for flag in red_flags:
                severity = flag.get('severity', 'low')
                if severity == 'high':
                    deductions['inconsistencies'] += 5
                elif severity == 'medium':
                    deductions['inconsistencies'] += 3
                else:
                    deductions['inconsistencies'] += 1

            # Cap inconsistency deductions
            deductions['inconsistencies'] = min(deductions['inconsistencies'], 15)

        # Unclear answers in fit responses
        if fit_responses:
            for response in fit_responses:
                text = response.get('response_text', '')
                if text and len(text) < 20:  # Very short free response
                    deductions['unclear_answers'] += 1

            deductions['unclear_answers'] = min(deductions['unclear_answers'], 5)

        return deductions

    def _build_explanation(
        self,
        buckets: List[ScoreBucket],
        deductions: Dict[str, int],
        completeness: Optional[Dict[str, bool]] = None
    ) -> Dict:
        """Build human-readable score breakdown."""
        explanation = {
            'buckets': {},
            'deductions': {},
            'summary': ''
        }

        for bucket in buckets:
            explanation['buckets'][bucket.name] = {
                'score': bucket.raw_score,
                'weight': f"{int(bucket.weight * 100)}%",
                'contribution': bucket.weighted_score,
                'evidence': bucket.evidence,
                'notes': bucket.notes
            }

        total_deductions = 0
        for key, value in deductions.items():
            if value > 0:
                explanation['deductions'][key] = f"-{value}"
                total_deductions += value

        # Build summary based on completeness and score
        top_bucket = max(buckets, key=lambda b: b.raw_score) if buckets else None

        # Add completeness info to explanation
        if completeness:
            explanation['completeness'] = completeness
            missing = []
            if not completeness.get('interview'):
                missing.append('interview')
            if not completeness.get('fit_responses'):
                missing.append('fit responses')

            if missing:
                explanation['missing_data'] = missing

        if completeness and not completeness.get('interview'):
            explanation['summary'] = "Score limited - interview not completed"
        elif top_bucket and top_bucket.raw_score >= 80:
            explanation['summary'] = f"Strong in {top_bucket.name.replace('_', ' ')}"
        elif total_deductions > 10:
            explanation['summary'] = "Some concerns identified"
        else:
            explanation['summary'] = "Moderate fit across categories"

        return explanation


def calculate_and_store_fit_score(conn, application_id: int) -> Optional[FitScoreResult]:
    """
    Calculate fit score for an application and store it in the database.

    This function fetches all necessary data, runs the scoring engine,
    and stores the results.
    """
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get application data
        cur.execute("""
            SELECT
                sa.id, sa.user_id, sa.position_id, sa.eligibility_data,
                sa.interview_transcript, sa.interview_evaluation,
                sp.extracted_profile, sp.resume_text,
                wp.role_type, wp.hard_requirements, wp.must_have_skills,
                wp.nice_to_have_skills, wp.experience_level, wp.title
            FROM shortlist_applications sa
            JOIN seeker_profiles sp ON sp.user_id = sa.user_id
            JOIN watchable_positions wp ON wp.id = sa.position_id
            WHERE sa.id = %s
        """, (application_id,))
        app = cur.fetchone()

        if not app:
            return None

        # Get candidate skills
        cur.execute("""
            SELECT os.skill_name as name, cs.confidence
            FROM candidate_skills cs
            JOIN onet_skills os ON os.id = cs.skill_id
            WHERE cs.user_id = %s
        """, (app['user_id'],))
        skills = cur.fetchall()

        # Get fit responses
        cur.execute("""
            SELECT question_id, response_value, response_text
            FROM application_fit_responses
            WHERE application_id = %s
        """, (application_id,))
        fit_responses = cur.fetchall()

    # Prepare data for scoring
    candidate_data = {
        'skills': [dict(s) for s in skills],
        'extracted_profile': app.get('extracted_profile') or {},
        'eligibility_data': app.get('eligibility_data') or {}
    }

    job_data = {
        'role_type': app.get('role_type'),
        'hard_requirements': app.get('hard_requirements') or {},
        'must_have_skills': app.get('must_have_skills') or [],
        'nice_to_have_skills': app.get('nice_to_have_skills') or [],
        'experience_level': app.get('experience_level'),
        'title': app.get('title')
    }

    interview_evaluation = app.get('interview_evaluation')

    # Run scoring engine
    engine = ScoringEngine(conn)
    result = engine.calculate_fit_score(
        application_id=application_id,
        candidate_data=candidate_data,
        job_data=job_data,
        interview_evaluation=interview_evaluation,
        fit_responses=[dict(r) for r in fit_responses]
    )

    # Store results
    with conn.cursor() as cur:
        # Update shortlist_applications (keep confidence_level column but set to NULL)
        cur.execute("""
            UPDATE shortlist_applications
            SET fit_score = %s, confidence_level = NULL, hard_filter_failed = %s
            WHERE id = %s
        """, (result.overall_score, not result.hard_filters_passed, application_id))

        # Insert or update candidate_fit_scores
        # Store completeness info in score_breakdown
        breakdown_with_completeness = result.breakdown_explanation.copy()
        breakdown_with_completeness['completeness'] = result.completeness

        cur.execute("""
            INSERT INTO candidate_fit_scores (
                application_id, overall_fit_score, confidence_level,
                hard_filters_passed, hard_filter_breakdown,
                must_have_skills_score, experience_alignment_score,
                interview_performance_score, fit_responses_score,
                nice_to_have_skills_score, deductions, score_breakdown
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (application_id) DO UPDATE SET
                overall_fit_score = EXCLUDED.overall_fit_score,
                confidence_level = EXCLUDED.confidence_level,
                hard_filters_passed = EXCLUDED.hard_filters_passed,
                hard_filter_breakdown = EXCLUDED.hard_filter_breakdown,
                must_have_skills_score = EXCLUDED.must_have_skills_score,
                experience_alignment_score = EXCLUDED.experience_alignment_score,
                interview_performance_score = EXCLUDED.interview_performance_score,
                fit_responses_score = EXCLUDED.fit_responses_score,
                nice_to_have_skills_score = EXCLUDED.nice_to_have_skills_score,
                deductions = EXCLUDED.deductions,
                score_breakdown = EXCLUDED.score_breakdown,
                updated_at = NOW()
        """, (
            application_id,
            result.overall_score,
            None,  # No longer using confidence_level
            result.hard_filters_passed,
            json.dumps(result.hard_filter_breakdown),
            next((b.raw_score for b in result.buckets if b.name == 'must_have_skills'), None),
            next((b.raw_score for b in result.buckets if b.name == 'experience_alignment'), None),
            next((b.raw_score for b in result.buckets if b.name == 'interview_performance'), None),
            next((b.raw_score for b in result.buckets if b.name == 'fit_responses'), None),
            next((b.raw_score for b in result.buckets if b.name == 'nice_to_have_skills'), None),
            json.dumps(result.deductions),
            json.dumps(breakdown_with_completeness)
        ))

        conn.commit()

    return result
