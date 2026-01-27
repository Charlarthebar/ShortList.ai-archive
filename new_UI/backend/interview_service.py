#!/usr/bin/env python3
"""
AI Interview Service for ShortList
-----------------------------------
WebSocket-based service for conducting AI-powered screening interviews.
Integrates with the existing AI screening interview engine.

Run with:
    uvicorn interview_service:app --reload --port 8001

Or:
    python interview_service.py
"""

import asyncio
import json
import os
import sys
import uuid
import base64
import tempfile
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the ai_screening_interview module to path
AI_INTERVIEW_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'ai_screening_interview')
sys.path.insert(0, AI_INTERVIEW_PATH)

# Import core interview logic
from ai_screening_interview import (
    LLMClient,
    InterviewPlan,
    InterviewState,
    ScreeningOutput,
    Question,
    QuestionResponse,
    create_interview_plan,
    generate_follow_up,
    evaluate_interview,
    resume_pdf_to_text,
    get_llm_client,
)

# Optional: Import STT for voice transcription
try:
    from ai_screening_interview import WhisperSTT
    STT_AVAILABLE = True
except ImportError:
    STT_AVAILABLE = False
    print("Warning: WhisperSTT not available. Voice transcription disabled.")

# Optional: Import TTS for voice output
try:
    from ai_screening_interview import OpenAITTS
    TTS_AVAILABLE = True
except ImportError:
    TTS_AVAILABLE = False
    print("Warning: OpenAITTS not available. Voice synthesis disabled.")

# =============================================================================
# CONFIGURATION
# =============================================================================

DATABASE_URL = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5432'),
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', '')
}

MAX_INTERVIEW_DURATION_SECONDS = 900  # 15 minutes hard limit
RAPPORT_DURATION_SECONDS = 30  # 30 seconds of rapport building

# =============================================================================
# MODELS
# =============================================================================

class SessionState(str, Enum):
    CREATED = "created"
    PERMISSIONS = "permissions"
    RAPPORT = "rapport"
    QUESTIONING = "questioning"
    FOLLOW_UP = "follow_up"
    WRAP_UP = "wrap_up"
    EVALUATING = "evaluating"
    COMPLETE = "complete"
    ERROR = "error"


class StartInterviewRequest(BaseModel):
    application_id: int
    token: str  # JWT token for auth


class InterviewMessage(BaseModel):
    type: str  # rapport, question, follow_up, info, error, complete, audio
    content: str
    question_number: Optional[int] = None
    total_questions: Optional[int] = None
    metadata: Optional[dict] = None
    audio_base64: Optional[str] = None  # TTS audio


class CandidateResponse(BaseModel):
    type: str  # text, audio
    content: str  # text or base64 audio
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_db_connection():
    """Get a database connection."""
    return psycopg2.connect(**DATABASE_URL)


def get_application_data(application_id: int, user_id: int) -> Optional[dict]:
    """Get application data including job and resume info."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    sa.id as application_id,
                    sa.user_id,
                    sa.position_id,
                    sa.resume_path,
                    sa.eligibility_data,
                    sa.interview_status,
                    wp.title as job_title,
                    wp.company_name,
                    wp.description as job_description,
                    wp.role_type,
                    pu.first_name,
                    pu.last_name,
                    pu.email
                FROM shortlist_applications sa
                JOIN watchable_positions wp ON wp.id = sa.position_id
                JOIN platform_users pu ON pu.id = sa.user_id
                WHERE sa.id = %s AND sa.user_id = %s
            """, (application_id, user_id))
            return cur.fetchone()
    finally:
        conn.close()


def get_resume_text(resume_path: str) -> str:
    """Extract text from resume PDF."""
    if not resume_path or not os.path.exists(resume_path):
        return ""

    try:
        return resume_pdf_to_text(resume_path, use_llm_cleanup=False)
    except Exception as e:
        print(f"Error extracting resume: {e}")
        return ""


def save_interview_results(
    application_id: int,
    transcript: list[dict],
    evaluation: dict,
    status: str = "completed"
) -> None:
    """Save interview results to database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shortlist_applications
                SET
                    interview_status = %s,
                    interview_transcript = %s,
                    interview_evaluation = %s,
                    interview_completed_at = NOW()
                WHERE id = %s
            """, (status, json.dumps(transcript), json.dumps(evaluation), application_id))
            conn.commit()
    finally:
        conn.close()


def verify_jwt_token(token: str) -> Optional[int]:
    """Verify JWT token and return user_id."""
    import jwt as pyjwt
    SECRET_KEY = os.environ.get('SECRET_KEY', '')

    try:
        payload = pyjwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return payload.get('user_id')
    except Exception:
        return None


# =============================================================================
# ENHANCED INTERVIEW PLAN FOR TECHNICAL ROLES
# =============================================================================

TECHNICAL_PLAN_SYSTEM_PROMPT = """You are an expert interview designer creating structured screening interviews.
Your task is to analyze a job description and candidate resume, then create an interview plan.

IMPORTANT CONSTRAINTS:
- Focus ONLY on job-related criteria
- Do NOT ask about: age, family status, health, religion, race, citizenship (unless legally required for role)
- Do NOT make hiring decisions - only identify signals for human review
- Only reference facts present in the job description or resume
- If information is missing, flag it as something to probe

For TECHNICAL ROLES (software engineering, data science, etc.), include:
- System design questions (e.g., "How would you design a URL shortener?", "Walk me through designing a real-time chat system")
- Conceptual questions (e.g., "Explain the difference between SQL and NoSQL databases", "What happens when you type a URL in a browser?")
- Problem-solving approach questions (e.g., "How would you approach optimizing a slow database query?", "Walk me through debugging a production issue")
- Algorithm/data structure discussion (verbal, no coding) (e.g., "When would you use a hash table vs a tree?", "Explain how you'd approach finding duplicates in a large dataset")

Keep questions conversational - the candidate will answer verbally, not write code.

Output your response as valid JSON matching the specified schema."""

TECHNICAL_PLAN_USER_PROMPT_TEMPLATE = """Analyze this job and candidate, then create an interview plan.

JOB TITLE: {job_title}

JOB DESCRIPTION:
{job_description}

CANDIDATE RESUME:
{resume_text}

ROLE TYPE: {role_type}

Create an interview plan with:
1. must_have_competencies: Critical skills/experiences required (list of {{id, name, is_must_have: true, description}})
2. nice_to_have_competencies: Preferred but not required (list of {{id, name, is_must_have: false, description}})
3. fit_signals: Top 5 positive indicators to validate (list of strings)
4. risks_to_probe: Top 5 uncertainties or concerns to explore (list of strings)
5. questions: 6-10 interview questions

For TECHNICAL roles ({role_type}), include a mix of:
   - 2 behavioral questions (past experience examples)
   - 2-3 technical questions:
     * 1 system design question (how would you architect/design something)
     * 1 conceptual/knowledge question (explain a concept, tradeoffs)
     * 1 problem-solving approach question (how would you debug/optimize)
   - 1 project deep-dive question (pick something from their resume to explore)
   - 1 situational judgment question
   - 1 logistics/availability question if relevant

For NON-TECHNICAL roles, include:
   - 2 behavioral questions (past experience examples)
   - 2 domain-specific questions (relevant to {role_type})
   - 1 project deep-dive question
   - 1 situational judgment question
   - 1 logistics/availability question if relevant

Each question should have: {{id, question_type, text, competency_ids: [...], probing_guidance}}
question_type can be: behavioral, technical, system_design, conceptual, problem_solving, project_deep_dive, situational, logistics_availability

Return as JSON:
{{
    "job_title": "...",
    "must_have_competencies": [...],
    "nice_to_have_competencies": [...],
    "fit_signals": [...],
    "risks_to_probe": [...],
    "questions": [...]
}}"""


RAPPORT_SYSTEM_PROMPT = """You are a friendly, casual interviewer starting a screening interview.
Your goal is to feel like a natural human conversation, not a formal interview.

Start with a warm, casual greeting using their FIRST NAME ONLY. Be conversational and natural.
Sound like you're chatting with a friend, not conducting a formal interview.

Example openings:
- "Hey Sarah! How's it going today?"
- "Hi Mike! Thanks for taking the time to chat. How are you doing?"
- "Hey! Good to meet you, Alex. How's your day been so far?"

After they respond, acknowledge what they said naturally before moving on.
Keep it brief and genuine - like a real conversation.
"""

RAPPORT_USER_PROMPT_TEMPLATE = """Generate a casual, friendly opening message for this candidate.

CANDIDATE FIRST NAME: {candidate_name}
JOB TITLE: {job_title}

Generate ONLY a warm greeting that:
1. Says "Hey [first name]!" or "Hi [first name]!"
2. Asks how they're doing in a casual way

Keep it under 15 words. Sound like a friendly person, not a robot.
Example: "Hey Noah! How's it going today?"

Just the greeting, nothing else."""


def create_enhanced_interview_plan(
    job_title: str,
    job_description: str,
    resume_text: str,
    role_type: str,
    llm_client: LLMClient
) -> InterviewPlan:
    """Generate an enhanced interview plan with technical questions for tech roles."""

    user_prompt = TECHNICAL_PLAN_USER_PROMPT_TEMPLATE.format(
        job_title=job_title,
        job_description=job_description,
        resume_text=resume_text,
        role_type=role_type or 'other'
    )

    result = llm_client.complete_json(TECHNICAL_PLAN_SYSTEM_PROMPT, user_prompt)

    # Parse into InterviewPlan
    from ai_screening_interview import Competency

    must_have = [
        Competency(
            id=c.get('id', f'mh_{i}'),
            name=c.get('name', ''),
            is_must_have=True,
            description=c.get('description', '')
        )
        for i, c in enumerate(result.get('must_have_competencies', []))
    ]

    nice_to_have = [
        Competency(
            id=c.get('id', f'nth_{i}'),
            name=c.get('name', ''),
            is_must_have=False,
            description=c.get('description', '')
        )
        for i, c in enumerate(result.get('nice_to_have_competencies', []))
    ]

    questions = [
        Question(
            id=q.get('id', f'q_{i}'),
            question_type=q.get('question_type', 'behavioral'),
            text=q.get('text', ''),
            competency_ids=q.get('competency_ids', []),
            probing_guidance=q.get('probing_guidance', '')
        )
        for i, q in enumerate(result.get('questions', []))
    ]

    return InterviewPlan(
        job_title=result.get('job_title', job_title),
        must_have_competencies=must_have,
        nice_to_have_competencies=nice_to_have,
        fit_signals=result.get('fit_signals', []),
        risks_to_probe=result.get('risks_to_probe', []),
        questions=questions
    )


def generate_rapport_message(
    candidate_name: str,
    job_title: str,
    resume_text: str,
    llm_client: LLMClient
) -> str:
    """Generate a personalized rapport-building message."""

    # Use first 1000 chars of resume for context
    resume_excerpt = resume_text[:1000] if resume_text else "No resume available"

    user_prompt = RAPPORT_USER_PROMPT_TEMPLATE.format(
        candidate_name=candidate_name,
        job_title=job_title,
        resume_excerpt=resume_excerpt
    )

    return llm_client.complete(RAPPORT_SYSTEM_PROMPT, user_prompt, max_tokens=200)


# =============================================================================
# INTERVIEW SESSION MANAGER
# =============================================================================

class InterviewSession:
    """Manages a single interview session with enhanced features."""

    def __init__(
        self,
        application_id: int,
        user_id: int,
        job_title: str,
        job_description: str,
        resume_text: str,
        role_type: str,
        candidate_name: str
    ):
        self.application_id = application_id
        self.user_id = user_id
        self.job_title = job_title
        self.job_description = job_description
        self.resume_text = resume_text
        self.role_type = role_type
        self.candidate_name = candidate_name

        # Use gpt-4o-mini for faster responses during interview
        # Lazy load the client when first needed
        self._llm_client = None
        self.state = SessionState.CREATED
        self.plan: Optional[InterviewPlan] = None
        self.responses: list[QuestionResponse] = []
        self.transcript: list[dict] = []
        self.current_question_index = 0
        self.start_time: Optional[datetime] = None
        self.rapport_message: Optional[str] = None

        # Voice support - lazy load to speed up session creation
        self._stt = None
        self._tts = None

    @property
    def llm_client(self):
        if self._llm_client is None:
            from ai_screening_interview import OpenAILLMClient
            self._llm_client = OpenAILLMClient(model="gpt-4o-mini")
        return self._llm_client

    @property
    def stt(self):
        if self._stt is None and STT_AVAILABLE:
            self._stt = WhisperSTT()
        return self._stt

    @property
    def tts(self):
        if self._tts is None and TTS_AVAILABLE:
            self._tts = OpenAITTS()
        return self._tts

    async def initialize(self) -> str:
        """Initialize the interview and generate rapport message.

        This method now returns immediately with the rapport message.
        The interview plan is generated in the background via start_plan_generation().
        """
        self.state = SessionState.RAPPORT
        self.start_time = datetime.now(timezone.utc)

        # Use a fast template-based rapport message instead of LLM call
        # This eliminates one LLM roundtrip and speeds up setup significantly
        first_name = self.candidate_name.split()[0] if self.candidate_name else "there"
        self.rapport_message = f"Hey {first_name}! Thanks for taking the time to interview for the {self.job_title} position. I'm excited to learn more about your background. Just tell me a bit about yourself and what drew you to this role."

        # Log rapport to transcript
        self.transcript.append({
            "speaker": "interviewer",
            "type": "rapport",
            "text": self.rapport_message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        return self.rapport_message

    def start_plan_generation(self):
        """Start generating the interview plan in a background thread.

        Call this after sending the rapport message so it runs while user responds.
        """
        import concurrent.futures
        self._plan_future = concurrent.futures.ThreadPoolExecutor().submit(
            create_enhanced_interview_plan,
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            role_type=self.role_type,
            llm_client=self.llm_client
        )

    async def ensure_plan_ready(self):
        """Wait for the interview plan to be ready if it's still generating."""
        if self.plan:
            return  # Already have plan

        if hasattr(self, '_plan_future') and self._plan_future:
            # Wait for background generation to complete
            loop = asyncio.get_event_loop()
            self.plan = await loop.run_in_executor(None, self._plan_future.result)
            self._plan_future = None

    async def get_current_question(self) -> Optional[Question]:
        """Get the current question to ask."""
        if not self.plan or self.current_question_index >= len(self.plan.questions):
            return None
        return self.plan.questions[self.current_question_index]

    async def process_response(
        self,
        answer: str,
        is_rapport_response: bool = False
    ) -> tuple[Optional[str], bool]:
        """
        Process a candidate's response.

        Returns:
            (follow_up_question, is_interview_complete)
        """
        # Log response to transcript
        response_type = "rapport_response" if is_rapport_response else "answer"
        self.transcript.append({
            "speaker": "candidate",
            "type": response_type,
            "text": answer,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        if is_rapport_response:
            # After rapport, transition to questioning
            self.state = SessionState.QUESTIONING
            return None, False

        if not self.plan:
            raise ValueError("Interview not initialized")

        question = self.plan.questions[self.current_question_index]

        # Check for follow-up
        all_competencies = (
            self.plan.must_have_competencies +
            self.plan.nice_to_have_competencies
        )

        follow_up = generate_follow_up(
            question=question,
            answer=answer,
            job_title=self.job_title,
            competencies=all_competencies,
            llm_client=self.llm_client
        )

        # Store response
        self.responses.append(QuestionResponse(
            question_id=question.id,
            question_text=question.text,
            answer=answer,
            follow_up_question=follow_up,
            follow_up_answer=None
        ))

        if follow_up:
            self.state = SessionState.FOLLOW_UP
            self.transcript.append({
                "speaker": "interviewer",
                "type": "follow_up",
                "text": follow_up,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            return follow_up, False

        # Move to next question
        self.current_question_index += 1

        if self.current_question_index >= len(self.plan.questions):
            self.state = SessionState.WRAP_UP
            return None, True

        self.state = SessionState.QUESTIONING
        return None, False

    async def process_follow_up_response(self, answer: str) -> bool:
        """Process a follow-up response. Returns True if interview is complete."""
        if not self.responses:
            raise ValueError("No question to follow up on")

        # Update last response with follow-up answer
        self.responses[-1].follow_up_answer = answer

        # Log to transcript
        self.transcript.append({
            "speaker": "candidate",
            "type": "follow_up_answer",
            "text": answer,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        # Move to next question
        self.current_question_index += 1

        if self.current_question_index >= len(self.plan.questions):
            self.state = SessionState.WRAP_UP
            return True

        self.state = SessionState.QUESTIONING
        return False

    def generate_evaluation_sync(self) -> dict:
        """Generate the final evaluation and save to database (synchronous version)."""
        self.state = SessionState.EVALUATING

        duration = int((datetime.now(timezone.utc) - self.start_time).total_seconds())

        output = evaluate_interview(
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            plan=self.plan,
            responses=self.responses,
            duration_seconds=duration,
            llm_client=self.llm_client
        )

        evaluation_dict = output.to_dict()

        # Save to database
        save_interview_results(
            application_id=self.application_id,
            transcript=self.transcript,
            evaluation=evaluation_dict,
            status="completed"
        )

        self.state = SessionState.COMPLETE

        return evaluation_dict

    async def generate_evaluation(self) -> dict:
        """Generate the final evaluation (async wrapper)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.generate_evaluation_sync)

    def _is_meaningful_response(self, text: str, min_words: int = 3) -> bool:
        """Check if a response contains meaningful content worth acknowledging."""
        if not text:
            return False

        # Clean and normalize
        cleaned = text.strip().lower()

        # Too short
        words = cleaned.split()
        if len(words) < min_words:
            return False

        # Common non-answers / filler responses
        non_answers = [
            "i don't know", "i'm not sure", "um", "uh", "hmm", "okay", "ok",
            "yeah", "yes", "no", "sure", "right", "i guess", "maybe",
            "nothing", "not much", "i can't think of anything"
        ]
        for phrase in non_answers:
            if cleaned == phrase or cleaned.startswith(phrase + " ") and len(words) < 5:
                return False

        return True

    def generate_acknowledgment(self, user_response: str) -> str:
        """Generate a personalized acknowledgment based on what the user said."""
        # Skip personalized acknowledgment if response isn't meaningful
        if not self._is_meaningful_response(user_response, min_words=5):
            return "Alright, let's dive into a few questions."

        # Use LLM to generate a natural, personalized acknowledgment
        system_prompt = """You are a friendly interviewer. Generate a VERY brief acknowledgment (max 2 short sentences, under 25 words total).
Reference ONE specific thing from their response, then transition to questions. Keep it natural.
Do NOT ask a question. Do NOT use generic phrases like "That's great!" or "Wonderful!"."""

        user_prompt = f"""The candidate said: "{user_response[:300]}"

Generate 1-2 short sentences (under 25 words) that:
1. Reference one specific thing they mentioned
2. Transition to "let's get into some questions"

Example: "Interesting that you worked on recommendation systems. Let's dive into a few questions about your experience." """

        try:
            acknowledgment = self.llm_client.complete(system_prompt, user_prompt, max_tokens=60)
            return acknowledgment.strip()
        except Exception as e:
            print(f"Error generating acknowledgment: {e}")
            return "Got it. Let's dive into a few questions."

    def generate_answer_acknowledgment(self, user_answer: str) -> str:
        """Generate a brief acknowledgment of the candidate's answer before moving to next question."""
        # Skip acknowledgment for short or non-meaningful answers
        if not self._is_meaningful_response(user_answer, min_words=8):
            return None

        # Use LLM to generate a quick, natural acknowledgment
        system_prompt = """Generate ONE short sentence (max 10 words) acknowledging what the candidate said.
Reference one specific thing. No questions. No generic praise like "Great!" or "Wonderful!".
Examples: "That distributed system work sounds challenging." / "Interesting approach to that conflict." """

        user_prompt = f"""Candidate said: "{user_answer[:300]}"

One sentence (max 10 words) referencing something specific:"""

        try:
            ack = self.llm_client.complete(system_prompt, user_prompt, max_tokens=25)
            return ack.strip()
        except Exception as e:
            print(f"Error generating answer acknowledgment: {e}")
            return None  # Skip acknowledgment on error

    def transcribe_audio(self, audio_base64: str) -> str:
        """Transcribe audio to text using Whisper."""
        print(f"[TRANSCRIBE] Starting transcription, STT available: {self.stt is not None}")
        if not self.stt:
            raise ValueError("Speech-to-text not available")

        # Decode base64 audio
        audio_bytes = base64.b64decode(audio_base64)
        print(f"[TRANSCRIBE] Decoded {len(audio_bytes)} bytes of audio")

        # Pass audio bytes directly to transcribe
        result = self.stt.transcribe(audio_bytes)
        print(f"[TRANSCRIBE] Result: '{result}'")
        return result

    def synthesize_speech(self, text: str) -> Optional[str]:
        """Synthesize speech from text, return base64 audio."""
        if not self.tts:
            return None

        try:
            audio_bytes = self.tts.synthesize(text)
            return base64.b64encode(audio_bytes).decode('utf-8')
        except Exception as e:
            print(f"TTS error: {e}")
            return None


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

active_sessions: dict[str, InterviewSession] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    yield


app = FastAPI(
    title="ShortList AI Interview Service",
    description="WebSocket-based API for conducting AI-powered screening interviews",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# REST ENDPOINTS
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "active_sessions": len(active_sessions),
        "stt_available": STT_AVAILABLE,
        "tts_available": TTS_AVAILABLE
    }


@app.post("/interview/prepare/{application_id}")
async def prepare_interview(application_id: int, token: str):
    """Prepare interview session - validate application and return session info."""
    user_id = verify_jwt_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    app_data = get_application_data(application_id, user_id)
    if not app_data:
        raise HTTPException(status_code=404, detail="Application not found")

    if app_data['interview_status'] == 'completed':
        raise HTTPException(status_code=400, detail="Interview already completed")

    return {
        "status": "ready",
        "job_title": app_data['job_title'],
        "company_name": app_data['company_name'],
        "candidate_name": f"{app_data['first_name']} {app_data['last_name']}",
        "websocket_url": f"/ws/interview/{application_id}"
    }


# =============================================================================
# WEBSOCKET ENDPOINT
# =============================================================================

@app.websocket("/ws/interview/{application_id}")
async def interview_websocket(websocket: WebSocket, application_id: int):
    """
    WebSocket endpoint for conducting the interview.

    Protocol:
    1. Client sends: {"type": "auth", "token": "jwt_token"}
    2. Server sends: {"type": "connected", "application_id": ...}
    3. Server sends: {"type": "rapport", "content": "...", "audio_base64": "..."}
    4. Client sends: {"type": "text|audio", "content": "..."}
    5. Server sends: {"type": "question", "content": "...", "question_number": 1, ...}
    6. ... repeat until complete
    7. Server sends: {"type": "complete", "content": "...", "metadata": {...}}
    """
    await websocket.accept()

    session: Optional[InterviewSession] = None

    try:
        # Wait for auth message
        auth_data = await asyncio.wait_for(
            websocket.receive_json(),
            timeout=30
        )

        if auth_data.get('type') != 'auth':
            await websocket.send_json({
                "type": "error",
                "content": "Expected auth message first"
            })
            await websocket.close()
            return

        token = auth_data.get('token')
        user_id = verify_jwt_token(token)
        if not user_id:
            await websocket.send_json({
                "type": "error",
                "content": "Invalid authentication token"
            })
            await websocket.close()
            return

        import time
        start_total = time.time()

        # Get application data first (fast DB query)
        app_data = get_application_data(application_id, user_id)
        if not app_data:
            await websocket.send_json({
                "type": "error",
                "content": "Application not found"
            })
            await websocket.close()
            return

        if app_data['interview_status'] == 'completed':
            await websocket.send_json({
                "type": "error",
                "content": "Interview already completed"
            })
            await websocket.close()
            return

        # Send connection confirmation immediately after validation
        await websocket.send_json({
            "type": "connected",
            "application_id": application_id,
            "job_title": app_data['job_title'],
            "company_name": app_data['company_name']
        })
        print(f"[INTERVIEW TIMING] connected sent: {time.time() - start_total:.2f}s")

        # Send rapport TEXT immediately with template (no LLM call)
        # This gets text on screen ASAP - user can start reading
        first_name = app_data['first_name'].split()[0] if app_data.get('first_name') else "there"
        rapport_message = f"Hey {first_name}! Thanks for taking the time to interview for the {app_data['job_title']} position. I'm excited to learn more about your background. Just tell me a bit about yourself and what drew you to this role."

        await websocket.send_json({
            "type": "rapport",
            "content": rapport_message,
            "audio_base64": None,  # Audio sent separately
            "total_questions": 4  # Estimated
        })
        print(f"[INTERVIEW TIMING] rapport text sent: {time.time() - start_total:.2f}s")

        # Now do slower operations in background while user reads/responds
        loop = asyncio.get_event_loop()

        # Get resume text in background (can be slow for large PDFs)
        resume_text_future = loop.run_in_executor(
            None,
            lambda: get_resume_text(app_data['resume_path']) if app_data['resume_path'] else ""
        )

        # Synthesize rapport audio in background
        async def send_audio():
            try:
                from ai_screening_interview import OpenAITTS
                tts = OpenAITTS() if TTS_AVAILABLE else None
                if tts:
                    audio_data = await loop.run_in_executor(None, tts.synthesize, rapport_message)
                    if audio_data:
                        import base64
                        audio_base64 = base64.b64encode(audio_data).decode('utf-8')
                        await websocket.send_json({
                            "type": "audio",
                            "audio_base64": audio_base64
                        })
                        print(f"[INTERVIEW TIMING] rapport audio sent: {time.time() - start_total:.2f}s")
            except Exception as e:
                print(f"[INTERVIEW] Audio synthesis error: {e}")

        # Start audio synthesis (don't await - it runs in parallel)
        asyncio.create_task(send_audio())

        # Wait for resume text (usually fast)
        resume_text = await resume_text_future
        print(f"[INTERVIEW TIMING] resume loaded: {time.time() - start_total:.2f}s")

        # Create interview session
        session = InterviewSession(
            application_id=application_id,
            user_id=user_id,
            job_title=app_data['job_title'],
            job_description=app_data['job_description'] or "",
            resume_text=resume_text,
            role_type=app_data['role_type'] or 'other',
            candidate_name=f"{app_data['first_name']} {app_data['last_name']}"
        )
        session.rapport_message = rapport_message
        session.state = SessionState.RAPPORT
        session.start_time = datetime.now(timezone.utc)
        session.transcript.append({
            "speaker": "interviewer",
            "type": "rapport",
            "text": rapport_message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        active_sessions[str(application_id)] = session
        print(f"[INTERVIEW TIMING] session created: {time.time() - start_total:.2f}s")

        # Start interview plan generation in background while user responds
        session.start_plan_generation()
        print(f"[INTERVIEW TIMING] plan generation started: {time.time() - start_total:.2f}s")

        # Wait for rapport response (just acknowledgment)
        try:
            data = await asyncio.wait_for(
                websocket.receive_json(),
                timeout=60
            )
            response = CandidateResponse(**data)

            # Transcribe if audio
            if response.type == "audio" and session.stt:
                response.content = session.transcribe_audio(response.content)
                # Send transcription back to frontend
                await websocket.send_json({
                    "type": "transcription",
                    "content": response.content
                })

            await session.process_response(response.content, is_rapport_response=True)

            # Generate personalized acknowledgment based on what they said
            acknowledgment = session.generate_acknowledgment(response.content)
            if acknowledgment:
                ack_audio = session.synthesize_speech(acknowledgment)
                await websocket.send_json({
                    "type": "acknowledgment",
                    "content": acknowledgment,
                    "audio_base64": ack_audio
                })
                # Log to transcript
                session.transcript.append({
                    "speaker": "interviewer",
                    "type": "acknowledgment",
                    "text": acknowledgment,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

        except asyncio.TimeoutError:
            # Continue anyway if they don't respond to rapport
            pass

        # Ensure interview plan is ready before starting questions
        # (it was generating in background while user responded to rapport)
        await session.ensure_plan_ready()

        # Main interview loop
        while True:
            # Check for timeout
            if session.start_time:
                elapsed = (datetime.now(timezone.utc) - session.start_time).total_seconds()
                if elapsed > MAX_INTERVIEW_DURATION_SECONDS:
                    await websocket.send_json({
                        "type": "info",
                        "content": "We've reached the time limit. Let me wrap up the interview."
                    })
                    break

            # Get and send current question
            question = await session.get_current_question()
            if not question:
                break

            # Log question to transcript
            session.transcript.append({
                "speaker": "interviewer",
                "type": "question",
                "text": question.text,
                "question_number": session.current_question_index + 1,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

            # Synthesize question audio
            question_audio = session.synthesize_speech(question.text)

            await websocket.send_json({
                "type": "question",
                "content": question.text,
                "question_number": session.current_question_index + 1,
                "total_questions": len(session.plan.questions),
                "audio_base64": question_audio
            })

            # Wait for response
            try:
                data = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=300  # 5 minute timeout per question
                )
            except asyncio.TimeoutError:
                await websocket.send_json({
                    "type": "info",
                    "content": "Take your time. I'm still here when you're ready."
                })
                continue

            response = CandidateResponse(**data)

            # Send immediate acknowledgment to reduce perceived latency
            await websocket.send_json({
                "type": "processing",
                "content": "Got it, thinking..."
            })

            # Transcribe if audio
            if response.type == "audio" and session.stt:
                response.content = session.transcribe_audio(response.content)
                # Send transcription back to frontend so they can display what user said
                await websocket.send_json({
                    "type": "transcription",
                    "content": response.content
                })

            # Process response
            follow_up, is_complete = await session.process_response(response.content)

            # Generate a brief acknowledgment of their answer before moving on
            answer_ack = session.generate_answer_acknowledgment(response.content)
            if answer_ack:
                ack_audio = session.synthesize_speech(answer_ack)
                await websocket.send_json({
                    "type": "acknowledgment",
                    "content": answer_ack,
                    "audio_base64": ack_audio
                })
                session.transcript.append({
                    "speaker": "interviewer",
                    "type": "acknowledgment",
                    "text": answer_ack,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

            if follow_up:
                # Synthesize follow-up audio
                follow_up_audio = session.synthesize_speech(follow_up)

                await websocket.send_json({
                    "type": "follow_up",
                    "content": follow_up,
                    "question_number": session.current_question_index + 1,
                    "total_questions": len(session.plan.questions),
                    "audio_base64": follow_up_audio
                })

                # Wait for follow-up response
                data = await websocket.receive_json()
                response = CandidateResponse(**data)

                if response.type == "audio" and session.stt:
                    response.content = session.transcribe_audio(response.content)
                    # Send transcription back to frontend
                    await websocket.send_json({
                        "type": "transcription",
                        "content": response.content
                    })

                is_complete = await session.process_follow_up_response(response.content)

            if is_complete:
                break

        # Send completion message IMMEDIATELY - don't wait for evaluation
        wrap_up_message = "Thank you so much for taking the time to interview with us today. We'll be reviewing your responses and the hiring team will be in touch soon regarding next steps. Best of luck!"

        # Send text immediately so user sees it right away
        await websocket.send_json({
            "type": "complete",
            "content": wrap_up_message,
            "audio_base64": None,  # Audio sent separately
            "metadata": {
                "duration_seconds": int((datetime.now(timezone.utc) - session.start_time).total_seconds()),
                "questions_asked": len(session.responses),
                "screening_category": "Evaluating..."  # Will be calculated in background
            }
        })

        # Run evaluation and audio synthesis in background (don't block user)
        loop = asyncio.get_event_loop()

        async def finalize_interview():
            try:
                # Generate evaluation in background thread
                evaluation = await loop.run_in_executor(None, lambda: session.generate_evaluation_sync())
                print(f"[INTERVIEW] Evaluation complete for {application_id}: {evaluation.get('final_screening_category', 'Unknown')}")

                # Now generate scoring and insights
                try:
                    from scoring_engine import calculate_and_store_fit_score
                    from insights_generator import generate_and_store_insights

                    conn = get_db_connection()

                    # Calculate fit score (includes interview performance bucket)
                    score_result = await loop.run_in_executor(
                        None,
                        lambda: calculate_and_store_fit_score(conn, application_id)
                    )
                    if score_result:
                        print(f"[INTERVIEW] Fit score calculated for {application_id}: {score_result.overall_score}% ({score_result.confidence})")

                    # Generate AI insights
                    insights = await loop.run_in_executor(
                        None,
                        lambda: generate_and_store_insights(conn, application_id)
                    )
                    if insights:
                        print(f"[INTERVIEW] Insights generated for {application_id}")
                except Exception as e:
                    print(f"[INTERVIEW] Scoring/insights error for {application_id}: {e}")
                    import traceback
                    traceback.print_exc()

            except Exception as e:
                print(f"[INTERVIEW] Evaluation error: {e}")

        async def send_wrap_up_audio():
            try:
                if session.tts:
                    wrap_up_audio = await loop.run_in_executor(None, session.tts.synthesize, wrap_up_message)
                    if wrap_up_audio:
                        import base64
                        audio_base64 = base64.b64encode(wrap_up_audio).decode('utf-8')
                        await websocket.send_json({
                            "type": "audio",
                            "audio_base64": audio_base64
                        })
            except Exception as e:
                print(f"[INTERVIEW] Wrap-up audio error: {e}")

        # Start both tasks - evaluation saves to DB, audio plays for user
        asyncio.create_task(finalize_interview())
        asyncio.create_task(send_wrap_up_audio())

    except WebSocketDisconnect:
        # User disconnected/exited early - DO NOT save anything
        # The interview should remain in its original state (not started)
        # Only completed interviews should be saved
        print(f"[INTERVIEW] User disconnected from interview {application_id} - no progress saved")

    except Exception as e:
        print(f"Interview error: {e}")
        import traceback
        traceback.print_exc()

        try:
            await websocket.send_json({
                "type": "error",
                "content": f"An error occurred: {str(e)}"
            })
        except:
            pass  # WebSocket might already be closed

        # Don't save error state either - let user retry fresh
        print(f"[INTERVIEW] Error in interview {application_id} - no progress saved")

    finally:
        active_sessions.pop(str(application_id), None)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
