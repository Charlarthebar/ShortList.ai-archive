#!/usr/bin/env python3
"""
AI Screening Interview - FastAPI Backend
-----------------------------------------
WebSocket-based backend for real-time interview sessions with voice support.

Run with:
    uvicorn interview_backend:app --reload --port 8000

Or:
    python interview_backend.py
"""

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import redis.asyncio as redis

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
    Competency,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
SESSION_TTL_SECONDS = 1800  # 30 minutes
MAX_INTERVIEW_DURATION_SECONDS = 900  # 15 minutes hard limit

# =============================================================================
# MODELS
# =============================================================================

class SessionState(str, Enum):
    CREATED = "created"
    INTRO = "intro"
    QUESTIONING = "questioning"
    FOLLOW_UP = "follow_up"
    WRAP_UP = "wrap_up"
    EVALUATING = "evaluating"
    COMPLETE = "complete"
    ERROR = "error"


class CreateSessionRequest(BaseModel):
    job_title: str
    job_description: str
    resume_text: Optional[str] = None
    candidate_name: Optional[str] = None
    candidate_email: Optional[str] = None


class CreateSessionResponse(BaseModel):
    session_id: str
    websocket_url: str
    status: str


class SessionInfo(BaseModel):
    session_id: str
    state: SessionState
    job_title: str
    current_question_index: int
    total_questions: int
    start_time: Optional[str]
    elapsed_seconds: Optional[int]


class InterviewMessage(BaseModel):
    type: str  # question, follow_up, info, error, complete
    content: str
    question_number: Optional[int] = None
    total_questions: Optional[int] = None
    metadata: Optional[dict] = None


class CandidateResponse(BaseModel):
    type: str  # text, audio
    content: str  # text or base64 audio
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


# =============================================================================
# SESSION STORAGE
# =============================================================================

class SessionStorage:
    """Redis-based session storage with fallback to in-memory."""

    def __init__(self):
        self._redis: Optional[redis.Redis] = None
        self._memory_store: dict[str, dict] = {}
        self._use_redis = True

    async def connect(self):
        """Initialize Redis connection."""
        try:
            self._redis = redis.from_url(REDIS_URL, decode_responses=True)
            await self._redis.ping()
            print(f"Connected to Redis at {REDIS_URL}")
        except Exception as e:
            print(f"Redis unavailable ({e}), using in-memory storage")
            self._use_redis = False

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    async def create_session(self, session_id: str, data: dict) -> None:
        """Create a new session."""
        data["created_at"] = datetime.utcnow().isoformat()
        data["last_activity"] = datetime.utcnow().isoformat()

        if self._use_redis and self._redis:
            await self._redis.setex(
                f"session:{session_id}",
                SESSION_TTL_SECONDS,
                json.dumps(data)
            )
        else:
            self._memory_store[session_id] = data

    async def get_session(self, session_id: str) -> Optional[dict]:
        """Retrieve a session."""
        if self._use_redis and self._redis:
            data = await self._redis.get(f"session:{session_id}")
            return json.loads(data) if data else None
        return self._memory_store.get(session_id)

    async def update_session(self, session_id: str, data: dict) -> None:
        """Update a session."""
        data["last_activity"] = datetime.utcnow().isoformat()

        if self._use_redis and self._redis:
            await self._redis.setex(
                f"session:{session_id}",
                SESSION_TTL_SECONDS,
                json.dumps(data)
            )
        else:
            self._memory_store[session_id] = data

    async def delete_session(self, session_id: str) -> None:
        """Delete a session."""
        if self._use_redis and self._redis:
            await self._redis.delete(f"session:{session_id}")
        else:
            self._memory_store.pop(session_id, None)


# =============================================================================
# AUDIT LOGGING
# =============================================================================

class AuditLogger:
    """Structured audit logging for compliance and debugging."""

    def __init__(self, log_file: str = "interview_audit.jsonl"):
        self.log_file = log_file

    def log(self, event_type: str, session_id: str, data: dict) -> None:
        """Log an event."""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "session_id": session_id,
            **data
        }

        # Write to JSONL file
        with open(self.log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def log_question_asked(
        self,
        session_id: str,
        question_id: str,
        question_text: str,
        question_number: int,
        competencies: list[str]
    ) -> None:
        self.log("question_asked", session_id, {
            "question_id": question_id,
            "question_text": question_text,
            "question_number": question_number,
            "competencies_assessed": competencies
        })

    def log_response_received(
        self,
        session_id: str,
        question_id: str,
        response_text: str,
        response_duration_ms: int,
        is_follow_up: bool = False
    ) -> None:
        self.log("response_received", session_id, {
            "question_id": question_id,
            "response_text": response_text,
            "response_duration_ms": response_duration_ms,
            "is_follow_up": is_follow_up
        })

    def log_evaluation_generated(
        self,
        session_id: str,
        final_category: str,
        confidence: str,
        competency_scores: dict
    ) -> None:
        self.log("evaluation_generated", session_id, {
            "final_category": final_category,
            "confidence": confidence,
            "competency_scores": competency_scores
        })


# =============================================================================
# INTERVIEW SESSION MANAGER
# =============================================================================

class InterviewSession:
    """Manages a single interview session."""

    def __init__(
        self,
        session_id: str,
        job_title: str,
        job_description: str,
        resume_text: str,
        storage: SessionStorage,
        audit_logger: AuditLogger
    ):
        self.session_id = session_id
        self.job_title = job_title
        self.job_description = job_description
        self.resume_text = resume_text
        self.storage = storage
        self.audit = audit_logger

        self.llm_client = LLMClient()
        self.state = SessionState.CREATED
        self.plan: Optional[InterviewPlan] = None
        self.responses: list[QuestionResponse] = []
        self.current_question_index = 0
        self.start_time: Optional[datetime] = None
        self.current_question_start: Optional[datetime] = None

    async def initialize(self) -> InterviewPlan:
        """Generate the interview plan."""
        self.state = SessionState.INTRO
        self.start_time = datetime.utcnow()

        # Generate plan (this is CPU-bound, consider running in executor for production)
        self.plan = create_interview_plan(
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            llm_client=self.llm_client
        )

        await self._save_state()
        return self.plan

    async def get_current_question(self) -> Optional[Question]:
        """Get the current question to ask."""
        if not self.plan or self.current_question_index >= len(self.plan.questions):
            return None
        return self.plan.questions[self.current_question_index]

    async def process_response(
        self,
        answer: str,
        response_duration_ms: int
    ) -> tuple[Optional[str], bool]:
        """
        Process a candidate's response.

        Returns:
            (follow_up_question, is_complete)
        """
        if not self.plan:
            raise ValueError("Interview not initialized")

        question = self.plan.questions[self.current_question_index]

        # Log response
        self.audit.log_response_received(
            session_id=self.session_id,
            question_id=question.id,
            response_text=answer,
            response_duration_ms=response_duration_ms
        )

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

        # Store response (we'll add follow-up answer later if needed)
        self.responses.append(QuestionResponse(
            question_id=question.id,
            question_text=question.text,
            answer=answer,
            follow_up_question=follow_up,
            follow_up_answer=None
        ))

        if follow_up:
            self.state = SessionState.FOLLOW_UP
            await self._save_state()
            return follow_up, False

        # Move to next question
        self.current_question_index += 1

        if self.current_question_index >= len(self.plan.questions):
            self.state = SessionState.WRAP_UP
            await self._save_state()
            return None, True

        self.state = SessionState.QUESTIONING
        await self._save_state()
        return None, False

    async def process_follow_up_response(
        self,
        answer: str,
        response_duration_ms: int
    ) -> bool:
        """
        Process a follow-up response.

        Returns:
            is_complete
        """
        if not self.responses:
            raise ValueError("No question to follow up on")

        # Update last response with follow-up answer
        self.responses[-1].follow_up_answer = answer

        # Log response
        self.audit.log_response_received(
            session_id=self.session_id,
            question_id=self.responses[-1].question_id,
            response_text=answer,
            response_duration_ms=response_duration_ms,
            is_follow_up=True
        )

        # Move to next question
        self.current_question_index += 1

        if self.current_question_index >= len(self.plan.questions):
            self.state = SessionState.WRAP_UP
            await self._save_state()
            return True

        self.state = SessionState.QUESTIONING
        await self._save_state()
        return False

    async def generate_evaluation(self) -> ScreeningOutput:
        """Generate the final evaluation."""
        self.state = SessionState.EVALUATING
        await self._save_state()

        duration = int((datetime.utcnow() - self.start_time).total_seconds())

        output = evaluate_interview(
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            plan=self.plan,
            responses=self.responses,
            duration_seconds=duration,
            llm_client=self.llm_client
        )

        # Log evaluation
        self.audit.log_evaluation_generated(
            session_id=self.session_id,
            final_category=output.final_screening_category.value,
            confidence=output.confidence.level.value,
            competency_scores={
                cs.competency_name: cs.score
                for cs in output.competency_scores
            }
        )

        self.state = SessionState.COMPLETE
        await self._save_state()

        return output

    async def _save_state(self) -> None:
        """Save session state to storage."""
        await self.storage.update_session(self.session_id, {
            "state": self.state.value,
            "job_title": self.job_title,
            "current_question_index": self.current_question_index,
            "total_questions": len(self.plan.questions) if self.plan else 0,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "responses_count": len(self.responses)
        })


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

# Global instances
storage = SessionStorage()
audit_logger = AuditLogger()
active_sessions: dict[str, InterviewSession] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    await storage.connect()
    yield
    await storage.close()


app = FastAPI(
    title="AI Screening Interview API",
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

@app.post("/sessions", response_model=CreateSessionResponse)
async def create_session(request: CreateSessionRequest):
    """Create a new interview session."""
    session_id = str(uuid.uuid4())

    # Create session in storage
    await storage.create_session(session_id, {
        "state": SessionState.CREATED.value,
        "job_title": request.job_title,
        "job_description": request.job_description,
        "resume_text": request.resume_text,
        "candidate_name": request.candidate_name,
        "candidate_email": request.candidate_email
    })

    return CreateSessionResponse(
        session_id=session_id,
        websocket_url=f"/ws/interview/{session_id}",
        status="created"
    )


@app.post("/sessions/{session_id}/upload-resume")
async def upload_resume(
    session_id: str,
    file: UploadFile = File(...),
    use_llm_cleanup: bool = Form(default=False)
):
    """Upload a PDF resume for a session."""
    session_data = await storage.get_session(session_id)
    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found")

    # Save uploaded file temporarily
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        # Extract text from PDF
        llm_client = LLMClient() if use_llm_cleanup else None
        resume_text = resume_pdf_to_text(
            tmp_path,
            use_llm_cleanup=use_llm_cleanup,
            llm_client=llm_client
        )

        # Update session
        session_data["resume_text"] = resume_text
        await storage.update_session(session_id, session_data)

        return {
            "status": "success",
            "characters_extracted": len(resume_text)
        }
    finally:
        os.unlink(tmp_path)


@app.get("/sessions/{session_id}", response_model=SessionInfo)
async def get_session(session_id: str):
    """Get session information."""
    session_data = await storage.get_session(session_id)
    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found")

    elapsed = None
    if session_data.get("start_time"):
        start = datetime.fromisoformat(session_data["start_time"])
        elapsed = int((datetime.utcnow() - start).total_seconds())

    return SessionInfo(
        session_id=session_id,
        state=SessionState(session_data["state"]),
        job_title=session_data["job_title"],
        current_question_index=session_data.get("current_question_index", 0),
        total_questions=session_data.get("total_questions", 0),
        start_time=session_data.get("start_time"),
        elapsed_seconds=elapsed
    )


@app.get("/sessions/{session_id}/evaluation")
async def get_evaluation(session_id: str):
    """Get the evaluation results for a completed session."""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found or not active")

    session = active_sessions[session_id]
    if session.state != SessionState.COMPLETE:
        raise HTTPException(status_code=400, detail="Interview not complete")

    # In production, you'd store and retrieve this from the database
    raise HTTPException(status_code=501, detail="Evaluation retrieval not yet implemented")


# =============================================================================
# WEBSOCKET ENDPOINT
# =============================================================================

@app.websocket("/ws/interview/{session_id}")
async def interview_websocket(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint for conducting the interview.

    Protocol:
    1. Server sends: {"type": "connected", "session_id": "..."}
    2. Server sends: {"type": "info", "content": "Welcome message..."}
    3. Server sends: {"type": "question", "content": "...", "question_number": 1, "total_questions": 8}
    4. Client sends: {"type": "text", "content": "candidate response..."}
    5. Server may send: {"type": "follow_up", "content": "follow-up question..."}
    6. ... repeat until complete
    7. Server sends: {"type": "complete", "content": "Thank you...", "metadata": {"evaluation": {...}}}
    """
    await websocket.accept()

    # Retrieve session data
    session_data = await storage.get_session(session_id)
    if not session_data:
        await websocket.send_json({
            "type": "error",
            "content": "Session not found"
        })
        await websocket.close()
        return

    if not session_data.get("resume_text"):
        await websocket.send_json({
            "type": "error",
            "content": "Resume not uploaded. Please upload a resume first."
        })
        await websocket.close()
        return

    # Create interview session
    session = InterviewSession(
        session_id=session_id,
        job_title=session_data["job_title"],
        job_description=session_data["job_description"],
        resume_text=session_data["resume_text"],
        storage=storage,
        audit_logger=audit_logger
    )
    active_sessions[session_id] = session

    try:
        # Send connection confirmation
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id
        })

        # Initialize interview and generate plan
        await websocket.send_json({
            "type": "info",
            "content": f"Welcome! I'll be conducting a screening interview for the {session.job_title} position. Please wait while I prepare the questions..."
        })

        plan = await session.initialize()

        # Send intro
        await websocket.send_json({
            "type": "info",
            "content": f"I have {len(plan.questions)} questions prepared. Please provide clear, specific answers with examples when possible. Let's begin."
        })

        # Interview loop
        response_start_time = None

        while True:
            # Check for timeout
            if session.start_time:
                elapsed = (datetime.utcnow() - session.start_time).total_seconds()
                if elapsed > MAX_INTERVIEW_DURATION_SECONDS:
                    await websocket.send_json({
                        "type": "info",
                        "content": "We've reached the time limit for this interview. Let me wrap up and generate your evaluation."
                    })
                    break

            # Get and send current question
            question = await session.get_current_question()
            if not question:
                break

            # Log question
            audit_logger.log_question_asked(
                session_id=session_id,
                question_id=question.id,
                question_text=question.text,
                question_number=session.current_question_index + 1,
                competencies=question.competency_ids
            )

            await websocket.send_json({
                "type": "question",
                "content": question.text,
                "question_number": session.current_question_index + 1,
                "total_questions": len(plan.questions)
            })

            response_start_time = datetime.utcnow()

            # Wait for response
            try:
                data = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=300  # 5 minute timeout per question
                )
            except asyncio.TimeoutError:
                await websocket.send_json({
                    "type": "info",
                    "content": "I haven't received a response. Would you like to continue?"
                })
                continue

            response = CandidateResponse(**data)
            response_duration_ms = int(
                (datetime.utcnow() - response_start_time).total_seconds() * 1000
            )

            # Handle audio responses (placeholder)
            if response.type == "audio":
                # In production, decode base64 and transcribe
                await websocket.send_json({
                    "type": "error",
                    "content": "Audio responses not yet implemented. Please use text."
                })
                continue

            # Process response
            follow_up, is_complete = await session.process_response(
                answer=response.content,
                response_duration_ms=response_duration_ms
            )

            if follow_up:
                # Send follow-up question
                await websocket.send_json({
                    "type": "follow_up",
                    "content": follow_up,
                    "question_number": session.current_question_index + 1,
                    "total_questions": len(plan.questions)
                })

                response_start_time = datetime.utcnow()

                # Wait for follow-up response
                data = await websocket.receive_json()
                response = CandidateResponse(**data)
                response_duration_ms = int(
                    (datetime.utcnow() - response_start_time).total_seconds() * 1000
                )

                is_complete = await session.process_follow_up_response(
                    answer=response.content,
                    response_duration_ms=response_duration_ms
                )

            if is_complete:
                break

        # Generate evaluation
        await websocket.send_json({
            "type": "info",
            "content": "Thank you for completing the interview. Please wait while I prepare your evaluation..."
        })

        evaluation = await session.generate_evaluation()

        # Send completion with evaluation
        await websocket.send_json({
            "type": "complete",
            "content": "Thank you for your time. A member of our team will be in touch regarding next steps.",
            "metadata": {
                "evaluation": evaluation.to_dict()
            }
        })

    except WebSocketDisconnect:
        audit_logger.log("session_disconnected", session_id, {
            "state": session.state.value,
            "questions_completed": len(session.responses)
        })

    except Exception as e:
        audit_logger.log("session_error", session_id, {
            "error": str(e),
            "state": session.state.value
        })
        await websocket.send_json({
            "type": "error",
            "content": f"An error occurred: {str(e)}"
        })

    finally:
        # Cleanup
        active_sessions.pop(session_id, None)


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "active_sessions": len(active_sessions)
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
