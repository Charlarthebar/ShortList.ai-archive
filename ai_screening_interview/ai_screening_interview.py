#!/usr/bin/env python3
"""
AI Screening Interview MVP
--------------------------
A standalone script that conducts job-relevant screening interviews.
Designed for easy integration with voice interfaces and web APIs.

Usage:
    python ai_screening_interview.py --job job_description.txt --resume resume.txt
    python ai_screening_interview.py --resume-pdf resume.pdf --job job.txt
    python ai_screening_interview.py --interactive  # prompts for input
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, Protocol
from abc import ABC, abstractmethod
import time
import re
import wave
import io
import tempfile
import threading
import random

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Load from parent directory's .env (where the main .env file is)
    parent_env = os.path.join(script_dir, '..', '.env')
    load_dotenv(parent_env)
    # Also try current directory
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars

# =============================================================================
# CONFIGURATION
# =============================================================================

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Model IDs
CLAUDE_MODEL_ID = "claude-sonnet-4-20250514"
OPENAI_MODEL_ID = "gpt-4o"  # Can also use "gpt-4o-mini" for lower cost

MAX_QUESTIONS = 10
MIN_QUESTIONS = 6
MAX_FOLLOW_UPS_PER_QUESTION = 1

# =============================================================================
# DATA MODELS
# =============================================================================

class ScreeningCategory(Enum):
    STRONG_PROCEED = "Strong Proceed"
    PROCEED = "Proceed"
    HOLD = "Hold"
    DO_NOT_PROCEED = "Do Not Proceed"


class ConfidenceLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Severity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class Competency:
    id: str
    name: str
    is_must_have: bool
    description: str = ""


@dataclass
class CompetencyScore:
    competency_id: str
    competency_name: str
    is_must_have: bool
    score: int  # 1-5
    evidence: list[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class RedFlag:
    flag: str
    severity: Severity
    evidence: str


@dataclass
class CandidateSummary:
    name: str
    current_role: str
    years_experience: Optional[int]
    location: str
    key_strengths: list[str]
    key_gaps: list[str]
    one_line_summary: str


@dataclass
class Confidence:
    level: ConfidenceLevel
    reasons: list[str]


@dataclass
class InterviewMetadata:
    interview_date: str
    job_title: str
    questions_asked: int
    follow_ups_asked: int
    duration_seconds: int


@dataclass
class Question:
    id: str
    question_type: str  # behavioral, technical, situational, logistics, project_deep_dive
    text: str
    competency_ids: list[str]
    probing_guidance: str = ""


@dataclass
class QuestionResponse:
    question_id: str
    question_text: str
    answer: str
    follow_up_question: Optional[str] = None
    follow_up_answer: Optional[str] = None


@dataclass
class ScreeningOutput:
    candidate_summary: CandidateSummary
    competency_scores: list[CompetencyScore]
    red_flags: list[RedFlag]
    follow_up_questions_for_human_interviewer: list[str]
    final_screening_category: ScreeningCategory
    confidence: Confidence
    metadata: InterviewMetadata

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dictionary."""
        return {
            "candidate_summary": asdict(self.candidate_summary),
            "competency_scores": [asdict(cs) for cs in self.competency_scores],
            "red_flags": [
                {"flag": rf.flag, "severity": rf.severity.value, "evidence": rf.evidence}
                for rf in self.red_flags
            ],
            "follow_up_questions_for_human_interviewer": self.follow_up_questions_for_human_interviewer,
            "final_screening_category": self.final_screening_category.value,
            "confidence": {
                "level": self.confidence.level.value,
                "reasons": self.confidence.reasons
            },
            "metadata": asdict(self.metadata)
        }


@dataclass
class InterviewPlan:
    job_title: str
    must_have_competencies: list[Competency]
    nice_to_have_competencies: list[Competency]
    fit_signals: list[str]
    risks_to_probe: list[str]
    questions: list[Question]


# =============================================================================
# PDF EXTRACTION MODULE
# =============================================================================

class PDFExtractor(Protocol):
    """Protocol for PDF extraction implementations."""
    def extract(self, pdf_path: str) -> str:
        """Extract text from a PDF file."""
        ...


class PyPDF2Extractor:
    """PDF extraction using PyPDF2."""

    def extract(self, pdf_path: str) -> str:
        try:
            from pypdf import PdfReader
        except ImportError:
            try:
                from PyPDF2 import PdfReader
            except ImportError:
                raise ImportError(
                    "PDF extraction requires pypdf or PyPDF2. "
                    "Install with: pip install pypdf"
                )

        reader = PdfReader(pdf_path)
        text_parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)
        return "\n\n".join(text_parts)


class PdfPlumberExtractor:
    """PDF extraction using pdfplumber (better for complex layouts)."""

    def extract(self, pdf_path: str) -> str:
        try:
            import pdfplumber
        except ImportError:
            raise ImportError(
                "pdfplumber required for this extractor. "
                "Install with: pip install pdfplumber"
            )

        text_parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)
        return "\n\n".join(text_parts)


class LLMPDFExtractor:
    """Use Claude to extract and structure resume content from raw PDF text."""

    def __init__(self, llm_client: "LLMClient"):
        self.llm_client = llm_client
        self.base_extractor = PyPDF2Extractor()

    def extract(self, pdf_path: str) -> str:
        # First get raw text
        raw_text = self.base_extractor.extract(pdf_path)

        # Then use LLM to clean and structure it
        system_prompt = """You are a resume parser. Given raw text extracted from a PDF resume,
clean it up and return a well-structured plain text version. Preserve all information but:
- Fix formatting issues from PDF extraction
- Organize into clear sections (Contact, Summary, Experience, Education, Skills)
- Remove redundant whitespace and artifacts
- Do NOT add any information not present in the original
Return ONLY the cleaned resume text, no commentary."""

        user_prompt = f"Clean and structure this resume text:\n\n{raw_text}"

        return self.llm_client.complete(system_prompt, user_prompt)


def resume_pdf_to_text(
    pdf_path: str,
    extractor: Optional[PDFExtractor] = None,
    use_llm_cleanup: bool = False,
    llm_client: Optional["LLMClient"] = None
) -> str:
    """
    Convert PDF resume to plain text.

    Args:
        pdf_path: Path to the PDF file
        extractor: PDF extractor to use (defaults to PyPDF2Extractor)
        use_llm_cleanup: Whether to use LLM to clean up extracted text
        llm_client: LLM client for cleanup (required if use_llm_cleanup=True)

    Returns:
        Extracted text content
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    if use_llm_cleanup and llm_client:
        extractor = LLMPDFExtractor(llm_client)
    elif extractor is None:
        # Try pdfplumber first (better quality), fall back to pypdf
        try:
            extractor = PdfPlumberExtractor()
            return extractor.extract(pdf_path)
        except ImportError:
            extractor = PyPDF2Extractor()

    return extractor.extract(pdf_path)


# =============================================================================
# SPEECH / VOICE MODULE INTERFACES
# =============================================================================

class SpeechToTextProvider(ABC):
    """Abstract base for speech-to-text providers."""

    @abstractmethod
    def transcribe(self, audio_data: bytes, sample_rate: int = 16000) -> str:
        """Transcribe audio to text."""
        pass

    @abstractmethod
    def transcribe_stream(self, audio_stream, sample_rate: int = 16000):
        """Stream transcription for real-time use."""
        pass


class TextToSpeechProvider(ABC):
    """Abstract base for text-to-speech providers."""

    @abstractmethod
    def synthesize(self, text: str) -> bytes:
        """Convert text to speech audio."""
        pass

    @abstractmethod
    def synthesize_stream(self, text: str):
        """Stream audio synthesis for real-time playback."""
        pass


class WhisperSTT(SpeechToTextProvider):
    """OpenAI Whisper speech-to-text implementation."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")

    def transcribe(self, audio_data: bytes, sample_rate: int = 16000) -> str:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")

        client = OpenAI(api_key=self.api_key)

        # Save audio to temp file (Whisper API requires file)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            f.write(audio_data)
            temp_path = f.name

        try:
            with open(temp_path, "rb") as audio_file:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file
                )
            return transcript.text
        finally:
            os.unlink(temp_path)

    def transcribe_stream(self, audio_stream, sample_rate: int = 16000):
        # Whisper doesn't support streaming natively
        # For streaming, use Deepgram or AssemblyAI
        raise NotImplementedError(
            "Whisper doesn't support streaming. Use DeepgramSTT for real-time transcription."
        )


class DeepgramSTT(SpeechToTextProvider):
    """Deepgram speech-to-text with streaming support."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("DEEPGRAM_API_KEY")

    def transcribe(self, audio_data: bytes, sample_rate: int = 16000) -> str:
        try:
            from deepgram import DeepgramClient, PrerecordedOptions
        except ImportError:
            raise ImportError("deepgram-sdk required. Install with: pip install deepgram-sdk")

        client = DeepgramClient(self.api_key)
        options = PrerecordedOptions(model="nova-2", smart_format=True)
        response = client.listen.prerecorded.v("1").transcribe_file(
            {"buffer": audio_data}, options
        )
        return response.results.channels[0].alternatives[0].transcript

    def transcribe_stream(self, audio_stream, sample_rate: int = 16000):
        """
        For streaming, use Deepgram's live transcription.
        Returns an async generator of partial transcripts.
        """
        raise NotImplementedError(
            "Streaming transcription requires async implementation. "
            "See FastAPI backend for WebSocket-based streaming."
        )


class ElevenLabsTTS(TextToSpeechProvider):
    """ElevenLabs text-to-speech implementation."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        voice_id: str = "21m00Tcm4TlvDq8ikWAM"  # Default: Rachel
    ):
        self.api_key = api_key or os.environ.get("ELEVENLABS_API_KEY")
        self.voice_id = voice_id

    def synthesize(self, text: str) -> bytes:
        try:
            from elevenlabs import generate
        except ImportError:
            raise ImportError("elevenlabs required. Install with: pip install elevenlabs")

        audio = generate(text=text, voice=self.voice_id, api_key=self.api_key)
        return audio

    def synthesize_stream(self, text: str):
        """Stream audio for real-time playback."""
        try:
            from elevenlabs import generate
        except ImportError:
            raise ImportError("elevenlabs required. Install with: pip install elevenlabs")

        return generate(text=text, voice=self.voice_id, api_key=self.api_key, stream=True)


class OpenAITTS(TextToSpeechProvider):
    """OpenAI text-to-speech implementation."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        voice: str = "alloy",
        model: str = "tts-1"
    ):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.voice = voice
        self.model = model

    def synthesize(self, text: str) -> bytes:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")

        client = OpenAI(api_key=self.api_key)
        response = client.audio.speech.create(
            model=self.model,
            voice=self.voice,
            input=text
        )
        return response.content

    def synthesize_stream(self, text: str):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")

        client = OpenAI(api_key=self.api_key)
        response = client.audio.speech.create(
            model=self.model,
            voice=self.voice,
            input=text,
            response_format="opus"
        )
        return response.iter_bytes()


# Convenience functions for simple usage
def speech_to_text(
    audio_data: bytes,
    provider: str = "whisper",
    **kwargs
) -> str:
    """
    Convert speech audio to text.

    Args:
        audio_data: Raw audio bytes (WAV format recommended)
        provider: "whisper" or "deepgram"
        **kwargs: Provider-specific options

    Returns:
        Transcribed text
    """
    providers = {
        "whisper": WhisperSTT,
        "deepgram": DeepgramSTT,
    }

    if provider not in providers:
        raise ValueError(f"Unknown provider: {provider}. Use: {list(providers.keys())}")

    stt = providers[provider](**kwargs)
    return stt.transcribe(audio_data)


def text_to_speech(
    text: str,
    provider: str = "openai",
    **kwargs
) -> bytes:
    """
    Convert text to speech audio.

    Args:
        text: Text to convert to speech
        provider: "openai" or "elevenlabs"
        **kwargs: Provider-specific options

    Returns:
        Audio data as bytes
    """
    providers = {
        "openai": OpenAITTS,
        "elevenlabs": ElevenLabsTTS,
    }

    if provider not in providers:
        raise ValueError(f"Unknown provider: {provider}. Use: {list(providers.keys())}")

    tts = providers[provider](**kwargs)
    return tts.synthesize(text)


# =============================================================================
# INPUT SOURCE INTERFACE
# =============================================================================

class InputSource(ABC):
    """Abstract interface for getting candidate responses."""

    @abstractmethod
    def get_response(self, prompt: str) -> str:
        """Get a response to a prompt."""
        pass

    @abstractmethod
    def display_message(self, message: str) -> None:
        """Display a message to the candidate."""
        pass


class TerminalInput(InputSource):
    """Terminal-based input for MVP."""

    def get_response(self, prompt: str) -> str:
        print(f"\n{prompt}")
        print("-" * 40)
        lines = []
        print("(Enter your response. Type 'DONE' on a new line when finished)")
        while True:
            line = input()
            if line.strip().upper() == "DONE":
                break
            lines.append(line)
        return "\n".join(lines)

    def display_message(self, message: str) -> None:
        print(f"\n{message}")


class VoiceInput(InputSource):
    """Voice-based input using STT/TTS providers with real audio capture/playback."""

    def __init__(
        self,
        stt_provider: SpeechToTextProvider,
        tts_provider: TextToSpeechProvider,
        sample_rate: int = 16000,
        silence_threshold: float = 0.01,
        silence_duration: float = 2.0,
        max_recording_duration: float = 120.0,
        transcript_file: Optional[str] = None
    ):
        self.stt = stt_provider
        self.tts = tts_provider
        self.sample_rate = sample_rate
        self.silence_threshold = silence_threshold
        self.silence_duration = silence_duration
        self.max_recording_duration = max_recording_duration
        self.transcript_file = transcript_file or f"interview_transcript_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.full_transcript: list[dict] = []

        # Initialize transcript file
        with open(self.transcript_file, 'w', encoding='utf-8') as f:
            f.write(f"Interview Transcript - {datetime.now().isoformat()}\n")
            f.write("=" * 60 + "\n\n")

    def _append_to_transcript(self, speaker: str, text: str):
        """Append a line to the transcript file."""
        entry = {"speaker": speaker, "text": text, "timestamp": datetime.now().isoformat()}
        self.full_transcript.append(entry)
        with open(self.transcript_file, 'a', encoding='utf-8') as f:
            f.write(f"[{speaker}]: {text}\n\n")

    def _record_audio(self) -> bytes:
        """Record audio from microphone until Enter is pressed."""
        try:
            import sounddevice as sd
            import numpy as np
        except ImportError:
            raise ImportError(
                "sounddevice and numpy required for voice mode. "
                "Install with: pip install sounddevice numpy"
            )

        print("\n🎤 Recording... (Press ENTER when you're done speaking)")

        recording = []
        is_recording = True

        # Thread to detect Enter key press
        enter_pressed = threading.Event()
        def wait_for_enter():
            input()
            enter_pressed.set()

        enter_thread = threading.Thread(target=wait_for_enter, daemon=True)
        enter_thread.start()

        def audio_callback(indata, frames, time_info, status):
            if status:
                print(f"Audio status: {status}")
            recording.append(indata.copy())

        # Start recording
        with sd.InputStream(
            samplerate=self.sample_rate,
            channels=1,
            dtype='float32',
            callback=audio_callback,
            blocksize=1024
        ):
            start_time = time.time()
            while is_recording:
                if enter_pressed.is_set():
                    break
                if time.time() - start_time > self.max_recording_duration:
                    print("(Max recording duration reached)")
                    break
                sd.sleep(100)

        print("✓ Recording stopped")

        if not recording:
            return b''

        # Convert to WAV format
        audio_data = np.concatenate(recording, axis=0)
        audio_data = (audio_data * 32767).astype(np.int16)

        # Create WAV file in memory
        wav_buffer = io.BytesIO()
        with wave.open(wav_buffer, 'wb') as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)  # 16-bit
            wav_file.setframerate(self.sample_rate)
            wav_file.writeframes(audio_data.tobytes())

        return wav_buffer.getvalue()

    def _play_audio(self, audio_data: bytes):
        """Play audio data (supports MP3 from OpenAI TTS)."""
        if not audio_data:
            print("    [No audio data to play]")
            return

        # Fallback: save to temp file and use macOS afplay (most reliable)
        with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as f:
            f.write(audio_data)
            temp_path = f.name

        try:
            import subprocess
            import platform
            if platform.system() == 'Darwin':  # macOS
                subprocess.run(['afplay', temp_path], check=True)
            elif platform.system() == 'Windows':
                os.startfile(temp_path)
                time.sleep(5)  # Wait for playback
            else:  # Linux
                subprocess.run(['aplay', temp_path], check=True)
        except Exception as e:
            print(f"    [Playback error: {e}]")
        finally:
            try:
                os.unlink(temp_path)
            except:
                pass

    def get_response(self, prompt: str) -> str:
        """Speak the prompt and capture voice response."""
        # Only speak if there's something to say
        if prompt and prompt.strip():
            print(f"\n{prompt}")
            print("-" * 40)
            print("🔊 Speaking question...")
            try:
                audio = self.tts.synthesize(prompt)
                self._play_audio(audio)
            except Exception as e:
                print(f"  [Speech error: {e}]")

        # Record response
        audio_response = self._record_audio()

        if not audio_response:
            return "(No audio captured)"

        # Transcribe
        print("📝 Transcribing response...")
        transcript = self.stt.transcribe(audio_response)

        # Log to transcript file
        self._append_to_transcript("Interviewer", prompt)
        self._append_to_transcript("Candidate", transcript)

        print(f"\n[Transcribed]: {transcript}")
        return transcript

    def display_message(self, message: str) -> None:
        """Display and speak a message."""
        print(f"\n{message}")

        # Speak the message
        try:
            print("  [Generating speech...]")
            audio = self.tts.synthesize(message)
            print("  [Playing audio...]")
            self._play_audio(audio)
            print("  [Done]")
        except Exception as e:
            print(f"  [Speech error: {e} - continuing without audio]")

        # Log to transcript
        self._append_to_transcript("Interviewer", message)

    def save_transcript(self, output_path: Optional[str] = None) -> str:
        """Save the full transcript to a JSON file."""
        path = output_path or self.transcript_file.replace('.txt', '.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({
                "transcript": self.full_transcript,
                "recorded_at": datetime.now().isoformat()
            }, f, indent=2)
        return path


# =============================================================================
# LLM INTEGRATION
# =============================================================================

class LLMClient(ABC):
    """Abstract base for LLM clients."""

    @abstractmethod
    def complete(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> str:
        """Get a completion from the LLM."""
        pass

    def complete_json(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> dict:
        """Get a JSON completion from the LLM."""
        response_text = self.complete(system_prompt, user_prompt, max_tokens)
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in response_text:
            start = response_text.find("```json") + 7
            end = response_text.find("```", start)
            response_text = response_text[start:end].strip()
        elif "```" in response_text:
            start = response_text.find("```") + 3
            end = response_text.find("```", start)
            response_text = response_text[start:end].strip()
        return json.loads(response_text)


class AnthropicLLMClient(LLMClient):
    """Client for interacting with Claude API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or ANTHROPIC_API_KEY
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "anthropic package required. Install with: pip install anthropic"
                )
        return self._client

    def complete(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> str:
        """Get a completion from Claude."""
        response = self.client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        return response.content[0].text


class OpenAILLMClient(LLMClient):
    """Client for interacting with OpenAI API."""

    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or OPENAI_API_KEY
        self.model = model or OPENAI_MODEL_ID
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "openai package required. Install with: pip install openai"
                )
        return self._client

    def complete(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> str:
        """Get a completion from OpenAI."""
        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return response.choices[0].message.content

    def complete_json(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> dict:
        """Get a JSON completion from OpenAI with JSON mode."""
        try:
            # Try using JSON mode for more reliable parsing
            response = self.client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt + "\n\nRespond with valid JSON only."},
                    {"role": "user", "content": user_prompt}
                ]
            )
            return json.loads(response.choices[0].message.content)
        except Exception:
            # Fall back to regular completion with JSON extraction
            return super().complete_json(system_prompt, user_prompt, max_tokens)


def get_llm_client(provider: str = "auto", api_key: str = None) -> LLMClient:
    """
    Get an LLM client based on provider preference.

    Args:
        provider: "openai", "anthropic", or "auto" (auto-detect based on available keys)
        api_key: Optional API key override

    Returns:
        Configured LLM client
    """
    if provider == "auto":
        # Prefer OpenAI if key is available (single key for everything)
        if OPENAI_API_KEY or api_key:
            return OpenAILLMClient(api_key=api_key)
        elif ANTHROPIC_API_KEY:
            return AnthropicLLMClient(api_key=api_key)
        else:
            raise ValueError(
                "No API key found. Set OPENAI_API_KEY or ANTHROPIC_API_KEY environment variable."
            )
    elif provider == "openai":
        return OpenAILLMClient(api_key=api_key or OPENAI_API_KEY)
    elif provider == "anthropic":
        return AnthropicLLMClient(api_key=api_key or ANTHROPIC_API_KEY)
    else:
        raise ValueError(f"Unknown provider: {provider}. Use 'openai', 'anthropic', or 'auto'")


# =============================================================================
# INTERVIEW PLANNING
# =============================================================================

PLAN_SYSTEM_PROMPT = """You are an expert interview designer creating structured screening interviews.
Your task is to analyze a job description and candidate resume, then create an interview plan.

IMPORTANT CONSTRAINTS:
- Focus ONLY on job-related criteria
- Do NOT ask about: age, family status, health, religion, race, citizenship (unless legally required for role)
- Do NOT make hiring decisions - only identify signals for human review
- Only reference facts present in the job description or resume
- If information is missing, flag it as something to probe

Output your response as valid JSON matching the specified schema."""

PLAN_USER_PROMPT_TEMPLATE = """Analyze this job and candidate, then create an interview plan.

JOB TITLE: {job_title}

JOB DESCRIPTION:
{job_description}

CANDIDATE RESUME:
{resume_text}

Create an interview plan with:
1. must_have_competencies: Critical skills/experiences required (list of {{id, name, is_must_have: true, description}})
2. nice_to_have_competencies: Preferred but not required (list of {{id, name, is_must_have: false, description}})
3. fit_signals: Top 5 positive indicators to validate (list of strings)
4. risks_to_probe: Top 5 uncertainties or concerns to explore (list of strings)
5. questions: 6-10 interview questions, including:
   - 2 behavioral questions (past experience examples)
   - 2 technical/domain questions (if applicable to role)
   - 1 project deep-dive question
   - 1 situational judgment question
   - 1 logistics/availability question
   - 1+ additional as needed to cover key competencies

   Each question should have: {{id, question_type, text, competency_ids: [...], probing_guidance}}

Return as JSON:
{{
    "job_title": "...",
    "must_have_competencies": [...],
    "nice_to_have_competencies": [...],
    "fit_signals": [...],
    "risks_to_probe": [...],
    "questions": [...]
}}"""


def create_interview_plan(
    job_title: str,
    job_description: str,
    resume_text: str,
    llm_client: LLMClient
) -> InterviewPlan:
    """Generate an interview plan based on job and candidate."""

    user_prompt = PLAN_USER_PROMPT_TEMPLATE.format(
        job_title=job_title,
        job_description=job_description,
        resume_text=resume_text
    )

    plan_dict = llm_client.complete_json(PLAN_SYSTEM_PROMPT, user_prompt)

    # Convert to InterviewPlan dataclass
    must_haves = [
        Competency(
            id=c["id"],
            name=c["name"],
            is_must_have=True,
            description=c.get("description", "")
        )
        for c in plan_dict["must_have_competencies"]
    ]

    nice_to_haves = [
        Competency(
            id=c["id"],
            name=c["name"],
            is_must_have=False,
            description=c.get("description", "")
        )
        for c in plan_dict["nice_to_have_competencies"]
    ]

    questions = [
        Question(
            id=q["id"],
            question_type=q["question_type"],
            text=q["text"],
            competency_ids=q["competency_ids"],
            probing_guidance=q.get("probing_guidance", "")
        )
        for q in plan_dict["questions"]
    ]

    return InterviewPlan(
        job_title=plan_dict["job_title"],
        must_have_competencies=must_haves,
        nice_to_have_competencies=nice_to_haves,
        fit_signals=plan_dict["fit_signals"],
        risks_to_probe=plan_dict["risks_to_probe"],
        questions=questions
    )


# =============================================================================
# FOLLOW-UP GENERATION
# =============================================================================

FOLLOW_UP_SYSTEM_PROMPT = """You are an expert interviewer conducting a screening interview.
Based on the candidate's response, determine if a follow-up question is needed.

A follow-up is needed when:
- The answer is vague and lacks specific examples
- The candidate claims experience not shown on their resume
- Important details are missing for evaluation
- The answer doesn't directly address the question

A follow-up is NOT needed when:
- The answer is clear, specific, and complete
- The candidate provided concrete examples with context
- Sufficient information exists to evaluate the competency

IMPORTANT: Keep follow-ups concise and focused. Do not ask leading questions.
Only reference information from the job description and resume - do not make assumptions."""

FOLLOW_UP_USER_PROMPT_TEMPLATE = """CONTEXT:
Job Title: {job_title}
Competencies being assessed: {competencies}
Probing guidance: {probing_guidance}

ORIGINAL QUESTION:
{question}

CANDIDATE'S ANSWER:
{answer}

Based on this answer, should we ask a follow-up question?

Respond with JSON:
{{
    "needs_follow_up": true/false,
    "reason": "Brief explanation of why follow-up is or isn't needed",
    "follow_up_question": "The follow-up question if needed, otherwise null"
}}"""


def generate_follow_up(
    question: Question,
    answer: str,
    job_title: str,
    competencies: list[Competency],
    llm_client: LLMClient
) -> Optional[str]:
    """Determine if a follow-up question is needed and generate it."""

    competency_names = [c.name for c in competencies if c.id in question.competency_ids]

    user_prompt = FOLLOW_UP_USER_PROMPT_TEMPLATE.format(
        job_title=job_title,
        competencies=", ".join(competency_names),
        probing_guidance=question.probing_guidance,
        question=question.text,
        answer=answer
    )

    result = llm_client.complete_json(FOLLOW_UP_SYSTEM_PROMPT, user_prompt)

    if result.get("needs_follow_up") and result.get("follow_up_question"):
        return result["follow_up_question"]
    return None


# =============================================================================
# EVALUATION
# =============================================================================

EVALUATION_SYSTEM_PROMPT = """You are an expert at evaluating screening interview responses.
Your task is to analyze the complete interview and produce a structured evaluation.

SCORING RUBRIC (1-5 scale):
5 = Exceeds: Clear, specific evidence of strong capability; multiple examples; depth demonstrated
4 = Meets: Solid evidence of capability; at least one concrete example with detail
3 = Partial: Some relevant experience but limited depth, or transferable skills only
2 = Weak: Minimal evidence; vague responses; significant gaps
1 = None: No evidence; candidate could not address; or disqualifying response

SCREENING CATEGORIES:
- Strong Proceed: All must-haves ≥ 4; no high-severity red flags; strong motivation fit
- Proceed: All must-haves ≥ 3; no more than 1 high-severity red flag; reasonable fit
- Hold: 1-2 must-haves scored 2; OR 2+ medium red flags; needs human review
- Do Not Proceed: Any must-have scored 1; OR 2+ high-severity red flags; OR disqualifying logistics

IMPORTANT CONSTRAINTS:
- Base scores ONLY on evidence from the interview responses and resume
- Flag uncertainties rather than making assumptions
- Do NOT make inferences about protected characteristics
- Provide specific evidence excerpts for each score
- Generate follow-up questions for areas a human interviewer should explore"""

EVALUATION_USER_PROMPT_TEMPLATE = """Evaluate this screening interview.

JOB TITLE: {job_title}

JOB DESCRIPTION:
{job_description}

CANDIDATE RESUME:
{resume_text}

COMPETENCIES TO EVALUATE:
Must-Have:
{must_have_competencies}

Nice-to-Have:
{nice_to_have_competencies}

INTERVIEW TRANSCRIPT:
{interview_transcript}

Produce a complete evaluation as JSON:
{{
    "candidate_summary": {{
        "name": "Candidate name from resume",
        "current_role": "Current/most recent role",
        "years_experience": number or null,
        "location": "Location from resume",
        "key_strengths": ["strength1", "strength2", ...],
        "key_gaps": ["gap1", "gap2", ...],
        "one_line_summary": "Brief assessment summary"
    }},
    "competency_scores": [
        {{
            "competency_id": "ID",
            "competency_name": "Name",
            "is_must_have": true/false,
            "score": 1-5,
            "evidence": ["Direct quote or observation 1", "..."],
            "notes": "Additional context"
        }}
    ],
    "red_flags": [
        {{
            "flag": "Description of concern",
            "severity": "low|medium|high",
            "evidence": "Supporting evidence"
        }}
    ],
    "follow_up_questions_for_human_interviewer": [
        "Question 1 for human to explore",
        "Question 2..."
    ],
    "final_screening_category": "Strong Proceed|Proceed|Hold|Do Not Proceed",
    "confidence": {{
        "level": "low|medium|high",
        "reasons": ["Reason for confidence level"]
    }}
}}"""


def format_transcript(responses: list[QuestionResponse]) -> str:
    """Format interview responses into a readable transcript."""
    transcript_parts = []
    for i, r in enumerate(responses, 1):
        transcript_parts.append(f"Q{i}: {r.question_text}")
        transcript_parts.append(f"A{i}: {r.answer}")
        if r.follow_up_question:
            transcript_parts.append(f"Follow-up Q{i}: {r.follow_up_question}")
            transcript_parts.append(f"Follow-up A{i}: {r.follow_up_answer}")
        transcript_parts.append("")
    return "\n".join(transcript_parts)


def format_competencies(competencies: list[Competency]) -> str:
    """Format competencies for the prompt."""
    return "\n".join(
        f"- {c.id}: {c.name} - {c.description}"
        for c in competencies
    )


def evaluate_interview(
    job_title: str,
    job_description: str,
    resume_text: str,
    plan: InterviewPlan,
    responses: list[QuestionResponse],
    duration_seconds: int,
    llm_client: LLMClient
) -> ScreeningOutput:
    """Evaluate the complete interview and produce screening output."""

    transcript = format_transcript(responses)

    user_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(
        job_title=job_title,
        job_description=job_description,
        resume_text=resume_text,
        must_have_competencies=format_competencies(plan.must_have_competencies),
        nice_to_have_competencies=format_competencies(plan.nice_to_have_competencies),
        interview_transcript=transcript
    )

    eval_dict = llm_client.complete_json(EVALUATION_SYSTEM_PROMPT, user_prompt)

    # Convert to ScreeningOutput
    summary = CandidateSummary(**eval_dict["candidate_summary"])

    competency_scores = [
        CompetencyScore(
            competency_id=cs["competency_id"],
            competency_name=cs["competency_name"],
            is_must_have=cs["is_must_have"],
            score=cs["score"],
            evidence=cs["evidence"],
            notes=cs.get("notes", "")
        )
        for cs in eval_dict["competency_scores"]
    ]

    red_flags = [
        RedFlag(
            flag=rf["flag"],
            severity=Severity(rf["severity"]),
            evidence=rf["evidence"]
        )
        for rf in eval_dict["red_flags"]
    ]

    category_map = {
        "Strong Proceed": ScreeningCategory.STRONG_PROCEED,
        "Proceed": ScreeningCategory.PROCEED,
        "Hold": ScreeningCategory.HOLD,
        "Do Not Proceed": ScreeningCategory.DO_NOT_PROCEED
    }

    follow_ups_count = sum(1 for r in responses if r.follow_up_question)

    return ScreeningOutput(
        candidate_summary=summary,
        competency_scores=competency_scores,
        red_flags=red_flags,
        follow_up_questions_for_human_interviewer=eval_dict["follow_up_questions_for_human_interviewer"],
        final_screening_category=category_map[eval_dict["final_screening_category"]],
        confidence=Confidence(
            level=ConfidenceLevel(eval_dict["confidence"]["level"]),
            reasons=eval_dict["confidence"]["reasons"]
        ),
        metadata=InterviewMetadata(
            interview_date=datetime.now().isoformat(),
            job_title=job_title,
            questions_asked=len(responses),
            follow_ups_asked=follow_ups_count,
            duration_seconds=duration_seconds
        )
    )


# =============================================================================
# INTERVIEW STATE MACHINE
# =============================================================================

class InterviewState(Enum):
    INTRO = "intro"
    QUESTIONS = "questions"
    FOLLOW_UP = "follow_up"
    WRAP_UP = "wrap_up"
    COMPLETE = "complete"


class AIInterviewer:
    """Main interview orchestrator implementing the state machine."""

    def __init__(
        self,
        job_title: str,
        job_description: str,
        resume_text: str,
        input_source: InputSource = None,
        llm_client: LLMClient = None
    ):
        self.job_title = job_title
        self.job_description = job_description
        self.resume_text = resume_text
        self.input_source = input_source or TerminalInput()
        self.llm_client = llm_client or get_llm_client("auto")

        self.state = InterviewState.INTRO
        self.plan: Optional[InterviewPlan] = None
        self.responses: list[QuestionResponse] = []
        self.current_question_index = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def _generate_conversational_response(
        self,
        context: str,
        candidate_said: str,
        next_action: str = ""
    ) -> str:
        """Generate a natural, conversational response based on what the candidate said."""
        system_prompt = """You are a friendly, professional interviewer having a natural conversation.
Generate a brief, warm response to what the candidate just said. Keep it natural and human-like.

Guidelines:
- Be empathetic and responsive to their tone and content
- Keep responses concise (1-2 sentences max)
- Sound like a real person, not a robot
- If they seem nervous, be reassuring
- If they're enthusiastic, match their energy
- Never be condescending or overly formal
- Don't use filler words excessively
- If transitioning to a question, make it flow naturally"""

        user_prompt = f"""Context: {context}
Candidate said: "{candidate_said}"
{f"Next: {next_action}" if next_action else ""}

Generate a natural response (1-2 sentences only):"""

        try:
            response = self.llm_client.complete(system_prompt, user_prompt, max_tokens=100)
            return response.strip().strip('"')
        except Exception as e:
            # Fallback to generic response
            return "Thanks for sharing that."

    def run(self) -> ScreeningOutput:
        """Run the complete interview flow."""
        self.start_time = time.time()

        while self.state != InterviewState.COMPLETE:
            if self.state == InterviewState.INTRO:
                self._handle_intro()
            elif self.state == InterviewState.QUESTIONS:
                self._handle_questions()
            elif self.state == InterviewState.WRAP_UP:
                self._handle_wrap_up()

        self.end_time = time.time()
        duration = int(self.end_time - self.start_time)

        # Generate evaluation (only show in terminal, don't speak)
        print("\n[Generating evaluation...]")

        output = evaluate_interview(
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            plan=self.plan,
            responses=self.responses,
            duration_seconds=duration,
            llm_client=self.llm_client
        )

        return output

    def _handle_intro(self):
        """Display introduction and generate interview plan."""
        # Print header to terminal (not spoken)
        print("\n" + "=" * 60)
        print(f"AI SCREENING INTERVIEW - {self.job_title}")
        print("=" * 60 + "\n")

        # Warm, natural greeting
        self.input_source.display_message(
            f"Hi! Thanks so much for taking the time to chat with me today. How are you doing?"
        )

        # Get their response to the greeting
        greeting_response = self.input_source.get_response("")

        # Generate interview plan in background
        print("  [Preparing questions based on the role...]")
        self.plan = create_interview_plan(
            job_title=self.job_title,
            job_description=self.job_description,
            resume_text=self.resume_text,
            llm_client=self.llm_client
        )

        # Generate dynamic response to their greeting
        response = self._generate_conversational_response(
            context="Starting a job interview, just asked how they're doing",
            candidate_said=greeting_response,
            next_action=f"Transitioning to introduce the {self.job_title} interview"
        )
        self.input_source.display_message(response)

        # Transition to the interview
        self.input_source.display_message(
            f"So, I'll be chatting with you about the {self.job_title} role today. "
            f"Just relax and tell me about your experiences - there's no right or wrong answers. Ready?"
        )

        # Get their ready response
        ready_response = self.input_source.get_response("")

        # Dynamic response to their readiness
        ready_ack = self._generate_conversational_response(
            context="Asked if they're ready to start the interview",
            candidate_said=ready_response,
            next_action="About to ask the first interview question"
        )
        self.input_source.display_message(ready_ack)

        self.state = InterviewState.QUESTIONS

    def _handle_questions(self):
        """Ask questions and collect responses."""
        all_competencies = (
            self.plan.must_have_competencies +
            self.plan.nice_to_have_competencies
        )

        while self.current_question_index < len(self.plan.questions):
            question = self.plan.questions[self.current_question_index]

            # For terminal display, show progress quietly
            q_num = self.current_question_index + 1
            total = len(self.plan.questions)
            print(f"\n[Question {q_num}/{total}]")

            # Just ask the question naturally (no numbering in speech)
            answer = self.input_source.get_response(question.text)

            # Check if follow-up is needed
            follow_up_q = None
            follow_up_a = None

            follow_up_q = generate_follow_up(
                question=question,
                answer=answer,
                job_title=self.job_title,
                competencies=all_competencies,
                llm_client=self.llm_client
            )

            if follow_up_q:
                # Generate a brief acknowledgment before the follow-up
                follow_up_intro = self._generate_conversational_response(
                    context=f"Asked: {question.text}",
                    candidate_said=answer,
                    next_action="Acknowledge briefly, then will ask a follow-up question"
                )
                # Speak acknowledgment first, then ask the follow-up
                self.input_source.display_message(follow_up_intro)
                follow_up_a = self.input_source.get_response(follow_up_q)

            # Record response
            self.responses.append(QuestionResponse(
                question_id=question.id,
                question_text=question.text,
                answer=answer,
                follow_up_question=follow_up_q,
                follow_up_answer=follow_up_a
            ))

            self.current_question_index += 1

            # Add a natural, dynamic transition if not the last question
            if self.current_question_index < len(self.plan.questions):
                last_answer = follow_up_a if follow_up_a else answer
                next_question = self.plan.questions[self.current_question_index]

                transition = self._generate_conversational_response(
                    context=f"In a job interview, candidate just answered a question about {question.question_type}",
                    candidate_said=last_answer,
                    next_action=f"Moving on to ask about: {next_question.text[:50]}..."
                )
                self.input_source.display_message(transition)

        self.state = InterviewState.WRAP_UP

    def _handle_wrap_up(self):
        """Wrap up the interview."""
        # Get the last answer to respond to naturally
        last_response = self.responses[-1] if self.responses else None
        last_answer = ""
        if last_response:
            last_answer = last_response.follow_up_answer or last_response.answer

        # Generate dynamic closing based on how the interview went
        closing = self._generate_conversational_response(
            context=f"Finishing a job interview for {self.job_title}. Asked {len(self.responses)} questions.",
            candidate_said=last_answer,
            next_action="Wrapping up the interview and asking if they have questions"
        )
        self.input_source.display_message(closing)

        self.input_source.display_message(
            "Before we wrap up, do you have any questions for me about the role or the process?"
        )

        # Allow candidate to ask questions
        candidate_questions = self.input_source.get_response("")

        # Respond dynamically to their questions
        if candidate_questions.strip() and len(candidate_questions) > 5:
            question_response = self._generate_conversational_response(
                context="End of interview, candidate asked a question about the role or process",
                candidate_said=candidate_questions,
                next_action="Acknowledge their question and let them know the hiring team will follow up"
            )
            self.input_source.display_message(question_response)

        # Generate a warm, personalized goodbye
        goodbye = self._generate_conversational_response(
            context=f"Ending the interview for {self.job_title}",
            candidate_said=candidate_questions if candidate_questions.strip() else "No questions",
            next_action="Say goodbye warmly and mention next steps"
        )
        self.input_source.display_message(goodbye)

        self.state = InterviewState.COMPLETE


# =============================================================================
# CLI INTERFACE
# =============================================================================

def load_file(path: str) -> str:
    """Load text content from a file."""
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def save_output(output: ScreeningOutput, path: str):
    """Save screening output to a JSON file."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output.to_dict(), f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="AI Screening Interview MVP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ai_screening_interview.py --job job.txt --resume resume.txt
  python ai_screening_interview.py --job job.txt --resume-pdf resume.pdf
  python ai_screening_interview.py --interactive
  python ai_screening_interview.py --job job.txt --resume resume.txt --output results.json
        """
    )

    parser.add_argument(
        "--job", "-j",
        help="Path to job description text file"
    )
    parser.add_argument(
        "--resume", "-r",
        help="Path to resume text file"
    )
    parser.add_argument(
        "--resume-pdf",
        help="Path to resume PDF file"
    )
    parser.add_argument(
        "--job-text",
        help="Job description as inline text"
    )
    parser.add_argument(
        "--resume-text",
        help="Resume as inline text"
    )
    parser.add_argument(
        "--title", "-t",
        help="Job title (extracted from description if not provided)"
    )
    parser.add_argument(
        "--output", "-o",
        default="screening_output.json",
        help="Output file path (default: screening_output.json)"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Interactive mode - prompts for all inputs"
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Only generate and display the interview plan, don't run interview"
    )
    parser.add_argument(
        "--llm-pdf-cleanup",
        action="store_true",
        help="Use LLM to clean up PDF extraction (higher quality, uses API)"
    )
    parser.add_argument(
        "--voice",
        action="store_true",
        help="Enable voice mode - speak questions and record spoken answers"
    )
    parser.add_argument(
        "--tts-provider",
        choices=["openai", "elevenlabs"],
        default="openai",
        help="Text-to-speech provider (default: openai)"
    )
    parser.add_argument(
        "--stt-provider",
        choices=["whisper", "deepgram"],
        default="whisper",
        help="Speech-to-text provider (default: whisper)"
    )
    parser.add_argument(
        "--tts-voice",
        default="alloy",
        help="Voice ID for TTS (default: alloy for OpenAI)"
    )
    parser.add_argument(
        "--transcript",
        help="Path for transcript file (default: auto-generated with timestamp)"
    )
    parser.add_argument(
        "--questions-only",
        action="store_true",
        help="Only generate and output questions, don't conduct interview"
    )
    parser.add_argument(
        "--llm-provider",
        choices=["openai", "anthropic", "auto"],
        default="auto",
        help="LLM provider for question generation and evaluation (default: auto - prefers OpenAI)"
    )
    parser.add_argument(
        "--openai-model",
        default="gpt-4o",
        help="OpenAI model to use (default: gpt-4o, can also use gpt-4o-mini for lower cost)"
    )

    args = parser.parse_args()

    # Collect inputs
    if args.interactive:
        print("=" * 60)
        print("AI SCREENING INTERVIEW - SETUP")
        print("=" * 60)

        job_title = input("\nJob Title: ").strip()

        print("\nJob Description (paste text, then type 'END' on a new line):")
        jd_lines = []
        while True:
            line = input()
            if line.strip().upper() == "END":
                break
            jd_lines.append(line)
        job_description = "\n".join(jd_lines)

        print("\nCandidate Resume (paste text, then type 'END' on a new line):")
        resume_lines = []
        while True:
            line = input()
            if line.strip().upper() == "END":
                break
            resume_lines.append(line)
        resume_text = "\n".join(resume_lines)
    else:
        # Load from files or inline text
        if args.job:
            job_description = load_file(args.job)
        elif args.job_text:
            job_description = args.job_text
        else:
            print("Error: Job description required. Use --job or --job-text")
            sys.exit(1)

        # Handle resume (text or PDF)
        if args.resume:
            resume_text = load_file(args.resume)
        elif args.resume_pdf:
            # Check for API key first if using LLM cleanup
            llm_client = None
            if args.llm_pdf_cleanup:
                if not ANTHROPIC_API_KEY and not OPENAI_API_KEY:
                    print("Error: OPENAI_API_KEY or ANTHROPIC_API_KEY required for --llm-pdf-cleanup")
                    sys.exit(1)
                llm_client = get_llm_client(args.llm_provider)

            print(f"Extracting text from PDF: {args.resume_pdf}")
            resume_text = resume_pdf_to_text(
                args.resume_pdf,
                use_llm_cleanup=args.llm_pdf_cleanup,
                llm_client=llm_client
            )
            print(f"Extracted {len(resume_text)} characters from PDF")
        elif args.resume_text:
            resume_text = args.resume_text
        else:
            print("Error: Resume required. Use --resume, --resume-pdf, or --resume-text")
            sys.exit(1)

        job_title = args.title or "Position"

    # Initialize LLM client based on provider preference
    try:
        if args.llm_provider == "openai" or (args.llm_provider == "auto" and OPENAI_API_KEY):
            global OPENAI_MODEL_ID
            OPENAI_MODEL_ID = args.openai_model
            llm_client = get_llm_client("openai")
            print(f"Using OpenAI ({args.openai_model}) for question generation and evaluation")
        else:
            llm_client = get_llm_client(args.llm_provider)
            if args.llm_provider == "anthropic" or (args.llm_provider == "auto" and ANTHROPIC_API_KEY):
                print("Using Anthropic (Claude) for question generation and evaluation")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if args.plan_only or args.questions_only:
        # Generate and display plan/questions only
        print("\nGenerating interview plan...")
        plan = create_interview_plan(
            job_title=job_title,
            job_description=job_description,
            resume_text=resume_text,
            llm_client=llm_client
        )

        print("\n" + "=" * 60)
        print("INTERVIEW PLAN")
        print("=" * 60)

        print(f"\nJob Title: {plan.job_title}")

        print("\nMust-Have Competencies:")
        for c in plan.must_have_competencies:
            print(f"  - [{c.id}] {c.name}: {c.description}")

        print("\nNice-to-Have Competencies:")
        for c in plan.nice_to_have_competencies:
            print(f"  - [{c.id}] {c.name}: {c.description}")

        print("\nFit Signals to Validate:")
        for s in plan.fit_signals:
            print(f"  - {s}")

        print("\nRisks to Probe:")
        for r in plan.risks_to_probe:
            print(f"  - {r}")

        print("\nQuestions:")
        for i, q in enumerate(plan.questions, 1):
            print(f"\n  Q{i} [{q.question_type}] (Competencies: {', '.join(q.competency_ids)})")
            print(f"      {q.text}")
            if q.probing_guidance:
                print(f"      Probing: {q.probing_guidance}")

        # If questions-only, also output as JSON
        if args.questions_only:
            questions_output = {
                "job_title": plan.job_title,
                "questions": [
                    {
                        "id": q.id,
                        "type": q.question_type,
                        "text": q.text,
                        "competency_ids": q.competency_ids,
                        "probing_guidance": q.probing_guidance
                    }
                    for q in plan.questions
                ]
            }
            questions_file = args.output.replace('.json', '_questions.json')
            with open(questions_file, 'w', encoding='utf-8') as f:
                json.dump(questions_output, f, indent=2)
            print(f"\nQuestions saved to: {questions_file}")

        return

    # Set up input source (voice or terminal)
    input_source = None
    if args.voice:
        print("\n🎙️  Setting up voice mode...")

        # Check for required API keys
        openai_key = os.environ.get("OPENAI_API_KEY")
        if not openai_key and (args.tts_provider == "openai" or args.stt_provider == "whisper"):
            print("Error: OPENAI_API_KEY environment variable required for voice mode")
            sys.exit(1)

        # Initialize TTS provider
        if args.tts_provider == "openai":
            tts = OpenAITTS(api_key=openai_key, voice=args.tts_voice)
        elif args.tts_provider == "elevenlabs":
            elevenlabs_key = os.environ.get("ELEVENLABS_API_KEY")
            if not elevenlabs_key:
                print("Error: ELEVENLABS_API_KEY required for ElevenLabs TTS")
                sys.exit(1)
            tts = ElevenLabsTTS(api_key=elevenlabs_key, voice_id=args.tts_voice)

        # Initialize STT provider
        if args.stt_provider == "whisper":
            stt = WhisperSTT(api_key=openai_key)
        elif args.stt_provider == "deepgram":
            deepgram_key = os.environ.get("DEEPGRAM_API_KEY")
            if not deepgram_key:
                print("Error: DEEPGRAM_API_KEY required for Deepgram STT")
                sys.exit(1)
            stt = DeepgramSTT(api_key=deepgram_key)

        # Create voice input source
        input_source = VoiceInput(
            stt_provider=stt,
            tts_provider=tts,
            transcript_file=args.transcript
        )
        print("✓ Voice mode ready! Questions will be spoken aloud.")
        print("  - Speak your answers clearly")
        print("  - Press Enter when done speaking, or wait for auto-stop on silence")
        print("  - Transcript will be saved automatically\n")
    else:
        input_source = TerminalInput()

    # Run full interview
    interviewer = AIInterviewer(
        job_title=job_title,
        job_description=job_description,
        resume_text=resume_text,
        input_source=input_source,
        llm_client=llm_client
    )

    output = interviewer.run()

    # Save and display results
    save_output(output, args.output)

    print("\n" + "=" * 60)
    print("SCREENING EVALUATION COMPLETE")
    print("=" * 60)

    print(f"\nCandidate: {output.candidate_summary.name}")
    print(f"One-line Summary: {output.candidate_summary.one_line_summary}")
    print(f"\nFinal Category: {output.final_screening_category.value}")
    print(f"Confidence: {output.confidence.level.value}")

    print("\nCompetency Scores:")
    for cs in output.competency_scores:
        marker = "*" if cs.is_must_have else " "
        print(f"  {marker}[{cs.score}/5] {cs.competency_name}")

    if output.red_flags:
        print("\nRed Flags:")
        for rf in output.red_flags:
            print(f"  [{rf.severity.value.upper()}] {rf.flag}")

    print(f"\nFull evaluation saved to: {args.output}")

    # Save voice transcript if voice mode was used
    if args.voice and isinstance(input_source, VoiceInput):
        transcript_json = input_source.save_transcript()
        print(f"Voice transcript saved to: {input_source.transcript_file}")
        print(f"Transcript JSON saved to: {transcript_json}")

    # Also print full JSON to stdout
    print("\n" + "-" * 60)
    print("JSON OUTPUT:")
    print("-" * 60)
    print(json.dumps(output.to_dict(), indent=2))


# =============================================================================
# CONVENIENCE FUNCTION FOR PROGRAMMATIC USE
# =============================================================================

def run_voice_interview(
    resume: str,
    job_description: str,
    job_title: str,
    voice: str = "alloy",
    transcript_file: Optional[str] = None,
    output_file: str = "screening_output.json",
    questions_only: bool = False,
    llm_provider: str = "auto"
) -> dict:
    """
    Run a voice-enabled AI screening interview programmatically.

    Args:
        resume: Resume text or path to resume file
        job_description: Job description text or path to file
        job_title: Title of the position
        voice: TTS voice ID (default: "alloy" for OpenAI)
        transcript_file: Path for transcript file (auto-generated if not provided)
        output_file: Path for evaluation output JSON
        questions_only: If True, only generate and return questions without conducting interview
        llm_provider: "openai", "anthropic", or "auto" (default: auto, prefers OpenAI)

    Returns:
        Dictionary with interview results:
        - If questions_only: {"questions": [...]}
        - Otherwise: Full screening output including transcript path

    Required environment variables:
        - OPENAI_API_KEY: For everything (LLM, Whisper STT, TTS) when using OpenAI provider
        - OR ANTHROPIC_API_KEY + OPENAI_API_KEY: When using Anthropic for LLM
    """
    # Load text if paths provided
    if os.path.exists(resume):
        with open(resume, 'r', encoding='utf-8') as f:
            resume_text = f.read()
    else:
        resume_text = resume

    if os.path.exists(job_description):
        with open(job_description, 'r', encoding='utf-8') as f:
            job_desc_text = f.read()
    else:
        job_desc_text = job_description

    # Initialize LLM client (defaults to OpenAI for single API key usage)
    llm_client = get_llm_client(llm_provider)

    # Generate interview plan
    plan = create_interview_plan(
        job_title=job_title,
        job_description=job_desc_text,
        resume_text=resume_text,
        llm_client=llm_client
    )

    # If questions only, return early
    if questions_only:
        return {
            "job_title": plan.job_title,
            "questions": [
                {
                    "id": q.id,
                    "type": q.question_type,
                    "text": q.text,
                    "competency_ids": q.competency_ids,
                    "probing_guidance": q.probing_guidance
                }
                for q in plan.questions
            ]
        }

    # Set up voice input
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        raise ValueError("OPENAI_API_KEY environment variable required for voice mode")

    tts = OpenAITTS(api_key=openai_key, voice=voice)
    stt = WhisperSTT(api_key=openai_key)

    input_source = VoiceInput(
        stt_provider=stt,
        tts_provider=tts,
        transcript_file=transcript_file
    )

    # Run interview
    interviewer = AIInterviewer(
        job_title=job_title,
        job_description=job_desc_text,
        resume_text=resume_text,
        input_source=input_source,
        llm_client=llm_client
    )

    output = interviewer.run()

    # Save results
    save_output(output, output_file)
    transcript_json = input_source.save_transcript()

    return {
        "evaluation": output.to_dict(),
        "transcript_file": input_source.transcript_file,
        "transcript_json": transcript_json,
        "output_file": output_file
    }


if __name__ == "__main__":
    main()
