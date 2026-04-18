"""FastAPI backend for the Riva speech-translation launchable.

Each client opens one WebSocket at ``/ws/session/{id}``. Raw int16 mono PCM
audio comes in as binary frames from the browser; Riva's
``StreamingTranslateSpeechToSpeech`` streams synthesized English audio back
as binary frames, and parallel S2T + zh-CN ASR streams produce English
captions and Chinese source transcripts as JSON text frames. The session
id is client-generated and used only for session-log filenames.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import struct
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, TextIO

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import grpc
import riva.client
from riva.client.proto import riva_asr_pb2, riva_audio_pb2, riva_nmt_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOG = logging.getLogger("riva-translator")

# ---------------------------------------------------------------------------
# Configuration

RIVA_URI = os.environ.get("RIVA_URI", "localhost:50051")
STATIC_DIR = Path(__file__).parent / "static"

# Language pair is configured via .env at bootstrap time. SOURCE_LANGUAGE
# must match the ASR model deployed by ./bootstrap.sh; TARGET_VOICE must
# match a Magpie-Multilingual subvoice for TARGET_LANGUAGE.
SOURCE_LANGUAGE = os.environ.get("SOURCE_LANGUAGE", "zh-CN")
TARGET_LANGUAGE = os.environ.get("TARGET_LANGUAGE", "en-US")
TARGET_VOICE = os.environ.get(
    "TARGET_VOICE", "Magpie-Multilingual.EN-US.Female.Neutral"
)
ASR_SAMPLE_RATE = 16000
TTS_SAMPLE_RATE = 44100

# Endpointing overrides.
#
# S2S (audio) pipeline: Riva's model-tuned zh-CN defaults commit on ~0.5-1s
# pauses, producing 1-1.5s utterances (2-4 English words) that sound like
# "one or two words at a time" in TTS. We force a long EOU (2500ms) so
# natural between-sentence pauses don't trigger a commit — utterances span
# full phrases and TTS output stays in longer, smoother bursts. Trade-off:
# ~2.5s of end-of-phrase latency before the listener hears it, which is
# acceptable for lecture-style speech.
#
# Caption (S2T) pipeline: held at 1500ms so English captions appear within
# ~1.5s of any natural pause, independent of the S2S cadence. Captions are
# read, not heard, so short fragments are fine.
S2S_STOP_HISTORY_MS = int(os.environ.get("S2S_STOP_HISTORY_MS", "1500"))
S2S_STOP_HISTORY_EOU_MS = int(os.environ.get("S2S_STOP_HISTORY_EOU_MS", "2500"))
CAPTION_STOP_HISTORY_MS = int(os.environ.get("CAPTION_STOP_HISTORY_MS", "1500"))
CAPTION_STOP_HISTORY_EOU_MS = int(os.environ.get("CAPTION_STOP_HISTORY_EOU_MS", "1500"))
SOURCE_STOP_HISTORY_MS = int(os.environ.get("SOURCE_STOP_HISTORY_MS", "1500"))
SOURCE_STOP_HISTORY_EOU_MS = int(os.environ.get("SOURCE_STOP_HISTORY_EOU_MS", "1500"))

# Per-session JSONL logs. Disabled if SESSIONS_DIR is unset or unwritable.
SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "/app/sessions"))

# ---------------------------------------------------------------------------
# Session logger


_SESSION_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class SessionLog:
    """Append-only JSONL logger for a single session.

    Written to by multiple threads (WS task, S2S worker, S2T worker, ASR
    worker), so writes are serialized with a lock.
    """

    def __init__(self, path: Path, session_id: str) -> None:
        self.path = path
        self.session_id = session_id
        self._lock = threading.Lock()
        self._file: Optional[TextIO] = path.open("a", buffering=1, encoding="utf-8")
        self._start = time.time()

    def log(self, kind: str, **fields: Any) -> None:
        f = self._file
        if f is None:
            return
        now = time.time()
        entry = {
            "ts": round(now, 3),
            "t_s": round(now - self._start, 3),
            "session": self.session_id,
            "kind": kind,
        }
        entry.update(fields)
        line = json.dumps(entry, ensure_ascii=False)
        with self._lock:
            try:
                f.write(line + "\n")
            except Exception:  # noqa: BLE001
                # Don't let a failed log write break the audio pipeline.
                LOG.exception("session log write failed for session=%s", self.session_id)

    def close(self) -> None:
        with self._lock:
            f = self._file
            self._file = None
            if f is not None:
                try:
                    f.close()
                except Exception:  # noqa: BLE001
                    pass


def _new_session_log(session_id: str) -> Optional[SessionLog]:
    try:
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:  # noqa: BLE001
        LOG.warning("sessions dir %s not writable; logging disabled", SESSIONS_DIR)
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = _SESSION_ID_SAFE_RE.sub("_", session_id)[:64] or "session"
    path = SESSIONS_DIR / f"{ts}-{safe}.jsonl"
    try:
        slog = SessionLog(path, session_id)
    except Exception:  # noqa: BLE001
        LOG.exception("failed to open session log at %s", path)
        return None
    LOG.info("[session=%s] session log -> %s", session_id, path)
    return slog


def _slog(session: "Session", kind: str, **fields: Any) -> None:
    """Safe shim — logs if the session has an active SessionLog, else no-op."""
    if session.log is not None:
        session.log.log(kind, **fields)


# ---------------------------------------------------------------------------
# Session state


class AudioPump:
    """Blocking iterator that yields audio chunks pushed in from an async task.

    The Riva Python client wants a synchronous iterator of `bytes` for
    `audio_chunks=...`. We run the iterator's consumer in a worker thread and
    push into a `queue.Queue` from the event loop.
    """

    _SENTINEL = object()

    def __init__(self) -> None:
        self._q: "queue.Queue[object]" = queue.Queue(maxsize=256)
        self._closed = False

    def put(self, chunk: bytes) -> None:
        if not self._closed:
            self._q.put(chunk)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._q.put(self._SENTINEL)

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        item = self._q.get()
        if item is self._SENTINEL:
            raise StopIteration
        return item  # type: ignore[return-value]


@dataclass
class Session:
    id: str
    ws: Optional[WebSocket] = None
    pump: Optional[AudioPump] = None
    worker: Optional[threading.Thread] = None
    caption_pump: Optional[AudioPump] = None
    caption_worker: Optional[threading.Thread] = None
    source_pump: Optional[AudioPump] = None
    source_worker: Optional[threading.Thread] = None
    log: Optional[SessionLog] = None


# ---------------------------------------------------------------------------
# Riva worker


def _apply_endpointing(
    asr_cfg: riva_asr_pb2.RecognitionConfig,
    *,
    stop_history_ms: int,
    stop_history_eou_ms: int,
) -> None:
    asr_cfg.endpointing_config.stop_history = stop_history_ms
    asr_cfg.endpointing_config.stop_history_eou = stop_history_eou_ms


def _build_streaming_config() -> riva_nmt_pb2.StreamingTranslateSpeechToSpeechConfig:
    asr_cfg = riva_asr_pb2.RecognitionConfig(
        encoding=riva_audio_pb2.LINEAR_PCM,
        language_code=SOURCE_LANGUAGE,
        max_alternatives=1,
        enable_automatic_punctuation=True,
        sample_rate_hertz=ASR_SAMPLE_RATE,
        audio_channel_count=1,
    )
    _apply_endpointing(
        asr_cfg,
        stop_history_ms=S2S_STOP_HISTORY_MS,
        stop_history_eou_ms=S2S_STOP_HISTORY_EOU_MS,
    )
    streaming_asr = riva_asr_pb2.StreamingRecognitionConfig(
        config=asr_cfg,
        interim_results=True,
    )
    translation_cfg = riva_nmt_pb2.TranslationConfig(
        source_language_code=SOURCE_LANGUAGE,
        target_language_code=TARGET_LANGUAGE,
    )
    tts_cfg = riva_nmt_pb2.SynthesizeSpeechConfig(
        encoding=riva_audio_pb2.LINEAR_PCM,
        language_code=TARGET_LANGUAGE,
        voice_name=TARGET_VOICE,
        sample_rate_hz=TTS_SAMPLE_RATE,
    )
    return riva_nmt_pb2.StreamingTranslateSpeechToSpeechConfig(
        asr_config=streaming_asr,
        translation_config=translation_cfg,
        tts_config=tts_cfg,
    )


def _run_source_asr_session(
    session: Session,
    pump: AudioPump,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Plain zh-CN ASR worker that feeds the 'Heard (Chinese)' pane.

    Runs independently of the S2T caption pipeline and the S2S audio
    pipeline. Produces raw Chinese transcripts — useful for the speaker to
    verify what the ASR actually picked up, which often exposes code-switched
    or misheard segments that the downstream translation struggles with.
    """
    LOG.info("[session=%s] starting source ASR (%s)", session.id, SOURCE_LANGUAGE)
    try:
        auth = riva.client.Auth(uri=RIVA_URI)
        asr_service = riva.client.ASRService(auth)
        asr_cfg = riva_asr_pb2.RecognitionConfig(
            encoding=riva_audio_pb2.LINEAR_PCM,
            language_code=SOURCE_LANGUAGE,
            max_alternatives=1,
            enable_automatic_punctuation=True,
            sample_rate_hertz=ASR_SAMPLE_RATE,
            audio_channel_count=1,
        )
        _apply_endpointing(
            asr_cfg,
            stop_history_ms=SOURCE_STOP_HISTORY_MS,
            stop_history_eou_ms=SOURCE_STOP_HISTORY_EOU_MS,
        )
        streaming_asr = riva_asr_pb2.StreamingRecognitionConfig(
            config=asr_cfg,
            interim_results=True,
        )
        responses = asr_service.streaming_response_generator(
            audio_chunks=pump,
            streaming_config=streaming_asr,
        )

        last_partial_logged = ""

        for resp in responses:
            for result in resp.results:
                alts = list(getattr(result, "alternatives", []) or [])
                if not alts:
                    continue
                transcript = (alts[0].transcript or "").strip()
                if not transcript:
                    continue
                is_final = bool(getattr(result, "is_final", False))
                msg: Dict[str, str] = {}
                if is_final:
                    msg["final"] = transcript
                    _slog(session, "asr_final", zh=transcript)
                    last_partial_logged = ""
                else:
                    msg["partial"] = transcript
                    if transcript != last_partial_logged:
                        _slog(session, "asr_partial", zh=transcript)
                        last_partial_logged = transcript
                asyncio.run_coroutine_threadsafe(
                    _send_transcripts(session, msg), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[session=%s] source ASR gRPC error: %s", session.id, exc)
        _slog(
            session,
            "asr_grpc_error",
            code=exc.code().name if hasattr(exc, "code") else None,
            error=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[session=%s] source ASR worker crashed", session.id)
        _slog(session, "asr_worker_crash", error=repr(exc))
    finally:
        LOG.info("[session=%s] source ASR worker exiting", session.id)
        _slog(session, "asr_worker_exit")


def _run_caption_session(
    session: Session,
    pump: AudioPump,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Streaming speech-to-text translation worker for English captions.

    Riva 2.19's S2S stream only emits audio (speech.meta.text comes back
    empty), so we can't read captions off the TTS pipeline. Instead, we run
    a parallel StreamingTranslateSpeechToText on the same audio — a single
    gRPC that does ASR+NMT and emits streaming English text with partials
    and finals. The resulting captions approximate what S2S is synthesizing
    (same source audio, same target language), without the garbled
    intermediate hypotheses that a parallel zh-CN ASR produced before.
    """
    LOG.info(
        "[session=%s] starting streaming S2T (%s -> %s)",
        session.id, SOURCE_LANGUAGE, TARGET_LANGUAGE,
    )
    target_lang_short = TARGET_LANGUAGE.split("-")[0]
    try:
        auth = riva.client.Auth(uri=RIVA_URI)
        nmt = riva.client.NeuralMachineTranslationClient(auth)
        asr_cfg = riva_asr_pb2.RecognitionConfig(
            encoding=riva_audio_pb2.LINEAR_PCM,
            language_code=SOURCE_LANGUAGE,
            max_alternatives=1,
            enable_automatic_punctuation=True,
            sample_rate_hertz=ASR_SAMPLE_RATE,
            audio_channel_count=1,
        )
        _apply_endpointing(
            asr_cfg,
            stop_history_ms=CAPTION_STOP_HISTORY_MS,
            stop_history_eou_ms=CAPTION_STOP_HISTORY_EOU_MS,
        )
        streaming_asr = riva_asr_pb2.StreamingRecognitionConfig(
            config=asr_cfg,
            interim_results=True,
        )
        translation_cfg = riva_nmt_pb2.TranslationConfig(
            source_language_code=SOURCE_LANGUAGE,
            target_language_code=target_lang_short,
        )
        streaming_cfg = riva_nmt_pb2.StreamingTranslateSpeechToTextConfig(
            asr_config=streaming_asr,
            translation_config=translation_cfg,
        )
        responses = nmt.streaming_s2t_response_generator(
            audio_chunks=pump,
            streaming_config=streaming_cfg,
        )

        last_partial_logged = ""

        for resp in responses:
            for result in resp.results:
                alts = list(getattr(result, "alternatives", []) or [])
                if not alts:
                    continue
                transcript = (alts[0].transcript or "").strip()
                if not transcript:
                    continue
                is_final = bool(getattr(result, "is_final", False))
                msg: Dict[str, str] = {}
                if is_final:
                    msg["translated"] = transcript
                    _slog(session, "s2t_final", en=transcript)
                    last_partial_logged = ""
                else:
                    msg["partial_translated"] = transcript
                    if transcript != last_partial_logged:
                        _slog(session, "s2t_partial", en=transcript)
                        last_partial_logged = transcript
                asyncio.run_coroutine_threadsafe(
                    _send_transcripts(session, msg), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[session=%s] S2T gRPC error: %s", session.id, exc)
        _slog(
            session,
            "s2t_grpc_error",
            code=exc.code().name if hasattr(exc, "code") else None,
            error=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[session=%s] S2T worker crashed", session.id)
        _slog(session, "s2t_worker_crash", error=repr(exc))
    finally:
        LOG.info("[session=%s] S2T worker exiting", session.id)
        _slog(session, "s2t_worker_exit")


def _run_riva_session(
    session: Session,
    pump: AudioPump,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Blocking worker that relays translated audio from Riva to the client."""

    LOG.info(
        "[session=%s] starting Riva S2S (%s -> %s, voice=%s)",
        session.id, SOURCE_LANGUAGE, TARGET_LANGUAGE, TARGET_VOICE,
    )
    _slog(
        session,
        "s2s_start",
        source=SOURCE_LANGUAGE,
        target=TARGET_LANGUAGE,
        voice=TARGET_VOICE,
        endpointing_override={
            "stop_history_ms": S2S_STOP_HISTORY_MS,
            "stop_history_eou_ms": S2S_STOP_HISTORY_EOU_MS,
        },
    )
    last_audio_time: Optional[float] = None
    try:
        auth = riva.client.Auth(uri=RIVA_URI)
        nmt = riva.client.NeuralMachineTranslationClient(auth)
        streaming_cfg = _build_streaming_config()

        responses = nmt.streaming_s2s_response_generator(
            audio_chunks=pump,
            streaming_config=streaming_cfg,
        )

        for resp in responses:
            speech = getattr(resp, "speech", None)
            audio = getattr(speech, "audio", b"") if speech is not None else b""
            if audio:
                now = time.monotonic()
                gap_ms = (
                    round((now - last_audio_time) * 1000, 1)
                    if last_audio_time is not None
                    else None
                )
                last_audio_time = now
                # 16-bit mono PCM at TTS_SAMPLE_RATE -> samples = bytes/2.
                duration_ms = round(
                    (len(audio) / 2) / TTS_SAMPLE_RATE * 1000, 1
                )
                _slog(
                    session,
                    "tts_audio_chunk",
                    bytes=len(audio),
                    duration_ms=duration_ms,
                    gap_ms=gap_ms,
                )
                asyncio.run_coroutine_threadsafe(
                    _send_audio(session, audio), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[session=%s] Riva gRPC error: %s", session.id, exc)
        _slog(
            session,
            "s2s_grpc_error",
            code=exc.code().name if hasattr(exc, "code") else None,
            error=str(exc),
        )
        asyncio.run_coroutine_threadsafe(
            _send_status(session, f"Riva error: {exc.code().name if hasattr(exc, 'code') else exc}"),
            loop,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[session=%s] Riva worker crashed", session.id)
        _slog(session, "s2s_worker_crash", error=repr(exc))
        asyncio.run_coroutine_threadsafe(
            _send_status(session, f"internal error: {exc}"),
            loop,
        )
    finally:
        LOG.info("[session=%s] Riva worker exiting", session.id)
        _slog(session, "s2s_worker_exit")


# ---------------------------------------------------------------------------
# Send helpers


async def _safe_send_bytes(ws: Optional[WebSocket], data: bytes) -> None:
    if ws is None:
        return
    try:
        await ws.send_bytes(data)
    except Exception as exc:  # noqa: BLE001
        LOG.debug("send_bytes failed: %s", exc)


async def _safe_send_json(ws: Optional[WebSocket], data: dict) -> None:
    if ws is None:
        return
    try:
        await ws.send_json(data)
    except Exception as exc:  # noqa: BLE001
        LOG.debug("send_json failed: %s", exc)


async def _send_audio(session: Session, audio: bytes) -> None:
    await _safe_send_bytes(session.ws, audio)


async def _send_transcripts(session: Session, transcripts: Dict[str, str]) -> None:
    await _safe_send_json(session.ws, {"type": "transcript", **transcripts})


async def _send_status(session: Session, text: str) -> None:
    await _safe_send_json(session.ws, {"type": "status", "text": text})


# ---------------------------------------------------------------------------
# FastAPI app


app = FastAPI(title="Riva Real-Time Translator")


@app.get("/api/config")
async def api_config() -> JSONResponse:
    return JSONResponse({
        "source_language": SOURCE_LANGUAGE,
        "target_language": TARGET_LANGUAGE,
        "target_voice": TARGET_VOICE,
        "asr_sample_rate": ASR_SAMPLE_RATE,
        "tts_sample_rate": TTS_SAMPLE_RATE,
    })


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.get("/api/sessions")
async def api_sessions_list() -> JSONResponse:
    """List available session logs (name, bytes, mtime)."""
    if not SESSIONS_DIR.exists():
        return JSONResponse({"dir": str(SESSIONS_DIR), "files": []})
    files = []
    for p in sorted(SESSIONS_DIR.glob("*.jsonl"), reverse=True):
        try:
            st = p.stat()
        except OSError:
            continue
        files.append({"name": p.name, "bytes": st.st_size, "mtime": int(st.st_mtime)})
    return JSONResponse({"dir": str(SESSIONS_DIR), "files": files})


@app.get("/api/sessions/{name}")
async def api_session_get(name: str):
    """Fetch a session log by filename (path traversal blocked)."""
    if "/" in name or ".." in name or not name.endswith(".jsonl"):
        return JSONResponse({"error": "invalid name"}, status_code=400)
    p = SESSIONS_DIR / name
    if not p.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(p, media_type="application/x-ndjson")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---- WebSocket -------------------------------------------------------------


@app.websocket("/ws/session/{session_id}")
async def ws_session(ws: WebSocket, session_id: str) -> None:
    await ws.accept()

    session = Session(id=session_id, ws=ws)
    session.pump = AudioPump()
    session.caption_pump = AudioPump()
    session.source_pump = AudioPump()
    session.log = _new_session_log(session_id)
    loop = asyncio.get_running_loop()
    session.worker = threading.Thread(
        target=_run_riva_session,
        args=(session, session.pump, loop),
        name=f"riva-s2s-{session_id}",
        daemon=True,
    )
    session.caption_worker = threading.Thread(
        target=_run_caption_session,
        args=(session, session.caption_pump, loop),
        name=f"riva-s2t-{session_id}",
        daemon=True,
    )
    session.source_worker = threading.Thread(
        target=_run_source_asr_session,
        args=(session, session.source_pump, loop),
        name=f"riva-asr-{session_id}",
        daemon=True,
    )
    session.worker.start()
    session.caption_worker.start()
    session.source_worker.start()

    LOG.info("[session=%s] connected", session_id)
    _slog(session, "session_connected", user_agent=ws.headers.get("user-agent"))
    await _safe_send_json(ws, {
        "type": "status",
        "text": f"Connected. Translating {SOURCE_LANGUAGE} → {TARGET_LANGUAGE}. Sample rate {TTS_SAMPLE_RATE} Hz.",
        "sample_rate": TTS_SAMPLE_RATE,
    })

    # Per-second input audio stats so we can tell whether the mic was
    # still sending audio during long TTS gaps. Bucket resets each second.
    bucket_start: Optional[float] = None
    bucket_bytes = 0
    bucket_chunks = 0
    bucket_peak = 0

    def _flush_bucket(now: float) -> None:
        nonlocal bucket_start, bucket_bytes, bucket_chunks, bucket_peak
        if bucket_start is None or bucket_chunks == 0:
            return
        _slog(
            session,
            "speaker_audio",
            bytes=bucket_bytes,
            chunks=bucket_chunks,
            peak_abs=bucket_peak,
            duration_ms=round((now - bucket_start) * 1000, 1),
        )
        bucket_start = None
        bucket_bytes = 0
        bucket_chunks = 0
        bucket_peak = 0

    try:
        while True:
            msg = await ws.receive()
            if "bytes" in msg and msg["bytes"] is not None:
                chunk = msg["bytes"]
                if session.pump is not None:
                    session.pump.put(chunk)
                if session.caption_pump is not None:
                    session.caption_pump.put(chunk)
                if session.source_pump is not None:
                    session.source_pump.put(chunk)
                now = time.monotonic()
                if bucket_start is None:
                    bucket_start = now
                bucket_bytes += len(chunk)
                bucket_chunks += 1
                if chunk:
                    samples = struct.unpack_from(f"<{len(chunk)//2}h", chunk)
                    if samples:
                        chunk_peak = max(-min(samples), max(samples))
                        if chunk_peak > bucket_peak:
                            bucket_peak = chunk_peak
                if now - bucket_start >= 1.0:
                    _flush_bucket(now)
            elif msg.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        LOG.exception("[session=%s] socket error", session_id)
    finally:
        _flush_bucket(time.monotonic())
        LOG.info("[session=%s] disconnected", session_id)
        _slog(session, "session_disconnected")
        session.ws = None
        if session.pump is not None:
            session.pump.close()
        session.pump = None
        if session.caption_pump is not None:
            session.caption_pump.close()
        session.caption_pump = None
        if session.source_pump is not None:
            session.source_pump.close()
        session.source_pump = None
        closing_log = session.log
        session.log = None
        if closing_log is not None:
            closing_log.close()
