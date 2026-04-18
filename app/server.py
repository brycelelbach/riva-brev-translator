"""FastAPI backend for the Riva speech-translation launchable.

One "room" pairs a **speaker** (captures microphone audio) with a **listener**
(plays back translated audio). Audio from the speaker's WebSocket is fed into
Riva's StreamingTranslateSpeechToSpeech gRPC, and the translated audio is
forwarded to the listener's WebSocket. Partial transcripts are sent to both
sides as JSON text frames for on-screen display.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, TextIO

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

# This launchable does Chinese → English S2S only (demo scope). The deployed
# streaming ASR is zh-CN and the fixed target is en-US synthesized by a Magpie
# EN-US voice.
SOURCE_LANGUAGE = "zh-CN"
TARGET_LANGUAGE = "en-US"
TARGET_VOICE = "Magpie-Multilingual.EN-US.Female.Neutral"
ASR_SAMPLE_RATE = 16000
TTS_SAMPLE_RATE = 44100

# Endpointing overrides (optional). Leaving these unset uses Riva's
# model-tuned defaults, which give 2-4 second segments on continuous speech --
# long enough that each segment's NMT+TTS latency is hidden by its own audio
# length, so playback stays smooth. Lowering them below the speaker's natural
# inter-phrase pauses fragments utterances into 2-3 word chunks and makes the
# audio choppy. The continuously-updating English caption pane comes from
# partial translations (below) instead.
_stop_history_env = os.environ.get("ASR_STOP_HISTORY_MS")
_stop_history_eou_env = os.environ.get("ASR_STOP_HISTORY_EOU_MS")
ASR_STOP_HISTORY_MS: Optional[int] = int(_stop_history_env) if _stop_history_env else None
ASR_STOP_HISTORY_EOU_MS: Optional[int] = (
    int(_stop_history_eou_env) if _stop_history_eou_env else None
)

# Partial-translation throttle. The ASR emits partials ~5–10x/sec; translating
# every one saturates NMT. We translate the current partial at most every
# PARTIAL_TRANSLATE_MIN_INTERVAL_S seconds, and only if it has grown by at
# least PARTIAL_TRANSLATE_MIN_CHAR_DELTA Chinese characters since last time.
PARTIAL_TRANSLATE_MIN_INTERVAL_S = float(
    os.environ.get("PARTIAL_TRANSLATE_MIN_INTERVAL_S", "0.6")
)
PARTIAL_TRANSLATE_MIN_CHAR_DELTA = int(
    os.environ.get("PARTIAL_TRANSLATE_MIN_CHAR_DELTA", "3")
)

# Per-session JSONL logs. Disabled if SESSIONS_DIR is unset or unwritable.
SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "/app/sessions"))

# ---------------------------------------------------------------------------
# Session logger


_ROOM_NAME_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class SessionLog:
    """Append-only JSONL logger for a single speaker session.

    Written to by multiple threads (speaker WS task, listener WS task, S2S
    worker, parallel-ASR worker), so writes are serialized with a lock.
    """

    def __init__(self, path: Path, room: str) -> None:
        self.path = path
        self.room = room
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
            "room": self.room,
            "kind": kind,
        }
        entry.update(fields)
        line = json.dumps(entry, ensure_ascii=False)
        with self._lock:
            try:
                f.write(line + "\n")
            except Exception:  # noqa: BLE001
                # Don't let a failed log write break the audio pipeline.
                LOG.exception("session log write failed for room=%s", self.room)

    def close(self) -> None:
        with self._lock:
            f = self._file
            self._file = None
            if f is not None:
                try:
                    f.close()
                except Exception:  # noqa: BLE001
                    pass


def _new_session_log(room_name: str) -> Optional[SessionLog]:
    try:
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:  # noqa: BLE001
        LOG.warning("sessions dir %s not writable; logging disabled", SESSIONS_DIR)
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_room = _ROOM_NAME_SAFE_RE.sub("_", room_name)[:64] or "room"
    path = SESSIONS_DIR / f"{ts}-{safe_room}.jsonl"
    try:
        slog = SessionLog(path, room_name)
    except Exception:  # noqa: BLE001
        LOG.exception("failed to open session log at %s", path)
        return None
    LOG.info("[room=%s] session log -> %s", room_name, path)
    return slog


def _slog(room: "Room", kind: str, **fields: Any) -> None:
    """Safe shim — logs if the room has an active SessionLog, else no-op."""
    if room.log is not None:
        room.log.log(kind, **fields)


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
class Room:
    name: str
    speaker: Optional[WebSocket] = None
    listener: Optional[WebSocket] = None
    pump: Optional[AudioPump] = None
    worker: Optional[threading.Thread] = None
    asr_pump: Optional[AudioPump] = None
    asr_worker: Optional[threading.Thread] = None
    log: Optional[SessionLog] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def has_listener(self) -> bool:
        return self.listener is not None

    def has_speaker(self) -> bool:
        return self.speaker is not None


rooms: Dict[str, Room] = {}
rooms_lock = asyncio.Lock()


async def get_or_create_room(name: str) -> Room:
    async with rooms_lock:
        room = rooms.get(name)
        if room is None:
            room = Room(name=name)
            rooms[name] = room
        return room


# ---------------------------------------------------------------------------
# Riva worker


def _endpointing_config() -> Optional[riva_asr_pb2.EndpointingConfig]:
    if ASR_STOP_HISTORY_MS is None and ASR_STOP_HISTORY_EOU_MS is None:
        return None
    cfg = riva_asr_pb2.EndpointingConfig()
    if ASR_STOP_HISTORY_MS is not None:
        cfg.stop_history = ASR_STOP_HISTORY_MS
    if ASR_STOP_HISTORY_EOU_MS is not None:
        cfg.stop_history_eou = ASR_STOP_HISTORY_EOU_MS
    return cfg


def _apply_endpointing(asr_cfg: riva_asr_pb2.RecognitionConfig) -> None:
    ep = _endpointing_config()
    if ep is not None:
        asr_cfg.endpointing_config.CopyFrom(ep)


def _build_streaming_config() -> riva_nmt_pb2.StreamingTranslateSpeechToSpeechConfig:
    asr_cfg = riva_asr_pb2.RecognitionConfig(
        encoding=riva_audio_pb2.LINEAR_PCM,
        language_code=SOURCE_LANGUAGE,
        max_alternatives=1,
        enable_automatic_punctuation=True,
        sample_rate_hertz=ASR_SAMPLE_RATE,
        audio_channel_count=1,
    )
    _apply_endpointing(asr_cfg)
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


def _run_asr_session(
    room: Room,
    pump: AudioPump,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Parallel streaming-ASR worker for on-screen captions.

    Riva 2.19's S2S response exposes only translated audio, never the ASR
    hypothesis or the NMT output text. To populate the UI's source/target
    caption panes, we run a second streaming ASR on the same audio and then
    synchronously translate each final Chinese utterance into English.
    """
    LOG.info("[room=%s] starting parallel ASR (%s)", room.name, SOURCE_LANGUAGE)
    try:
        auth = riva.client.Auth(uri=RIVA_URI)
        asr = riva.client.ASRService(auth)
        nmt = riva.client.NeuralMachineTranslationClient(auth)
        asr_cfg = riva_asr_pb2.RecognitionConfig(
            encoding=riva_audio_pb2.LINEAR_PCM,
            language_code=SOURCE_LANGUAGE,
            max_alternatives=1,
            enable_automatic_punctuation=True,
            sample_rate_hertz=ASR_SAMPLE_RATE,
            audio_channel_count=1,
        )
        _apply_endpointing(asr_cfg)
        streaming_cfg = riva_asr_pb2.StreamingRecognitionConfig(
            config=asr_cfg,
            interim_results=True,
        )
        responses = asr.streaming_response_generator(
            audio_chunks=pump,
            streaming_config=streaming_cfg,
        )

        target_lang_short = TARGET_LANGUAGE.split("-")[0]
        last_partial_translate_time = 0.0
        last_partial_translate_text = ""
        last_partial_logged = ""

        def _translate(text: str, log_kind: str) -> Optional[str]:
            t0 = time.monotonic()
            try:
                tr = nmt.translate(
                    texts=[text],
                    model="megatronnmt_any_any_1b",
                    source_language=SOURCE_LANGUAGE,
                    target_language=target_lang_short,
                )
                translations = list(getattr(tr, "translations", []) or [])
                out = translations[0].text if translations else None
                _slog(
                    room,
                    log_kind,
                    zh=text,
                    en=out,
                    latency_ms=round((time.monotonic() - t0) * 1000, 1),
                )
                return out
            except Exception as exc:  # noqa: BLE001
                LOG.exception("[room=%s] translate() failed", room.name)
                _slog(
                    room,
                    "nmt_error",
                    zh=text,
                    error=repr(exc),
                    latency_ms=round((time.monotonic() - t0) * 1000, 1),
                )
                return None

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
                    _slog(room, "asr_final", zh=transcript)
                    translated = _translate(transcript, "nmt_translated")
                    if translated is not None:
                        msg["translated"] = translated
                    last_partial_translate_time = 0.0
                    last_partial_translate_text = ""
                    last_partial_logged = ""
                else:
                    msg["partial"] = transcript
                    # Log partials only when they grow, to avoid flooding with
                    # repeated snapshots of the same hypothesis.
                    if transcript != last_partial_logged:
                        _slog(room, "asr_partial", zh=transcript)
                        last_partial_logged = transcript
                    now = time.monotonic()
                    grew_enough = (
                        len(transcript) - len(last_partial_translate_text)
                        >= PARTIAL_TRANSLATE_MIN_CHAR_DELTA
                    )
                    if (
                        now - last_partial_translate_time
                        >= PARTIAL_TRANSLATE_MIN_INTERVAL_S
                        and grew_enough
                    ):
                        last_partial_translate_time = now
                        last_partial_translate_text = transcript
                        partial_translated = _translate(
                            transcript, "nmt_partial_translated"
                        )
                        if partial_translated is not None:
                            msg["partial_translated"] = partial_translated
                asyncio.run_coroutine_threadsafe(
                    _broadcast_transcripts(room, msg), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[room=%s] ASR gRPC error: %s", room.name, exc)
        _slog(
            room,
            "asr_grpc_error",
            code=exc.code().name if hasattr(exc, "code") else None,
            error=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[room=%s] ASR worker crashed", room.name)
        _slog(room, "asr_worker_crash", error=repr(exc))
    finally:
        LOG.info("[room=%s] ASR worker exiting", room.name)
        _slog(room, "asr_worker_exit")


def _run_riva_session(
    room: Room,
    pump: AudioPump,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Blocking worker that relays audio/text between Riva and the room."""

    LOG.info(
        "[room=%s] starting Riva S2S (%s -> %s, voice=%s)",
        room.name, SOURCE_LANGUAGE, TARGET_LANGUAGE, TARGET_VOICE,
    )
    _slog(
        room,
        "s2s_start",
        source=SOURCE_LANGUAGE,
        target=TARGET_LANGUAGE,
        voice=TARGET_VOICE,
        endpointing_override={
            "stop_history_ms": ASR_STOP_HISTORY_MS,
            "stop_history_eou_ms": ASR_STOP_HISTORY_EOU_MS,
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
                    room,
                    "tts_audio_chunk",
                    bytes=len(audio),
                    duration_ms=duration_ms,
                    gap_ms=gap_ms,
                )
                asyncio.run_coroutine_threadsafe(
                    _broadcast_audio(room, audio), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[room=%s] Riva gRPC error: %s", room.name, exc)
        _slog(
            room,
            "s2s_grpc_error",
            code=exc.code().name if hasattr(exc, "code") else None,
            error=str(exc),
        )
        asyncio.run_coroutine_threadsafe(
            _broadcast_status(room, f"Riva error: {exc.code().name if hasattr(exc, 'code') else exc}"),
            loop,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[room=%s] Riva worker crashed", room.name)
        _slog(room, "s2s_worker_crash", error=repr(exc))
        asyncio.run_coroutine_threadsafe(
            _broadcast_status(room, f"internal error: {exc}"),
            loop,
        )
    finally:
        LOG.info("[room=%s] Riva worker exiting", room.name)
        _slog(room, "s2s_worker_exit")


# ---------------------------------------------------------------------------
# Broadcast helpers


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


async def _broadcast_audio(room: Room, audio: bytes) -> None:
    # Listener gets the audio; speaker doesn't (no echo).
    await _safe_send_bytes(room.listener, audio)


async def _broadcast_transcripts(room: Room, transcripts: Dict[str, str]) -> None:
    msg = {"type": "transcript", **transcripts}
    await asyncio.gather(
        _safe_send_json(room.speaker, msg),
        _safe_send_json(room.listener, msg),
    )


async def _broadcast_status(room: Room, text: str) -> None:
    msg = {"type": "status", "text": text}
    await asyncio.gather(
        _safe_send_json(room.speaker, msg),
        _safe_send_json(room.listener, msg),
    )


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


@app.get("/api/rooms/{name}")
async def api_room_status(name: str) -> JSONResponse:
    room = rooms.get(name)
    if room is None:
        return JSONResponse({"exists": False})
    return JSONResponse({
        "exists": True,
        "has_speaker": room.has_speaker(),
        "has_listener": room.has_listener(),
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


# ---- WebSockets ------------------------------------------------------------


@app.websocket("/ws/speaker/{room_name}")
async def ws_speaker(ws: WebSocket, room_name: str) -> None:
    await ws.accept()
    room = await get_or_create_room(room_name)

    async with room.lock:
        if room.has_speaker():
            await ws.close(code=1008, reason="room already has a speaker")
            return
        room.speaker = ws
        room.pump = AudioPump()
        room.asr_pump = AudioPump()
        room.log = _new_session_log(room_name)
        loop = asyncio.get_running_loop()
        room.worker = threading.Thread(
            target=_run_riva_session,
            args=(room, room.pump, loop),
            name=f"riva-s2s-{room_name}",
            daemon=True,
        )
        room.asr_worker = threading.Thread(
            target=_run_asr_session,
            args=(room, room.asr_pump, loop),
            name=f"riva-asr-{room_name}",
            daemon=True,
        )
        room.worker.start()
        room.asr_worker.start()

    LOG.info(
        "[room=%s] speaker connected (listener_present=%s)",
        room_name, room.has_listener(),
    )
    _slog(
        room,
        "speaker_connected",
        listener_present=room.has_listener(),
        user_agent=ws.headers.get("user-agent"),
    )
    await _safe_send_json(ws, {
        "type": "status",
        "text": f"Connected as speaker. Translating {SOURCE_LANGUAGE} → {TARGET_LANGUAGE}.",
    })
    await _broadcast_status(room, "speaker connected")

    try:
        while True:
            msg = await ws.receive()
            if "bytes" in msg and msg["bytes"] is not None:
                chunk = msg["bytes"]
                if room.pump is not None:
                    room.pump.put(chunk)
                if room.asr_pump is not None:
                    room.asr_pump.put(chunk)
            elif msg.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        LOG.exception("[room=%s] speaker socket error", room_name)
    finally:
        LOG.info("[room=%s] speaker disconnected", room_name)
        _slog(room, "speaker_disconnected")
        async with room.lock:
            room.speaker = None
            if room.pump is not None:
                room.pump.close()
            room.pump = None
            if room.asr_pump is not None:
                room.asr_pump.close()
            room.asr_pump = None
            closing_log = room.log
            room.log = None
        if closing_log is not None:
            closing_log.close()
        await _broadcast_status(room, "speaker disconnected")


@app.websocket("/ws/listener/{room_name}")
async def ws_listener(ws: WebSocket, room_name: str) -> None:
    await ws.accept()
    room = await get_or_create_room(room_name)

    async with room.lock:
        if room.has_listener():
            await ws.close(code=1008, reason="room already has a listener")
            return
        room.listener = ws

    LOG.info(
        "[room=%s] listener connected (speaker_present=%s)",
        room_name, room.has_speaker(),
    )
    _slog(
        room,
        "listener_connected",
        speaker_present=room.has_speaker(),
        user_agent=ws.headers.get("user-agent"),
    )
    await _safe_send_json(ws, {
        "type": "status",
        "text": f"Connected as listener. Sample rate {TTS_SAMPLE_RATE} Hz, 16-bit PCM.",
        "sample_rate": TTS_SAMPLE_RATE,
    })
    await _broadcast_status(room, "listener connected")

    try:
        while True:
            # Listener doesn't send data; we just wait for disconnect.
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        LOG.exception("[room=%s] listener socket error", room_name)
    finally:
        LOG.info("[room=%s] listener disconnected", room_name)
        _slog(room, "listener_disconnected")
        async with room.lock:
            room.listener = None
        await _broadcast_status(room, "listener disconnected")
