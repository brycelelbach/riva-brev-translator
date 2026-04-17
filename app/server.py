"""FastAPI backend for the Riva speech-translation launchable.

One "room" pairs a **speaker** (captures microphone audio) with a **listener**
(plays back translated audio). Audio from the speaker's WebSocket is fed into
Riva's StreamingTranslateSpeechToSpeech gRPC, and the translated audio is
forwarded to the listener's WebSocket. Partial transcripts are sent to both
sides as JSON text frames for on-screen display.
"""

from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Optional

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

# Quick Start streaming ASR in this launchable is English-only; that is the
# only supported source. Target language is chosen by the speaker at runtime.
SOURCE_LANGUAGE = "en-US"
ASR_SAMPLE_RATE = 16000
TTS_SAMPLE_RATE = 44100

# Default Magpie-Multilingual voices. Names follow
# "Magpie-Multilingual.<LANG>.<SpeakerName>" as deployed by the quickstart.
# If a voice does not exist on your server, the UI's voice_name override lets
# the user replace it; if still unknown, Riva will pick a default for the
# language.
DEFAULT_VOICES: Dict[str, str] = {
    "es-US": "Magpie-Multilingual.ES-US.Diego",
    "fr-FR": "Magpie-Multilingual.FR-FR.Lea",
    "de-DE": "Magpie-Multilingual.DE-DE.Ralph",
    "zh-CN": "Magpie-Multilingual.ZH-CN.Guo",
    "it-IT": "Magpie-Multilingual.IT-IT.Giulia",
    "vi-VN": "Magpie-Multilingual.VI-VN.Khanh",
    "en-US": "Magpie-Multilingual.EN-US.Sofia",
}

TARGET_LANGUAGES = [
    {"code": "es-US", "name": "Spanish"},
    {"code": "fr-FR", "name": "French"},
    {"code": "de-DE", "name": "German"},
    {"code": "zh-CN", "name": "Chinese (Simplified)"},
    {"code": "it-IT", "name": "Italian"},
    {"code": "vi-VN", "name": "Vietnamese"},
]

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
    target_lang: str = "es-US"
    voice: str = DEFAULT_VOICES["es-US"]
    pump: Optional[AudioPump] = None
    worker: Optional[threading.Thread] = None
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


def _build_streaming_config(target_lang: str, voice: str) -> riva_nmt_pb2.StreamingTranslateSpeechToSpeechConfig:
    asr_cfg = riva_asr_pb2.RecognitionConfig(
        encoding=riva_audio_pb2.LINEAR_PCM,
        language_code=SOURCE_LANGUAGE,
        max_alternatives=1,
        enable_automatic_punctuation=True,
        sample_rate_hertz=ASR_SAMPLE_RATE,
        audio_channel_count=1,
    )
    streaming_asr = riva_asr_pb2.StreamingRecognitionConfig(
        config=asr_cfg,
        interim_results=True,
    )
    translation_cfg = riva_nmt_pb2.TranslationConfig(
        source_language_code=SOURCE_LANGUAGE,
        target_language_code=target_lang,
    )
    tts_cfg = riva_nmt_pb2.SynthesizeSpeechConfig(
        encoding=riva_audio_pb2.LINEAR_PCM,
        language_code=target_lang,
        voice_name=voice,
        sample_rate_hz=TTS_SAMPLE_RATE,
    )
    return riva_nmt_pb2.StreamingTranslateSpeechToSpeechConfig(
        asr_config=streaming_asr,
        translation_config=translation_cfg,
        tts_config=tts_cfg,
    )


def _extract_transcripts(resp) -> Dict[str, str]:
    """Best-effort pull of source/translated transcripts from a S2S response.

    Riva has shuffled these field names across versions -- check a couple of
    likely locations and return whatever we find. Missing fields are fine;
    the UI degrades to audio-only playback.
    """
    out: Dict[str, str] = {}
    # Speech-to-text sub-message (seen in 2.15+): `speech_to_text.results[*]`
    stt = getattr(resp, "speech_to_text", None)
    if stt is not None:
        for result in getattr(stt, "results", []):
            alts = getattr(result, "alternatives", None) or []
            if not alts:
                continue
            transcript = getattr(alts[0], "transcript", "") or ""
            if transcript:
                key = "final" if getattr(result, "is_final", False) else "partial"
                out[key] = transcript
    # Some builds expose translated text on `speech.meta.processed_text`
    speech = getattr(resp, "speech", None)
    if speech is not None:
        meta = getattr(speech, "meta", None)
        if meta is not None:
            txt = getattr(meta, "processed_text", "") or ""
            if txt:
                out["translated"] = txt
    return out


def _run_riva_session(
    room: Room,
    pump: AudioPump,
    target_lang: str,
    voice: str,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Blocking worker that relays audio/text between Riva and the room."""

    LOG.info(
        "[room=%s] starting Riva S2S (target=%s voice=%s)",
        room.name, target_lang, voice,
    )
    try:
        auth = riva.client.Auth(uri=RIVA_URI)
        nmt = riva.client.NeuralMachineTranslationClient(auth)
        streaming_cfg = _build_streaming_config(target_lang, voice)

        responses = nmt.streaming_s2s_response_generator(
            audio_chunks=pump,
            streaming_config=streaming_cfg,
        )

        for resp in responses:
            audio = b""
            speech = getattr(resp, "speech", None)
            if speech is not None:
                audio = getattr(speech, "audio", b"") or b""
            if audio:
                asyncio.run_coroutine_threadsafe(
                    _broadcast_audio(room, audio), loop
                )
            transcripts = _extract_transcripts(resp)
            if transcripts:
                asyncio.run_coroutine_threadsafe(
                    _broadcast_transcripts(room, transcripts), loop
                )
    except grpc.RpcError as exc:
        LOG.warning("[room=%s] Riva gRPC error: %s", room.name, exc)
        asyncio.run_coroutine_threadsafe(
            _broadcast_status(room, f"Riva error: {exc.code().name if hasattr(exc, 'code') else exc}"),
            loop,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[room=%s] Riva worker crashed", room.name)
        asyncio.run_coroutine_threadsafe(
            _broadcast_status(room, f"internal error: {exc}"),
            loop,
        )
    finally:
        LOG.info("[room=%s] Riva worker exiting", room.name)


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
        "target_languages": TARGET_LANGUAGES,
        "default_voices": DEFAULT_VOICES,
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
        "target_lang": room.target_lang,
    })


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---- WebSockets ------------------------------------------------------------


@app.websocket("/ws/speaker/{room_name}")
async def ws_speaker(
    ws: WebSocket,
    room_name: str,
    target_lang: str = "es-US",
    voice: Optional[str] = None,
) -> None:
    await ws.accept()
    if target_lang not in {l["code"] for l in TARGET_LANGUAGES}:
        await ws.close(code=1008, reason=f"unsupported target_lang {target_lang}")
        return

    resolved_voice = voice or DEFAULT_VOICES.get(target_lang, "")
    room = await get_or_create_room(room_name)

    async with room.lock:
        if room.has_speaker():
            await ws.close(code=1008, reason="room already has a speaker")
            return
        room.speaker = ws
        room.target_lang = target_lang
        room.voice = resolved_voice
        room.pump = AudioPump()
        loop = asyncio.get_running_loop()
        room.worker = threading.Thread(
            target=_run_riva_session,
            args=(room, room.pump, target_lang, resolved_voice, loop),
            name=f"riva-{room_name}",
            daemon=True,
        )
        room.worker.start()

    LOG.info(
        "[room=%s] speaker connected (target=%s, listener_present=%s)",
        room_name, target_lang, room.has_listener(),
    )
    await _safe_send_json(ws, {
        "type": "status",
        "text": f"Connected as speaker. Target language: {target_lang}.",
    })
    await _broadcast_status(room, "speaker connected")

    try:
        while True:
            msg = await ws.receive()
            if "bytes" in msg and msg["bytes"] is not None:
                if room.pump is not None:
                    room.pump.put(msg["bytes"])
            elif msg.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        LOG.exception("[room=%s] speaker socket error", room_name)
    finally:
        LOG.info("[room=%s] speaker disconnected", room_name)
        async with room.lock:
            room.speaker = None
            if room.pump is not None:
                room.pump.close()
            room.pump = None
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
        async with room.lock:
            room.listener = None
        await _broadcast_status(room, "listener disconnected")
