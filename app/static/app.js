// Riva Real-Time Translator -- client-side controller.
//
// - Speaker flow: getUserMedia -> AudioContext -> AudioWorklet -> WebSocket
//   (int16 mono PCM @ 16 kHz).
// - Listener flow: WebSocket receives int16 mono PCM @ 44.1 kHz (binary) and
//   JSON transcript/status frames (text); PCM is scheduled onto an
//   AudioContext output in arrival order.

const LISTENER_SAMPLE_RATE = 44100;
const SPEAKER_SAMPLE_RATE = 16000;

const els = {
  room: document.getElementById("room"),
  roleButtons: document.querySelectorAll(".role-button"),
  setup: document.getElementById("setup"),
  speakerPanel: document.getElementById("speaker-panel"),
  listenerPanel: document.getElementById("listener-panel"),
  targetLang: document.getElementById("target-lang"),
  startCapture: document.getElementById("start-capture"),
  stopCapture: document.getElementById("stop-capture"),
  levelBar: document.getElementById("level-bar"),
  partialSource: document.getElementById("partial-source"),
  finalSource: document.getElementById("final-source"),
  translationList: document.getElementById("translation-list"),
  startPlayback: document.getElementById("start-playback"),
  playbackStatus: document.getElementById("playback-status"),
  listenerPartial: document.getElementById("listener-partial"),
  listenerCaptions: document.getElementById("listener-captions"),
  statusLog: document.getElementById("status-log"),
};

let cfg = null;

(async function init() {
  // Hydrate languages dropdown.
  try {
    const r = await fetch("/api/config");
    cfg = await r.json();
    for (const lang of cfg.target_languages) {
      const opt = document.createElement("option");
      opt.value = lang.code;
      opt.textContent = lang.name;
      els.targetLang.appendChild(opt);
    }
  } catch (err) {
    log(`Failed to load /api/config: ${err}`);
  }

  // Restore previous room code if any.
  const saved = localStorage.getItem("room");
  if (saved) els.room.value = saved;

  els.room.addEventListener("input", () => {
    localStorage.setItem("room", els.room.value.trim());
  });

  els.roleButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const role = btn.dataset.role;
      const roomName = (els.room.value || "").trim();
      if (!roomName) {
        alert("Enter a room code first.");
        els.room.focus();
        return;
      }
      els.roleButtons.forEach((b) => b.classList.remove("selected"));
      btn.classList.add("selected");
      if (role === "speaker") startSpeaker(roomName);
      else startListener(roomName);
    });
  });
})();

// ---------------------------------------------------------------------------
// Status log

function log(msg) {
  const li = document.createElement("li");
  const ts = new Date().toLocaleTimeString();
  li.textContent = `[${ts}] ${msg}`;
  els.statusLog.prepend(li);
  while (els.statusLog.children.length > 40) {
    els.statusLog.removeChild(els.statusLog.lastChild);
  }
}

// ---------------------------------------------------------------------------
// Speaker

let speakerState = null;

async function startSpeaker(roomName) {
  els.speakerPanel.classList.remove("hidden");
  els.listenerPanel.classList.add("hidden");
  log(`Speaker mode, room "${roomName}"`);

  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    alert("This browser does not expose getUserMedia. Use Chrome/Edge/Firefox over HTTPS.");
    return;
  }

  els.startCapture.disabled = false;
  els.stopCapture.disabled = true;

  els.startCapture.onclick = () => startCapture(roomName);
  els.stopCapture.onclick = () => stopCapture();
}

async function startCapture(roomName) {
  els.startCapture.disabled = true;
  els.stopCapture.disabled = false;
  els.partialSource.textContent = "";
  els.finalSource.innerHTML = "";
  els.translationList.innerHTML = "";

  const targetLang = els.targetLang.value;

  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
      video: false,
    });
  } catch (err) {
    log(`Microphone access denied: ${err.message}`);
    alert(`Microphone access denied: ${err.message}`);
    els.startCapture.disabled = false;
    els.stopCapture.disabled = true;
    return;
  }

  const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  try {
    await audioCtx.audioWorklet.addModule("/static/audio-processor.js");
  } catch (err) {
    log(`AudioWorklet failed: ${err.message}. Falling back to no capture.`);
    stream.getTracks().forEach((t) => t.stop());
    return;
  }

  const src = audioCtx.createMediaStreamSource(stream);
  const worklet = new AudioWorkletNode(audioCtx, "capture-processor", {
    processorOptions: { targetSampleRate: SPEAKER_SAMPLE_RATE },
  });
  src.connect(worklet);
  // Do NOT connect worklet to destination -- avoids echo.

  const wsUrl = buildWsUrl(
    `/ws/speaker/${encodeURIComponent(roomName)}?target_lang=${encodeURIComponent(targetLang)}`
  );
  const ws = new WebSocket(wsUrl);
  ws.binaryType = "arraybuffer";

  ws.addEventListener("open", () => log(`speaker WS open (target=${targetLang})`));
  ws.addEventListener("close", (ev) => log(`speaker WS closed (${ev.code} ${ev.reason || ""})`));
  ws.addEventListener("error", () => log("speaker WS error"));
  ws.addEventListener("message", (ev) => onSpeakerMessage(ev));

  worklet.port.onmessage = (ev) => {
    const { type, buffer, rms } = ev.data;
    if (type === "level") {
      const pct = Math.min(100, Math.round(rms * 250));
      els.levelBar.style.width = `${pct}%`;
    } else if (type === "pcm") {
      if (ws.readyState === WebSocket.OPEN) ws.send(buffer);
    }
  };

  speakerState = { stream, audioCtx, worklet, ws };
}

function stopCapture() {
  if (!speakerState) return;
  const { stream, audioCtx, worklet, ws } = speakerState;
  speakerState = null;
  try { worklet.disconnect(); } catch {}
  try { audioCtx.close(); } catch {}
  stream.getTracks().forEach((t) => t.stop());
  try { ws.close(); } catch {}
  els.startCapture.disabled = false;
  els.stopCapture.disabled = true;
  els.levelBar.style.width = "0%";
  log("capture stopped");
}

function onSpeakerMessage(ev) {
  if (typeof ev.data !== "string") return;  // speaker doesn't receive audio
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }
  if (msg.type === "transcript") {
    if (msg.partial) els.partialSource.textContent = msg.partial;
    if (msg.final) {
      els.partialSource.textContent = "";
      appendListItem(els.finalSource, msg.final);
    }
    if (msg.translated) appendListItem(els.translationList, msg.translated);
  } else if (msg.type === "status") {
    log(msg.text || "");
  }
}

// ---------------------------------------------------------------------------
// Listener

let listenerState = null;

async function startListener(roomName) {
  els.listenerPanel.classList.remove("hidden");
  els.speakerPanel.classList.add("hidden");
  log(`Listener mode, room "${roomName}"`);

  els.startPlayback.disabled = false;
  els.playbackStatus.textContent = "";
  els.listenerPartial.textContent = "";
  els.listenerCaptions.innerHTML = "";

  els.startPlayback.onclick = async () => {
    if (listenerState) return;
    await enableListenerPlayback(roomName);
  };
}

async function enableListenerPlayback(roomName) {
  const audioCtx = new (window.AudioContext || window.webkitAudioContext)({
    sampleRate: LISTENER_SAMPLE_RATE,
  });
  // Resume -- required after user gesture on some browsers.
  try { await audioCtx.resume(); } catch {}

  const state = {
    audioCtx,
    ws: null,
    nextStartTime: 0,
    scheduled: 0,
  };
  listenerState = state;

  const wsUrl = buildWsUrl(`/ws/listener/${encodeURIComponent(roomName)}`);
  const ws = new WebSocket(wsUrl);
  ws.binaryType = "arraybuffer";
  state.ws = ws;

  ws.addEventListener("open", () => {
    log(`listener WS open`);
    els.playbackStatus.textContent = "Connected. Waiting for audio...";
    els.startPlayback.disabled = true;
  });
  ws.addEventListener("close", (ev) => {
    log(`listener WS closed (${ev.code} ${ev.reason || ""})`);
    els.playbackStatus.textContent = "Disconnected.";
    els.startPlayback.disabled = false;
    listenerState = null;
  });
  ws.addEventListener("error", () => log("listener WS error"));
  ws.addEventListener("message", (ev) => onListenerMessage(ev, state));
}

function onListenerMessage(ev, state) {
  if (typeof ev.data === "string") {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg.type === "transcript") {
      if (msg.partial) els.listenerPartial.textContent = msg.partial;
      if (msg.translated || msg.final) {
        els.listenerPartial.textContent = "";
        appendListItem(els.listenerCaptions, msg.translated || msg.final);
      }
    } else if (msg.type === "status") {
      log(msg.text || "");
    }
    return;
  }
  // Binary: int16 PCM at LISTENER_SAMPLE_RATE.
  scheduleAudioChunk(state, ev.data);
}

function scheduleAudioChunk(state, arrayBuffer) {
  if (!state || !state.audioCtx) return;
  const int16 = new Int16Array(arrayBuffer);
  if (int16.length === 0) return;
  const float32 = new Float32Array(int16.length);
  for (let i = 0; i < int16.length; i++) {
    float32[i] = int16[i] / (int16[i] < 0 ? 0x8000 : 0x7fff);
  }
  const buf = state.audioCtx.createBuffer(1, float32.length, LISTENER_SAMPLE_RATE);
  buf.copyToChannel(float32, 0, 0);

  const src = state.audioCtx.createBufferSource();
  src.buffer = buf;
  src.connect(state.audioCtx.destination);

  const now = state.audioCtx.currentTime;
  // Keep a small jitter buffer (80 ms ahead of now). If we fall behind,
  // restart the schedule cursor at now.
  if (state.nextStartTime < now + 0.02) state.nextStartTime = now + 0.08;
  src.start(state.nextStartTime);
  state.nextStartTime += buf.duration;
  state.scheduled += 1;
  if (state.scheduled === 1) {
    els.playbackStatus.textContent = "Playing translated audio.";
  }
}

// ---------------------------------------------------------------------------
// Helpers

function appendListItem(listEl, text) {
  if (!text) return;
  const li = document.createElement("li");
  li.textContent = text;
  listEl.prepend(li);
  while (listEl.children.length > 30) {
    listEl.removeChild(listEl.lastChild);
  }
}

function buildWsUrl(path) {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${path}`;
}
