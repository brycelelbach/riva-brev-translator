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
  duplexPanel: document.getElementById("duplex-panel"),
  startCapture: document.getElementById("start-capture"),
  stopCapture: document.getElementById("stop-capture"),
  levelBar: document.getElementById("level-bar"),
  partialSource: document.getElementById("partial-source"),
  finalSource: document.getElementById("final-source"),
  partialTranslation: document.getElementById("partial-translation"),
  translationList: document.getElementById("translation-list"),
  startPlayback: document.getElementById("start-playback"),
  playbackStatus: document.getElementById("playback-status"),
  listenerPartial: document.getElementById("listener-partial"),
  listenerCaptions: document.getElementById("listener-captions"),
  duplexStart: document.getElementById("duplex-start"),
  duplexStop: document.getElementById("duplex-stop"),
  duplexStatus: document.getElementById("duplex-status"),
  duplexInput: document.getElementById("duplex-input"),
  duplexOutput: document.getElementById("duplex-output"),
  duplexOutputHint: document.getElementById("duplex-output-hint"),
  duplexLevelBar: document.getElementById("duplex-level-bar"),
  duplexPartialSource: document.getElementById("duplex-partial-source"),
  duplexFinalSource: document.getElementById("duplex-final-source"),
  duplexPartialTranslation: document.getElementById("duplex-partial-translation"),
  duplexTranslationList: document.getElementById("duplex-translation-list"),
  statusLog: document.getElementById("status-log"),
};

(async function init() {
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
      else if (role === "listener") startListener(roomName);
      else if (role === "duplex") startDuplex(roomName);
    });
  });
})();

// ---------------------------------------------------------------------------
// Screen wake lock -- keep the device awake while a role is active.
//
// Mobile browsers aggressively lock the screen, which suspends the
// AudioContext (listener) or the microphone stream (speaker) and kills the
// WebSocket after ~30 s. Screen Wake Lock API holds the screen on while held;
// releases automatically when the page is hidden. We re-acquire on
// `visibilitychange` if the user tabs back.

let wakeLockSentinel = null;
let wakeLockHandlersBound = false;

async function acquireWakeLock() {
  if (!("wakeLock" in navigator)) {
    log("Screen Wake Lock not supported on this browser; device may sleep.");
    return;
  }
  try {
    wakeLockSentinel = await navigator.wakeLock.request("screen");
    wakeLockSentinel.addEventListener("release", () => {
      wakeLockSentinel = null;
    });
    log("screen wake lock acquired");
  } catch (err) {
    log(`wake lock request failed: ${err.message}`);
  }
  if (!wakeLockHandlersBound) {
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible" && wakeLockSentinel === null) {
        acquireWakeLock();
      }
    });
    wakeLockHandlersBound = true;
  }
}

async function releaseWakeLock() {
  if (wakeLockSentinel) {
    try { await wakeLockSentinel.release(); } catch {}
    wakeLockSentinel = null;
  }
}

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
  els.partialTranslation.textContent = "";
  els.translationList.innerHTML = "";

  acquireWakeLock();

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

  const wsUrl = buildWsUrl(`/ws/speaker/${encodeURIComponent(roomName)}`);
  const ws = new WebSocket(wsUrl);
  ws.binaryType = "arraybuffer";

  ws.addEventListener("open", () => log("speaker WS open (zh-CN → en-US)"));
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
  releaseWakeLock();
  log("capture stopped");
}

function onSpeakerMessage(ev) {
  if (typeof ev.data !== "string") return;  // speaker doesn't receive audio
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }
  if (msg.type === "transcript") {
    if (msg.partial) els.partialSource.textContent = msg.partial;
    if (msg.partial_translated) els.partialTranslation.textContent = msg.partial_translated;
    if (msg.final) {
      els.partialSource.textContent = "";
      appendListItem(els.finalSource, msg.final);
    }
    if (msg.translated) {
      els.partialTranslation.textContent = "";
      appendListItem(els.translationList, msg.translated);
    }
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
  acquireWakeLock();

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
    releaseWakeLock();
  });
  ws.addEventListener("error", () => log("listener WS error"));
  ws.addEventListener("message", (ev) => onListenerMessage(ev, state));
}

function onListenerMessage(ev, state) {
  if (typeof ev.data === "string") {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg.type === "transcript") {
      if (msg.partial_translated) els.listenerPartial.textContent = msg.partial_translated;
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
// Duplex (one device: input + output with device pickers)
//
// Opens both /ws/speaker and /ws/listener in the same tab. Mic audio flows
// through an AudioWorklet to the speaker WS; translated audio from the
// listener WS is scheduled onto an AudioContext routed via a
// MediaStreamDestination into a hidden <audio> element so we can pick the
// physical output with HTMLMediaElement.setSinkId().

let duplexState = null;

async function startDuplex(roomName) {
  els.duplexPanel.classList.remove("hidden");
  els.speakerPanel.classList.add("hidden");
  els.listenerPanel.classList.add("hidden");
  log(`Duplex mode, room "${roomName}"`);

  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    alert("This browser does not expose getUserMedia. Use Chrome/Edge over HTTPS.");
    return;
  }

  // Probe for mic permission so device labels populate.
  let probe;
  try {
    probe = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
  } catch (err) {
    log(`Microphone access denied: ${err.message}`);
    alert(`Microphone access denied: ${err.message}`);
    return;
  }
  probe.getTracks().forEach((t) => t.stop());

  await populateDuplexDevices();
  navigator.mediaDevices.addEventListener("devicechange", populateDuplexDevices);

  const sinkSupported = typeof HTMLMediaElement !== "undefined"
    && typeof HTMLMediaElement.prototype.setSinkId === "function";
  if (!sinkSupported) {
    els.duplexOutput.disabled = true;
    els.duplexOutputHint.textContent =
      "Output device selection isn't supported on this browser (common on mobile). " +
      "Audio follows the system default — plug in headphones or connect Bluetooth " +
      "before tapping Start.";
  } else {
    els.duplexOutputHint.textContent = "";
  }

  els.duplexStart.disabled = false;
  els.duplexStop.disabled = true;

  els.duplexStart.onclick = () => beginDuplex(roomName);
  els.duplexStop.onclick = () => endDuplex();

  els.duplexInput.onchange = () => {
    if (duplexState) rebuildDuplexCapture();
  };
  els.duplexOutput.onchange = async () => {
    if (duplexState && duplexState.audioEl && sinkSupported) {
      try {
        await duplexState.audioEl.setSinkId(els.duplexOutput.value || "");
        log(`output switched to "${els.duplexOutput.selectedOptions[0].text}"`);
      } catch (err) {
        log(`setSinkId failed: ${err.message}`);
      }
    }
  };
}

async function populateDuplexDevices() {
  let devices;
  try {
    devices = await navigator.mediaDevices.enumerateDevices();
  } catch (err) {
    log(`enumerateDevices failed: ${err.message}`);
    return;
  }
  fillDeviceSelect(
    els.duplexInput,
    devices.filter((d) => d.kind === "audioinput"),
    "Default microphone",
  );
  fillDeviceSelect(
    els.duplexOutput,
    devices.filter((d) => d.kind === "audiooutput"),
    "Default speaker",
  );
}

function fillDeviceSelect(selectEl, devices, defaultLabel) {
  const prev = selectEl.value;
  selectEl.innerHTML = "";
  const defOpt = document.createElement("option");
  defOpt.value = "";
  defOpt.textContent = defaultLabel;
  selectEl.appendChild(defOpt);
  devices.forEach((d, i) => {
    const opt = document.createElement("option");
    opt.value = d.deviceId;
    opt.textContent = d.label || `Device ${i + 1}`;
    selectEl.appendChild(opt);
  });
  if (prev && Array.from(selectEl.options).some((o) => o.value === prev)) {
    selectEl.value = prev;
  }
}

async function beginDuplex(roomName) {
  els.duplexStart.disabled = true;
  els.duplexStop.disabled = false;
  els.duplexPartialSource.textContent = "";
  els.duplexFinalSource.innerHTML = "";
  els.duplexPartialTranslation.textContent = "";
  els.duplexTranslationList.innerHTML = "";
  els.duplexStatus.textContent = "Connecting...";

  // --- Capture side: mic -> worklet -> /ws/speaker --------------------------
  const captureCtx = new (window.AudioContext || window.webkitAudioContext)();
  try {
    await captureCtx.audioWorklet.addModule("/static/audio-processor.js");
  } catch (err) {
    log(`AudioWorklet failed: ${err.message}`);
    try { captureCtx.close(); } catch {}
    els.duplexStart.disabled = false;
    els.duplexStop.disabled = true;
    return;
  }

  const inputId = els.duplexInput.value;
  let micStream;
  try {
    micStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        deviceId: inputId ? { exact: inputId } : undefined,
        channelCount: 1,
        // IMPORTANT: leave these OFF in duplex mode. Android Chrome promotes
        // getUserMedia with AEC/NS/AGC into VOICE_COMMUNICATION mode, which
        // routes playback to the phone earpiece/speakerphone and bypasses
        // Bluetooth A2DP and wired headphones. Turning them off keeps the
        // page in MEDIA mode so headphones and BT devices receive the audio.
        // Echo isn't a concern here because duplex users listen on
        // headphones; no mic-feedback loop.
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false,
      },
      video: false,
    });
  } catch (err) {
    log(`Microphone access failed: ${err.message}`);
    try { captureCtx.close(); } catch {}
    els.duplexStart.disabled = false;
    els.duplexStop.disabled = true;
    return;
  }

  const worklet = new AudioWorkletNode(captureCtx, "capture-processor", {
    processorOptions: { targetSampleRate: SPEAKER_SAMPLE_RATE },
  });
  const micSource = captureCtx.createMediaStreamSource(micStream);
  micSource.connect(worklet);

  const speakerWs = new WebSocket(
    buildWsUrl(`/ws/speaker/${encodeURIComponent(roomName)}`),
  );
  speakerWs.binaryType = "arraybuffer";
  speakerWs.addEventListener("open", () => log("speaker WS open (duplex)"));
  speakerWs.addEventListener("close", (ev) =>
    log(`speaker WS closed (${ev.code} ${ev.reason || ""})`));
  speakerWs.addEventListener("error", () => log("speaker WS error"));
  speakerWs.addEventListener("message", (ev) => onDuplexSpeakerMessage(ev));

  worklet.port.onmessage = (ev) => {
    const { type, buffer, rms } = ev.data;
    if (type === "level") {
      els.duplexLevelBar.style.width = `${Math.min(100, Math.round(rms * 250))}%`;
    } else if (type === "pcm") {
      if (speakerWs.readyState === WebSocket.OPEN) speakerWs.send(buffer);
    }
  };

  // --- Playback side: /ws/listener -> AudioContext -> <audio>.setSinkId ----
  // latencyHint:"playback" signals a media stream (vs "interactive" which can
  // land in the voice-comm pipeline on Android) so the audio honors the
  // system's media-audio routing (BT A2DP, wired headphones).
  const playbackCtx = new (window.AudioContext || window.webkitAudioContext)({
    sampleRate: LISTENER_SAMPLE_RATE,
    latencyHint: "playback",
  });
  try { await playbackCtx.resume(); } catch {}
  const destNode = playbackCtx.createMediaStreamDestination();

  const audioEl = new Audio();
  audioEl.autoplay = true;
  audioEl.srcObject = destNode.stream;
  const outputId = els.duplexOutput.value;
  if (outputId && typeof audioEl.setSinkId === "function") {
    try { await audioEl.setSinkId(outputId); }
    catch (err) { log(`setSinkId failed: ${err.message}`); }
  }
  try { await audioEl.play(); }
  catch (err) { log(`audio.play() failed: ${err.message}`); }

  const playState = {
    audioCtx: playbackCtx,
    destNode,
    nextStartTime: 0,
    scheduled: 0,
  };

  const listenerWs = new WebSocket(
    buildWsUrl(`/ws/listener/${encodeURIComponent(roomName)}`),
  );
  listenerWs.binaryType = "arraybuffer";
  listenerWs.addEventListener("open", () => {
    log("listener WS open (duplex)");
    els.duplexStatus.textContent = "Connected. Waiting for audio...";
  });
  listenerWs.addEventListener("close", (ev) => {
    log(`listener WS closed (${ev.code} ${ev.reason || ""})`);
    els.duplexStatus.textContent = "Disconnected.";
  });
  listenerWs.addEventListener("error", () => log("listener WS error"));
  listenerWs.addEventListener("message", (ev) => {
    if (typeof ev.data === "string") return;  // transcripts shown via speaker WS
    scheduleDuplexAudio(playState, ev.data);
  });

  duplexState = {
    captureCtx,
    micStream,
    micSource,
    worklet,
    speakerWs,
    playbackCtx,
    destNode,
    audioEl,
    listenerWs,
    playState,
    roomName,
  };

  acquireWakeLock();
}

async function rebuildDuplexCapture() {
  if (!duplexState) return;
  const inputId = els.duplexInput.value;
  let newStream;
  try {
    newStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        deviceId: inputId ? { exact: inputId } : undefined,
        channelCount: 1,
        // Keep voice-processing off in duplex — see beginDuplex() comment.
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false,
      },
      video: false,
    });
  } catch (err) {
    log(`switching microphone failed: ${err.message}`);
    return;
  }
  try { duplexState.micSource.disconnect(); } catch {}
  duplexState.micStream.getTracks().forEach((t) => t.stop());
  const newSource = duplexState.captureCtx.createMediaStreamSource(newStream);
  newSource.connect(duplexState.worklet);
  duplexState.micSource = newSource;
  duplexState.micStream = newStream;
  log(`microphone switched to "${els.duplexInput.selectedOptions[0].text}"`);
}

function scheduleDuplexAudio(state, arrayBuffer) {
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
  src.connect(state.destNode);
  const now = state.audioCtx.currentTime;
  if (state.nextStartTime < now + 0.02) state.nextStartTime = now + 0.08;
  src.start(state.nextStartTime);
  state.nextStartTime += buf.duration;
  state.scheduled += 1;
  if (state.scheduled === 1) {
    els.duplexStatus.textContent = "Playing translated audio.";
  }
}

function onDuplexSpeakerMessage(ev) {
  if (typeof ev.data !== "string") return;
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }
  if (msg.type === "transcript") {
    if (msg.partial) els.duplexPartialSource.textContent = msg.partial;
    if (msg.partial_translated) {
      els.duplexPartialTranslation.textContent = msg.partial_translated;
    }
    if (msg.final) {
      els.duplexPartialSource.textContent = "";
      appendListItem(els.duplexFinalSource, msg.final);
    }
    if (msg.translated) {
      els.duplexPartialTranslation.textContent = "";
      appendListItem(els.duplexTranslationList, msg.translated);
    }
  } else if (msg.type === "status") {
    log(msg.text || "");
  }
}

function endDuplex() {
  if (!duplexState) {
    els.duplexStart.disabled = false;
    els.duplexStop.disabled = true;
    return;
  }
  const s = duplexState;
  duplexState = null;
  try { s.worklet.disconnect(); } catch {}
  try { s.micSource.disconnect(); } catch {}
  try { s.captureCtx.close(); } catch {}
  s.micStream.getTracks().forEach((t) => t.stop());
  try { s.speakerWs.close(); } catch {}
  try { s.audioEl.pause(); s.audioEl.srcObject = null; } catch {}
  try { s.playbackCtx.close(); } catch {}
  try { s.listenerWs.close(); } catch {}
  els.duplexStart.disabled = false;
  els.duplexStop.disabled = true;
  els.duplexLevelBar.style.width = "0%";
  els.duplexStatus.textContent = "Stopped.";
  releaseWakeLock();
  log("duplex stopped");
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
