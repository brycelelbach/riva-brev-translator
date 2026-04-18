// Riva Real-Time Translator — client-side controller.
//
// Single duplex flow. One WebSocket at /ws/session/{id}: mic audio (int16
// mono PCM @ 16 kHz) goes up, translated English audio (int16 mono PCM @
// 44.1 kHz) comes down alongside JSON transcript/status frames. The
// session id is generated client-side for log correlation only. A mute
// toggle silences local playback without stopping the session.

const LISTENER_SAMPLE_RATE = 44100;
const SPEAKER_SAMPLE_RATE = 16000;

const els = {
  duplexStart: document.getElementById("duplex-start"),
  duplexStop: document.getElementById("duplex-stop"),
  duplexStatus: document.getElementById("duplex-status"),
  duplexInput: document.getElementById("duplex-input"),
  duplexOutput: document.getElementById("duplex-output"),
  duplexOutputRow: document.getElementById("duplex-output-row"),
  duplexOutputHint: document.getElementById("duplex-output-hint"),
  duplexLevelBar: document.getElementById("duplex-level-bar"),
  duplexPartialSource: document.getElementById("duplex-partial-source"),
  duplexFinalSource: document.getElementById("duplex-final-source"),
  duplexPartialTranslation: document.getElementById("duplex-partial-translation"),
  duplexTranslationList: document.getElementById("duplex-translation-list"),
  muteOutput: document.getElementById("mute-output"),
  statusLog: document.getElementById("status-log"),
};

const sessionId = generateSessionId();

(function init() {
  els.duplexStart.onclick = () => beginDuplex();
  els.duplexStop.onclick = () => endDuplex();
  els.duplexInput.onchange = () => {
    if (duplexState) rebuildDuplexCapture();
  };
  els.duplexOutput.onchange = async () => {
    if (!duplexState) return;
    const deviceId = els.duplexOutput.value || "";
    try {
      if (typeof duplexState.playbackCtx.setSinkId === "function") {
        await duplexState.playbackCtx.setSinkId(deviceId);
      } else if (duplexState.audioEl) {
        await duplexState.audioEl.setSinkId(deviceId);
      } else {
        return;
      }
      log(`output switched to "${els.duplexOutput.selectedOptions[0].text}"`);
    } catch (err) {
      log(`setSinkId failed: ${err.message}`);
    }
  };
  els.muteOutput.onchange = () => applyMute();
  log(`session ${sessionId}`);
})();

function generateSessionId() {
  if (window.crypto && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID().split("-")[0];
  }
  return Math.random().toString(36).slice(2, 10);
}

// ---------------------------------------------------------------------------
// Screen wake lock — hold the screen on while a session is active. Mobile
// browsers otherwise suspend the AudioContext and drop the WebSocket after
// ~30 s of screen-off.

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
// Duplex (single device: mic + speaker)

let duplexState = null;
// Decided in beginDuplex(): true = route playback through an <audio> element
// (required on mobile so Android's media-session output switcher has a hook);
// false = wire the audio graph straight to playbackCtx.destination and pick
// the sink with AudioContext.setSinkId().
let duplexUseAudioElementRoute = true;

async function beginDuplex() {
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    alert("This browser does not expose getUserMedia. Use Chrome/Edge over HTTPS.");
    return;
  }

  els.duplexStart.disabled = true;
  els.duplexStop.disabled = false;
  els.duplexPartialSource.textContent = "";
  els.duplexFinalSource.innerHTML = "";
  els.duplexPartialTranslation.textContent = "";
  els.duplexTranslationList.innerHTML = "";
  els.duplexStatus.textContent = "Connecting...";

  // Probe for mic permission so enumerateDevices() returns labels.
  let probe;
  try {
    probe = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
  } catch (err) {
    log(`Microphone access denied: ${err.message}`);
    alert(`Microphone access denied: ${err.message}`);
    els.duplexStart.disabled = false;
    els.duplexStop.disabled = true;
    els.duplexStatus.textContent = "";
    return;
  }
  probe.getTracks().forEach((t) => t.stop());

  const { outputs } = await populateDuplexDevices();
  navigator.mediaDevices.addEventListener("devicechange", populateDuplexDevices);

  // Output-device picking needs a sink-selection API AND a non-empty output
  // list. On Android/iOS enumerateDevices returns no audiooutputs even with
  // BT paired — routing is system-level, not per-app — so we hide the picker
  // and direct the user to the OS output switcher.
  const audioCtxSinkSupported = typeof AudioContext !== "undefined"
    && "setSinkId" in AudioContext.prototype;
  const audioElSinkSupported = typeof HTMLMediaElement !== "undefined"
    && typeof HTMLMediaElement.prototype.setSinkId === "function";
  const sinkApiSupported = audioCtxSinkSupported || audioElSinkSupported;
  const canPickOutput = sinkApiSupported && outputs.length > 0;
  duplexUseAudioElementRoute = !(canPickOutput && audioCtxSinkSupported);
  if (!canPickOutput) {
    els.duplexOutputRow.classList.add("hidden");
    els.duplexOutputHint.innerHTML =
      "This browser doesn't let the page list audio output devices " +
      "(Android and iOS route audio at the OS level, not per-app). " +
      "Connect Bluetooth / wired headphones before starting. " +
      "After audio begins you can also tap the output-switcher icon in " +
      "the media notification (pull-down shade) to move audio between " +
      "speaker, earpiece, and connected devices.";
  } else {
    els.duplexOutputRow.classList.remove("hidden");
    els.duplexOutputHint.textContent = "";
  }

  // --- Capture side: mic -> worklet -> WebSocket ---------------------------
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
        // Leave voice processing OFF: Android Chrome otherwise promotes the
        // stream into VOICE_COMMUNICATION mode, which routes playback through
        // the earpiece/speakerphone and bypasses A2DP and wired headphones.
        // Echo is not a concern because duplex users wear headphones.
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

  const ws = new WebSocket(buildWsUrl(`/ws/session/${encodeURIComponent(sessionId)}`));
  ws.binaryType = "arraybuffer";
  ws.addEventListener("open", () => log(`session WS open (id=${sessionId})`));
  ws.addEventListener("close", (ev) => {
    log(`session WS closed (${ev.code} ${ev.reason || ""})`);
    els.duplexStatus.textContent = "Disconnected.";
  });
  ws.addEventListener("error", () => log("session WS error"));
  ws.addEventListener("message", (ev) => onSessionMessage(ev));

  worklet.port.onmessage = (ev) => {
    const { type, buffer, rms } = ev.data;
    if (type === "level") {
      els.duplexLevelBar.style.width = `${Math.min(100, Math.round(rms * 250))}%`;
    } else if (type === "pcm") {
      if (ws.readyState === WebSocket.OPEN) ws.send(buffer);
    }
  };

  // --- Playback side ------------------------------------------------------
  // latencyHint:"playback" signals a media stream (vs "interactive" which can
  // land in the voice-comm pipeline on Android) so the audio honors the
  // system's media-audio routing (BT A2DP, wired headphones).
  const playbackCtx = new (window.AudioContext || window.webkitAudioContext)({
    sampleRate: LISTENER_SAMPLE_RATE,
    latencyHint: "playback",
  });
  try { await playbackCtx.resume(); } catch {}

  // Gain stage lets us mute the output without tearing down the graph.
  const gainNode = playbackCtx.createGain();
  gainNode.gain.value = els.muteOutput.checked ? 0 : 1;

  let outputNode;
  let audioEl = null;
  if (!duplexUseAudioElementRoute && audioCtxSinkSupported) {
    // Desktop path: pick output via AudioContext.setSinkId, route straight
    // to playbackCtx.destination.
    const outputId = els.duplexOutput.value;
    if (outputId) {
      try { await playbackCtx.setSinkId(outputId); }
      catch (err) { log(`AudioContext.setSinkId failed: ${err.message}`); }
    }
    gainNode.connect(playbackCtx.destination);
    outputNode = gainNode;
  } else {
    // Fallback path: route through <audio> fed from a MediaStreamDestination.
    // Needed by (a) browsers that only implement HTMLMediaElement.setSinkId,
    // and (b) mobile browsers where we use the <audio> to attach to Android's
    // media session so the output-switcher chip appears.
    const destNode = playbackCtx.createMediaStreamDestination();
    gainNode.connect(destNode);
    audioEl = new Audio();
    audioEl.autoplay = true;
    audioEl.srcObject = destNode.stream;
    audioEl.muted = els.muteOutput.checked;
    if (!duplexUseAudioElementRoute && audioElSinkSupported) {
      const outputId = els.duplexOutput.value;
      if (outputId) {
        try { await audioEl.setSinkId(outputId); }
        catch (err) { log(`setSinkId failed: ${err.message}`); }
      }
    }
    try { await audioEl.play(); }
    catch (err) { log(`audio.play() failed: ${err.message}`); }
    outputNode = gainNode;
  }

  const playState = {
    audioCtx: playbackCtx,
    outputNode,
    nextStartTime: 0,
    scheduled: 0,
  };

  // Register a MediaSession when we have an <audio> element so Android's
  // media notification (and its output-switcher chip) appears.
  let keepaliveAudio = null;
  if (audioEl) {
    keepaliveAudio = installMediaSession(audioEl);
  }

  duplexState = {
    captureCtx,
    micStream,
    micSource,
    worklet,
    ws,
    playbackCtx,
    gainNode,
    audioEl,
    keepaliveAudio,
    playState,
  };

  acquireWakeLock();
}

function applyMute() {
  if (!duplexState) return;
  const muted = els.muteOutput.checked;
  try { duplexState.gainNode.gain.value = muted ? 0 : 1; } catch {}
  if (duplexState.audioEl) duplexState.audioEl.muted = muted;
  log(muted ? "output muted" : "output unmuted");
}

async function populateDuplexDevices() {
  let devices;
  try {
    devices = await navigator.mediaDevices.enumerateDevices();
  } catch (err) {
    log(`enumerateDevices failed: ${err.message}`);
    return { inputs: [], outputs: [] };
  }
  const inputs = devices.filter((d) => d.kind === "audioinput");
  const outputs = devices.filter((d) => d.kind === "audiooutput");
  fillDeviceSelect(els.duplexInput, inputs, "Default microphone");
  fillDeviceSelect(els.duplexOutput, outputs, "Default speaker");
  return { inputs, outputs };
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

async function rebuildDuplexCapture() {
  if (!duplexState) return;
  const inputId = els.duplexInput.value;
  let newStream;
  try {
    newStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        deviceId: inputId ? { exact: inputId } : undefined,
        channelCount: 1,
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
  src.connect(state.outputNode);
  const now = state.audioCtx.currentTime;
  // 150ms jitter buffer — absorbs Riva's intra-utterance chunk jitter
  // (arrivals p90 ~90ms; smaller buffers stutter inside a single phrase).
  if (state.nextStartTime < now + 0.02) state.nextStartTime = now + 0.15;
  src.start(state.nextStartTime);
  state.nextStartTime += buf.duration;
  state.scheduled += 1;
  if (state.scheduled === 1) {
    els.duplexStatus.textContent = "Playing translated audio.";
  }
}

function onSessionMessage(ev) {
  if (typeof ev.data !== "string") {
    if (duplexState) scheduleDuplexAudio(duplexState.playState, ev.data);
    return;
  }
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
  try { s.gainNode.disconnect(); } catch {}
  if (s.audioEl) {
    try { s.audioEl.pause(); s.audioEl.srcObject = null; } catch {}
    teardownMediaSession();
  }
  if (s.keepaliveAudio) {
    try { s.keepaliveAudio.pause(); s.keepaliveAudio.removeAttribute("src"); } catch {}
  }
  try { s.playbackCtx.close(); } catch {}
  try { s.ws.close(); } catch {}
  els.duplexStart.disabled = false;
  els.duplexStop.disabled = true;
  els.duplexLevelBar.style.width = "0%";
  els.duplexStatus.textContent = "Stopped.";
  releaseWakeLock();
  log("session stopped");
}

// 10s silent WAV loop: works around w3c/mediasession#261 where <audio>
// backed only by srcObject doesn't bind to MediaSession. A parallel
// silent <audio src> gives Chrome Android a src-backed element to anchor
// the session + output-switcher chip to.
const SILENT_WAV_10S = buildSilentWavDataUrl(10);

function buildSilentWavDataUrl(seconds) {
  const sampleRate = 8000;
  const numSamples = sampleRate * seconds;
  const dataSize = numSamples * 2;
  const buf = new ArrayBuffer(44 + dataSize);
  const view = new DataView(buf);
  const writeStr = (off, s) => {
    for (let i = 0; i < s.length; i++) view.setUint8(off + i, s.charCodeAt(i));
  };
  writeStr(0, "RIFF");
  view.setUint32(4, 36 + dataSize, true);
  writeStr(8, "WAVE");
  writeStr(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeStr(36, "data");
  view.setUint32(40, dataSize, true);
  const bytes = new Uint8Array(buf);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return `data:audio/wav;base64,${btoa(binary)}`;
}

function installMediaSession(audioEl) {
  if (!("mediaSession" in navigator)) return null;
  try {
    navigator.mediaSession.metadata = new window.MediaMetadata({
      title: "Live translation",
      artist: "Chinese \u2192 English",
      album: "Riva",
    });
    navigator.mediaSession.playbackState = "playing";
    navigator.mediaSession.setActionHandler("play", () => {
      if (!duplexState || !duplexState.audioEl) return;
      duplexState.audioEl.play().catch(() => {});
      if (duplexState.keepaliveAudio) {
        duplexState.keepaliveAudio.play().catch(() => {});
      }
      navigator.mediaSession.playbackState = "playing";
    });
    navigator.mediaSession.setActionHandler("pause", () => {
      if (!duplexState) return;
      if (duplexState.audioEl) duplexState.audioEl.pause();
      if (duplexState.keepaliveAudio) duplexState.keepaliveAudio.pause();
      navigator.mediaSession.playbackState = "paused";
    });
    navigator.mediaSession.setActionHandler("stop", () => {
      endDuplex();
    });
  } catch (err) {
    log(`MediaSession setup failed: ${err.message}`);
  }
  const keepalive = new Audio();
  keepalive.src = SILENT_WAV_10S;
  keepalive.loop = true;
  keepalive.preload = "auto";
  keepalive.volume = 0;
  keepalive.play().catch((err) => {
    log(`keepalive audio failed: ${err.message}`);
  });
  return keepalive;
}

function teardownMediaSession() {
  if (!("mediaSession" in navigator)) return;
  try {
    navigator.mediaSession.playbackState = "none";
    navigator.mediaSession.metadata = null;
    navigator.mediaSession.setActionHandler("play", null);
    navigator.mediaSession.setActionHandler("pause", null);
    navigator.mediaSession.setActionHandler("stop", null);
  } catch {}
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
