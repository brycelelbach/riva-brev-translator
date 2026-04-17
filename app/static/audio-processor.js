// AudioWorklet that converts microphone audio to 16 kHz mono int16 PCM
// chunks and posts them back to the main thread. It also posts a crude RMS
// level every frame for the UI meter.
//
// The browser delivers 128-sample Float32 frames at whatever the input
// AudioContext sample rate is (typically 48000). We downsample with a
// sinc-free polyphase using naive averaging + linear interpolation, which is
// adequate for speech intelligibility at 16 kHz.

class CaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const targetRate = (options && options.processorOptions && options.processorOptions.targetSampleRate) || 16000;
    this._targetRate = targetRate;
    this._ratio = sampleRate / targetRate;
    this._residual = 0;
    this._buffer = [];
    this._samplesPerChunk = Math.floor(targetRate * 0.08); // ~80 ms chunks
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || input.length === 0) return true;
    const channel = input[0];
    if (!channel) return true;

    // Compute RMS level for the UI meter.
    let sumSq = 0;
    for (let i = 0; i < channel.length; i++) {
      const v = channel[i];
      sumSq += v * v;
    }
    const rms = Math.sqrt(sumSq / channel.length);
    this.port.postMessage({ type: "level", rms });

    // Downsample.
    let idx = this._residual;
    while (idx < channel.length) {
      const i0 = Math.floor(idx);
      const i1 = Math.min(i0 + 1, channel.length - 1);
      const frac = idx - i0;
      const sample = channel[i0] * (1 - frac) + channel[i1] * frac;
      // Clip and convert to int16.
      let s = Math.max(-1, Math.min(1, sample));
      this._buffer.push(s < 0 ? s * 0x8000 : s * 0x7fff);
      idx += this._ratio;
    }
    this._residual = idx - channel.length;

    while (this._buffer.length >= this._samplesPerChunk) {
      const chunk = this._buffer.splice(0, this._samplesPerChunk);
      const int16 = new Int16Array(chunk.length);
      for (let i = 0; i < chunk.length; i++) int16[i] = chunk[i] | 0;
      this.port.postMessage({ type: "pcm", buffer: int16.buffer }, [int16.buffer]);
    }
    return true;
  }
}

registerProcessor("capture-processor", CaptureProcessor);
