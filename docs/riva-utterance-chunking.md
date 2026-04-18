# Riva streaming S2S: utterance-chunking tradeoffs

## Observation

The TTS output cadence is visibly sensitive to how we configure end-of-
utterance (EOU) detection. Depending on the setting and the speaker's
style we see three distinct failure shapes:

1. **Choppy output (short EOU).** With Riva's zh-CN defaults (~500-1000 ms
   silence to commit) the decoder emits 1-1.5 s utterances — often 2-4
   English words — and TTS renders them in staccato bursts. Listenable but
   jarring, especially on lecture-style speech with short intra-clause
   pauses.
2. **Delayed output (long EOU).** Pushing the threshold to 3500 ms made
   NMT see fuller clauses and smoothed TTS, but the audible lag was
   ~3-4 s between speech and playback, which the user perceived as the
   system lagging. We reverted to 2500 ms as a compromise.
3. **Runaway utterances under acoustic feedback.** When TTS plays through
   the same device's speaker, the mic picks it up and keeps the decoder's
   silence counter from ever firing. A single utterance can grow
   unboundedly, producing one long translated block that feeds its own
   input and drifts into repetition (see `riva-nmt-repetition.md`).

## Why this is a knob, not a solved problem

Riva's `StreamingTranslateSpeechToSpeech` operates per-utterance: ASR
commits a segment, NMT translates that segment as a unit, TTS synthesizes
that translation as a unit. There is no sliding NMT window and no partial
re-translation. That means the utterance boundary — chosen by
`stop_history` / `stop_history_eou` in the ASR endpointing config — is
also the granularity of every downstream stage. The dial trades:

- **Latency to first audio** (shorter EOU wins)
- **Coherence of translated clauses** (longer EOU wins — more source
  context before NMT fires)
- **Resistance to hallucination on weak audio** (shorter EOU wins — less
  time for silence/filler to accumulate; see `riva-nmt-repetition.md`)
- **TTS smoothness** (longer EOU wins — fewer, longer synthesis bursts)

No single setting wins across all four axes.

## Three parallel streams, three endpointing configs

The app runs S2S + S2T (captions) + source zh-CN ASR on the same mic
audio but with separate streams and separate endpointing settings. Each
one independently decides where utterances begin and end, so the captions,
the translated audio, and the source transcript can disagree on
segmentation. Current settings (`app/server.py`):

| Stream | `stop_history` | `stop_history_eou` |
|---|---|---|
| S2S (audio)        | 1500 ms | 2500 ms |
| S2T (captions)     | 1500 ms | 1500 ms |
| Source ASR (panel) | 1500 ms | 1500 ms |

The S2T + source-ASR panes commit sooner than the S2S audio, so captions
and the Chinese transcript typically appear 1-1.5 s before the English
voice plays. That is intentional — text is read, audio is heard, and the
two modalities tolerate different latencies.

## Client-side chunking

On the way in, the browser's AudioWorklet ships ~80 ms frames of int16
mono PCM at 16 kHz over a single WebSocket. On the way out, TTS audio
arrives at 44.1 kHz in chunks whose inter-arrival gap is p90 ~90 ms; we
hold a 150 ms jitter buffer before scheduling playback. Smaller buffers
stutter inside a single TTS phrase; larger buffers push perceived latency
up without fixing anything the EOU knob doesn't also control.

A per-stream `AudioPump` (a `queue.Queue` wrapped in a blocking iterator)
bridges the async WebSocket to the synchronous Riva gRPC iterator. It is
bounded at 256 chunks; in practice the consumer keeps up and the queue
never fills, but if Riva ever stalled the inbound WebSocket would
back-pressure naturally.

## Status

Current configuration (1500 / 2500 ms on S2S, 1500 / 1500 ms on text
streams) is a pragmatic midpoint chosen by ear. There is no setting that
simultaneously wins on latency, coherence, and hallucination-resistance
— it is a pick-two shape of the space, and the right pick depends on
speaker style and acoustic environment.
