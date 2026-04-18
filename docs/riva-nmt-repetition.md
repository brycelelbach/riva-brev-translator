# Riva NMT: runaway repetition on low-information input

## Observation

During testing we see the English side occasionally emit a single short
sentence repeated dozens of times, e.g.:

> "I am a student of the University of California, Berkeley. I am a student
> of the University of California, Berkeley. I am a student of the University
> of California, Berkeley. [...]"

The repeated sentence is topically unrelated to anything being spoken, and
the loop only ends when the decoder hits its output-length budget.

## Likely cause

Two well-known failure modes of autoregressive NMT, compounding:

1. **Decoder hallucination on weak source.** When the ASR transcript fed
   into NMT is short, garbled, mostly-silence, or otherwise low-information,
   the encoder produces a vague context vector. The decoder, unconstrained
   by the source, falls back to high-prior English sentences from its
   training distribution. Generic biographical/institutional sentences
   ("I am a student of the University of California, Berkeley", "Thank you
   for watching this video", etc.) are over-represented in web-scraped
   parallel corpora (OPUS, CCMatrix, WMT) and have very high unconditional
   likelihood. That is why the specific sentence is so random-looking — it
   is not coming from the source at all; it is a training-set artifact.

2. **Repetition degeneration under greedy / low-beam decoding.** Once the
   decoder emits "...Berkeley." the n-gram state strongly predicts "I am a
   student..." next — the model saw the same sentence many times in
   training, so the transition from the end of the sentence back to its
   start is a high-probability cycle. Riva's streaming NMT uses greedy or
   very small-beam decoding for latency and, based on the output, does not
   appear to apply a repetition penalty or n-gram blocking. The decoder
   keeps generating until the `max_target_length` budget is exhausted,
   filling it with copies of the trapped cycle.

Both pathologies are documented in the literature (Holtzman et al. 2019 on
neural text degeneration; Stahlberg & Byrne 2019 on NMT hallucination).
They occur in production ASR→NMT pipelines whenever the source signal is
weak.

## What in our setup makes it more likely

- **Long end-of-utterance threshold.** We raised `S2S_STOP_HISTORY_EOU_MS`
  to 3500 ms so NMT sees fuller clauses. When the speaker pauses or the
  room is noisy, this can also produce a long zh-CN transcript that is
  mostly filler ("嗯", "那个", "就是") or empty — low-information input
  that is precisely the hallucination trigger.
- **Code-switching errors (see riva-code-switching.md).** When the ASR
  mishears English loanwords as Chinese syllables, the resulting zh-CN
  transcript is partially nonsense; the information that reaches NMT is
  degraded even when audio was present.
- **Streaming decode with aggressive latency budget.** Streaming NMT is
  tuned for low-latency incremental output, which trades off decode-time
  safety nets (large beams, repetition penalties, length normalization)
  that offline NMT would apply.

## Possible mitigations

1. **Client-side runaway detection** — drop any final translation where
   a short n-gram repeats more than N times. Cheap, doesn't require Riva
   changes; does hide the problem rather than fix it.
2. **Voice-activity gating on the input.** If we pre-filter audio through
   a VAD and only forward active speech to Riva, we starve the pipeline
   of the silence/filler chunks that trigger hallucination. Adds a
   dependency and some latency.
3. **Shorter EOU or confidence-gated commits.** Reverting to a shorter
   EOU (say 2000 ms) reduces the window in which weak audio accumulates,
   at the cost of choppier NMT output.
4. **LLM post-edit.** A small LLM pass on the NMT output can detect and
   collapse repeated sentences, and optionally rewrite incoherent spans.
   Drastic quality lift but adds 200-500 ms latency and an API cost.
5. **Offline NMT with repetition penalty.** Not available in Riva 2.19's
   streaming S2S API. Would require rolling our own ASR→NMT pipeline.

## Status

Documented, not fixed. The behavior is a property of Riva 2.19's streaming
NMT decoder and appears when the audio signal is weak; it is not
specifically broken in our client or pipeline configuration. Mitigation 1
(client-side runaway detection) is the cheapest if we want to hide the
symptom without changing the model path.
