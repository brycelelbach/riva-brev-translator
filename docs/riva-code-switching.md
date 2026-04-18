# Riva 2.19 zh-CN ASR: code-switching limitation

## Observation

During testing on a technical AI/ML talk (Mooncake inference system, ~3½ minutes
of spoken Chinese with frequent English loanwords), the English translation
produced by Riva's `StreamingTranslateSpeechToSpeech` pipeline was
semantically disconnected from the source. Representative outputs:

- "The computer area and the Chen vinegar area reach the network card used to connect" (the speaker actually said "KV cache" in this utterance)
- "Mobile driver's license. We can draw some new lines specifically"
- "biochemical things they are getting bigger"
- "a cross on the TV"

Speaker-audio diagnostics (per-second byte counts and peak int16 sample values,
logged to the session JSONL) showed continuous active audio throughout the
session with no silent buckets and no dropouts, so the problem was not mic
input, WebSocket stalls, or client-side audio suspension.

## Likely cause

The Riva 2.19 zh-CN ASR deployed by the Quick Start is a monolingual Chinese
acoustic model. Technical talks of this kind tend to intersperse English
loanwords (model names, library names, API terms). The monolingual model has
no phonetic mapping for those English segments and appears to either drop them
or force-fit them to the nearest Chinese syllables; NMT then translates those
nonsensical Chinese fragments into unrelated English.

The result is that technical vocabulary is systematically invisible in the
output — not because of translation failure but because the ASR never produced
a faithful source transcript in the first place. Non-technical phrases
recovered reasonably well in the same session ("thanks", "question", "Hello, I
ask two questions", "the second question is that"), which is consistent with
the monolingual-ASR hypothesis.

A useful contrast: a parallel Google Translate pass on the same audio
rendered technical terms like "KV cache" correctly in its English output,
while our pipeline produced none of them. The speaker did not necessarily
utter the English string "KV cache" verbatim — they may have said it with
Chinese intonation, or used a Chinese-language rendering of the term.
Either way, a bilingual-aware ASR (like Google's) resolved the intent
back to "KV cache"; the monolingual zh-CN ASR in our pipeline either
force-fit those segments to the nearest Chinese syllable or dropped them,
and NMT has no way to reconstruct what was never transcribed. That
Google Translate on the same audio produced the term is itself evidence
the information was present in the source — the failure is specifically
at the monolingual-ASR stage.

## What doesn't fix it

Endpointing (`stop_history`, `stop_history_eou`), client jitter-buffer size,
and the S2S vs S2T pipeline choice were all tuned during debugging. They
affect fragmentation, latency, and audible stutter, but none of them change
what the ASR hears. Code-switching garbage is produced at the ASR stage and
propagates unchanged through every downstream knob.

## Possible mitigations

1. **Word boost** (`RecognitionConfig.speech_contexts` with `phrases` and a
   `boost` weight). Riva supports phrase hints. Giving the ASR a list of the
   relevant English technical terms up-front biases it toward recognizing
   them. Effectiveness for English-embedded-in-Chinese is uncertain — boost
   helps when the model can produce the phoneme sequence but assigns it low
   prior probability, and is less useful when the acoustic model simply lacks
   coverage for the phonemes.
2. **Code-switching ASR model**. Not shipped in the Riva 2.19 Quick Start.
   Would require deploying a custom model; out of scope for the demo.
3. **Two-pass ASR** (parallel zh-CN and en-US streams, pick the
   higher-confidence segments). Doubles GPU cost; Riva's streaming API isn't
   designed for this and stitching the outputs coherently is non-trivial.
4. **Accept the limitation in the demo scope**. For conversational or
   lecture-style Chinese speech without heavy technical vocabulary, the
   pipeline performs reasonably. Technical bilingual talks are out of the
   demo's useful range.

## Status

Documented, not fixed. The launchable's primary audience is users demoing
real-time S2S — non-technical Chinese speech is the supported path.
