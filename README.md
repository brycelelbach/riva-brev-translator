# Riva Speech Translator — Brev Launchable

Real-time speech-to-speech translation using NVIDIA Riva's
`StreamingTranslateSpeechToSpeech` gRPC API. Source and target languages
are configured in `.env` at bootstrap time (default: Chinese → English).
Three docker-compose services:

| Service       | What it does                                                       | Ports |
|---|---|---|
| `riva`        | NVIDIA Riva Speech Server (ASR + NMT + TTS)                        | `50051` gRPC |
| `app`         | FastAPI web UI that pairs a speaker device with a listener device  | `8081` HTTP |
| `cloudflared` | Cloudflare quick tunnel → publishes the app at `https://*.trycloudflare.com` | — |

Open the `https://*.trycloudflare.com` URL printed at the end of the deploy
on a device with a mic and speaker, hit **Start**, and speak the configured
source language. The translated audio plays back in real time through the
same device.

The Cloudflare tunnel gives the browser a real CA-signed TLS cert (required
for `getUserMedia`) without needing DNS or security-group changes — convenient
for a single-click launchable.

## Requirements

- An NVIDIA GPU with ≥ 24 GB of VRAM (L4, L40, L40S, A10, A100, H100, …).
  CPU-only is not supported by Riva.
- An NGC API key from <https://ngc.nvidia.com/setup/api-key>.
- Docker 24+ with the NVIDIA Container Toolkit, and Docker Compose v2.
- A host that can reach `nvcr.io`, `api.ngc.nvidia.com`, and
  `*.trycloudflare.com`.
- ~150 GB of free disk for the model repo and container images. On Brev AMIs
  the root volume is often only ~120 GB — point Docker's data-root at a
  larger scratch disk (e.g. `/opt/dlami/nvme`) before running `bootstrap.sh`
  if needed.

## First-time setup (Brev VM or any Linux host)

```bash
# 1. Create .env: paste your NGC key and (optionally) pick the language pair.
cp .env.example .env
$EDITOR .env        # NGC_API_KEY, SOURCE_LANGUAGE, TARGET_LANGUAGE, TARGET_VOICE

# 2. Download Riva + deploy models into a docker volume (takes 30-60 min)
./bootstrap.sh

# 3. Bring up Riva, the app, and the Cloudflare tunnel
docker compose up -d

# 4. Grab the public URL
docker logs riva-translator-tunnel 2>&1 \
    | grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' | head -1
```

`scripts/deploy-on-vm.sh` runs steps 2-4 end-to-end and prints the URL.

### Opening the UI

Navigate to the `https://*.trycloudflare.com` URL, hit **Start**, and begin
speaking the source language. Translated audio plays back through the same
device. "Listen only" mutes local output (for debugging); "Echo cancellation"
breaks the speaker-to-mic feedback loop when using built-in hardware.

Quick-tunnel URLs are random and regenerate whenever `cloudflared` restarts.
For a stable URL, replace the `cloudflared` service with a named tunnel
authenticated to a Cloudflare account.

## Creating the Brev Launchable

Brev reads its launchable config from the console wizard, not from a file in
this repo. To publish:

1. Push this repo to GitHub.
2. Go to <https://brev.nvidia.com/launchables/create>.
3. Step 1 **Files and Runtime** — "I have code in a GitHub repository",
   paste the repo URL, runtime = **Docker Compose**.
4. Step 2 **Configure Environment** — paste the GitHub URL of
   `docker-compose.yaml`. Click Validate.
5. Step 3 **Jupyter and Networking** — expose port **8081** (the web app)
   as a public port. That gives the Brev console an "Open App" button
   pointing directly at the UI over Brev's HTTPS endpoint. The
   `cloudflared` service still runs and publishes a `*.trycloudflare.com`
   URL as a fallback; either URL works for `getUserMedia`. Skip Jupyter.
6. Step 4 **Compute** — pick an L4 (24 GB) or larger. Give the VM **≥ 200
   GB** of disk.
7. Step 5 **Publish**.

The launchable still requires the user to provide their own NGC key
(`.env`) and run `./bootstrap.sh` once on first boot — Brev does not ship a
first-class secret store for compose launchables yet, and Riva's model
download is gated on the key.

## Supported languages

Language pair is chosen in `.env` before `./bootstrap.sh` and baked into the
deployed Riva models.

- **`SOURCE_LANGUAGE`** — BCP-47 code for what the user speaks. The default
  `conformer` ASR acoustic model covers: `en-US`, `de-DE`, `es-US`, `es-ES`,
  `fr-FR`, `it-IT`, `ja-JP`, `ko-KR`, `pt-BR`, `ru-RU`, `zh-CN` (per Riva
  2.19's `asr_models_languages_map`). Override with `ASR_ACOUSTIC_MODEL`
  (e.g. `parakeet-1.1b` for higher-quality English-only).
- **`TARGET_LANGUAGE`** — BCP-47 code for the output. NMT (Megatron 1B
  any-to-any) supports 36 languages; TTS is the bottleneck — Riva 2.19's
  Magpie-Multilingual quickstart ships only `en-US`, `es-US`, `fr-FR`
  subvoices.
- **`TARGET_VOICE`** — Magpie voice_name for the target, e.g.
  `Magpie-Multilingual.EN-US.Female.Neutral`. Must match a deployed subvoice
  for `TARGET_LANGUAGE`.

Changing `SOURCE_LANGUAGE` (or `ASR_ACOUSTIC_MODEL`) requires re-running
`./bootstrap.sh` so Riva redeploys the ASR model; the bootstrap marker is
per `(language, model)` so switching triggers a fresh deploy automatically.
Changing `TARGET_LANGUAGE` or `TARGET_VOICE` only needs
`docker compose up -d` to restart the app.

## Architecture

```
Browser (Speaker)                                        Browser (Listener)
       │                                                          ▲
       │                                                          │
       │  HTTPS (Cloudflare) → cloudflared → http://app:8081      │
       ▼                                                          │
   mic → AudioWorklet → ws/speaker ─►  app (FastAPI)  ◄─ ws/listener ← playback
                                          │  ▲
                                   gRPC :50051 │
                                          ▼  │
                        Riva StreamingTranslateSpeechToSpeech
                             (ASR → NMT → TTS, streaming)
```

Audio is int16 mono PCM on the wire. The speaker browser downsamples
microphone audio to 16 kHz in an `AudioWorkletProcessor` and ships ~80 ms
frames over a WebSocket. The server feeds those frames into Riva's streaming
S2S gRPC and forwards translated TTS audio (44.1 kHz) to the listener browser
as binary WebSocket frames, which are scheduled onto an `AudioContext` output
with a small jitter buffer.

## Files

| Path | Purpose |
|---|---|
| `docker-compose.yaml` | `riva` + `app` + `cloudflared` |
| `bootstrap.sh` | One-shot host script: login, download quickstart, patch `config.sh`, patch `riva_init.sh` TTY flags, run `riva_init.sh` |
| `.env.example` | Template for `NGC_API_KEY`, `SOURCE_LANGUAGE`, `TARGET_LANGUAGE`, `TARGET_VOICE`, `ASR_ACOUSTIC_MODEL` |
| `app/Dockerfile` | Python 3.11 + fastapi + nvidia-riva-client |
| `app/entrypoint.sh` | Launches uvicorn on HTTP (TLS is done by cloudflared) |
| `app/server.py` | FastAPI app: REST config endpoint + two WebSocket endpoints + Riva S2S relay |
| `app/static/` | Static UI assets (HTML, CSS, JS, AudioWorklet) |
| `scripts/deploy-on-vm.sh` | Wrapper that runs bootstrap + compose up + prints the public URL |

## Troubleshooting

- **`riva` container crash-loops with `CUDA error: no kernel image`** —
  the GPU is too old. Riva 2.19 needs Turing (T4) or newer.
- **`Model not found` from Riva** — `bootstrap.sh` didn't finish. Check
  `docker volume ls` — you should see `riva-model-repo` populated
  (`docker run --rm -v riva-model-repo:/data alpine ls /data/models`).
- **Disk fills up during `bootstrap.sh`** — the Riva image + models need
  ~100 GB. Move Docker's data-root to a bigger disk before bootstrap:
  ```bash
  sudo systemctl stop docker containerd
  sudo sed -i 's|#root = "/var/lib/containerd"|root = "/opt/dlami/nvme/containerd"|' /etc/containerd/config.toml
  echo '{"data-root":"/opt/dlami/nvme/docker","runtimes":{"nvidia":{"path":"nvidia-container-runtime"}}}' | sudo tee /etc/docker/daemon.json
  sudo systemctl start containerd docker
  ```
- **Browser refuses `getUserMedia`** — make sure you're on the
  `https://*.trycloudflare.com` URL, not the raw `http://<host>:8081`.
- **Listener plays nothing** — click **Enable playback** first. Browsers
  gate `AudioContext` behind a user gesture.
- **NGC download rate-limited** — try again later, or use `ngc config set`
  to authenticate the CLI independently and resume.
- **No Cloudflare tunnel URL printed** — `docker logs riva-translator-tunnel`
  will show whether the tunnel reached Cloudflare; egress HTTPS must be open.

## Licenses

Uses NVIDIA Riva (subject to the NVIDIA AI Product License) and the
`nvidia-riva-client` Python package (Apache 2.0). This project's own code
is unlicensed (pick one before publishing).
