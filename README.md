# Riva Real-Time Translator — Brev Launchable

Real-time speech-to-speech translation using NVIDIA Riva's
`StreamingTranslateSpeechToSpeech` gRPC API. Two docker-compose services:

| Service | What it does | Ports |
|---|---|---|
| `riva`  | NVIDIA Riva Speech Server (ASR + NMT + TTS) | `50051` gRPC |
| `app`   | FastAPI + HTTPS web UI that pairs a speaker device with a listener device | `8081` HTTPS |

Open `https://<host>:8081/` on **two** devices, enter the same room code on
both, pick *Speaker* on one and *Listener* on the other, and start talking.
English speech on the Speaker device is translated in real time and played
back in the target language on the Listener device.

## Requirements

- An NVIDIA GPU with ≥ 24 GB of VRAM (L4, L40, A10, A100, H100, RTX 6000
  Ada, RTX PRO 6000, …). CPU-only is not supported by Riva.
- An NGC API key from <https://ngc.nvidia.com/setup/api-key>. The key is
  needed to pull the Riva container and the ~40 GB of model artifacts.
- Docker 24+ with the NVIDIA Container Toolkit, and Docker Compose v2.
- A host that can reach `nvcr.io` and `api.ngc.nvidia.com`.
- ~150 GB of free disk for the model repository and container images.

## First-time setup (Brev VM or any Linux host)

```bash
# 1. Create .env with your NGC key
cp .env.example .env
$EDITOR .env        # paste your NGC_API_KEY; optionally set PUBLIC_HOSTNAME

# 2. Download Riva + deploy models into a docker volume (takes 30-60 min)
./bootstrap.sh

# 3. Bring up Riva and the web app
docker compose up -d

# 4. Wait ~1-2 min for Riva to warm up, then check
docker compose ps
curl -sk https://localhost:8081/healthz
```

On Brev, pass `PUBLIC_HOSTNAME` so the self-signed cert's SAN matches what
the browser sees:

```bash
PUBLIC_HOSTNAME="gpu-abc123.brevlab.com" docker compose up -d
```

### Opening the UI

Navigate to `https://<PUBLIC_HOSTNAME>:8081/`. Because the cert is
self-signed, your browser will warn you — accept the warning once per
device. Browser microphone APIs require HTTPS, which is why we self-sign.

On each device:

1. Type the same room code (any short string, e.g. `alpha`).
2. On one device tap **Speaker** → pick a target language → **Start
   microphone**.
3. On the other device tap **Listener** → **Enable playback** (the click
   gesture is required by browsers before audio can start).

## Creating the Brev Launchable

Brev reads its launchable config from the console wizard, not from a file in
this repo. To publish:

1. Push this repo to GitHub.
2. Go to <https://brev.nvidia.com/launchables/create>.
3. Step 1 **Files and Runtime** — "I have code in a GitHub repository",
   paste the repo URL, runtime = **Docker Compose**.
4. Step 2 **Configure Environment** — paste the GitHub URL of
   `docker-compose.yaml` (not the raw URL). Click Validate.
5. Step 3 **Jupyter and Networking** — expose **port 8081** with any name
   (e.g. `translator`). Skip port 8888 / Jupyter.
6. Step 4 **Compute** — pick an L4 (24 GB) or larger. Give the VM **≥ 200
   GB** of disk. CUDA 12 for Ampere/Ada/Hopper, CUDA 13 for Blackwell.
7. Step 5 **Publish**.

The launchable still requires the user to provide their own NGC key
(`.env`) and run `./bootstrap.sh` once on first boot — Brev does not ship a
first-class secret store for compose launchables yet, and Riva's model
download is gated on the key.

## Supported languages

- **Source** — English (`en-US`). The quickstart streaming ASR deployed
  here is English-only.
- **Targets** — Spanish, French, German, Chinese (Simplified), Italian,
  Vietnamese. These match the Riva *Magpie-Multilingual* TTS voice set. The
  underlying NMT model (Megatron 1B any-to-any) supports 36 languages;
  adding more targets is possible if you deploy additional TTS voices.

## Architecture

```
Browser (Speaker)   HTTPS :8081 + WS binary PCM 16k        Browser (Listener)
       │                     │                                      │
       ▼                     ▼                                      ▲
   mic → AudioWorklet → ws/speaker ─►  app (FastAPI)  ◄─ ws/listener ← playback
                                          │  ▲
                                   gRPC :50051 │
                                          ▼  │
                        Riva StreamingTranslateSpeechToSpeech
                             (ASR → NMT → TTS, streaming)
```

Audio is int16 mono PCM on the wire. The speaker browser downsamples
microphone audio to 16 kHz in an `AudioWorkletProcessor` and ships
~80 ms frames over a WebSocket. The server feeds those frames into Riva's
streaming S2S gRPC and forwards translated TTS audio (44.1 kHz) to the
listener browser as binary WebSocket frames, which are scheduled onto an
`AudioContext` output with a small jitter buffer.

## Files

| Path | Purpose |
|---|---|
| `docker-compose.yaml` | Two-service stack: `riva` + `app` |
| `bootstrap.sh` | One-shot host script: login, download quickstart, patch its `config.sh`, run `riva_init.sh` |
| `.env.example` | Template for `NGC_API_KEY` and optional `PUBLIC_HOSTNAME` |
| `app/Dockerfile` | Python 3.11 + fastapi + nvidia-riva-client |
| `app/entrypoint.sh` | Generates a self-signed cert (SAN = `PUBLIC_HOSTNAME`) and launches uvicorn with TLS |
| `app/server.py` | FastAPI app: REST config endpoint + two WebSocket endpoints + Riva S2S relay |
| `app/static/` | Static UI assets (HTML, CSS, JS, AudioWorklet) |

## Troubleshooting

- **`riva` container crash-loops with `CUDA error: no kernel image`** —
  the GPU is too old. Riva 2.19 needs Turing (T4) or newer.
- **`Model not found` from Riva** — `bootstrap.sh` didn't finish. Check
  `docker volume ls` — you should see `riva-model-repo` populated
  (`docker run --rm -v riva-model-repo:/data alpine ls /data/models`).
- **Browser refuses `getUserMedia`** — make sure you're on `https://`, not
  `http://`. All modern browsers require HTTPS (or `localhost`) for mic.
- **Listener plays nothing** — click **Enable playback** first. Browsers
  gate `AudioContext` behind a user gesture.
- **NGC download rate-limited** — try again later, or use `ngc config set`
  to authenticate the CLI independently and resume.
- **Cert name mismatch warning** — set `PUBLIC_HOSTNAME=<actual-host>` in
  `.env` and remove `app-certs` volume (`docker volume rm
  riva-translator_app-certs`), then `docker compose up -d` to regenerate.

## Licenses

Uses NVIDIA Riva (subject to the NVIDIA AI Product License) and the
`nvidia-riva-client` Python package (Apache 2.0). This project's own code
is unlicensed (pick one before publishing).
