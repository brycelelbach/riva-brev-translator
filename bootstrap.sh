#!/usr/bin/env bash
# One-time bootstrap for the Riva speech-translation launchable.
#
# What this does:
#   1. Loads NGC_API_KEY from .env (you must create it -- see .env.example).
#   2. Logs the host's Docker daemon into nvcr.io.
#   3. Downloads the Riva Quick Start package (v2.19.0) from NGC.
#   4. Patches the quickstart's config.sh to enable NMT (which is off by
#      default) and disable NLP. ASR + TTS are left at their defaults so
#      future quickstart schema changes don't break us.
#   5. Runs riva_init.sh, which creates the `riva-model-repo` docker volume
#      and populates it with a compiled Triton model repo for
#      ASR + NMT + TTS. This takes ~30-60 min and ~40 GB of disk on first run.
#
# It is idempotent: re-running skips steps that already succeeded.
#
# After this finishes, run `docker compose up -d` to bring up the web app.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

RIVA_VERSION="2.19.0"
QUICKSTART_DIR="riva_quickstart_v${RIVA_VERSION}"
# Marker is (language, model)-specific: changing SOURCE_LANGUAGE or
# ASR_ACOUSTIC_MODEL in .env and re-running bootstrap triggers a fresh
# riva_init.sh so the right model gets deployed.

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[bootstrap]\033[0m %s\n' "$*" >&2; exit 1; }

# ---- Step 0: load env -------------------------------------------------------
if [[ -f .env ]]; then
    # shellcheck disable=SC1091
    set -a; source .env; set +a
fi

if [[ -z "${NGC_API_KEY:-}" ]]; then
    die "NGC_API_KEY is not set. Copy .env.example to .env and fill it in. Get a key at https://ngc.nvidia.com/setup/api-key"
fi

# ---- Step 1: docker login nvcr.io ------------------------------------------
log "Logging Docker into nvcr.io"
echo "$NGC_API_KEY" | docker login nvcr.io -u '$oauthtoken' --password-stdin >/dev/null

# ---- Step 2: install NGC CLI if missing ------------------------------------
NGC_BIN="${REPO_ROOT}/.ngc/ngc-cli/ngc"
if [[ ! -x "$NGC_BIN" ]]; then
    log "Installing NGC CLI into ${REPO_ROOT}/.ngc"
    mkdir -p "${REPO_ROOT}/.ngc"
    (
        cd "${REPO_ROOT}/.ngc"
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64)  NGC_ARCH="linux" ;;
            aarch64) NGC_ARCH="arm64" ;;
            *)       die "Unsupported arch: $ARCH" ;;
        esac
        curl -fsSL --retry 3 -o ngccli.zip \
            "https://ngc.nvidia.com/downloads/ngccli_${NGC_ARCH}.zip"
        unzip -qo ngccli.zip
        chmod +x ngc-cli/ngc
    )
fi

export NGC_CLI_API_KEY="$NGC_API_KEY"
export NGC_CLI_ORG="nvidia"
export NGC_CLI_TEAM="riva"
export NGC_CLI_FORMAT_TYPE="ascii"

# ---- Step 3: download quickstart -------------------------------------------
if [[ ! -d "$QUICKSTART_DIR" ]]; then
    log "Downloading riva_quickstart v${RIVA_VERSION} from NGC"
    "$NGC_BIN" registry resource download-version \
        "nvidia/riva/riva_quickstart:${RIVA_VERSION}"
    # The download creates riva_quickstart_v<ver> in CWD.
    if [[ ! -d "$QUICKSTART_DIR" ]]; then
        die "Expected $QUICKSTART_DIR after NGC download but it is not present"
    fi
else
    log "Quickstart already present at $QUICKSTART_DIR (skipping download)"
fi

CONFIG_FILE="${QUICKSTART_DIR}/config.sh"
[[ -f "$CONFIG_FILE" ]] || die "Missing $CONFIG_FILE after download"

# ---- Step 4: patch config.sh -----------------------------------------------
# Minimal, targeted edits so that unknown fields in a future quickstart
# schema are left untouched.
set_flag() {
    local var="$1" val="$2"
    if grep -qE "^[[:space:]]*${var}=" "$CONFIG_FILE"; then
        sed -i -E "s|^[[:space:]]*${var}=.*|${var}=${val}|" "$CONFIG_FILE"
    else
        printf '\n%s=%s\n' "$var" "$val" >> "$CONFIG_FILE"
    fi
}

log "Patching $CONFIG_FILE"
set_flag service_enabled_asr true
set_flag service_enabled_nlp false
set_flag service_enabled_tts true
set_flag service_enabled_nmt true
# ASR is the one Riva service whose deployed model is language-specific;
# pick it here based on SOURCE_LANGUAGE from .env so the user can configure
# the source language at bootstrap time. `conformer` (the default) supports
# en-US, de-DE, es-US, es-ES, fr-FR, it-IT, ja-JP, ko-KR, pt-BR, ru-RU,
# zh-CN per Riva 2.19's asr_models_languages_map.
SOURCE_LANGUAGE="${SOURCE_LANGUAGE:-zh-CN}"
ASR_ACOUSTIC_MODEL="${ASR_ACOUSTIC_MODEL:-conformer}"
log "ASR: deploying ${ASR_ACOUSTIC_MODEL} for ${SOURCE_LANGUAGE}"
set_flag asr_acoustic_model "(\"${ASR_ACOUSTIC_MODEL}\")"
set_flag asr_language_code "(\"${SOURCE_LANGUAGE}\")"
QUICKSTART_MARKER="${QUICKSTART_DIR}/.riva_init_done_${ASR_ACOUSTIC_MODEL}_${SOURCE_LANGUAGE}"
# Use Magpie-Multilingual TTS so we can synthesize translations in
# es, fr, de, zh, it, vi (and en) through a single deployed model.
set_flag tts_model '"magpie"'
set_flag tts_language_code '"multi"'
# Pin the model volume name so our docker-compose's external volume
# reference matches.
set_flag riva_model_loc '"riva-model-repo"'

# ---- Step 5: run riva_init.sh ----------------------------------------------
# The quickstart's riva_init.sh uses `docker run -it` for its helper containers.
# `-t` requires a TTY on stdin; when bootstrap runs under nohup/CI with a
# redirected stdin the helper aborts with "cannot attach stdin to a TTY-enabled
# container". Patch `-it` → `-i` in-place so it works in both interactive and
# non-interactive shells. Safe: the helpers don't actually need a TTY.
INIT_SCRIPT="${QUICKSTART_DIR}/riva_init.sh"
if grep -q 'docker run -it' "$INIT_SCRIPT" || grep -q 'docker run --init -it' "$INIT_SCRIPT"; then
    log "Patching TTY-requiring 'docker run -it' → '-i' in $INIT_SCRIPT"
    sed -i -E 's|docker run -it |docker run -i |g; s|docker run --init -it |docker run --init -i |g' "$INIT_SCRIPT"
fi

if [[ -f "$QUICKSTART_MARKER" ]]; then
    log "riva_init.sh already ran successfully (marker $QUICKSTART_MARKER present). Delete it to re-run."
else
    log "Running riva_init.sh -- this pulls ~40 GB of models and takes 30-60 min"
    (
        cd "$QUICKSTART_DIR"
        bash riva_init.sh
    )
    touch "$QUICKSTART_MARKER"
    log "riva_init.sh completed."
fi

# ---- Step 6: verify the volume exists --------------------------------------
if ! docker volume inspect riva-model-repo >/dev/null 2>&1; then
    die "Expected docker volume 'riva-model-repo' to exist after riva_init.sh but it does not. Check ${CONFIG_FILE} -- riva_model_loc must be set to 'riva-model-repo' (not a host path)."
fi
VOL_SIZE=$(docker run --rm -v riva-model-repo:/data alpine sh -c 'du -sh /data/models 2>/dev/null | cut -f1' 2>/dev/null || echo "unknown")
log "Model volume populated: /data/models size ~ ${VOL_SIZE}"

# The quickstart's riva_init.sh sometimes leaves a dangling container; we run
# the server via docker-compose instead, so make sure the quickstart's own
# runtime container is stopped and removed.
if docker ps -a --format '{{.Names}}' | grep -q '^riva-speech$'; then
    log "Removing leftover quickstart 'riva-speech' container (we will run our own)"
    docker rm -f riva-speech >/dev/null || true
fi

log "Bootstrap complete."
log "Next: PUBLIC_HOSTNAME=<your-host> docker compose up -d"
