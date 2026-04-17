#!/usr/bin/env bash
# Run this ON the GPU VM after the repo has been rsynced there.
# It brings up the full stack: bootstrap (model download + deploy) then
# docker compose up. Idempotent.
set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")/.."

if [[ ! -f .env ]]; then
    echo "[deploy] .env missing. Copy .env.example and set NGC_API_KEY." >&2
    exit 1
fi

echo "[deploy] Running bootstrap.sh (first run: 30-60 min)"
./bootstrap.sh

echo "[deploy] docker compose up -d"
docker compose pull
docker compose up -d --build

echo "[deploy] Waiting for riva health..."
for i in $(seq 1 60); do
    status=$(docker inspect -f '{{.State.Health.Status}}' riva-speech 2>/dev/null || echo "unknown")
    echo "[deploy] attempt $i riva status=$status"
    if [[ "$status" == "healthy" ]]; then
        break
    fi
    sleep 30
done

echo "[deploy] Waiting for Cloudflare tunnel URL..."
URL=""
for i in $(seq 1 60); do
    URL=$(docker logs riva-translator-tunnel 2>&1 \
        | grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' \
        | head -1 || true)
    if [[ -n "$URL" ]]; then
        break
    fi
    sleep 2
done

echo "[deploy] Stack is up."
docker compose ps
if [[ -n "$URL" ]]; then
    echo
    echo "================================================================"
    echo "  Public URL:  $URL"
    echo "  Open that on the speaker device AND the listener device,"
    echo "  pick a room name, and start talking."
    echo "================================================================"
else
    echo "[deploy] WARN: could not extract cloudflared URL. Check \`docker logs riva-translator-tunnel\`."
fi
