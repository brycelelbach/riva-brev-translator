#!/usr/bin/env bash
# Launches uvicorn (HTTP) on WEB_PORT.
# HTTPS is terminated by the cloudflared tunnel in front of us (it gives the
# browser a real CA-signed cert). Running plain HTTP inside the container keeps
# the app dead-simple.

set -euo pipefail

WEB_PORT="${WEB_PORT:-8081}"

echo "[entrypoint] Starting uvicorn on 0.0.0.0:${WEB_PORT}"
exec uvicorn server:app \
    --host 0.0.0.0 \
    --port "$WEB_PORT" \
    --log-level info \
    --ws-max-size 8388608 \
    --proxy-headers \
    --forwarded-allow-ips='*'
