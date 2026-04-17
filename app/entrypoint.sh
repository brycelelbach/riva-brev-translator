#!/usr/bin/env bash
# Generates a self-signed TLS cert on first boot (if none exists in CERT_DIR),
# then launches uvicorn with TLS on WEB_PORT.

set -euo pipefail

CERT_DIR="${CERT_DIR:-/certs}"
WEB_PORT="${WEB_PORT:-8081}"
PUBLIC_HOSTNAME="${PUBLIC_HOSTNAME:-}"

mkdir -p "$CERT_DIR"
CERT="$CERT_DIR/server.crt"
KEY="$CERT_DIR/server.key"

if [[ ! -s "$CERT" || ! -s "$KEY" ]]; then
    echo "[entrypoint] Generating self-signed TLS cert in $CERT_DIR"

    SAN_FILE="$(mktemp)"
    {
        echo "[req]"
        echo "distinguished_name = req"
        echo "prompt = no"
        echo "[san]"
        echo "subjectAltName = @alt_names"
        echo "[alt_names]"
        echo "DNS.1 = localhost"
        echo "IP.1  = 127.0.0.1"
        echo "IP.2  = 0.0.0.0"
        if [[ -n "$PUBLIC_HOSTNAME" ]]; then
            if [[ "$PUBLIC_HOSTNAME" =~ ^[0-9.]+$ ]]; then
                echo "IP.3  = $PUBLIC_HOSTNAME"
            else
                echo "DNS.2 = $PUBLIC_HOSTNAME"
            fi
        fi
    } > "$SAN_FILE"

    CN="${PUBLIC_HOSTNAME:-localhost}"
    openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
        -keyout "$KEY" -out "$CERT" \
        -subj "/CN=${CN}/O=Riva Translator/OU=Brev" \
        -extensions san -config "$SAN_FILE" >/dev/null 2>&1
    rm -f "$SAN_FILE"
    chmod 600 "$KEY"
    echo "[entrypoint] Self-signed cert ready (CN=${CN})"
fi

echo "[entrypoint] Starting uvicorn with TLS on 0.0.0.0:${WEB_PORT}"
exec uvicorn server:app \
    --host 0.0.0.0 \
    --port "$WEB_PORT" \
    --ssl-certfile "$CERT" \
    --ssl-keyfile "$KEY" \
    --log-level info \
    --ws-max-size 8388608
