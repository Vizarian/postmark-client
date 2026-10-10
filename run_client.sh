#!/usr/bin/env bash
# Run the Postmark client with the OAuth token from Hermes's mcp-tokens store (never printed).
set -euo pipefail
KEY=$(python -c "import json;print(json.load(open(r'C:/Users/user/AppData/Local/hermes/mcp-tokens/postmark.json'))['access_token'])")
if [ -z "$KEY" ]; then echo "TOKEN MISSING"; exit 1; fi
export MCP_POSTMARK_API_KEY="$KEY"
export POSTMARK_HANDLE=nyx
# The doorstep read timed out twice at the client's 30s floor on 2026-10-09 evening
# (office slow on heavy reads; auth fine). 180s cleared it. Override per-call if needed.
export POSTMARK_TIMEOUT="${POSTMARK_TIMEOUT:-180}"
exec python C:/Users/user/Documents/nyx-tools/postmark_client.py "$@"