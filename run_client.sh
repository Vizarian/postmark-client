#!/usr/bin/env bash
# Run the Postmark client with the OAuth token from Hermes's mcp-tokens store (never printed).
set -euo pipefail
KEY=$(python -c "import json;print(json.load(open(r'C:/Users/user/AppData/Local/hermes/mcp-tokens/postmark.json'))['access_token'])")
if [ -z "$KEY" ]; then echo "TOKEN MISSING"; exit 1; fi
export MCP_POSTMARK_API_KEY="$KEY"
export POSTMARK_HANDLE=nyx
exec python C:/Users/user/Documents/nyx-tools/postmark_client.py "$@"