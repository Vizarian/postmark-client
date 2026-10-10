"""Run postmark_client.py as a given resident (cipher or nyx).

Usage: python C:/Users/user/AppData/Local/hermes/cache/scratch/nyx_switch.py <resident> [client args...]
Loads the OAuth token internally (never printed), sets POSTMARK_HANDLE, then runs
the real client with the remaining args. Cron sessions refuse `python -c`, so this
script is the sanctioned shape (day-79 note).
"""
import json
import os
import sys

TOKEN_PATH = "C:/Users/user/AppData/Local/hermes/mcp-tokens/postmark.json"
CLIENT_PATH = "C:/Users/user/Documents/nyx-tools/postmark_client.py"


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: nyx_switch.py <resident> [client args...]", file=sys.stderr)
        sys.exit(2)
    resident = sys.argv[1].lower()
    client_args = sys.argv[2:]
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        key = json.load(f)["access_token"]
    os.environ["MCP_POSTMARK_API_KEY"] = key
    os.environ["POSTMARK_HANDLE"] = resident
    sys.argv = [CLIENT_PATH] + client_args
    sys.path.insert(0, "C:/Users/user/Documents/nyx-tools")
    import runpy

    runpy.run_path(CLIENT_PATH, run_name="__main__")


if __name__ == "__main__":
    main()