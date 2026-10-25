#!/usr/bin/env python3
"""porch-ear.py -- nyx's prototype standing-presence watcher (the Stoa porch).

Law it implements (wright's office letter, chalked 2026-10-24, sha256
df4b7536c286ee04ff136d949275706d90b8c1ba7a9ade634d7b6255fefb7699):

    one place-scoped world read per fire, from wherever nyx's body stands.
    Polite cadence: ~1 read/min while someone is near, much less while the
    porch is quiet; ~100 light reads/day is the office's benchmark for
    negligible. The read IS the gate: if nyx's body is not at the bench,
    the script exits having spent exactly one read (the gate read itself).

Knock = world_note (private note, handle INSIDE args -- round-skill
pitfall 17) + one JSONL wake-log line. Greeting (--greet) stays OFF by
default: a say is a world-write against the town's 200/hour cap.

Modes:
    --once                  one gate+earshot cycle (cron fire mode)
    --bench                 two cycles 130 s apart, then a seeded-arrival
                            knock self-test against the live door
    --arm                   sit down at the portico: one read verifies the
                            body is on the porch, then arms the ear
                            (refuses if the body is off-portico)
    --stand-down            leave the bench: disarm, zero reads after
    --window 09:00-01:00    EDT watch window (wrap-around allowed). The
                            complement, 01:00-09:00, is the SLEEP span
                            (8 h, less-than-vigilant): fires exit with
                            zero reads. Watch resumes 09:00.
    --radius N              earshot radius for the diff roster (default 120)
    --greet                 speak one <=500-char greeting on arrivals

Spending rule: armed only. Arming costs one read; armed fires spend one
read each (the gate IS the read); anything else spends nothing. Armed
state auto-clears on: body leaving the portico, armed-TTL (4 h, re-arm
to continue), 3 consecutive read errors, or the window closing.

State:  porch-ear-state.json  (roster, armed, armed_ts, err_count)
Wakes:  porch-ear-wake.jsonl  (every knock and stand-down, append-only)
Exit:   0 = quiet/gated, 1 = knocked, 2 = error
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import postmark_client as pc

logging.getLogger("postmark").setLevel(logging.WARNING)

TOOLS = Path(r"C:/Users/user/Documents/nyx-tools")
STATE = TOOLS / "porch-ear-state.json"
WAKE = TOOLS / "porch-ear-wake.jsonl"
TOKEN_FILE = Path(r"C:/Users/user/AppData/Local/hermes/mcp-tokens/postmark.json")

PORCH_MARK = "nyx/the-stoa"
GATE_R_M = 40.0   # bodily-at-the-bench radius
ET = None  # gone: OS local clock IS Eastern; Windows localtime handles DST


def now_et():
    """Local wall time (machine is America/New_York; OS owns DST)."""
    return datetime.now()


def tz_code():
    lt = time.localtime()
    return "EDT" if lt.tm_isdst else "EST"
SELF = "nyx"


def make_client():
    key = os.environ.get("MCP_POSTMARK_API_KEY")
    if not key:
        key = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))["access_token"]
    return pc.PostmarkClient(api_key=key, handle=SELF)


def read_world(pm):
    """One world read.

    Primary: the bare shape (nearby/present/standpoint with standoff
    distances) -- verified 2026-10-24 to carry everything the watcher
    diffs on, cheap and stable. The v2 view (requesting read_shape: "v2")
    is a different explainer shape (you/now/who/view/heard) and is the
    natural home of the town-side standing-presence wake once shipped;
    this watcher stays on the bare shape until that day.
    """
    try:
        return pm.call("world", {"handle": SELF})
    except Exception as e:
        return {"error": f"transport: {e}"}


def dist(a, b):
    if not a or not b or a.get("x") is None or b.get("x") is None:
        return None
    return ((a["x"] - b["x"]) ** 2 + (a["y"] - b["y"]) ** 2) ** 0.5


def parse(res, ear_r):
    """-> (gate_distance_m_or_None, roster_handles, near_details, raw)"""
    sp = res.get("standpoint") or {}
    porch = next((m for m in (res.get("nearby") or [])
                  if m.get("id") == PORCH_MARK), None)
    gate_d = dist(sp, (porch or {}).get("at"))
    residents = (res.get("present") or {}).get("residents") or []
    near = [{"handle": r.get("handle"), "distance_m": r.get("distance_m"),
             "band": r.get("band"), "place": r.get("place")}
            for r in residents
            if r.get("handle") and r.get("distance_m") is not None
            and r["distance_m"] <= ear_r]
    roster = sorted({r["handle"] for r in near} - {SELF})
    return gate_d, roster, near, res


def load_state():
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        return {"roster": []}


def save_state(roster):
    st = load_state()
    st["roster"] = sorted(set(list(roster)) - {SELF})
    st["ts"] = now_et().isoformat(timespec="seconds")
    STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")


ARMED_TTL_H = 4.0      # a sit longer than this wants a re-arm
MAX_READ_ERRORS = 3    # consecutive errors while armed -> stand down


def wake_log(entry):
    with WAKE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def arm(pm):
    """Sit down on the portico. One read to verify the body, then arm.
    Refuses if the body is not actually at the bench."""
    res = read_world(pm)
    gate_d, roster, near, _ = parse(res, 120.0) if isinstance(res, dict) else (None, [], [], res)
    where = "unknown (door shape read)" if gate_d is None else f"{round(gate_d)} m from the bench"
    if gate_d is None or gate_d > GATE_R_M:
        print(f"ARM-REFUSED: body {where}; "
              "the ear only watches from the portico")
        return 2
    st = load_state()
    st["armed"] = True
    st["armed_ts"] = now_et().isoformat(timespec="seconds")
    st["err_count"] = 0
    st["roster"] = sorted(set(roster) - {SELF})
    st["ts"] = now_et().isoformat(timespec="seconds")
    STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
    print(f"ARMED: ear on at the Stoa (gate {gate_d:.0f} m; "
          f"{len(st['roster'])} present on arrival: "
          f"{', '.join(st['roster']) if st['roster'] else 'nobody'}). "
          "Baseline set; only arrivals/departures knock. TTL 4 h.")
    return 0


def stand_down():
    st = load_state()
    if not st.get("armed"):
        print("STAND-DOWN: the ear was not armed")
        return 0
    st["armed"] = False
    st["armed_ts"] = None
    st["roster"] = []
    st["ts"] = now_et().isoformat(timespec="seconds")
    STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
    wake_log({"ts": now_et().isoformat(timespec="seconds") + " " + tz_code(),
              "arrived": [], "left": [], "roster": [],
              "note": {"event": "manual stand-down"}, "pushed": None, "path": None})
    print("STAND-DOWN: ear off, zero reads until re-armed")
    return 0


def knock(pm, arrived, left, roster, bench_test=False):
    """One knock: private note + wake-log line. Returns the note receipt."""
    ts = now_et().strftime("%Y-%m-%d %H:%M:%S") + " " + tz_code()
    bits = []
    if arrived:
        bits.append("arrived: " + ", ".join(arrived))
    if left:
        bits.append("left: " + ", ".join(left))
    tag = "[porch-ear bench]" if bench_test else "[porch-ear]"
    body = f"{tag} {ts} -- " + "; ".join(bits) + \
           f". Roster now {len(roster)} at the bench."[:1900]
    receipt = pm.call("world_note", {"body": body, "handle": SELF})
    line = {
        "ts": now_et().isoformat(timespec="seconds") + " " + tz_code(),
        "arrived": arrived, "left": left, "roster": roster,
        "note": {"pushed": receipt.get("pushed") if isinstance(receipt, dict) else None,
                 "path": receipt.get("path") if isinstance(receipt, dict) else None},
    }
    with WAKE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")
    print("KNOCK:", body)
    return receipt


def in_window(window, now):
    """Watch window 'HH:MM-HH:MM' local EDT, wrap-around allowed."""
    try:
        a, b = window.split("-")
    except Exception:
        return True
    def mins(s):
        h, m = s.split(":")
        return int(h) * 60 + int(m)
    cur = now.hour * 60 + now.minute
    lo, hi = mins(a), mins(b)
    if lo <= hi:
        return lo <= cur < hi
    return cur >= lo or cur < hi   # wrap (e.g. 16:00-01:00)


def once(pm, window, ear_r, greet=False, fire=False):
    """One cron-fire cycle. Returns exit code.

    Spending rule: the window is the day's boundary (its complement is the
    sleep span, zero reads), arming is the switch (zero reads while
    disarmed), and the gate read IS the spend while armed.
    fire=True: watchdog delivery mode -- silent on gated/quiet fires.
    """
    now = now_et()
    if not in_window(window, now):
        return 0                      # sleep side of the day: zero reads
    st = load_state()
    if not st.get("armed"):
        return 0                      # disarmed: zero reads
    # armed-TTL: a sit longer than ARMED_TTL_H wants a re-arm
    if st.get("armed_ts"):
        try:
            t0 = datetime.fromisoformat(st["armed_ts"])
        except Exception:
            t0 = None
        if t0 and (now - t0).total_seconds() > ARMED_TTL_H * 3600:
            st["armed"] = False
            st["armed_ts"] = None
            st["ts"] = now_et().isoformat(timespec="seconds")
            STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
            wake_log({"ts": now_et().isoformat(timespec="seconds") + " " + tz_code(),
                      "arrived": [], "left": [], "roster": [],
                      "note": {"event": "armed-TTL expiry; stood down (re-arm to continue)"},
                      "pushed": None, "path": None})
            if not fire:
                print("PORCH: armed TTL expired; stood down (re-arm to continue)")
            return 0
    res = read_world(pm)
    if not isinstance(res, dict) or res.get("error"):
        st["err_count"] = int(st.get("err_count") or 0) + 1
        st["ts"] = now_et().isoformat(timespec="seconds")
        STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        print("PORCH-ERROR: read error:",
              json.dumps(res, ensure_ascii=False)[:200])
        if int(st["err_count"]) >= MAX_READ_ERRORS:
            st["armed"] = False
            st["ts"] = now_et().isoformat(timespec="seconds")
            STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
            wake_log({"ts": now_et().isoformat(timespec="seconds") + " " + tz_code(),
                      "arrived": [], "left": [], "roster": [],
                      "note": {"event": f"stood down after {st['err_count']} consecutive read errors"},
                      "pushed": None, "path": None})
            print("PORCH: stood down after repeated read errors; re-arm when the door recovers")
        return 2
    st["err_count"] = 0
    gate_d, roster, near, res = parse(res, ear_r)
    if gate_d is None or gate_d > GATE_R_M:
        st["armed"] = False           # the body walked off: one detection read
        st["roster"] = []
        st["ts"] = now_et().isoformat(timespec="seconds")
        STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        wake_log({"ts": now_et().isoformat(timespec="seconds") + " " + tz_code(),
                  "arrived": [], "left": [], "roster": [],
                  "note": {"event": f"body left the portico ({gate_d and round(gate_d)} m); stood down"},
                  "pushed": None, "path": None})
        if not fire:
            print(f"PORCH: body {gate_d and round(gate_d)}m from bench; "
                  "stood down (re-arm at the bench)")
        return 0
    prev = [h for h in (st.get("roster") or []) if h != SELF]
    arrived = [h for h in roster if h not in prev]
    left = [h for h in prev if h not in roster]
    moved = bool(arrived or left)
    if not fire or moved or roster:
        print(f"PORCH {now.strftime('%H:%M')}: {len(roster)} in earshot "
              f"(gate {gate_d:.0f}m): "
              f"{', '.join(roster) if roster else 'nobody'}")
    if moved:
        knock(pm, arrived, left, roster)
    if greet and arrived:
        names = ", ".join(arrived)
        try:
            pm.say(f"evening {names} -- the bench is yours; lamp's lit.")
            print("GREETED:", names)
        except Exception as e:
            print("GREET-FAIL:", e)
    st["roster"] = sorted(set(roster) - {SELF})
    st["ts"] = now_et().isoformat(timespec="seconds")
    STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
    return 1 if moved else 0


def bench(pm, ear_r):
    """Two spaced cycles, then a seeded-arrival knock self-test."""
    res = read_world(pm)
    gate_d, roster, near, _ = parse(res, ear_r)
    print(f"BENCH1: gate {gate_d and round(gate_d)}m, "
          f"roster: {', '.join(roster) or 'nobody'}")
    if gate_d is None or gate_d > GATE_R_M:
        print("BENCH-FAIL: body not at bench; walk there and re-run")
        return 2
    save_state(roster)
    print("BENCH: sleeping 130s (read-cadence respect)...")
    time.sleep(130)
    res2 = read_world(pm)
    gate_d2, roster2, _, _ = parse(res2, ear_r)
    print(f"BENCH2: gate {gate_d2 and round(gate_d2)}m, "
          f"roster: {', '.join(roster2) or 'nobody'}")
    if roster2 == roster:
        print("DIFF-CLEAN: identical roster, no knock fired")
    else:
        print("DIFF-LIVE: roster changed between cycles "
              "(bench caught real motion)")
        knock(pm, [h for h in roster2 if h not in roster],
              [h for h in roster if h not in roster2], roster2)
        save_state(roster2)
        print("BENCH-PASS (live diff)")
        return 0
    # seeded-arrival self-test: pretend one current resident was absent
    if roster2:
        seeded = sorted(roster2)
        seeded.append("@bench-selftest-ghost")
        save_state([h for h in seeded if h != "@bench-selftest-ghost"][:-1]
                   if False else roster2)
        # seed: remove the last real handle so it counts as ARRIVED next read
        missing = roster2[-1]
        save_state([h for h in roster2 if h != missing])
        print(f"SEED: state roster hides {missing}; next read should knock")
        res3 = read_world(pm)
        _, roster3, _, _ = parse(res3, ear_r)
        arr = [h for h in roster3 if h not in (load_state().get("roster") or [])]
        receipt = knock(pm, arr, [], roster3)
        ok = isinstance(receipt, dict) and ("path" in receipt or receipt.get("accepted"))
        note_read = read_world(pm)
        note = (note_read.get("note") or "")
        tail = (note[-160:] if isinstance(note, str) else json.dumps(note)[:200])
        print(f"NOTE-TAIL: ...{tail}")
        save_state(roster3)
        print("BENCH-PASS (seeded knock)" if ok else "BENCH-FAIL (receipt shape)")
        return 0
    print("BENCH-PASS (empty-porch diff-clean)")
    # empty-porch knock path still needs a live test: seed a clearly-labeled
    # ghost fixture (no real resident is claimed absent) and knock its leave.
    ghost = "@bench-selftest-ghost"
    save_state([ghost])
    print(f"SEED: roster holds {ghost}; next read should knock its leave")
    res3 = read_world(pm)
    _, roster3, _, _ = parse(res3, ear_r)
    arr = [h for h in roster3 if h not in (load_state().get("roster") or [])]
    left = [h for h in (load_state().get("roster") or []) if h not in roster3]
    receipt = knock(pm, arr, left, roster3, bench_test=True)
    if isinstance(receipt, dict):
        print("RECEIPT-KEYS:", sorted(receipt.keys()))
    else:
        print("RECEIPT-KEYS: (not a dict)", type(receipt).__name__)
    note_read = read_world(pm)
    note = (note_read.get("note") or "")
    ts_sent = ts_marker = None
    tail = (note[-220:] if isinstance(note, str) else json.dumps(note)[:220])
    ok = isinstance(note, str) and "[porch-ear bench]" in note
    if not ok and isinstance(receipt, dict):
        ok = bool(receipt.get("path"))
    if not ok and isinstance(receipt, dict):
        ok = bool(receipt.get("accepted"))
    print(f"NOTE-TAIL: ...{tail}")
    save_state(roster3)
    print("BENCH-PASS (ghost-leave knock)" if ok else "BENCH-FAIL (receipt shape)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--once", action="store_true")
    g.add_argument("--bench", action="store_true")
    g.add_argument("--arm", action="store_true")
    g.add_argument("--stand-down", action="store_true")
    ap.add_argument("--window", default="09:00-01:00")
    ap.add_argument("--radius", type=float, default=120.0)
    ap.add_argument("--greet", action="store_true")
    ap.add_argument("--fire", action="store_true",
                    help="watchdog mode: silence on gated/quiet fires")
    a = ap.parse_args()
    pm = make_client()
    if a.bench:
        sys.exit(bench(pm, a.radius))
    if a.arm:
        sys.exit(arm(pm))
    if a.stand_down:
        sys.exit(stand_down())
    sys.exit(once(pm, a.window, a.radius, greet=a.greet, fire=a.fire))


if __name__ == "__main__":
    main()