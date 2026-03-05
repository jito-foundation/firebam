#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional

PROCESSED_COMMITMENT = "processed"
FAST_POLL_WINDOW_SEC = 10.0
FAST_POLL_INTERVAL_SEC = 1.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Capture tcpdump + metrics around the next leader slot."
    )
    p.add_argument("--rpc-url", default=os.getenv("RPC_URL", "http://127.0.0.1:8899"))
    p.add_argument("--metrics-url", default=os.getenv("METRICS_URL", "http://127.0.0.1:7999/metrics"))
    p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "leader_rotations"))
    p.add_argument(
        "--capture-lead-time-sec",
        type=float,
        default=float(os.getenv("CAPTURE_LEAD_TIME_SEC", "5")),
    )
    p.add_argument("--capture-time-sec", type=float, default=float(os.getenv("CAPTURE_TIME_SEC", "10")))
    p.add_argument("--metrics-interval", type=float, default=float(os.getenv("METRICS_INTERVAL", "0.2")))
    p.add_argument("--websocket-host", default=os.getenv("WEBSOCKET_HOST"))
    p.add_argument(
        "--websocket-mode",
        choices=("recent", "since-startup"),
        default=os.getenv("WEBSOCKET_MODE", "recent"),
    )
    p.add_argument(
        "--websocket-recent-count",
        type=int,
        default=int(os.getenv("WEBSOCKET_RECENT_COUNT", "16")),
    )
    return p.parse_args()


def rpc_call(rpc_url: str, method: str, params=None) -> Any:
    rpc_params = [] if params is None else list(params)
    if method in {"getEpochInfo", "getSlot"}:
        if rpc_params and isinstance(rpc_params[-1], dict):
            rpc_params[-1] = {**rpc_params[-1], "commitment": PROCESSED_COMMITMENT}
        else:
            rpc_params.append({"commitment": PROCESSED_COMMITMENT})

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": rpc_params,
    }
    req = urllib.request.Request(
        rpc_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"RPC call failed for method '{method}': {exc}") from exc

    return json.loads(body)["result"]


def get_next_leader_slot(rpc_url: str) -> tuple[str, int, int]:
    identity = rpc_call(rpc_url, "getIdentity")["identity"]

    epoch_info = rpc_call(rpc_url, "getEpochInfo")
    current_slot = int(epoch_info["absoluteSlot"])
    slot_index = int(epoch_info["slotIndex"])
    slots_in_epoch = int(epoch_info["slotsInEpoch"])
    epoch_start_slot = current_slot - slot_index

    schedule = rpc_call(rpc_url, "getLeaderSchedule", [current_slot]) or {}
    my_slots = schedule.get(identity) or []
    next_idx = next((slot for slot in my_slots if slot > slot_index), None)

    if next_idx is not None:
        return identity, current_slot, epoch_start_slot + int(next_idx)

    next_epoch_start_slot = epoch_start_slot + slots_in_epoch
    next_schedule = rpc_call(rpc_url, "getLeaderSchedule", [next_epoch_start_slot]) or {}
    next_epoch_slots = next_schedule.get(identity) or []
    if not next_epoch_slots:
        raise RuntimeError(f"Identity {identity} has no upcoming leader slots in current/next epoch")

    return identity, current_slot, next_epoch_start_slot + int(next_epoch_slots[0])


def get_slot_seconds(rpc_url: str) -> float:
    samples = rpc_call(rpc_url, "getRecentPerformanceSamples", [1]) or []
    if not samples:
        return 0.4

    num_slots = int(samples[0].get("numSlots", 0))
    sample_period = float(samples[0].get("samplePeriodSecs", 0))
    if num_slots <= 0 or sample_period <= 0:
        return 0.4

    return sample_period / num_slots


def resolve_websocket_host(websocket_host: Optional[str], rpc_url: str) -> str:
    if websocket_host:
        return websocket_host

    parsed = urllib.parse.urlparse(rpc_url)
    if parsed.hostname:
        return parsed.hostname

    return "127.0.0.1"


def main() -> int:
    args = parse_args()

    identity, current_slot, next_leader_slot = get_next_leader_slot(args.rpc_url)
    slot_seconds = get_slot_seconds(args.rpc_url)

    slots_before_start = max(1, math.ceil(args.capture_lead_time_sec / slot_seconds))
    start_slot = max(0, next_leader_slot - slots_before_start)

    print(f"validator identity: {identity}")
    print(f"current slot: {current_slot}")
    print(f"next leader slot: {next_leader_slot}")
    print(f"estimated slot seconds: {slot_seconds:.6f}")
    print(f"starting capture at slot >= {start_slot} (~{args.capture_lead_time_sec}s before leader slot)")

    while True:
        now_slot = int(rpc_call(args.rpc_url, "getSlot"))
        if now_slot >= start_slot:
            break

        slots_remaining = start_slot - now_slot
        secs_remaining = slots_remaining * slot_seconds
        print(
            f"waiting: slot {now_slot}, start_slot {start_slot} "
            f"(est {secs_remaining:.2f}s remaining)"
        )
        if secs_remaining <= FAST_POLL_WINDOW_SEC:
            sleep_for = min(FAST_POLL_INTERVAL_SEC, max(0.1, secs_remaining))
        else:
            sleep_for = max(0.1, min(2.0, secs_remaining / 2.0))
        time.sleep(sleep_for)

    run_dir = (Path(args.output_root) / f"slot_{next_leader_slot}_{dt.datetime.now(dt.timezone.utc)}")
    run_dir.mkdir(parents=True, exist_ok=True)
    pcap_cmd = ["timeout", str(args.capture_time_sec), "sudo", "tcpdump", "-nn", "-w", f"dump_{next_leader_slot}.pcap"]

    print(f"capture directory: {run_dir}; starting tcpdump and metrics capture now for ~{args.capture_time_sec}s")
    tcpdump_proc = subprocess.Popen(pcap_cmd, cwd=run_dir)
    capture_deadline = time.monotonic() + args.capture_time_sec

    i = 0
    try:
        while True:
            remaining = capture_deadline - time.monotonic()
            if remaining <= 0:
                break

            with urllib.request.urlopen(args.metrics_url, timeout=(max(0.1, min(10.0, remaining)))) as resp:
                log_path = run_dir / f"log_{i}.txt"
                log_path.write_bytes(resp.read())
            i += 1

            remaining = capture_deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(args.metrics_interval, remaining))
    finally:
        rc = tcpdump_proc.wait()

    if rc not in (0, 124):
        print(f"error: tcpdump failed with exit code {rc}", file=sys.stderr)
        return rc

    scrape_script = Path(__file__).resolve().with_name("scrape_websocket.sh")
    websocket_output_path = run_dir / "websocket_slots.ndjson"
    websocket_stderr_path = run_dir / "websocket_scrape.stderr.log"

    if scrape_script.is_file():
        websocket_host = resolve_websocket_host(args.websocket_host, args.rpc_url)
        scrape_cmd = [
            str(scrape_script),
            "--host",
            websocket_host,
            "--mode",
            args.websocket_mode,
            "--output-file",
            str(websocket_output_path),
        ]
        if args.websocket_mode == "recent":
            scrape_cmd.extend(["--recent-count", str(args.websocket_recent_count)])

        print(f"running websocket scrape at end of capture: {' '.join(scrape_cmd)}")
        with websocket_stderr_path.open("wb") as websocket_stderr:
            scrape_rc = subprocess.run(
                scrape_cmd,
                stdout=subprocess.DEVNULL,
                stderr=websocket_stderr,
                cwd=run_dir,
                check=False,
            ).returncode

        if scrape_rc == 0:
            print(f"websocket scrape written to {websocket_output_path}")
        else:
            print(
                f"warning: websocket scrape failed with exit code {scrape_rc}; "
                f"see {websocket_stderr_path}",
                file=sys.stderr,
            )
    else:
        print(f"warning: websocket scrape script not found at {scrape_script}", file=sys.stderr)

    print(f"done: files written under {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
