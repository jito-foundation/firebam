#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import math
import os
import select
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

PROCESSED_COMMITMENT = "processed"
FAST_POLL_WINDOW_SEC = 10.0
FAST_POLL_INTERVAL_SEC = 1.0
WEBSOCAT_BUFFER_BYTES = "12000000"


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def non_empty_string(value: str) -> str:
    parsed = value.strip()
    if not parsed:
        raise argparse.ArgumentTypeError("must be non-empty")
    return parsed


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Capture tcpdump + metrics around the next leader slot."
    )
    p.add_argument("--host", type=non_empty_string, default=os.getenv("HOST", "127.0.0.1"))
    p.add_argument("--rpc-url", type=non_empty_string, default=os.getenv("RPC_URL"))
    p.add_argument("--metrics-url", type=non_empty_string, default=os.getenv("METRICS_URL"))
    p.add_argument(
        "--websocket-url",
        type=non_empty_string,
        default=os.getenv("WEBSOCKET_URL", os.getenv("WEBSOCKET_HOST")),
        help="websocket URL (default: ws://<host>:80/websocket)",
    )
    p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "leader_rotations"))
    p.add_argument(
        "--capture-lead-time-sec",
        type=non_negative_float,
        default=non_negative_float(os.getenv("CAPTURE_LEAD_TIME_SEC", "5")),
    )
    p.add_argument(
        "--capture-time-sec",
        type=non_negative_float,
        default=non_negative_float(os.getenv("CAPTURE_TIME_SEC", "10")),
    )
    p.add_argument(
        "--metrics-interval",
        type=non_negative_float,
        default=non_negative_float(os.getenv("METRICS_INTERVAL", "0.2")),
    )
    p.add_argument(
        "--websocket-mode",
        choices=("recent", "since-startup"),
        default=os.getenv("WEBSOCKET_MODE", "recent"),
    )
    p.add_argument(
        "--websocket-recent-count",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_RECENT_COUNT", "16")),
    )
    p.add_argument(
        "--websocket-snapshot-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_SNAPSHOT_SECS", "2")),
    )
    p.add_argument(
        "--websocket-query-wait-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_QUERY_WAIT_SECS", "8")),
    )
    p.add_argument(
        "--websocket-detail-timeout-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_DETAIL_TIMEOUT_SECS", "60")),
    )
    args = p.parse_args()
    if args.rpc_url is None:
        args.rpc_url = f"http://{args.host}:8899"
    if args.metrics_url is None:
        args.metrics_url = f"http://{args.host}:7999/metrics"
    if args.websocket_url is None:
        args.websocket_url = f"ws://{args.host}:80/websocket"
    if shutil.which("websocat") is None:
        p.error("websocket scrape requires `websocat` in PATH")
    return args


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


def parse_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


class WebsocketJsonSession:
    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self.proc: Optional[subprocess.Popen[str]] = None

    def __enter__(self) -> "WebsocketJsonSession":
        self.proc = subprocess.Popen(
            ["websocat", "-B", WEBSOCAT_BUFFER_BYTES, self.websocket_url],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.proc is None:
            return

        if self.proc.stdin is not None and not self.proc.stdin.closed:
            self.proc.stdin.close()
        if self.proc.stdout is not None and not self.proc.stdout.closed:
            self.proc.stdout.close()

        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def send_json(self, payload: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("websocket process is not available")
        self.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()

    def read_json(self, timeout_sec: float) -> Optional[dict[str, Any]]:
        if self.proc is None or self.proc.stdout is None:
            return None

        timeout = max(0.0, timeout_sec)
        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            return None

        line = self.proc.stdout.readline()
        if not line:
            return None

        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            return None

        return payload if isinstance(payload, dict) else None


def capture_snapshot_until_ready(websocket_url: str, max_wait_secs: int) -> Optional[list[dict[str, Any]]]:
    snapshot: list[dict[str, Any]] = []
    ready_fields: set[tuple[str, str]] = set()
    deadline = time.monotonic() + max_wait_secs

    with WebsocketJsonSession(websocket_url) as ws:
        while time.monotonic() <= deadline:
            row = ws.read_json(1.0)
            if row is None:
                continue

            snapshot.append(row)
            topic = row.get("topic")
            key = row.get("key")
            if topic == "summary" and key in {"identity_key", "startup_time_nanos", "completed_slot"} and row.get("value") is not None:
                ready_fields.add((topic, key))
            elif topic == "epoch" and key == "new":
                ready_fields.add((topic, key))

            if len(ready_fields) == 4:
                return snapshot

    return None


def compute_produced_slots(snapshot: list[dict[str, Any]], identity: str, completed_slot: int) -> list[int]:
    produced: set[int] = set()
    for row in snapshot:
        if row.get("topic") != "epoch" or row.get("key") != "new":
            continue

        epoch_value = row.get("value")
        if not isinstance(epoch_value, dict):
            continue

        staked_pubkeys = epoch_value.get("staked_pubkeys")
        if not isinstance(staked_pubkeys, list):
            continue

        try:
            validator_idx = staked_pubkeys.index(identity)
        except ValueError:
            continue

        start_slot = parse_int(epoch_value.get("start_slot"))
        leader_slots = epoch_value.get("leader_slots")
        if isinstance(leader_slots, dict):
            leader_entries = leader_slots.items()
        elif isinstance(leader_slots, list):
            leader_entries = enumerate(leader_slots)
        else:
            continue

        for slot_group, leader_idx in leader_entries:
            if parse_int(leader_idx) != validator_idx:
                continue
            slot_group_idx = parse_int(slot_group)

            base_slot = start_slot + (slot_group_idx * 4)
            for slot in range(base_slot, base_slot + 4):
                if slot <= completed_slot:
                    produced.add(slot)

    return sorted(produced)


def build_snapshot_metadata(snapshot: list[dict[str, Any]]) -> tuple[str, list[int]]:
    identity: Optional[str] = None
    startup_time_nanos: Optional[str] = None
    completed_slot: Optional[int] = None
    for row in snapshot:
        topic = row.get("topic")
        key = row.get("key")
        value = row.get("value")
        if topic == "summary" and key == "identity_key" and value is not None and identity is None:
            identity = str(value)
        elif topic == "summary" and key == "startup_time_nanos" and value is not None and startup_time_nanos is None:
            startup_time_nanos = str(value)
        elif topic == "summary" and key == "completed_slot" and value is not None and completed_slot is None:
            completed_slot = parse_int(value)

    if identity is None or startup_time_nanos is None or completed_slot is None:
        raise RuntimeError("missing required summary fields in websocket snapshot")

    produced_slots = compute_produced_slots(snapshot, identity, completed_slot)
    if not produced_slots:
        raise RuntimeError("no produced slots discovered from epoch schedules")

    return startup_time_nanos, produced_slots


def collect_query_details(
    websocket_url: str,
    requests: list[dict[str, Any]],
    idle_wait_secs: int,
    max_wait_secs: int,
) -> list[dict[str, Any]]:
    pending_ids = {
        str(req.get("id"))
        for req in requests
        if req.get("id") is not None
    }
    details: list[dict[str, Any]] = []

    seen_ids: set[str] = set()
    deadline = time.monotonic() + max_wait_secs
    idle_deadline = time.monotonic() + idle_wait_secs

    with WebsocketJsonSession(websocket_url) as ws:
        for req in requests:
            ws.send_json(req)

        while time.monotonic() <= deadline and len(seen_ids) < len(pending_ids):
            row = ws.read_json(1.0)
            if row is None:
                if time.monotonic() > idle_deadline:
                    break
                continue

            details.append(row)
            if (
                row.get("topic") == "slot"
                and row.get("key") in {"query", "query_detailed"}
                and row.get("id") is not None
            ):
                response_id = str(row.get("id"))
                if response_id in pending_ids and response_id not in seen_ids:
                    seen_ids.add(response_id)
                    idle_deadline = time.monotonic() + idle_wait_secs

            if time.monotonic() > idle_deadline:
                break

    return details


def parse_slot_result_row(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    if row.get("topic") != "slot" or row.get("key") not in {"query", "query_detailed"}:
        return None

    value = row.get("value")
    if not isinstance(value, dict):
        return None

    publish = value.get("publish")
    if not isinstance(publish, dict):
        return None

    waterfall = value.get("waterfall")
    waterfall_in: dict[str, Any] = {}
    waterfall_drops: dict[str, Any] = {}
    if isinstance(waterfall, dict):
        if isinstance(waterfall.get("in"), dict):
            waterfall_in = waterfall["in"]
        if isinstance(waterfall.get("out"), dict):
            waterfall_drops = waterfall["out"]

    in_quic = parse_int(waterfall_in.get("quic"))
    in_udp = parse_int(waterfall_in.get("udp"))
    in_gossip = parse_int(waterfall_in.get("gossip"))
    in_block_engine = (
        parse_int(waterfall_in.get("block_engine"))
        if "block_engine" in waterfall_in
        else parse_int(waterfall_in.get("bam"))
    )
    in_pack_cranked = parse_int(waterfall_in.get("pack_cranked"))
    in_pack_retained = parse_int(waterfall_in.get("pack_retained"))
    in_resolv_retained = parse_int(waterfall_in.get("resolv_retained"))

    drop_malformed = (
        parse_int(waterfall_drops.get("tpu_quic_invalid"))
        + parse_int(waterfall_drops.get("tpu_udp_invalid"))
        + parse_int(waterfall_drops.get("quic_frag_drop"))
    )
    drop_abandoned = parse_int(waterfall_drops.get("quic_abandoned"))
    drop_net_overrun = parse_int(waterfall_drops.get("net_overrun"))
    drop_quic_overrun = parse_int(waterfall_drops.get("quic_overrun"))
    drop_verify_overrun = parse_int(waterfall_drops.get("verify_overrun"))
    drop_unparseable = parse_int(waterfall_drops.get("verify_parse"))
    drop_bad_signature = parse_int(waterfall_drops.get("verify_failed"))
    drop_verify_duplicate = parse_int(waterfall_drops.get("verify_duplicate"))
    drop_dedup_duplicate = parse_int(waterfall_drops.get("dedup_duplicate"))
    drop_resolv_lut_failed = parse_int(waterfall_drops.get("resolv_lut_failed"))
    drop_resolv_expired = parse_int(waterfall_drops.get("resolv_expired"))
    drop_resolv_ancient = parse_int(waterfall_drops.get("resolv_ancient"))
    drop_resolv_no_ledger = parse_int(waterfall_drops.get("resolv_no_ledger"))
    drop_pack_retained = parse_int(waterfall_drops.get("pack_retained"))
    drop_pack_invalid = parse_int(waterfall_drops.get("pack_invalid"))
    drop_pack_expired = parse_int(waterfall_drops.get("pack_expired"))
    drop_pack_already_executed = parse_int(waterfall_drops.get("pack_already_executed"))
    drop_bank_invalid = parse_int(waterfall_drops.get("bank_invalid"))
    drop_block_success = parse_int(waterfall_drops.get("block_success"))
    drop_block_fail = parse_int(waterfall_drops.get("block_fail"))
    unresolved = max(0, parse_int(waterfall_drops.get("resolv_retained")) - in_resolv_retained)

    received = in_quic + in_udp
    verify = (
        received
        + in_gossip
        + in_block_engine
        - drop_malformed
        - drop_abandoned
        - drop_net_overrun
        - drop_quic_overrun
        - drop_verify_overrun
    )
    dedup = verify - drop_unparseable - drop_bad_signature - drop_verify_duplicate
    resolv = dedup - drop_dedup_duplicate
    pack = (
        resolv
        + in_pack_cranked
        + in_pack_retained
        - unresolved
        - drop_resolv_lut_failed
        - drop_resolv_expired
        - drop_resolv_ancient
        - drop_resolv_no_ledger
    )
    bank = (
        pack
        - drop_pack_retained
        - drop_pack_invalid
        - drop_pack_expired
        - drop_pack_already_executed
    )
    packed = bank - drop_bank_invalid

    return {
        "slot": parse_int(publish.get("slot")),
        "sankey_nodes": {
            "quic": in_quic,
            "udp": in_udp,
            "received": received,
            "gossip": in_gossip,
            "block_engine": in_block_engine,
            "verify": verify,
            "dedup": dedup,
            "resolv": resolv,
            "crank": in_pack_cranked,
            "buffered_in": in_pack_retained,
            "pack": pack,
            "bank": bank,
            "packed": packed,
        },
        "sankey_drops": {
            "malformed": drop_malformed,
            "abandoned": drop_abandoned,
            "unparseable": drop_unparseable,
            "bad_signature": drop_bad_signature,
            "verify_duplicate": drop_verify_duplicate,
            "dedup_duplicate": drop_dedup_duplicate,
            "unresolved": unresolved,
            "bad_lut": drop_resolv_lut_failed,
            "resolv_expired": drop_resolv_expired,
            "buffered": drop_pack_retained,
            "unpackable": drop_pack_invalid,
            "pack_expired": drop_pack_expired,
            "already_executed": drop_pack_already_executed,
            "unexecutable": drop_bank_invalid,
            "block_success": drop_block_success,
            "block_fail": drop_block_fail,
        },
        "slot_metrics": {
            "votes": (
                (
                    parse_int(publish.get("success_vote_transaction_cnt"))
                    if "success_vote_transaction_cnt" in publish
                    else parse_int(publish.get("success_vote_transactions"))
                )
                + (
                    parse_int(publish.get("failed_vote_transaction_cnt"))
                    if "failed_vote_transaction_cnt" in publish
                    else parse_int(publish.get("failed_vote_transactions"))
                )
            ),
            "non_vote_failure": (
                parse_int(publish.get("failed_nonvote_transaction_cnt"))
                if "failed_nonvote_transaction_cnt" in publish
                else parse_int(publish.get("failed_nonvote_transactions"))
            ),
            "non_vote_success": (
                parse_int(publish.get("success_nonvote_transaction_cnt"))
                if "success_nonvote_transaction_cnt" in publish
                else parse_int(publish.get("success_nonvote_transactions"))
            ),
            "compute_units": parse_int(publish.get("compute_units")),
            "priority_fees_sol": round((parse_int(publish.get("priority_fee")) / 1_000_000_000) * 10000) / 10000,
            "transaction_fees_sol": round((parse_int(publish.get("transaction_fee")) / 1_000_000_000) * 10000) / 10000,
            "tips_sol": round((parse_int(publish.get("tips")) / 1_000_000_000) * 10000) / 10000,
        },
        "publish": publish,
    }


def parse_slot_results(
    details: list[dict[str, Any]],
    mode: str,
    startup_time_nanos: str,
    recent_count: int,
) -> list[dict[str, Any]]:
    slots_by_id: dict[int, dict[str, Any]] = {}
    for row in details:
        parsed = parse_slot_result_row(row)
        if parsed is not None:
            slots_by_id[int(parsed["slot"])] = parsed

    parsed_rows = [slots_by_id[slot] for slot in sorted(slots_by_id.keys())]
    if mode == "since-startup":
        startup_nanos = parse_int(startup_time_nanos)
        parsed_rows = [
            row
            for row in parsed_rows
            if parse_int((row.get("publish") or {}).get("completed_time_nanos")) >= startup_nanos
        ]
    elif mode == "recent" and recent_count > 0:
        parsed_rows = parsed_rows[-recent_count:]

    return parsed_rows


def scrape_websocket_slots(
    websocket_url: str,
    mode: str,
    recent_count: int,
    snapshot_secs: int,
    query_wait_secs: int,
    detail_timeout_secs: int,
) -> list[dict[str, Any]]:
    websocket_urls = (
        [websocket_url]
        if websocket_url.startswith(("ws://", "wss://"))
        else [f"ws://{websocket_url}:80/websocket", f"wss://{websocket_url}/websocket"]
    )
    last_error = RuntimeError(
        "timed out waiting for required websocket snapshot data; "
        "try increasing --websocket-snapshot-secs"
    )

    for websocket_url in websocket_urls:
        snapshot = capture_snapshot_until_ready(websocket_url, snapshot_secs)
        if snapshot is None:
            continue

        startup_time_nanos, produced_slots = build_snapshot_metadata(snapshot)
        query_slots = produced_slots
        if mode == "recent" and recent_count > 0:
            query_slots = query_slots[-max(recent_count * 4, recent_count + 8):]

        query_payloads = [
            {
                "topic": "slot",
                "key": "query_detailed",
                "id": 1000 + i,
                "params": {"slot": slot},
            }
            for i, slot in enumerate(query_slots)
        ]

        seen_query_results = False
        for wait_secs, timeout_secs in (
            (query_wait_secs, detail_timeout_secs),
            (query_wait_secs + 4, detail_timeout_secs + 40),
        ):
            details = collect_query_details(
                websocket_url,
                query_payloads,
                wait_secs,
                timeout_secs,
            )
            seen_query_results = seen_query_results or any(
                row.get("topic") == "slot"
                and row.get("key") in {"query", "query_detailed"}
                and row.get("value") is not None
                for row in details
            )

            parsed_rows = parse_slot_results(details, mode, startup_time_nanos, recent_count)
            if parsed_rows:
                return parsed_rows

        if seen_query_results:
            return []
        last_error = RuntimeError(
            "no slot query results returned; "
            "try increasing --websocket-query-wait-secs/--websocket-detail-timeout-secs",
        )

    raise last_error

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
        while (remaining := capture_deadline - time.monotonic()) > 0:
            with urllib.request.urlopen(args.metrics_url, timeout=max(0.1, min(10.0, remaining))) as resp:
                (run_dir / f"log_{i}.txt").write_bytes(resp.read())
            i += 1

            sleep_for = min(args.metrics_interval, capture_deadline - time.monotonic())
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        rc = tcpdump_proc.wait()

    if rc not in (0, 124):
        print(f"error: tcpdump failed with exit code {rc}", file=sys.stderr)
        return rc

    websocket_output_path = run_dir / "websocket_slots.ndjson"
    print(
        "running websocket scrape at end of capture: "
        f"url={args.websocket_url} mode={args.websocket_mode}"
    )
    websocket_rows = scrape_websocket_slots(
        websocket_url=args.websocket_url,
        mode=args.websocket_mode,
        recent_count=args.websocket_recent_count,
        snapshot_secs=args.websocket_snapshot_secs,
        query_wait_secs=args.websocket_query_wait_secs,
        detail_timeout_secs=args.websocket_detail_timeout_secs,
    )
    if websocket_rows:
        with websocket_output_path.open("w", encoding="utf-8") as out:
            for row in websocket_rows:
                out.write(json.dumps(row, separators=(",", ":")))
                out.write("\n")
        print(f"websocket scrape written to {websocket_output_path}")
    else:
        print(f"warning: no produced slots matched mode='{args.websocket_mode}'", file=sys.stderr)

    print(f"done: files written under {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
