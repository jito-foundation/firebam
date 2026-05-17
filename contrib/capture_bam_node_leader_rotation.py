#!/usr/bin/env python3

"""Capture tcpdump on a BAM node around upcoming leader rotations."""

import argparse
import datetime as dt
import json
import math
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

PROCESSED_COMMITMENT = "processed"
DEFAULT_VALIDATORS_URL = "https://explorer.bam.dev/api/v1/validators"
DEFAULT_CAPTURE_FILTER = "not (net 127.0.0.0/8 or host ::1)"
FAST_POLL_WINDOW_SEC = 10.0
FAST_POLL_INTERVAL_SEC = 1.0
MATCH_MODES = ("exact", "prefix", "contains")


@dataclass(frozen=True)
class ConnectedValidator:
    validator_pubkey: str
    bam_node_connection: str
    stake: float
    stake_percentage: float


@dataclass(frozen=True)
class ScheduledRotation:
    validator: ConnectedValidator
    current_slot: int
    next_leader_slot: int
    next_rotation_end_slot: int


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


def tcp_port(value: str) -> int:
    parsed = non_negative_int(value)
    if parsed == 0 or parsed > 65535:
        raise argparse.ArgumentTypeError("must be in 1..65535")
    return parsed


def parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Query BAM explorer validators, find pubkey matches connected to this BAM node, "
            "and capture tcpdump around their next leader rotations."
        )
    )
    p.add_argument("pubkey", type=non_empty_string, help="validator pubkey to match")
    p.add_argument(
        "--validators-url",
        type=non_empty_string,
        default=os.getenv("BAM_VALIDATORS_URL", DEFAULT_VALIDATORS_URL),
        help=f"BAM explorer validators API (default: {DEFAULT_VALIDATORS_URL})",
    )
    p.add_argument(
        "--bam-node-connection",
        type=non_empty_string,
        default=os.getenv("BAM_NODE_CONNECTION", socket.gethostname().split(".", 1)[0]),
        help="restrict to this BAM node connection name (default: local hostname)",
    )
    p.add_argument(
        "--all-bam-nodes",
        action="store_true",
        help="do not restrict matches to a single bam_node_connection",
    )
    p.add_argument(
        "--bam-node-match-mode",
        choices=MATCH_MODES,
        default=os.getenv("BAM_NODE_MATCH_MODE", "exact"),
        help="how to match --bam-node-connection against bam_node_connection",
    )
    p.add_argument(
        "--pubkey-match-mode",
        choices=MATCH_MODES,
        default=os.getenv("PUBKEY_MATCH_MODE", "exact"),
        help="how to match the pubkey argument against validator_pubkey",
    )
    p.add_argument("--rpc-url", type=non_empty_string, default=os.getenv("RPC_URL", "http://127.0.0.1:8899"))
    p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "bam_leader_rotations"))
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
    p.add_argument("--iface", type=non_empty_string, default=os.getenv("CAPTURE_IFACE", "any"))
    p.add_argument(
        "--validator-ip",
        type=non_empty_string,
        default=os.getenv("VALIDATOR_IP"),
        help="optional validator IP to narrow the tcpdump filter",
    )
    p.add_argument(
        "--validator-port",
        type=tcp_port,
        default=tcp_port(os.getenv("VALIDATOR_PORT")) if os.getenv("VALIDATOR_PORT") else None,
        help="optional validator port to narrow the tcpdump filter",
    )
    p.add_argument(
        "--tcpdump-filter",
        type=non_empty_string,
        default=os.getenv("TCPDUMP_FILTER"),
        help="additional tcpdump filter to AND with the default filter",
    )
    p.add_argument(
        "--request-timeout-sec",
        type=non_negative_float,
        default=non_negative_float(os.getenv("REQUEST_TIMEOUT_SEC", "10")),
    )
    p.add_argument(
        "--sudo",
        dest="use_sudo",
        action="store_true",
        default=os.geteuid() != 0,
        help="run tcpdump via sudo (default: enabled when not already root)",
    )
    p.add_argument(
        "--no-sudo",
        dest="use_sudo",
        action="store_false",
        help="run tcpdump directly without sudo",
    )

    args = p.parse_args()
    parsed_rpc_url = urllib.parse.urlparse(args.rpc_url)
    if parsed_rpc_url.scheme not in {"http", "https"} or not parsed_rpc_url.netloc:
        p.error(f"invalid RPC URL '{args.rpc_url}'")
    parsed_validators_url = urllib.parse.urlparse(args.validators_url)
    if parsed_validators_url.scheme not in {"http", "https"} or not parsed_validators_url.netloc:
        p.error(f"invalid validators URL '{args.validators_url}'")
    if args.all_bam_nodes:
        args.bam_node_connection = None
    return args


def rpc_call(rpc_url: str, method: str, params=None, timeout_sec: float = 10.0) -> Any:
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
        with urllib.request.urlopen(req, timeout=max(0.1, timeout_sec)) as resp:
            body = resp.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"RPC call failed for method '{method}': {exc}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"RPC response for method '{method}' was not valid JSON") from exc

    if "error" in payload:
        raise RuntimeError(f"RPC method '{method}' returned error: {payload['error']}")
    return payload["result"]


def get_next_leader_rotation_for_pubkey(
    rpc_url: str,
    validator_pubkey: str,
    min_start_slot_exclusive: Optional[int] = None,
    timeout_sec: float = 10.0,
) -> tuple[int, int, int]:
    epoch_info = rpc_call(rpc_url, "getEpochInfo", timeout_sec=timeout_sec)
    current_slot = int(epoch_info["absoluteSlot"])
    slot_index = int(epoch_info["slotIndex"])
    slots_in_epoch = int(epoch_info["slotsInEpoch"])
    epoch_start_slot = current_slot - slot_index
    search_after_slot = current_slot
    if min_start_slot_exclusive is not None:
        search_after_slot = max(search_after_slot, min_start_slot_exclusive)

    schedule = rpc_call(rpc_url, "getLeaderSchedule", [current_slot], timeout_sec=timeout_sec) or {}
    next_epoch_start_slot = epoch_start_slot + slots_in_epoch
    next_schedule = rpc_call(rpc_url, "getLeaderSchedule", [next_epoch_start_slot], timeout_sec=timeout_sec) or {}

    for schedule_epoch_start_slot, candidate_schedule in (
        (epoch_start_slot, schedule),
        (next_epoch_start_slot, next_schedule),
    ):
        my_slots_set: set[int] = set()
        for slot_idx in candidate_schedule.get(validator_pubkey) or []:
            if slot_idx is None:
                my_slots_set.add(0)
                continue
            try:
                my_slots_set.add(int(slot_idx))
            except (TypeError, ValueError):
                try:
                    my_slots_set.add(int(float(slot_idx)))
                except (TypeError, ValueError):
                    my_slots_set.add(0)
        my_slots = sorted(my_slots_set)
        if not my_slots:
            continue

        rotation_start_idx = my_slots[0]
        rotation_end_idx = my_slots[0]
        for slot_idx in my_slots[1:]:
            if slot_idx == rotation_end_idx + 1:
                rotation_end_idx = slot_idx
                continue

            rotation_start_slot = schedule_epoch_start_slot + rotation_start_idx
            if rotation_start_slot > search_after_slot:
                return current_slot, rotation_start_slot, schedule_epoch_start_slot + rotation_end_idx

            rotation_start_idx = slot_idx
            rotation_end_idx = slot_idx

        rotation_start_slot = schedule_epoch_start_slot + rotation_start_idx
        if rotation_start_slot > search_after_slot:
            return current_slot, rotation_start_slot, schedule_epoch_start_slot + rotation_end_idx

    raise RuntimeError(
        f"validator {validator_pubkey} has no upcoming leader rotations in the current or next epoch"
    )


def sanitize_component(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)


def capture_rotation(args: argparse.Namespace, rotation: ScheduledRotation, capture_idx: int) -> int:
    samples = rpc_call(args.rpc_url, "getRecentPerformanceSamples", [1], timeout_sec=args.request_timeout_sec) or []
    slot_seconds = 0.4
    if samples:
        num_slots = int(samples[0].get("numSlots", 0))
        sample_period = float(samples[0].get("samplePeriodSecs", 0))
        if num_slots > 0 and sample_period > 0:
            slot_seconds = sample_period / num_slots
    slots_before_start = max(1, math.ceil(args.capture_lead_time_sec / slot_seconds))
    start_slot = max(0, rotation.next_leader_slot - slots_before_start)
    filter_parts: list[str] = []
    if args.validator_ip:
        filter_parts.append(f"host {args.validator_ip}")
    if args.validator_port is not None:
        filter_parts.append(f"port {args.validator_port}")
    if args.tcpdump_filter:
        filter_parts.append(f"({args.tcpdump_filter})")
    filter_parts.append(DEFAULT_CAPTURE_FILTER)
    tcpdump_filter = " and ".join(filter_parts)

    if capture_idx > 1:
        print()
    print(f"capture #{capture_idx}")
    print(f"validator pubkey: {rotation.validator.validator_pubkey}")
    print(f"bam node connection: {rotation.validator.bam_node_connection}")
    print(f"current slot: {rotation.current_slot}")
    print(
        "next leader rotation: "
        f"{rotation.next_leader_slot}-{rotation.next_rotation_end_slot} "
        f"({rotation.next_rotation_end_slot - rotation.next_leader_slot + 1} slots)"
    )
    print(f"estimated slot duration: {slot_seconds:.2f}s")
    print(f"starting capture at slot >= {start_slot} (~{args.capture_lead_time_sec}s before leader rotation)")

    while True:
        now_slot = int(rpc_call(args.rpc_url, "getSlot", timeout_sec=args.request_timeout_sec))
        if now_slot >= start_slot:
            break

        slots_remaining = start_slot - now_slot
        secs_remaining = slots_remaining * slot_seconds
        print(f"current slot: {now_slot}, capture start slot: {start_slot}, (~{secs_remaining:.1f}s remaining)")
        if secs_remaining <= FAST_POLL_WINDOW_SEC:
            sleep_for = min(FAST_POLL_INTERVAL_SEC, max(0.1, secs_remaining))
        else:
            sleep_for = max(0.1, min(2.0, secs_remaining / 2.0))
        time.sleep(sleep_for)

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_dir = (
        Path(args.output_root)
        / sanitize_component(rotation.validator.bam_node_connection)
        / sanitize_component(rotation.validator.validator_pubkey)
        / f"slot_{rotation.next_leader_slot}_{timestamp}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    tcpdump_cmd: list[str] = ["timeout", str(args.capture_time_sec)]
    if args.use_sudo:
        tcpdump_cmd.append("sudo")
    tcpdump_cmd.extend(
        [
            "tcpdump",
            "-i",
            args.iface,
            "-B",
            "8192",
            "-n",
            "-s",
            "0",
            "-w",
            f"dump_{rotation.next_leader_slot}.pcap",
            tcpdump_filter,
        ]
    )

    metadata = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "validator": asdict(rotation.validator),
        "current_slot": rotation.current_slot,
        "next_leader_slot": rotation.next_leader_slot,
        "next_rotation_end_slot": rotation.next_rotation_end_slot,
        "slot_seconds": slot_seconds,
        "capture_start_slot": start_slot,
        "capture_lead_time_sec": args.capture_lead_time_sec,
        "capture_time_sec": args.capture_time_sec,
        "iface": args.iface,
        "tcpdump_filter": tcpdump_filter,
        "tcpdump_command": tcpdump_cmd,
        "tcpdump_command_display": shlex.join(tcpdump_cmd),
        "rpc_url": args.rpc_url,
        "validators_url": args.validators_url,
        "request_timeout_sec": args.request_timeout_sec,
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        "capture directory: "
        f"{run_dir}; starting tcpdump for ~{args.capture_time_sec}s with filter '{tcpdump_filter}'"
    )
    proc = subprocess.Popen(tcpdump_cmd, cwd=run_dir)
    rc = proc.wait()
    if rc not in (0, 124):
        print(f"error: tcpdump failed with exit code {rc}", file=sys.stderr)
        return rc

    print(f"done: files written under {run_dir}")
    return 0


def main() -> int:
    args = parse_args()
    req = urllib.request.Request(
        args.validators_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "capture_bam_node_leader_rotation/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=max(0.1, args.request_timeout_sec)) as resp:
            payload = json.loads(resp.read())
    except urllib.error.URLError as exc:
        raise RuntimeError(f"request failed for '{args.validators_url}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"response from '{args.validators_url}' was not valid JSON") from exc

    if not isinstance(payload, list):
        raise RuntimeError("validators payload was not a JSON array")

    validators: list[ConnectedValidator] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        validator_pubkey = str(row.get("validator_pubkey", "")).strip()
        bam_node_connection = str(row.get("bam_node_connection", "")).strip()
        if not validator_pubkey or not bam_node_connection:
            continue
        validators.append(
            ConnectedValidator(
                validator_pubkey=validator_pubkey,
                bam_node_connection=bam_node_connection,
                stake=parse_float(row.get("stake")),
                stake_percentage=parse_float(row.get("stake_percentage")),
            )
        )
    if not validators:
        raise RuntimeError("validators payload contained no usable validator rows")

    matches: list[ConnectedValidator] = []
    for validator in validators:
        checks = [(validator.validator_pubkey, args.pubkey, args.pubkey_match_mode)]
        if args.bam_node_connection is not None:
            checks.append(
                (
                    validator.bam_node_connection,
                    args.bam_node_connection,
                    args.bam_node_match_mode,
                )
            )

        for candidate, needle, mode in checks:
            if mode == "exact":
                matched = candidate == needle
            elif mode == "prefix":
                matched = candidate.startswith(needle)
            elif mode == "contains":
                matched = needle in candidate
            else:
                raise ValueError(f"unsupported match mode: {mode}")
            if not matched:
                break
        else:
            matches.append(validator)
    matches.sort(key=lambda validator: (validator.validator_pubkey, validator.bam_node_connection))

    if not matches:
        local_hint = ""
        if args.bam_node_connection is not None:
            local_hint = (
                f" on bam_node_connection '{args.bam_node_connection}'"
                f" using match mode '{args.bam_node_match_mode}'"
            )
        raise RuntimeError(
            f"no validators matched pubkey '{args.pubkey}'{local_hint}; "
            "try adjusting --pubkey-match-mode, --bam-node-connection, or --all-bam-nodes"
        )

    print(f"matched validators: {len(matches)}")
    for match in matches:
        print(
            f"- {match.validator_pubkey} "
            f"(bam_node_connection={match.bam_node_connection}, "
            f"stake={match.stake:.2f}, stake_percentage={match.stake_percentage:.4f})"
        )

    missing: list[str] = []
    if shutil.which("timeout") is None:
        missing.append("timeout")
    if shutil.which("tcpdump") is None:
        missing.append("tcpdump")
    if args.use_sudo and shutil.which("sudo") is None:
        missing.append("sudo")
    if missing:
        raise RuntimeError(f"missing required commands: {', '.join(sorted(missing))}")

    remaining = list(matches)
    capture_idx = 1
    while remaining:
        rotations: list[ScheduledRotation] = []
        next_remaining: list[ConnectedValidator] = []
        for match in remaining:
            try:
                current_slot, next_leader_slot, next_rotation_end_slot = get_next_leader_rotation_for_pubkey(
                    args.rpc_url,
                    match.validator_pubkey,
                    timeout_sec=args.request_timeout_sec,
                )
            except RuntimeError as exc:
                print(f"warning: {exc}", file=sys.stderr)
                continue

            rotations.append(
                ScheduledRotation(
                    validator=match,
                    current_slot=current_slot,
                    next_leader_slot=next_leader_slot,
                    next_rotation_end_slot=next_rotation_end_slot,
                )
            )
            next_remaining.append(match)

        if not rotations:
            return 1

        next_rotation = min(
            rotations,
            key=lambda item: (
                item.next_leader_slot,
                item.next_rotation_end_slot,
                item.validator.validator_pubkey,
                item.validator.bam_node_connection,
            ),
        )

        rc = capture_rotation(args, next_rotation, capture_idx)
        if rc != 0:
            return rc

        remaining = [match for match in next_remaining if match != next_rotation.validator]
        capture_idx += 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        raise SystemExit(130)
