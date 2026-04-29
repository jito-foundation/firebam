#!/usr/bin/env python3

"""Capture tcpdump/metrics around leader rotations and scrape websocket slot results."""

import argparse
import datetime as dt
import ipaddress
import json
import math
import os
import re
import select
import shutil
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
WEBSOCAT_BUFFER_BYTES = "12000000"
ETHTOOL_STATS_FILTER_RE = re.compile(r"drop|error|discard|miss|fifo", re.IGNORECASE)
WEBSOCKET_SUMMARY_KEYS = frozenset({"identity_key", "startup_time_nanos", "completed_slot"})
WEBSOCKET_SLOT_QUERY_REQUEST_KEY = "query_detailed"
WEBSOCKET_SLOT_QUERY_RESPONSE_KEY = "query"
WEBSOCKET_BAM_UPDATE_TOPIC = "bam"
WEBSOCKET_BAM_UPDATE_KEY = "update"

# Representative websocket-scrape slot rows verified against the TPU Waterfall
# screenshots and a live scrape from
# wss://fd-mainnet.stakingfacilities.com/websocket on 2026-03-19.
WEBSOCKET_SLOT_OUTPUT_EXAMPLES: list[dict[str, Any]] = [
    {
        "record_type": "slot",
        "slot": 407546504,
        "sankey_nodes": {
            "quic": 226432,
            "udp": 16629,
            "received": 243061,
            "gossip": 775,
            "block_engine": 2005,
            "verify": 245531,
            "dedup": 72631,
            "resolv": 23484,
            "crank": 1,
            "buffered_in": 1544,
            "pack": 24732,
            "bank": 2996,
            "packed": 1396,
        },
        "sankey_drops": {
            "malformed": 296,
            "abandoned": 14,
            "unparseable": 3777,
            "bad_signature": 1166,
            "verify_duplicate": 167957,
            "dedup_duplicate": 49157,
            "unresolved": 10,
            "bad_lut": 7,
            "resolv_expired": 290,
            "buffered": 12,
            "unpackable": 94,
            "pack_expired": 6913,
            "already_executed": 14717,
            "unexecutable": 1600,
            "block_success": 1173,
            "block_fail": 40,
        },
        "slot_metrics": {
            "votes": 792,
            "non_vote_failure": 38,
            "non_vote_success": 383,
            "compute_units": 28254168,
            "priority_fees_sol": 0.0065,
            "transaction_fees_sol": 0.0032,
            "tips_sol": 0.0003,
        },
        "publish": {
            "slot": 407546504,
            "mine": True,
            "start_timestamp_nanos": "1773961567957924856",
            "target_end_timestamp_nanos": "1773961568307924992",
            "skipped": False,
            "level": "rooted",
            "duration_nanos": 363301926,
            "completed_time_nanos": "1773961568315791427",
            "success_nonvote_transaction_cnt": 383,
            "failed_nonvote_transaction_cnt": 38,
            "success_vote_transaction_cnt": 790,
            "failed_vote_transaction_cnt": 2,
            "max_compute_units": 60000000,
            "compute_units": 28254168,
            "shreds": None,
            "transaction_fee": 3160000,
            "priority_fee": 6458807,
            "tips": 294454,
        },
    },
    {
        "record_type": "slot",
        "slot": 407546528,
        "sankey_nodes": {
            "quic": 84456,
            "udp": 8687,
            "received": 93143,
            "gossip": 649,
            "block_engine": 1707,
            "verify": 95352,
            "dedup": 24321,
            "resolv": 10010,
            "crank": 1,
            "buffered_in": 5,
            "pack": 9867,
            "bank": 2206,
            "packed": 1222,
        },
        "sankey_drops": {
            "malformed": 21,
            "abandoned": 126,
            "unparseable": 366,
            "bad_signature": 1106,
            "verify_duplicate": 69559,
            "dedup_duplicate": 14404,
            "unresolved": 93,
            "bad_lut": 0,
            "resolv_expired": 149,
            "buffered": 7,
            "unpackable": 21,
            "pack_expired": 2003,
            "already_executed": 5630,
            "unexecutable": 984,
            "block_success": 1289,
            "block_fail": 66,
        },
        "slot_metrics": {
            "votes": 737,
            "non_vote_failure": 62,
            "non_vote_success": 556,
            "compute_units": 34655814,
            "priority_fees_sol": 0.0114,
            "transaction_fees_sol": 0.0035,
            "tips_sol": 0.0008,
        },
        "publish": {
            "slot": 407546528,
            "mine": True,
            "start_timestamp_nanos": "1773961576935436679",
            "target_end_timestamp_nanos": "1773961577285436928",
            "skipped": False,
            "level": "rooted",
            "duration_nanos": 362852511,
            "completed_time_nanos": "1773961577293099707",
            "success_nonvote_transaction_cnt": 556,
            "failed_nonvote_transaction_cnt": 62,
            "success_vote_transaction_cnt": 733,
            "failed_vote_transaction_cnt": 4,
            "max_compute_units": 60000000,
            "compute_units": 34655814,
            "shreds": None,
            "transaction_fee": 3507500,
            "priority_fee": 11423704,
            "tips": 817945,
        },
    },
]


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


def add_websocket_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--websocket-url",
        type=non_empty_string,
        default=os.getenv("WEBSOCKET_URL", os.getenv("WEBSOCKET_HOST")),
        help=(
            "websocket URL (default: ws://<host>:80/websocket). Expected to be a Firedancer "
            "GUI endpoint that streams summary/epoch rows and accepts slot.query_detailed."
        ),
    )
    parser.add_argument(
        "--websocket-mode",
        choices=("recent", "since-startup"),
        default=os.getenv("WEBSOCKET_MODE", "recent"),
        help=(
            "recent = emit the last N produced slots discovered from the snapshot; "
            "since-startup = emit all produced slots completed since startup_time_nanos"
        ),
    )
    parser.add_argument(
        "--websocket-recent-count",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_RECENT_COUNT", "16")),
        help="number of slot rows to keep in recent mode after the detailed query pass",
    )
    parser.add_argument(
        "--websocket-snapshot-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_SNAPSHOT_SECS", "2")),
        help="seconds to wait for summary.identity_key/startup_time_nanos/completed_slot and epoch.new rows",
    )
    parser.add_argument(
        "--websocket-query-wait-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_QUERY_WAIT_SECS", "8")),
        help="idle grace period after the last slot query response before ending a query pass",
    )
    parser.add_argument(
        "--websocket-detail-timeout-secs",
        type=non_negative_int,
        default=non_negative_int(os.getenv("WEBSOCKET_DETAIL_TIMEOUT_SECS", "60")),
        help=f"absolute timeout for one slot.{WEBSOCKET_SLOT_QUERY_REQUEST_KEY} pass",
    )


def parse_args() -> argparse.Namespace:
    argv = sys.argv[1:] or ["-h"]
    if argv[0] not in {"capture", "websocket-scrape", "-h", "--help"}:
        argv = ["capture", *argv]

    p = argparse.ArgumentParser(
        description=(
            "Capture tcpdump + metrics around leader rotations or run the websocket slot scrape."
        )
    )
    subparsers = p.add_subparsers(dest="cmd")

    capture_p = subparsers.add_parser(
        "capture",
        help="capture tcpdump and metrics around successive leader rotations",
        description=(
            "Capture tcpdump + metrics around leader rotations. "
            "By default, continue capturing successive rotations until interrupted."
        ),
    )
    capture_p.add_argument("--host", type=non_empty_string, default=os.getenv("HOST", "127.0.0.1"))
    capture_p.add_argument("--rpc-url", type=non_empty_string, default=os.getenv("RPC_URL"))
    capture_p.add_argument("--metrics-url", type=non_empty_string, default=os.getenv("METRICS_URL"))
    capture_p.add_argument(
        "--bam-only",
        action="store_true",
        help="filter tcpdump to the BAM peer IP parsed from the Firedancer log",
    )
    capture_p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "leader_rotations"))
    capture_p.add_argument(
        "--capture-lead-time-sec",
        type=non_negative_float,
        default=non_negative_float(os.getenv("CAPTURE_LEAD_TIME_SEC", "5")),
    )
    capture_p.add_argument(
        "--capture-time-sec",
        type=non_negative_float,
        default=non_negative_float(os.getenv("CAPTURE_TIME_SEC", "10")),
    )
    capture_p.add_argument(
        "--metrics-interval",
        type=non_negative_float,
        default=non_negative_float(os.getenv("METRICS_INTERVAL", "0.2")),
    )
    capture_p.add_argument(
        "--one-shot",
        action="store_true",
        help="capture only the next leader rotation and then exit",
    )
    add_websocket_args(capture_p)

    websocket_p = subparsers.add_parser(
        "websocket-scrape",
        help="run only the websocket slot scrape",
        description="Run only the websocket scrape and emit NDJSON results.",
    )
    websocket_p.add_argument("--host", type=non_empty_string, default=os.getenv("HOST", "127.0.0.1"))
    websocket_p.add_argument(
        "--output-path",
        type=non_empty_string,
        default=os.getenv("WEBSOCKET_OUTPUT_PATH", "-"),
        help="write NDJSON to PATH instead of stdout; use '-' for stdout",
    )
    add_websocket_args(websocket_p)

    args = p.parse_args(argv)
    if args.cmd == "capture":
        if args.rpc_url is None:
            args.rpc_url = f"http://{args.host}:8899"
        if args.metrics_url is None:
            args.metrics_url = f"http://{args.host}:7999/metrics"
    if args.websocket_url is None:
        args.websocket_url = f"ws://{args.host}:80/websocket"
    args.websocket_url = args.websocket_url.strip()
    parsed_websocket_url = urllib.parse.urlparse(args.websocket_url)
    if parsed_websocket_url.scheme not in {"ws", "wss"} or not parsed_websocket_url.netloc:
        p.error(
            f"invalid websocket URL '{args.websocket_url}'; expected ws://<host>[/path] or wss://<host>[/path]"
        )
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


def run_ip_route_json(*args: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["ip", "-json", "route", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)

    if not isinstance(payload, list):
        raise RuntimeError(f"unexpected route info format from `ip route {' '.join(args)}`")
    return [route for route in payload if isinstance(route, dict)]


def get_next_leader_rotation(rpc_url: str, min_start_slot_exclusive: Optional[int] = None) -> tuple[str, int, int, int]:
    identity = rpc_call(rpc_url, "getIdentity")["identity"]

    epoch_info = rpc_call(rpc_url, "getEpochInfo")
    current_slot = int(epoch_info["absoluteSlot"])
    slot_index = int(epoch_info["slotIndex"])
    slots_in_epoch = int(epoch_info["slotsInEpoch"])
    epoch_start_slot = current_slot - slot_index
    search_after_slot = current_slot
    if min_start_slot_exclusive is not None:
        search_after_slot = max(search_after_slot, min_start_slot_exclusive)

    schedule = rpc_call(rpc_url, "getLeaderSchedule", [current_slot]) or {}
    next_epoch_start_slot = epoch_start_slot + slots_in_epoch
    next_schedule = rpc_call(rpc_url, "getLeaderSchedule", [next_epoch_start_slot]) or {}

    for schedule_epoch_start_slot, candidate_schedule in (
        (epoch_start_slot, schedule),
        (next_epoch_start_slot, next_schedule),
    ):
        my_slots = sorted({parse_int(slot_idx) for slot_idx in (candidate_schedule.get(identity) or [])})
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
                return identity, current_slot, rotation_start_slot, schedule_epoch_start_slot + rotation_end_idx

            rotation_start_idx = slot_idx
            rotation_end_idx = slot_idx

        rotation_start_slot = schedule_epoch_start_slot + rotation_start_idx
        if rotation_start_slot > search_after_slot:
            return identity, current_slot, rotation_start_slot, schedule_epoch_start_slot + rotation_end_idx

    raise RuntimeError(f"Identity {identity} has no upcoming leader rotations in current/next epoch")


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
        self._exit_stderr: Optional[str] = None

    def __enter__(self) -> "WebsocketJsonSession":
        self.proc = subprocess.Popen(
            ["websocat", "-B", WEBSOCAT_BUFFER_BYTES, self.websocket_url],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        return self

    def _connection_error(self) -> RuntimeError:
        exit_code = None if self.proc is None else self.proc.poll()
        if self._exit_stderr is None:
            if self.proc is None or self.proc.stderr is None:
                self._exit_stderr = ""
            else:
                try:
                    self._exit_stderr = self.proc.stderr.read() or ""
                except (OSError, ValueError):
                    self._exit_stderr = ""
        stderr_text = self._exit_stderr.strip()
        message = f"websocket connection failed (exit code {exit_code})"
        if stderr_text:
            message = f"{message}: {stderr_text}"
        return RuntimeError(message)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.proc is None:
            return

        if self.proc.stdin is not None and not self.proc.stdin.closed:
            self.proc.stdin.close()

        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

        if self.proc.stdout is not None and not self.proc.stdout.closed:
            self.proc.stdout.close()
        if self.proc.stderr is not None and not self.proc.stderr.closed:
            self.proc.stderr.close()

    def send_json(self, payload: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("websocket process is not available")
        if self.proc.poll() is not None:
            raise self._connection_error()
        try:
            self.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise self._connection_error() from exc

    def read_json(self, timeout_sec: float) -> Optional[dict[str, Any]]:
        if self.proc is None or self.proc.stdout is None:
            return None
        if self.proc.poll() is not None:
            raise self._connection_error()

        timeout = max(0.0, timeout_sec)
        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            if self.proc.poll() is not None:
                raise self._connection_error()
            return None

        line = self.proc.stdout.readline()
        if not line:
            if self.proc.poll() is not None:
                raise self._connection_error()
            return None

        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            return None

        return payload if isinstance(payload, dict) else None


def compute_produced_slots(epoch_values: list[dict[str, Any]], identity: str, completed_slot: int) -> list[int]:
    produced: set[int] = set()
    for epoch_value in epoch_values:
        staked_pubkeys = epoch_value.get("staked_pubkeys")
        if not isinstance(staked_pubkeys, list):
            continue

        try:
            validator_idx = staked_pubkeys.index(identity)
        except ValueError:
            continue

        start_slot = parse_int(epoch_value.get("start_slot"))
        leader_slots = epoch_value.get("leader_slots")
        if not isinstance(leader_slots, list):
            continue

        for slot_group_idx, leader_idx in enumerate(leader_slots):
            if parse_int(leader_idx) != validator_idx:
                continue

            base_slot = start_slot + (slot_group_idx * 4)
            for slot in range(base_slot, base_slot + 4):
                if slot <= completed_slot:
                    produced.add(slot)

    return sorted(produced)


def parse_slot_result_row(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Parse one slot.query_detailed response row from the Firedancer GUI websocket.

    Expected wire shape:
      - request: {"topic":"slot","key":"query_detailed","params":{"slot":<u64>}}
      - response: {"topic":"slot","key":"query","value":{"publish":...,"waterfall":...}}
    """

    # Live GUI servers currently reply to slot.query_detailed with key="query".
    if row.get("topic") != "slot" or row.get("key") != WEBSOCKET_SLOT_QUERY_RESPONSE_KEY:
        return None

    value = row.get("value")
    if not isinstance(value, dict):
        return None

    publish = value.get("publish")
    if not isinstance(publish, dict):
        return None

    waterfall = value.get("waterfall")
    if not isinstance(waterfall, dict):
        return None
    waterfall_in = waterfall.get("in")
    waterfall_drops = waterfall.get("out")
    if not isinstance(waterfall_in, dict) or not isinstance(waterfall_drops, dict):
        return None

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
    drop_pack_invalid_bundle = parse_int(waterfall_drops.get("pack_invalid_bundle"))
    drop_pack_expired = parse_int(waterfall_drops.get("pack_expired"))
    drop_pack_already_executed = parse_int(waterfall_drops.get("pack_already_executed"))
    drop_pack_leader_slow = parse_int(waterfall_drops.get("pack_leader_slow"))
    drop_bank_invalid = parse_int(waterfall_drops.get("bank_invalid"))
    drop_block_success = parse_int(waterfall_drops.get("block_success"))
    drop_block_fail = parse_int(waterfall_drops.get("block_fail"))
    # Historical slot queries expose resolv_retained with a wrapped 16-bit delta
    # representation. Normalize that first, then feed the result into the same
    # stage accounting the GUI uses for the TPU waterfall.
    unresolved = parse_int(waterfall_drops.get("resolv_retained")) - in_resolv_retained
    if unresolved < 0:
        unresolved += 1 << 16
    if unresolved >= (1 << 15):
        unresolved = (1 << 16) - unresolved
    drop_resolv_expired_total = drop_resolv_expired + drop_resolv_ancient
    drop_resolv_failed = drop_resolv_lut_failed + drop_resolv_expired_total + drop_resolv_no_ledger
    # The UI groups leader-slow with buffered, and invalid-bundle with
    # unpackable, so collapse those counters into the same buckets here.
    drop_pack_buffered = drop_pack_retained + drop_pack_leader_slow
    drop_pack_unpacked = drop_pack_invalid + drop_pack_invalid_bundle

    # These derived node counts intentionally follow the current GUI formulas
    # from src/disco/gui/fd_gui_printf.c and src/disco/gui/sankey_debug.py so
    # websocket-scrape matches the on-screen TPU Waterfall exactly.
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
    resolv = dedup - drop_dedup_duplicate + unresolved
    pack = (
        resolv
        + in_pack_cranked
        + in_pack_retained
        - drop_resolv_failed
    )
    bank = (
        pack
        - drop_pack_buffered
        - drop_pack_unpacked
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
            "resolv_expired": drop_resolv_expired_total,
            "buffered": drop_pack_buffered,
            "unpackable": drop_pack_unpacked,
            "pack_expired": drop_pack_expired,
            "already_executed": drop_pack_already_executed,
            "unexecutable": drop_bank_invalid,
            "block_success": drop_block_success,
            "block_fail": drop_block_fail,
        },
        "slot_metrics": {
            "votes": parse_int(publish.get("success_vote_transaction_cnt"))
            + parse_int(publish.get("failed_vote_transaction_cnt")),
            "non_vote_failure": parse_int(publish.get("failed_nonvote_transaction_cnt")),
            "non_vote_success": parse_int(publish.get("success_nonvote_transaction_cnt")),
            "compute_units": parse_int(publish.get("compute_units")),
            "priority_fees_sol": round((parse_int(publish.get("priority_fee")) / 1_000_000_000) * 10000) / 10000,
            "transaction_fees_sol": round((parse_int(publish.get("transaction_fee")) / 1_000_000_000) * 10000) / 10000,
            "tips_sol": round((parse_int(publish.get("tips")) / 1_000_000_000) * 10000) / 10000,
        },
        "publish": publish,
    }


def scrape_websocket_slots(
    websocket_url: str,
    mode: str,
    recent_count: int,
    snapshot_secs: int,
    query_wait_secs: int,
    detail_timeout_secs: int,
) -> list[dict[str, Any]]:
    parsed_websocket_url = urllib.parse.urlparse(websocket_url)
    if parsed_websocket_url.scheme not in {"ws", "wss"} or not parsed_websocket_url.netloc:
        raise RuntimeError(
            f"invalid websocket URL '{websocket_url}'; expected ws://<host>[/path] or wss://<host>[/path]"
        )
    snapshot_deadline = time.monotonic() + snapshot_secs
    summary_values: dict[str, Any] = {}
    summary_rows: list[dict[str, Any]] = []
    epoch_values: list[dict[str, Any]] = []
    bam_value: Optional[dict[str, Any]] = None

    with WebsocketJsonSession(websocket_url) as ws:
        while time.monotonic() <= snapshot_deadline:
            row = ws.read_json(1.0)
            if row is None:
                continue

            topic = row.get("topic")
            key = row.get("key")
            value = row.get("value")
            if topic == "summary" and key in WEBSOCKET_SUMMARY_KEYS and value is not None:
                summary_key = str(key)
                if summary_key not in summary_values:
                    summary_values[summary_key] = value
                    summary_rows.append(
                        {
                            "record_type": "summary",
                            "topic": topic,
                            "key": summary_key,
                            "value": value,
                        }
                    )
            elif topic == "epoch" and key == "new" and isinstance(value, dict):
                epoch_values.append(value)
            elif topic == WEBSOCKET_BAM_UPDATE_TOPIC and key == WEBSOCKET_BAM_UPDATE_KEY and isinstance(value, dict):
                bam_value = value

            if len(summary_values) != len(WEBSOCKET_SUMMARY_KEYS):
                continue

            completed_slot = parse_int(summary_values["completed_slot"])
            if any(
                parse_int(epoch_value.get("start_slot")) <= completed_slot
                <= parse_int(epoch_value.get("end_slot"))
                for epoch_value in epoch_values
            ):
                break

    if len(summary_values) != len(WEBSOCKET_SUMMARY_KEYS):
        raise RuntimeError(
            "timed out waiting for required websocket summary data; "
            "try increasing --websocket-snapshot-secs"
        )
    identity = str(summary_values["identity_key"])
    startup_time_nanos = str(summary_values["startup_time_nanos"])
    completed_slot = parse_int(summary_values["completed_slot"])
    epoch_ranges = [
        (
            parse_int(epoch_value.get("start_slot")),
            parse_int(epoch_value.get("end_slot")),
        )
        for epoch_value in epoch_values
    ]
    if not any(start_slot <= completed_slot <= end_slot for start_slot, end_slot in epoch_ranges):
        ranges_display = ", ".join(f"{start_slot}-{end_slot}" for start_slot, end_slot in epoch_ranges) or "none"
        raise RuntimeError(
            "timed out waiting for websocket epoch schedule covering "
            f"completed_slot={completed_slot}; captured epoch ranges: {ranges_display}; "
            "try increasing --websocket-snapshot-secs"
        )

    produced_slots = compute_produced_slots(epoch_values, identity, completed_slot)
    if not produced_slots:
        raise RuntimeError("no produced slots discovered from epoch schedules")
    query_slots = (
        produced_slots[-max(recent_count * 4, recent_count + 8):]
        if mode == "recent" and recent_count > 0
        else produced_slots
    )

    query_payloads = [
        {
            "topic": "slot",
            "key": WEBSOCKET_SLOT_QUERY_REQUEST_KEY,
            "id": 1000 + i,
            "params": {"slot": slot},
        }
        for i, slot in enumerate(query_slots)
    ]

    seen_query_results = False
    pending_ids = {req["id"] for req in query_payloads}
    for wait_secs, timeout_secs in (
        (query_wait_secs, detail_timeout_secs),
        (query_wait_secs + 4, detail_timeout_secs + 40),
    ):
        details: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        deadline = time.monotonic() + timeout_secs
        idle_deadline = time.monotonic() + wait_secs

        with WebsocketJsonSession(websocket_url) as ws:
            for req in query_payloads:
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
                    and row.get("key") == WEBSOCKET_SLOT_QUERY_RESPONSE_KEY
                    and isinstance(row.get("id"), int)
                ):
                    response_id = row["id"]
                    if response_id in pending_ids and response_id not in seen_ids:
                        seen_ids.add(response_id)
                        idle_deadline = time.monotonic() + wait_secs

                if time.monotonic() > idle_deadline:
                    break

        seen_query_results = seen_query_results or any(
            row.get("topic") == "slot"
            and row.get("key") == WEBSOCKET_SLOT_QUERY_RESPONSE_KEY
            and row.get("value") is not None
            for row in details
        )

        slots_by_id = {
            int(parsed["slot"]): parsed
            for row in details
            if (parsed := parse_slot_result_row(row)) is not None
        }
        parsed_rows = [slots_by_id[slot] for slot in sorted(slots_by_id)]
        if mode == "since-startup":
            startup_nanos = parse_int(startup_time_nanos)
            parsed_rows = [
                row
                for row in parsed_rows
                if parse_int((row.get("publish") or {}).get("completed_time_nanos")) >= startup_nanos
            ]
        elif mode == "recent" and recent_count > 0:
            parsed_rows = parsed_rows[-recent_count:]

        if parsed_rows:
            query_bam_value = next(
                (
                    row.get("value")
                    for row in reversed(details)
                    if row.get("topic") == WEBSOCKET_BAM_UPDATE_TOPIC
                    and row.get("key") == WEBSOCKET_BAM_UPDATE_KEY
                    and isinstance(row.get("value"), dict)
                ),
                None,
            )
            epoch_rows = [
                {
                    "record_type": "epoch",
                    "topic": "epoch",
                    "key": "new",
                    "value": epoch_value,
                }
                for epoch_value in epoch_values
            ]
            output_rows = [*summary_rows, *epoch_rows]
            selected_bam_value = query_bam_value if query_bam_value is not None else bam_value
            if selected_bam_value is not None:
                output_rows.append(
                    {
                        "record_type": "bam",
                        "topic": WEBSOCKET_BAM_UPDATE_TOPIC,
                        "key": WEBSOCKET_BAM_UPDATE_KEY,
                        "value": selected_bam_value,
                    }
                )
            output_rows.extend({"record_type": "slot", **row} for row in parsed_rows)
            return output_rows

    if seen_query_results:
        return []
    raise RuntimeError(
        "no slot query results returned; "
        "try increasing --websocket-query-wait-secs/--websocket-detail-timeout-secs",
    )


def write_ndjson_rows(rows: list[dict[str, Any]], out) -> None:
    for row in rows:
        out.write(json.dumps(row, separators=(",", ":")))
        out.write("\n")


def run_websocket_scrape(args: argparse.Namespace, output_path: Optional[Path]) -> int:
    websocket_rows = scrape_websocket_slots(
        websocket_url=args.websocket_url.strip(),
        mode=args.websocket_mode,
        recent_count=args.websocket_recent_count,
        snapshot_secs=args.websocket_snapshot_secs,
        query_wait_secs=args.websocket_query_wait_secs,
        detail_timeout_secs=args.websocket_detail_timeout_secs,
    )
    if not websocket_rows:
        return 0

    if output_path is None:
        write_ndjson_rows(websocket_rows, sys.stdout)
        return len(websocket_rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out:
        write_ndjson_rows(websocket_rows, out)
    return len(websocket_rows)


def capture_next_leader_rotation(
    args: argparse.Namespace, capture_idx: int, min_start_slot_exclusive: Optional[int] = None
) -> tuple[int, int]:
    identity, current_slot, next_leader_slot, next_rotation_end_slot = get_next_leader_rotation(
        args.rpc_url,
        min_start_slot_exclusive=min_start_slot_exclusive,
    )
    slot_seconds = get_slot_seconds(args.rpc_url)

    slots_before_start = max(1, math.ceil(args.capture_lead_time_sec / slot_seconds))
    start_slot = max(0, next_leader_slot - slots_before_start)

    if capture_idx > 1:
        print()
    print(f"capture #{capture_idx}")
    print(f"validator identity: {identity}")
    print(f"current slot: {current_slot}")
    print(
        "next leader rotation: "
        f"{next_leader_slot}-{next_rotation_end_slot} "
        f"({next_rotation_end_slot - next_leader_slot + 1} slots)"
    )
    print(f"estimated slot duration: {slot_seconds:.2f}s")
    print(f"starting capture at slot >= {start_slot} (~{args.capture_lead_time_sec}s before leader rotation)")

    while True:
        now_slot = int(rpc_call(args.rpc_url, "getSlot"))
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

    run_dir = Path(args.output_root) / f"slot_{next_leader_slot}_{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        proc = subprocess.run(
            ["ps", "-eo", "cmd"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"failed to inspect running Firedancer process for --log-path: {exc}") from exc

    firedancer_log_path: Optional[Path] = None
    for line in proc.stdout.splitlines():
        if "--log-path" not in line:
            continue
        if (
            "fddev dev" not in line
            and "fdctl run" not in line
            and not re.search(r"build/.*/bin/(?:firedancer|fdctl)\b", line)
        ):
            continue
        match = re.search(r"(?:^|\s)--log-path(?:=|\s+)(?P<path>\S+)", line)
        if match is None:
            continue
        candidate = Path(match.group("path")).expanduser()
        if candidate.exists() and candidate.is_file():
            firedancer_log_path = candidate
            break
    if firedancer_log_path is None:
        raise RuntimeError("could not find an active Firedancer --log-path")

    if args.bam_only:
        bam_log_endpoint_patterns = (
            re.compile(r"Connected to BAM node at .*?\((?P<ip>(?:\d{1,3}\.){3}\d{1,3}):(?P<port>\d+)\)"),
            re.compile(r"Connecting to \w+://(?P<ip>(?:\d{1,3}\.){3}\d{1,3}):(?P<port>\d+)"),
            re.compile(r"\bBAM\b.*?/(?P<ip>(?:\d{1,3}\.){3}\d{1,3}):(?P<port>\d+)"),
        )
        bam_ip = ""
        bam_port = 0
        try:
            with firedancer_log_path.open("r", encoding="utf-8", errors="replace") as log_file:
                for line in log_file:
                    for pattern in bam_log_endpoint_patterns:
                        match = pattern.search(line)
                        if match is None:
                            continue
                        ip_text = match.group("ip")
                        port = int(match.group("port"))
                        try:
                            ipaddress.IPv4Address(ip_text)
                        except ipaddress.AddressValueError:
                            continue
                        bam_ip = ip_text
                        bam_port = port
                        break
                    if bam_ip:
                        break
        except OSError as exc:
            raise RuntimeError(f"failed to read Firedancer log '{firedancer_log_path}': {exc}") from exc
        if not bam_ip:
            raise RuntimeError(
                "could not find a BAM endpoint in Firedancer log "
                f"'{firedancer_log_path}'; expected a BAM connect/connected line"
            )
    firedancer_log_start = firedancer_log_path.stat()

    tcpdump_filter = "not (net 127.0.0.0/8 or host ::1)"
    route_args = ("show", "default")
    route_error = "could not determine capture interface from default route"
    if args.bam_only:
        route_args = ("get", bam_ip)
        route_error = f"could not determine capture interface for route to {bam_ip}"
        tcpdump_filter = f"host {bam_ip} and {tcpdump_filter}"
    capture_routes = [
        route
        for route in run_ip_route_json(*route_args)
        if isinstance(route.get("dev"), str) and route["dev"]
    ]
    selected_route = (
        next(iter(capture_routes), None)
        if args.bam_only
        else min(
            capture_routes,
            key=lambda route: parse_int(route.get("metric")),
            default=None,
        )
    )
    if selected_route is None:
        raise RuntimeError(route_error)
    capture_interface = str(selected_route["dev"])
    if args.bam_only:
        print(
            "bam-only capture enabled: "
            f"parsed BAM endpoint {bam_ip}:{bam_port} from {firedancer_log_path}; "
            f"capture interface='{capture_interface}'; "
            f"tcpdump filter='{tcpdump_filter}'"
        )
    public_internet_interfaces = list(
        dict.fromkeys(
            route["dev"]
            for route in run_ip_route_json("show", "default")
            if isinstance(route.get("dev"), str) and route["dev"]
            and (Path("/sys/class/net") / route["dev"] / "device").exists()
        )
    )

    if public_internet_interfaces:
        print(f"default-route diagnostics interfaces: {', '.join(public_internet_interfaces)}")
    else:
        print("warning: no default-route interfaces discovered for diagnostics", file=sys.stderr)
    pcap_cmd = [
        "timeout",
        str(args.capture_time_sec),
        "sudo",
        "tcpdump",
        "-i",
        capture_interface,
        "-B",
        "8192", # default is 2MiB, change to 8MiB to avoid drops
        "-n",
        "-w",
        f"dump_{next_leader_slot}.pcap",
        tcpdump_filter,
    ]

    print(
        "capture directory: "
        f"{run_dir}; starting tcpdump and metrics capture now for ~{args.capture_time_sec}s "
        f"on interface '{capture_interface}' with filter '{tcpdump_filter}'"
    )
    diag_dir: Optional[Path] = None
    ethtool_path = "/sbin/ethtool" if Path("/sbin/ethtool").exists() else (shutil.which("ethtool") or "/sbin/ethtool")
    tc_path = "/sbin/tc" if Path("/sbin/tc").exists() else (shutil.which("tc") or "/sbin/tc")

    def capture_diag_phase(phase: str) -> None:
        if diag_dir is None:
            return

        print(f"{phase.replace('_', ' ')}: collecting interface diagnostics for {', '.join(public_internet_interfaces)}")
        for iface in public_internet_interfaces:
            try:
                completed = subprocess.run(
                    [ethtool_path, "-S", iface],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                output = "\n".join(
                    line for line in completed.stdout.splitlines() if ETHTOOL_STATS_FILTER_RE.search(line)
                ) or "[no matching lines]"
                if completed.stderr:
                    output = f"{output}\n\n[stderr]\n{completed.stderr}" if output else f"[stderr]\n{completed.stderr}"
            except OSError as exc:
                output = f"{exc}\n"
            (diag_dir / f"{phase}_{iface}_ethtool_stats.txt").write_text(
                output if output.endswith("\n") else f"{output}\n",
                encoding="utf-8",
            )

            try:
                completed = subprocess.run(
                    ["sudo", tc_path, "-s", "qdisc", "show", "dev", iface],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                output = completed.stdout
                if completed.stderr:
                    output = f"{output}\n\n[stderr]\n{completed.stderr}" if output else f"[stderr]\n{completed.stderr}"
            except OSError as exc:
                output = f"{exc}\n"
            (diag_dir / f"{phase}_{iface}_tc_qdisc.txt").write_text(
                output if output.endswith("\n") else f"{output}\n",
                encoding="utf-8",
            )

            try:
                completed = subprocess.run(
                    ["ip", "-json", "-s", "link", "show", iface],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                output = completed.stdout
                if completed.stderr:
                    output = f"{output}\n\n[stderr]\n{completed.stderr}" if output else f"[stderr]\n{completed.stderr}"
            except OSError as exc:
                output = f"{exc}\n"
            (diag_dir / f"{phase}_{iface}_ip_link.txt").write_text(
                output if output.endswith("\n") else f"{output}\n",
                encoding="utf-8",
            )

    if public_internet_interfaces:
        diag_dir = run_dir / "interface_diagnostics"
        try:
            diag_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            print(f"warning: failed to create {diag_dir}: {exc}", file=sys.stderr)
            diag_dir = None
        else:
            capture_diag_phase("before_capture")
    tcpdump_proc = subprocess.Popen(pcap_cmd, cwd=run_dir)
    capture_deadline = time.monotonic() + args.capture_time_sec

    i = 0
    try:
        while (remaining := capture_deadline - time.monotonic()) > 0:
            with urllib.request.urlopen(args.metrics_url, timeout=max(0.1, min(10.0, remaining))) as resp:
                (run_dir / f"metrics_{i}.txt").write_bytes(resp.read())
            i += 1

            sleep_for = min(args.metrics_interval, capture_deadline - time.monotonic())
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        rc = tcpdump_proc.wait()
        capture_diag_phase("after_capture")

    clipped_log_path = run_dir / "firedancer.log"
    firedancer_log_end = firedancer_log_path.stat()
    if (
        firedancer_log_end.st_dev == firedancer_log_start.st_dev
        and firedancer_log_end.st_ino == firedancer_log_start.st_ino
        and firedancer_log_end.st_size >= firedancer_log_start.st_size
    ):
        with firedancer_log_path.open("rb") as src, clipped_log_path.open("wb") as dst:
            src.seek(firedancer_log_start.st_size)
            dst.write(src.read(firedancer_log_end.st_size - firedancer_log_start.st_size))
        print(f"captured Firedancer log slice to {clipped_log_path}")
    else:
        shutil.copy2(firedancer_log_path, clipped_log_path)
        print(
            "warning: Firedancer log rotated or truncated during capture; "
            f"copied full log to {clipped_log_path}",
            file=sys.stderr,
        )

    if rc not in (0, 124):
        print(f"error: tcpdump failed with exit code {rc}", file=sys.stderr)
        return rc, next_rotation_end_slot

    websocket_output_path = run_dir / "websocket.ndjson"
    print(
        "running websocket scrape at end of capture: "
        f"url={args.websocket_url} mode={args.websocket_mode}"
    )
    websocket_row_count = run_websocket_scrape(args, websocket_output_path)
    if websocket_row_count:
        print(f"websocket scrape written to {websocket_output_path}")
    else:
        print(f"warning: no produced slots matched mode='{args.websocket_mode}'", file=sys.stderr)

    print(f"done: files written under {run_dir}")
    return 0, next_rotation_end_slot


def main() -> int:
    args = parse_args()

    if args.cmd == "websocket-scrape":
        output_path = None if args.output_path == "-" else Path(args.output_path).expanduser()
        print(
            f"running websocket scrape: url={args.websocket_url} mode={args.websocket_mode}",
            file=sys.stderr,
        )
        websocket_row_count = run_websocket_scrape(args, output_path)
        if websocket_row_count == 0:
            print(f"warning: no produced slots matched mode='{args.websocket_mode}'", file=sys.stderr)
        elif output_path is not None:
            print(f"websocket scrape written to {output_path}", file=sys.stderr)
        return 0

    capture_idx = 1
    min_start_slot_exclusive: Optional[int] = None
    while True:
        rc, captured_rotation_end_slot = capture_next_leader_rotation(
            args,
            capture_idx,
            min_start_slot_exclusive=min_start_slot_exclusive,
        )
        if rc != 0:
            return rc
        if args.one_shot:
            return 0

        min_start_slot_exclusive = captured_rotation_end_slot
        print("capture succeeded; waiting for the next leader rotation")
        capture_idx += 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        raise SystemExit(130)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
