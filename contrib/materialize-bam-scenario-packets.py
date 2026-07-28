#!/usr/bin/env python3
"""Rewrite packet_count BAM scenario events into target-local packet files."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any


GEN_RE = re.compile(r"\bfrom=([^ ]+) target=([^ ]+) total_lamports=([0-9]+)\b")

TOP_LEVEL_ORDER = (
    "description",
    "heartbeat_interval_ms",
    "ping_interval_ms",
    "replay_on_reconnect",
    "resume_on_reconnect",
    "auth_status_sequence",
    "auth_delay_ms_sequence",
    "auth_challenge_sequence",
    "auth_raw_hex_sequence",
    "config_status_sequence",
    "config_delay_ms_sequence",
    "config_variant_sequence",
    "config_raw_hex_sequence",
    "stream_status_sequence",
    "stream_delay_ms_sequence",
    "expected_terminal_results",
)

EVENT_FIELD_ORDER = {
    "sleep": ("type", "ms"),
    "sleep_resume_safe": ("type", "ms"),
    "send_batch": (
        "type",
        "seq_id",
        "expect_result",
        "packet_count",
        "program_invocations",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "revert_on_error",
        "revert_on_error_sequence",
        "packet_meta_sequence",
        "packet_data_size",
        "cu_per_tx",
    ),
    "send_batch_flood": (
        "type",
        "batch_count",
        "start_seq_id",
        "packet_count",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "revert_on_error",
        "revert_on_error_sequence",
        "packet_meta_sequence",
        "packet_data_size",
        "cu_per_tx",
    ),
    "send_multi_batch": ("type",),
    "send_split_batch": (
        "type",
        "seq_id",
        "splits",
        "packet_count",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "revert_on_error",
        "revert_on_error_sequence",
        "packet_meta_sequence",
        "packet_data_size",
        "cu_per_tx",
    ),
    "send_ping": ("type", "id"),
    "send_raw_response": ("type", "name", "hex"),
    "send_raw_overflow_faults": ("type", "start_seq_id"),
    "wait_inbound": ("type", "kind", "min_count", "timeout_ms"),
    "close_stream": ("type",),
}

BATCH_FIELD_ORDER = (
    "seq_id",
    "expect_result",
    "packet_count",
    "program_invocations",
    "packets_base64",
    "packets_base64_file",
    "max_schedule_slot",
    "simple_vote_tx",
    "revert_on_error",
    "revert_on_error_sequence",
    "packet_meta_sequence",
    "packet_data_size",
    "cu_per_tx",
)


def die(message: str) -> None:
    raise SystemExit(f"error: {message}")


def render_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(render_value(item) for item in value) + "]"
    if isinstance(value, dict):
        fields = ", ".join(
            f"{key} = {render_value(item)}" for key, item in sorted(value.items())
        )
        return "{ " + fields + " }"
    die(f"cannot render TOML value {value!r}")


def ordered_keys(record: dict[str, Any], preferred: tuple[str, ...]) -> list[str]:
    preferred_set = set(preferred)
    keys = [key for key in preferred if key in record]
    keys.extend(sorted(key for key in record if key not in preferred_set and key != "batches"))
    return keys


def render_kv(lines: list[str], record: dict[str, Any], preferred: tuple[str, ...]) -> None:
    for key in ordered_keys(record, preferred):
        lines.append(f"{key} = {render_value(record[key])}")


def render_scenario(data: dict[str, Any]) -> str:
    lines: list[str] = []
    top = {key: value for key, value in data.items() if key != "events"}
    render_kv(lines, top, TOP_LEVEL_ORDER)
    if lines:
        lines.append("")

    for event in data.get("events", []):
        event_type = event.get("type")
        if not isinstance(event_type, str):
            die("scenario event missing string type")
        lines.append("[[events]]")
        render_kv(lines, event, EVENT_FIELD_ORDER.get(event_type, ("type",)))
        lines.append("")
        if event_type == "send_multi_batch":
            batches = event.get("batches")
            if not isinstance(batches, list):
                die("send_multi_batch event missing batches")
            for batch in batches:
                if not isinstance(batch, dict):
                    die("send_multi_batch batch entry is not a table")
                lines.append("[[events.batches]]")
                render_kv(lines, batch, BATCH_FIELD_ORDER)
                lines.append("")

    return "\n".join(lines).rstrip() + "\n"


class PacketMaterializer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.packet_dir = args.packet_dir.resolve()
        self.packet_dir.mkdir(parents=True, exist_ok=True)
        self.packet_set_idx = 0
        self.packet_idx = 0
        self.materialized_sets = 0
        self.materialized_packets = 0
        self.skipped_simple_vote_sets = 0
        self.zero_packet_sets = 0
        self.payer = ""
        self.first_recipient = ""
        self.last_recipient = ""
        self.total_lamports = 0

    def materialize_record(self, record: dict[str, Any]) -> None:
        has_count = "packet_count" in record
        has_programs = "program_invocations" in record
        has_inline = "packets_base64" in record
        has_file = "packets_base64_file" in record
        if sum(1 for present in (has_count, has_programs, has_inline, has_file) if present) > 1:
            die("scenario event specifies more than one packet source")
        if has_programs:
            specs = record["program_invocations"]
            if not isinstance(specs, list) or not specs:
                die("program_invocations must be a non-empty array")
            packet_file = self._build_program_packet_file(specs)
            del record["program_invocations"]
            record["packets_base64_file"] = str(packet_file)
            return
        if not has_count:
            return

        count = record["packet_count"]
        if not isinstance(count, int) or count < 0:
            die(f"packet_count must be a non-negative integer, got {count!r}")
        if count == 0:
            self.zero_packet_sets += 1
            return
        if bool(record.get("simple_vote_tx", False)):
            self.skipped_simple_vote_sets += 1
            return

        packet_file = self._build_packet_file(count, int(record.get("cu_per_tx") or self.args.cu_limit))
        del record["packet_count"]
        record["packets_base64_file"] = str(packet_file)

    def _build_packet_file(self, count: int, cu_limit: int) -> Path:
        packet_file = self.packet_dir / f"packets-{self.packet_set_idx:04d}.txt"
        self.packet_set_idx += 1
        lines: list[str] = []
        for _ in range(count):
            packet_idx = self.packet_idx
            self.packet_idx += 1
            to_seed = self.args.to_seed_start + (packet_idx if self.args.vary_recipient else 0)
            if to_seed == self.args.from_seed_index:
                to_seed += 1
            lamports = self.args.lamports + self.args.lamports_step * packet_idx
            txnctx = self.packet_dir / f"txn-{packet_idx:05d}.txnctx"
            packet_one = self.packet_dir / f"packet-{packet_idx:05d}.txt"
            gen_out = self.packet_dir / f"gen-{packet_idx:05d}.out"
            bridge_out = self.packet_dir / f"bridge-{packet_idx:05d}.out"

            gen_cmd = [
                str(self.args.gen_simple_system_txnctx),
                "--output",
                str(txnctx),
                "--system-kind",
                "transfer",
                "--transfer-count",
                "1",
                "--lamports",
                str(lamports),
                "--cu-limit",
                str(cu_limit),
                "--recent-blockhash",
                self.args.recent_blockhash,
                "--from-seed-index",
                str(self.args.from_seed_index),
                "--to-seed-index",
                str(to_seed),
            ]
            gen = subprocess.run(gen_cmd, check=True, text=True, capture_output=True)
            gen_out.write_text(gen.stdout + gen.stderr)
            match = GEN_RE.search(gen.stdout)
            if not match:
                die(f"failed to parse generator summary for packet {packet_idx}")
            payer, recipient, lamports_text = match.groups()
            self.payer = self.payer or payer
            self.first_recipient = self.first_recipient or recipient
            self.last_recipient = recipient
            self.total_lamports += int(lamports_text)

            bridge = subprocess.run(
                [
                    str(self.args.bridge),
                    "--input",
                    str(txnctx),
                    "--output",
                    str(packet_one),
                    "--adapt-mode",
                    "raw",
                ],
                check=True,
                text=True,
                capture_output=True,
            )
            bridge_out.write_text(bridge.stdout + bridge.stderr)
            encoded = packet_one.read_text().strip()
            if not encoded:
                die(f"bridge produced an empty packet file for packet {packet_idx}")
            lines.append(encoded)

        packet_file.write_text("\n".join(lines) + "\n")
        self.materialized_sets += 1
        self.materialized_packets += count
        return packet_file

    def _build_program_packet_file(self, specs: list[Any]) -> Path:
        packet_file = self.packet_dir / f"packets-{self.packet_set_idx:04d}.txt"
        self.packet_set_idx += 1
        lines: list[str] = []
        for spec_idx, spec in enumerate(specs):
            if not isinstance(spec, dict) or not isinstance(spec.get("program_id"), str):
                die(f"program_invocations[{spec_idx}] requires a string program_id")
            cmd = [
                str(self.args.gen_program_invocation),
                "--payer-seed-index",
                str(self.args.from_seed_index),
                "--program-id",
                spec["program_id"],
                "--recent-blockhash",
                self.args.recent_blockhash,
                "--data-hex",
                str(spec.get("data_hex", "")),
                "--account-count",
                str(int(spec.get("account_count", 0))),
            ]
            if bool(spec.get("unsigned", False)):
                cmd.append("--unsigned")
            generated = subprocess.run(cmd, check=True, text=True, capture_output=True)
            fields = dict(
                line.split("=", 1)
                for line in generated.stdout.splitlines()
                if "=" in line
            )
            encoded = fields.get("transaction_base64", "")
            if not encoded:
                die(f"program generator produced no transaction for spec {spec_idx}")
            self.payer = self.payer or fields.get("payer", "")
            lines.append(encoded)

        packet_file.write_text("\n".join(lines) + "\n")
        self.materialized_sets += 1
        self.materialized_packets += len(lines)
        return packet_file

    def write_metadata(self, path: Path, expected_terminal_results: int | None) -> None:
        lines = [
                    f"materialized_packet_sets={self.materialized_sets}",
                    f"materialized_packet_count={self.materialized_packets}",
                    f"zero_packet_sets={self.zero_packet_sets}",
                    f"skipped_simple_vote_sets={self.skipped_simple_vote_sets}",
                    f"payer={self.payer}",
                    f"first_recipient={self.first_recipient}",
                    f"last_recipient={self.last_recipient}",
                    f"total_lamports={self.total_lamports}",
                    f"packet_dir={self.packet_dir}",
                ]
        if expected_terminal_results is not None:
            lines.append(f"expected_terminal_results={expected_terminal_results}")
        path.write_text("\n".join(lines) + "\n")


def materialize(data: dict[str, Any], mat: PacketMaterializer) -> None:
    events = data.get("events")
    if not isinstance(events, list):
        die("scenario missing events array")
    for event in events:
        if not isinstance(event, dict):
            die("scenario event is not a table")
        event_type = event.get("type")
        if event_type in ("send_batch", "send_batch_flood", "send_split_batch"):
            if event_type == "send_split_batch" and "packet_count" in event:
                splits = event.get("splits")
                if not isinstance(splits, list) or sum(int(item) for item in splits) != int(event["packet_count"]):
                    die("send_split_batch splits do not match packet_count")
            mat.materialize_record(event)
        elif event_type == "send_multi_batch":
            batches = event.get("batches")
            if not isinstance(batches, list):
                die("send_multi_batch event missing batches")
            for batch in batches:
                if not isinstance(batch, dict):
                    die("send_multi_batch batch entry is not a table")
                mat.materialize_record(batch)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--packet-dir", required=True, type=Path)
    parser.add_argument("--metadata-output", required=True, type=Path)
    parser.add_argument("--gen-simple-system-txnctx", required=True, type=Path)
    parser.add_argument("--gen-program-invocation", required=True, type=Path)
    parser.add_argument("--bridge", required=True, type=Path)
    parser.add_argument("--recent-blockhash", required=True)
    parser.add_argument("--from-seed-index", type=int, default=0)
    parser.add_argument("--to-seed-start", type=int, default=1)
    parser.add_argument("--vary-recipient", action="store_true")
    parser.add_argument("--lamports", type=int, default=1_000_000)
    parser.add_argument("--lamports-step", type=int, default=1)
    parser.add_argument("--cu-limit", type=int, default=300_000)
    args = parser.parse_args()

    data = tomllib.loads(args.scenario.read_text())
    expected_terminal_results = data.get("expected_terminal_results")
    if expected_terminal_results is not None and (
        not isinstance(expected_terminal_results, int) or expected_terminal_results < 0
    ):
        die("expected_terminal_results must be a non-negative integer")
    mat = PacketMaterializer(args)
    materialize(data, mat)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_scenario(data))
    mat.write_metadata(args.metadata_output, expected_terminal_results)
    print(
        "materialized "
        f"{mat.materialized_packets} packet(s) across {mat.materialized_sets} packet file(s) "
        f"into {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
