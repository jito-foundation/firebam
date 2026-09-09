#!/usr/bin/env python3
"""Build target-local packets for scripted BAM scenarios."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
import json
import random
import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any


GEN_RE = re.compile(r"\bfrom=([^ ]+) target=([^ ]+) total_lamports=([0-9]+)\b")
ADAPT_RE = re.compile(
    r"\bsystem_kind=([^ ]+) from=([^ ]+) to=([^ ]+) lamports=([0-9]+)\b"
)
DUMMY_BLOCKHASH = "11111111111111111111111111111111"
MAX_BAM_BATCHES = 128
MAX_BAM_BATCH_PACKETS = 5

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
        "generated_transactions",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "simple_vote_tx_sequence",
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
        "generated_transactions",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "simple_vote_tx_sequence",
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
        "generated_transactions",
        "packets_base64",
        "packets_base64_file",
        "max_schedule_slot",
        "simple_vote_tx",
        "simple_vote_tx_sequence",
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
    "generated_transactions",
    "packets_base64",
    "packets_base64_file",
    "max_schedule_slot",
    "simple_vote_tx",
    "simple_vote_tx_sequence",
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


def generated_transaction_recipe(
    rng: random.Random,
    pool_seed: int,
    mutation_seed: int,
    packet_index: int,
    have_previous: bool,
) -> dict[str, Any]:
    family = rng.choices(
        (
            "kunorpus_system",
            "kunorpus_raw",
            "synthetic_transfer",
            "malformed",
            "duplicate_previous",
        ),
        weights=(40, 20, 20, 15, 5 if have_previous else 0),
        k=1,
    )[0]
    recipe: dict[str, Any] = {
        "family": family,
        "seed": pool_seed,
        "mutation_seed": mutation_seed,
        "candidate_index": rng.randrange(0, 1 << 31),
        "to_seed_index": 1 + packet_index,
        "mutation": "none",
    }

    if family == "kunorpus_system":
        recipe["mutation"] = rng.choices(
            ("none", "bad_signature", "truncate", "bitflip"),
            weights=(65, 15, 10, 10),
            k=1,
        )[0]
    elif family == "kunorpus_raw":
        recipe["mutation"] = rng.choices(
            ("none", "truncate", "bitflip"),
            weights=(70, 15, 15),
            k=1,
        )[0]
    elif family == "synthetic_transfer":
        recipe["lamports"] = 1_000 + rng.randrange(1, 1_000_000)
        recipe["cu_limit"] = 200_000 + rng.randrange(0, 100_001)
        recipe["mutation"] = rng.choices(
            ("none", "bad_signature", "truncate", "bitflip"),
            weights=(70, 15, 5, 10),
            k=1,
        )[0]
    elif family == "malformed":
        recipe["size"] = rng.choice((1, 2, 4, 5, 63, 64, 127, 255, 1232, 1233))
    elif family == "duplicate_previous":
        recipe["source_packet_index"] = rng.randrange(packet_index)

    return recipe


def generate_bundle_recipe(seed: int, max_batches: int, max_packets: int) -> dict[str, Any]:
    if not 1 <= max_batches <= MAX_BAM_BATCHES:
        die(f"shape max batches must be in 1..={MAX_BAM_BATCHES}")
    if not 0 <= max_packets <= MAX_BAM_BATCH_PACKETS:
        die(f"shape max packets must be in 0..={MAX_BAM_BATCH_PACKETS}")

    rng = random.Random(seed)
    batch_count = 1 + rng.randrange(max_batches)
    seq_base = (1000 + seed * max_batches) & 0xFFFFFFFF
    packet_index = 0
    batches: list[dict[str, Any]] = []
    packet_counts: list[int] = []

    for batch_index in range(batch_count):
        packet_count = rng.randrange(max_packets + 1)
        packet_counts.append(packet_count)
        transactions = [
            generated_transaction_recipe(
                rng,
                seed,
                seed ^ ((batch_index + 1) << 32) ^ member_index,
                packet_index + member_index,
                packet_index + member_index > 0,
            )
            for member_index in range(packet_count)
        ]
        packet_index += packet_count

        batch: dict[str, Any] = {
            "seq_id": (seq_base + batch_index) & 0xFFFFFFFF,
            "expect_result": True,
            "max_schedule_slot": rng.choices(
                ("max", "leader", "leader+1", 0),
                weights=(70, 15, 10, 5),
                k=1,
            )[0],
            "revert_on_error": bool(rng.getrandbits(1)),
            "simple_vote_tx": False,
        }
        if transactions:
            batch["generated_transactions"] = transactions
            batch["revert_on_error_sequence"] = [
                bool(rng.getrandbits(1)) for _ in transactions
            ]
            batch["simple_vote_tx_sequence"] = [
                rng.randrange(8) == 0 for _ in transactions
            ]
            batch["packet_meta_sequence"] = rng.choices(
                ("full", "missing_meta", "missing_flags"),
                weights=(80, 10, 10),
                k=len(transactions),
            )
        else:
            batch["packet_count"] = 0
        batches.append(batch)

    return {
        "description": (
            f"Generated heterogeneous BAM bundle seed={seed} "
            f"batches={batch_count} packet_counts={packet_counts}"
        ),
        "heartbeat_interval_ms": 1000,
        "expected_terminal_results": batch_count,
        "events": [
            {"type": "sleep", "ms": 1000},
            {"type": "send_multi_batch", "batches": batches},
            {
                "type": "wait_inbound",
                "kind": "batch_result",
                "min_count": 1,
                "timeout_ms": 12_000,
            },
            {"type": "sleep", "ms": 1000},
        ],
    }


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
        self.generated_packets: list[bytes] = []
        self.generated_families: Counter[str] = Counter()
        self._kunorpus_system_pools: dict[int, list[Path]] = {}
        self._kunorpus_raw_pools: dict[int, list[Path]] = {}

    def materialize_record(self, record: dict[str, Any]) -> None:
        has_count = "packet_count" in record
        has_programs = "program_invocations" in record
        has_generated = "generated_transactions" in record
        has_inline = "packets_base64" in record
        has_file = "packets_base64_file" in record
        if sum(
            1
            for present in (
                has_count,
                has_programs,
                has_generated,
                has_inline,
                has_file,
            )
            if present
        ) > 1:
            die("scenario event specifies more than one packet source")
        if has_programs:
            specs = record["program_invocations"]
            if not isinstance(specs, list) or not specs:
                die("program_invocations must be a non-empty array")
            packet_file = self._build_program_packet_file(specs)
            del record["program_invocations"]
            record["packets_base64_file"] = str(packet_file)
            return
        if has_generated:
            specs = record["generated_transactions"]
            if not isinstance(specs, list) or not specs:
                die("generated_transactions must be a non-empty array")
            packet_file = self._build_generated_packet_file(specs)
            del record["generated_transactions"]
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

    def _run(
        self, cmd: list[str], log_path: Path | None = None
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        if log_path is not None:
            log_path.write_text(result.stdout + result.stderr)
        return result

    def _kunorpus_pool_dir(self, family: str, seed: int) -> Path:
        return self.packet_dir / f"{family}-seed-{seed}"

    def _generate_kunorpus_pool(self, family: str, seed: int) -> list[Path]:
        if self.args.kunorpus is None:
            die("generated Kunorpus transactions require --kunorpus")
        pool_dir = self._kunorpus_pool_dir(family, seed)
        if pool_dir.exists():
            shutil.rmtree(pool_dir)
        pool_dir.mkdir(parents=True)

        if family == "kunorpus_system":
            cmd = [
                str(self.args.kunorpus),
                "generate",
                "instr",
                "system",
                "--output-dir",
                str(pool_dir),
            ]
            pattern = "*.instrctx"
        elif family == "kunorpus_raw":
            cmd = [
                str(self.args.kunorpus),
                "generate",
                "txn",
                "--output-dir",
                str(pool_dir),
            ]
            pattern = "*.txnctx"
        else:
            die(f"unsupported Kunorpus pool family {family!r}")

        cmd.extend(
            (
                "--workers",
                "1",
                "--count",
                str(self.args.kunorpus_count),
                "--seed",
                str(seed),
                "--mutation-chance",
                "0.0",
            )
        )
        self._run(cmd, pool_dir / "kunorpus.out")
        candidates = sorted(pool_dir.rglob(pattern))
        candidates.extend(sorted(pool_dir.rglob("*.fix")))
        return candidates

    def _compatible_system_pool(self, seed: int) -> list[Path]:
        if seed in self._kunorpus_system_pools:
            return self._kunorpus_system_pools[seed]

        for offset in range(self.args.kunorpus_seed_window):
            selected_seed = seed + offset
            candidates = self._generate_kunorpus_pool("kunorpus_system", selected_seed)
            compatible: list[Path] = []
            probe_dir = self._kunorpus_pool_dir("kunorpus_system", selected_seed) / "probes"
            probe_dir.mkdir()
            for idx, candidate in enumerate(candidates):
                packet = probe_dir / f"packet-{idx:05d}.txt"
                result = subprocess.run(
                    [
                        str(self.args.bridge),
                        "--input",
                        str(candidate),
                        "--output",
                        str(packet),
                        "--adapt-mode",
                        "local-system-transfer",
                        "--recent-blockhash",
                        DUMMY_BLOCKHASH,
                        "--from-genesis-account",
                        str(self.args.from_seed_index),
                        "--to-genesis-account",
                        str(self.args.to_seed_start),
                    ],
                    text=True,
                    capture_output=True,
                )
                (probe_dir / f"bridge-{idx:05d}.out").write_text(
                    result.stdout + result.stderr
                )
                if result.returncode != 0:
                    continue
                match = ADAPT_RE.search(result.stdout)
                if match is None:
                    continue
                if int(match.group(4)) > self.args.kunorpus_max_transfer_lamports:
                    continue
                compatible.append(candidate)
            if compatible:
                self._kunorpus_system_pools[seed] = compatible
                return compatible

        die(
            "no executable-compatible Kunorpus system transaction found for "
            f"seed window {seed}..{seed + self.args.kunorpus_seed_window - 1}"
        )

    def _serializable_raw_pool(self, seed: int) -> list[Path]:
        if seed in self._kunorpus_raw_pools:
            return self._kunorpus_raw_pools[seed]

        for offset in range(self.args.kunorpus_seed_window):
            selected_seed = seed + offset
            candidates = self._generate_kunorpus_pool("kunorpus_raw", selected_seed)
            serializable: list[Path] = []
            probe_dir = self._kunorpus_pool_dir("kunorpus_raw", selected_seed) / "probes"
            probe_dir.mkdir()
            for idx, candidate in enumerate(candidates):
                packet = probe_dir / f"packet-{idx:05d}.txt"
                result = subprocess.run(
                    [
                        str(self.args.bridge),
                        "--input",
                        str(candidate),
                        "--output",
                        str(packet),
                        "--adapt-mode",
                        "raw",
                    ],
                    text=True,
                    capture_output=True,
                )
                (probe_dir / f"bridge-{idx:05d}.out").write_text(
                    result.stdout + result.stderr
                )
                if result.returncode == 0 and packet.exists() and packet.read_text().strip():
                    serializable.append(candidate)
            if serializable:
                self._kunorpus_raw_pools[seed] = serializable
                return serializable

        die(
            "no serializable Kunorpus transaction found for "
            f"seed window {seed}..{seed + self.args.kunorpus_seed_window - 1}"
        )

    def _bridge_generated(
        self,
        candidate: Path,
        packet_idx: int,
        adapt_mode: str,
        to_seed_index: int,
    ) -> bytes:
        packet_path = self.packet_dir / f"generated-{packet_idx:05d}.txt"
        bridge_log = self.packet_dir / f"generated-{packet_idx:05d}.bridge.out"
        cmd = [
            str(self.args.bridge),
            "--input",
            str(candidate),
            "--output",
            str(packet_path),
            "--adapt-mode",
            adapt_mode,
        ]
        if adapt_mode == "local-system-transfer":
            cmd.extend(
                (
                    "--recent-blockhash",
                    self.args.recent_blockhash,
                    "--from-genesis-account",
                    str(self.args.from_seed_index),
                    "--to-genesis-account",
                    str(to_seed_index),
                )
            )
        result = self._run(cmd, bridge_log)
        encoded = packet_path.read_text().strip()
        if not encoded:
            die(f"bridge produced an empty generated packet for member {packet_idx}")

        if adapt_mode == "local-system-transfer":
            match = ADAPT_RE.search(result.stdout)
            if match is not None:
                _, payer, recipient, lamports = match.groups()
                self.payer = self.payer or payer
                self.first_recipient = self.first_recipient or recipient
                self.last_recipient = recipient
                self.total_lamports += int(lamports)
        return base64.b64decode(encoded, validate=True)

    def _synthetic_transfer(self, spec: dict[str, Any], packet_idx: int) -> bytes:
        txnctx = self.packet_dir / f"generated-{packet_idx:05d}.txnctx"
        packet = self.packet_dir / f"generated-{packet_idx:05d}.txt"
        to_seed = int(spec.get("to_seed_index", self.args.to_seed_start + packet_idx))
        lamports = int(spec.get("lamports", self.args.lamports + packet_idx))
        cu_limit = int(spec.get("cu_limit", self.args.cu_limit))
        result = self._run(
            [
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
            ],
            self.packet_dir / f"generated-{packet_idx:05d}.gen.out",
        )
        match = GEN_RE.search(result.stdout)
        if match is None:
            die(f"failed to parse generated transfer summary for member {packet_idx}")
        payer, recipient, total_lamports = match.groups()
        self.payer = self.payer or payer
        self.first_recipient = self.first_recipient or recipient
        self.last_recipient = recipient
        self.total_lamports += int(total_lamports)
        self._run(
            [
                str(self.args.bridge),
                "--input",
                str(txnctx),
                "--output",
                str(packet),
                "--adapt-mode",
                "raw",
            ],
            self.packet_dir / f"generated-{packet_idx:05d}.bridge.out",
        )
        return base64.b64decode(packet.read_text().strip(), validate=True)

    @staticmethod
    def _mutate_packet(raw: bytes, mutation: str, seed: int) -> bytes:
        if mutation == "none":
            return raw
        if mutation == "truncate":
            if not raw:
                return raw
            return raw[: 1 + seed % min(len(raw), 64)]
        if mutation == "bitflip":
            if not raw:
                return b"\x01"
            mutated = bytearray(raw)
            index = seed % len(mutated)
            mutated[index] ^= 1 << (seed % 8)
            return bytes(mutated)
        if mutation == "bad_signature":
            mutated = bytearray(raw)
            value = 0
            shift = 0
            cursor = None
            for idx, byte in enumerate(mutated[:5]):
                value |= (byte & 0x7F) << shift
                if byte & 0x80 == 0:
                    cursor = idx + 1
                    break
                shift += 7
            if cursor is None or value < 1 or len(mutated) < cursor + 64:
                return PacketMaterializer._mutate_packet(raw, "bitflip", seed)
            for idx in range(cursor, cursor + 64):
                mutated[idx] ^= 0xFF
            return bytes(mutated)
        die(f"unsupported generated transaction mutation {mutation!r}")

    def _build_generated_packet_file(self, specs: list[Any]) -> Path:
        if len(specs) > MAX_BAM_BATCH_PACKETS:
            die(
                "generated_transactions contains "
                f"{len(specs)} packets; BAM batches support at most {MAX_BAM_BATCH_PACKETS}"
            )
        packet_file = self.packet_dir / f"packets-{self.packet_set_idx:04d}.txt"
        self.packet_set_idx += 1
        lines: list[str] = []

        for local_idx, spec in enumerate(specs):
            if not isinstance(spec, dict):
                die(f"generated_transactions[{local_idx}] is not a table")
            packet_idx = self.packet_idx
            self.packet_idx += 1
            family = spec.get("family")
            seed = int(spec.get("seed", packet_idx))
            mutation_seed = int(spec.get("mutation_seed", seed ^ packet_idx))
            candidate_index = int(spec.get("candidate_index", packet_idx))
            to_seed = int(spec.get("to_seed_index", self.args.to_seed_start + packet_idx))
            mutation = str(spec.get("mutation", "none"))

            if family == "kunorpus_system":
                pool = self._compatible_system_pool(seed)
                raw = self._bridge_generated(
                    pool[candidate_index % len(pool)],
                    packet_idx,
                    "local-system-transfer",
                    to_seed,
                )
            elif family == "kunorpus_raw":
                pool = self._serializable_raw_pool(seed)
                raw = self._bridge_generated(
                    pool[candidate_index % len(pool)],
                    packet_idx,
                    "raw",
                    to_seed,
                )
            elif family == "synthetic_transfer":
                raw = self._synthetic_transfer(spec, packet_idx)
            elif family == "malformed":
                size = int(spec.get("size", 1))
                if not 1 <= size <= 1_048_576:
                    die("malformed generated transaction size must be in 1..=1048576")
                raw = random.Random(mutation_seed).randbytes(size)
            elif family == "duplicate_previous":
                source_idx = int(spec.get("source_packet_index", packet_idx - 1))
                if not 0 <= source_idx < len(self.generated_packets):
                    die(
                        "duplicate_previous source_packet_index "
                        f"{source_idx} is not before packet {packet_idx}"
                    )
                raw = self.generated_packets[source_idx]
            else:
                die(f"unsupported generated transaction family {family!r}")

            raw = self._mutate_packet(raw, mutation, mutation_seed)
            self.generated_packets.append(raw)
            self.generated_families[str(family)] += 1
            lines.append(base64.b64encode(raw).decode("ascii"))

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
                    f"generated_family_counts={json.dumps(self.generated_families, sort_keys=True, separators=(',', ':'))}",
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
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--scenario", type=Path)
    source.add_argument("--shape-seed", type=int)
    parser.add_argument("--recipe-output", type=Path)
    parser.add_argument("--shape-max-batches", type=int, default=8)
    parser.add_argument("--shape-max-packets", type=int, default=MAX_BAM_BATCH_PACKETS)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--packet-dir", required=True, type=Path)
    parser.add_argument("--metadata-output", required=True, type=Path)
    parser.add_argument("--gen-simple-system-txnctx", required=True, type=Path)
    parser.add_argument("--gen-program-invocation", required=True, type=Path)
    parser.add_argument("--bridge", required=True, type=Path)
    parser.add_argument("--kunorpus", type=Path)
    parser.add_argument("--kunorpus-count", type=int, default=64)
    parser.add_argument("--kunorpus-seed-window", type=int, default=16)
    parser.add_argument(
        "--kunorpus-max-transfer-lamports", type=int, default=50_000_000_000_000
    )
    parser.add_argument("--recent-blockhash", required=True)
    parser.add_argument("--from-seed-index", type=int, default=0)
    parser.add_argument("--to-seed-start", type=int, default=1)
    parser.add_argument("--vary-recipient", action="store_true")
    parser.add_argument("--lamports", type=int, default=1_000_000)
    parser.add_argument("--lamports-step", type=int, default=1)
    parser.add_argument("--cu-limit", type=int, default=300_000)
    args = parser.parse_args()

    if args.kunorpus_count < 1:
        die("--kunorpus-count must be at least 1")
    if args.kunorpus_seed_window < 1:
        die("--kunorpus-seed-window must be at least 1")
    if args.shape_seed is not None:
        data = generate_bundle_recipe(
            args.shape_seed,
            args.shape_max_batches,
            args.shape_max_packets,
        )
        if args.recipe_output is None:
            die("--shape-seed requires --recipe-output")
        args.recipe_output.parent.mkdir(parents=True, exist_ok=True)
        args.recipe_output.write_text(render_scenario(data))
    else:
        assert args.scenario is not None
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
