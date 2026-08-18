#!/usr/bin/env python3
"""Normalize live BAM validator run artifacts into a diffable JSON outcome."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback is not expected here.
    tomllib = None


BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        return []


def read_summary(path: Path) -> dict[str, str]:
    summary: dict[str, str] = {}
    for line in read_lines(path):
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        summary[key] = value
    return summary


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in read_lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            items.append(item)
    return items


def read_json(path: Path) -> dict[str, Any]:
    try:
        item = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return item if isinstance(item, dict) else {}


def maybe_int(value: str | None) -> int | str | None:
    if value is None or value == "" or value == "n/a":
        return None
    try:
        return int(value)
    except ValueError:
        return value


def maybe_int_any(value: Any) -> int | Any | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return maybe_int(value)
    return value


def parse_kv_fields(text: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key, value in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=([^ ]+)", text):
        fields[key] = maybe_int_any(value)
    return fields


def parse_int_list(value: str) -> list[int]:
    parsed: list[int] = []
    for item in value.split(","):
        item = item.strip()
        if item:
            parsed.append(int(item))
    return parsed


def b58encode(raw: bytes) -> str:
    if not raw:
        return ""

    value = int.from_bytes(raw, byteorder="big")
    encoded = ""
    while value:
        value, rem = divmod(value, 58)
        encoded = BASE58_ALPHABET[rem] + encoded

    leading_zeroes = 0
    for byte in raw:
        if byte != 0:
            break
        leading_zeroes += 1
    return (BASE58_ALPHABET[0] * leading_zeroes) + (encoded or BASE58_ALPHABET[0])


def read_shortvec(data: bytes, offset: int = 0) -> tuple[int, int]:
    value = 0
    shift = 0
    for idx in range(offset, min(len(data), offset + 5)):
        byte = data[idx]
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, idx + 1
        shift += 7
    raise ValueError("unterminated compact-u16 value")


def packet_first_signature(encoded: str) -> str | None:
    try:
        raw = base64.b64decode(encoded, validate=True)
        sig_count, sig_offset = read_shortvec(raw)
    except (binascii.Error, ValueError):
        return None
    if sig_count < 1 or len(raw) < sig_offset + 64:
        return None
    return b58encode(raw[sig_offset : sig_offset + 64])


def packet_file_signatures(path: Path) -> list[str | None]:
    signatures: list[str | None] = []
    for line in read_lines(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        signatures.append(packet_first_signature(line))
    return signatures


def packet_inline_signatures(encoded_packets: list[Any]) -> list[str | None]:
    signatures: list[str | None] = []
    for encoded in encoded_packets:
        if isinstance(encoded, str):
            signatures.append(packet_first_signature(encoded))
    return signatures


def resolve_scenario_path(summary: dict[str, str]) -> Path | None:
    for key in ("effective_scenario_file", "scenario_file"):
        value = summary.get(key)
        if value and value != "n/a":
            path = Path(value)
            if path.exists():
                return path
    return None


def resolve_packet_path(path_value: str, scenario_path: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return scenario_path.parent / path


def next_non_terminal_seq_id(seq_id: int) -> int:
    seq_id = (seq_id + 1) & 0xFFFFFFFF
    return 1 if seq_id == 0xFFFFFFFF else seq_id


def batch_packet_signatures(spec: dict[str, Any], scenario_path: Path) -> list[str | None]:
    if isinstance(spec.get("packets_base64"), list):
        return packet_inline_signatures(spec["packets_base64"])
    if isinstance(spec.get("packets_base64_file"), str):
        return packet_file_signatures(resolve_packet_path(spec["packets_base64_file"], scenario_path))
    packet_count = spec.get("packet_count")
    if isinstance(packet_count, int) and packet_count >= 0:
        return [None] * packet_count
    return []


def parse_scripted_scenario(summary: dict[str, str]) -> dict[str, Any]:
    scenario_path = resolve_scenario_path(summary)
    if scenario_path is None:
        return {"source": None, "batches": [], "expected_signatures": []}
    if tomllib is None:
        return {
            "source": str(scenario_path),
            "parse_error": "tomllib unavailable",
            "batches": [],
            "expected_signatures": [],
        }

    try:
        data = tomllib.loads(scenario_path.read_text())
    except Exception as err:  # TOMLDecodeError is only present when tomllib loaded.
        return {
            "source": str(scenario_path),
            "parse_error": str(err),
            "batches": [],
            "expected_signatures": [],
        }

    batches: list[dict[str, Any]] = []
    expected_signatures: list[dict[str, Any]] = []
    next_seq_id = 1
    batch_ordinal = 0

    def append_batch(
        event_type: str,
        spec: dict[str, Any],
        batch_index: int,
        signatures: list[str | None],
        tx_index_offset: int = 0,
    ) -> None:
        nonlocal batch_ordinal, next_seq_id
        seq_value = spec.get("seq_id")
        if isinstance(seq_value, int):
            seq_id = seq_value
        else:
            seq_id = next_seq_id
            next_seq_id = next_non_terminal_seq_id(next_seq_id)

        packet_count = len(signatures)
        batches.append(
            {
                "ordinal": batch_ordinal,
                "event_type": event_type,
                "batch_index": batch_index,
                "seq_id": seq_id,
                "packet_count": packet_count,
                "known_signature_count": sum(sig is not None for sig in signatures),
                "max_schedule_slot": spec.get("max_schedule_slot"),
                "revert_on_error": spec.get("revert_on_error"),
            }
        )
        for offset, signature in enumerate(signatures):
            if signature is None:
                continue
            expected_signatures.append(
                {
                    "ordinal": len(expected_signatures),
                    "batch_ordinal": batch_ordinal,
                    "seq_id": seq_id,
                    "tx_index": tx_index_offset + offset,
                    "signature": signature,
                }
            )
        batch_ordinal += 1

    for event in data.get("events", []):
        if not isinstance(event, dict):
            continue
        event_type = event.get("type")
        if event_type == "send_batch":
            append_batch("send_batch", event, 0, batch_packet_signatures(event, scenario_path))
        elif event_type == "send_multi_batch":
            for batch_index, batch in enumerate(event.get("batches", [])):
                if isinstance(batch, dict):
                    append_batch(
                        "send_multi_batch",
                        batch,
                        batch_index,
                        batch_packet_signatures(batch, scenario_path),
                    )
        elif event_type == "send_split_batch":
            signatures = batch_packet_signatures(event, scenario_path)
            cursor = 0
            for batch_index, split_size in enumerate(event.get("splits", [])):
                if not isinstance(split_size, int) or split_size < 0:
                    continue
                split_signatures = signatures[cursor : cursor + split_size]
                append_batch(
                    "send_split_batch",
                    event,
                    batch_index,
                    split_signatures,
                    tx_index_offset=cursor,
                )
                cursor += split_size

    return {
        "source": str(scenario_path),
        "description": data.get("description"),
        "batches": batches,
        "expected_signatures": expected_signatures,
    }


def parse_bam_log(lines: list[str]) -> dict[str, Any]:
    batch_results: list[dict[str, Any]] = []
    batch_result_txs: list[dict[str, Any]] = []
    scripted_sent_batches: list[dict[str, Any]] = []
    leader_states: list[dict[str, Any]] = []
    wait_inbound: list[dict[str, Any]] = []
    scheduler_streams: list[dict[str, Any]] = []
    stream_closes: list[dict[str, Any]] = []
    auth_proofs: list[dict[str, Any]] = []
    builder_configs: list[dict[str, Any]] = []
    config_updates: list[dict[str, Any]] = []
    sent_ordinal = 0
    current_leader_slot: int | None = None

    for line_no, line in enumerate(lines, start=1):
        if line.startswith("GetBuilderConfig:"):
            fields = parse_kv_fields(line)
            builder_configs.append(
                {
                    "line": line_no,
                    "prio_fee_recipient_pubkey": fields.get("prio_fee_recipient_pubkey"),
                    "commission_bps": fields.get("commission_bps"),
                    "block_engine": fields.get("block_engine"),
                    "bam": fields.get("bam"),
                    "tpu": fields.get("tpu"),
                    "fwd": fields.get("fwd"),
                    "shred": fields.get("shred"),
                }
            )
            continue

        if line.startswith("scripted config_update "):
            fields = parse_kv_fields(line)
            config_updates.append(
                {
                    "line": line_no,
                    "conn": fields.get("conn"),
                    "prio_fee_recipient_pubkey": fields.get("prio_fee_recipient_pubkey"),
                    "commission_bps": fields.get("commission_bps"),
                    "block_engine": fields.get("block_engine"),
                    "bam": fields.get("bam"),
                    "tpu": fields.get("tpu"),
                    "fwd": fields.get("fwd"),
                    "shred": fields.get("shred"),
                }
            )
            continue

        if m := re.search(r"^InitSchedulerStream: .* conn=(\d+)", line):
            scheduler_streams.append({"conn": int(m.group(1))})
            continue

        if m := re.search(r"^scheduler<-validator leader state slot=(\d+) conn=(\d+)", line):
            current_leader_slot = int(m.group(1))
            leader_states.append(
                {
                    "line": line_no,
                    "slot": current_leader_slot,
                    "conn": int(m.group(2)),
                }
            )
            continue

        m = re.search(
            r"^scripted send_batch seq_id=(\d+) packets=(\d+) "
            r"max_schedule_slot=([^ ]+)(?: expect_result=(true|false))?$",
            line,
        )
        if m:
            if m.group(4) != "false":
                scripted_sent_batches.append(
                    {
                        "ordinal": sent_ordinal,
                        "event_type": "send_batch",
                        "batch_index": 0,
                        "seq_id": int(m.group(1)),
                        "packet_count": int(m.group(2)),
                        "max_schedule_slot": m.group(3),
                        "leader_slot_at_send": current_leader_slot,
                        "line": line_no,
                    }
                )
            sent_ordinal += 1
            continue

        m = re.search(
            r"^scripted send_batch_flood batch_count=(\d+) start_seq_id=(\d+) "
            r"packets_per_batch=(\d+) max_schedule_slot=(.*)$",
            line,
        )
        if m:
            batch_count = int(m.group(1))
            start_seq_id = int(m.group(2))
            for batch_index in range(batch_count):
                scripted_sent_batches.append(
                    {
                        "ordinal": sent_ordinal,
                        "event_type": "send_batch_flood",
                        "batch_index": batch_index,
                        "seq_id": (start_seq_id + batch_index) & 0xFFFFFFFF,
                        "packet_count": int(m.group(3)),
                        "max_schedule_slot": m.group(4),
                        "leader_slot_at_send": current_leader_slot,
                        "line": line_no,
                    }
                )
                sent_ordinal += 1
            continue

        m = re.search(
            r"^scripted send_raw_overflow_faults start_seq_id=(\d+) "
            r"expected_result_count=(\d+)$",
            line,
        )
        if m:
            start_seq_id = int(m.group(1))
            result_count = int(m.group(2))
            for batch_index in range(result_count):
                scripted_sent_batches.append(
                    {
                        "ordinal": sent_ordinal,
                        "event_type": "send_raw_overflow_faults",
                        "batch_index": batch_index,
                        "seq_id": (start_seq_id + batch_index) & 0xFFFFFFFF,
                        "packet_count": 0,
                        "leader_slot_at_send": current_leader_slot,
                        "line": line_no,
                    }
                )
                sent_ordinal += 1
            continue

        m = re.search(
            r"^scripted send_multi_batch seq_ids=\[(.*?)\] packet_counts=\[(.*?)\]$",
            line,
        )
        if m:
            seq_ids = parse_int_list(m.group(1))
            packet_counts = parse_int_list(m.group(2))
            for batch_index, seq_id in enumerate(seq_ids):
                packet_count = packet_counts[batch_index] if batch_index < len(packet_counts) else None
                scripted_sent_batches.append(
                    {
                        "ordinal": sent_ordinal,
                        "event_type": "send_multi_batch",
                        "batch_index": batch_index,
                        "seq_id": seq_id,
                        "packet_count": packet_count,
                        "leader_slot_at_send": current_leader_slot,
                        "line": line_no,
                    }
                )
                sent_ordinal += 1
            continue

        m = re.search(
            r"^scripted send_split_batch seq_id=(\d+) splits=\[(.*?)\] max_schedule_slot=(.*)$",
            line,
        )
        if m:
            seq_id = int(m.group(1))
            split_counts = parse_int_list(m.group(2))
            for batch_index, packet_count in enumerate(split_counts):
                scripted_sent_batches.append(
                    {
                        "ordinal": sent_ordinal,
                        "event_type": "send_split_batch",
                        "batch_index": batch_index,
                        "seq_id": seq_id,
                        "packet_count": packet_count,
                        "max_schedule_slot": m.group(3),
                        "leader_slot_at_send": current_leader_slot,
                        "line": line_no,
                    }
                )
                sent_ordinal += 1
            continue

        m = re.search(
            r"^scheduler<-validator auth proof validator_pubkey=([^ ]+) "
            r"challenge_len=(\d+) sig_len=(\d+) conn=(\d+)",
            line,
        )
        if m:
            auth_proofs.append(
                {
                    "validator_pubkey": m.group(1),
                    "challenge_len": int(m.group(2)),
                    "sig_len": int(m.group(3)),
                    "conn": int(m.group(4)),
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result seq_id=(\d+) "
            r"status=committed txns=(\d+) conn=(\d+)",
            line,
        )
        if m:
            batch_results.append(
                {
                    "seq_id": int(m.group(1)),
                    "status": "committed",
                    "txns": int(m.group(2)),
                    "conn": int(m.group(3)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result seq_id=(\d+) "
            r"status=not_committed reason=([^ ]+) index=(\d+) detail=([^ ]+) conn=(\d+)",
            line,
        )
        if m:
            batch_results.append(
                {
                    "seq_id": int(m.group(1)),
                    "status": "not_committed",
                    "reason": m.group(2),
                    "index": int(m.group(3)),
                    "detail": m.group(4),
                    "conn": int(m.group(5)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result seq_id=(\d+) "
            r"status=not_committed reason=([^ ]+) detail=(.*?) conn=(\d+)",
            line,
        )
        if m:
            batch_results.append(
                {
                    "seq_id": int(m.group(1)),
                    "status": "not_committed",
                    "reason": m.group(2),
                    "detail": m.group(3),
                    "conn": int(m.group(4)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result seq_id=(\d+) "
            r"status=not_committed reason=missing conn=(\d+)",
            line,
        )
        if m:
            batch_results.append(
                {
                    "seq_id": int(m.group(1)),
                    "status": "not_committed",
                    "reason": "missing",
                    "conn": int(m.group(2)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result seq_id=(\d+) "
            r"status=missing_result conn=(\d+)",
            line,
        )
        if m:
            batch_results.append(
                {
                    "seq_id": int(m.group(1)),
                    "status": "missing_result",
                    "conn": int(m.group(2)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scheduler<-validator batch_result_tx seq_id=(\d+) tx_index=(\d+) "
            r"execution_success=(true|false) cus_consumed=(\d+) "
            r"feepayer_balance_lamports=(\d+) loaded_accounts_data_size=(\d+) conn=(\d+)",
            line,
        )
        if m:
            batch_result_txs.append(
                {
                    "seq_id": int(m.group(1)),
                    "tx_index": int(m.group(2)),
                    "execution_success": m.group(3) == "true",
                    "cus_consumed": int(m.group(4)),
                    "feepayer_balance_lamports": int(m.group(5)),
                    "loaded_accounts_data_size": int(m.group(6)),
                    "conn": int(m.group(7)),
                    "leader_slot_at_result": current_leader_slot,
                    "line": line_no,
                }
            )
            continue

        m = re.search(
            r"^scripted wait_inbound satisfied conn=(\d+) kind=([^ ]+) observed=(\d+)",
            line,
        )
        if m:
            wait_inbound.append(
                {
                    "conn": int(m.group(1)),
                    "kind": m.group(2),
                    "observed": int(m.group(3)),
                }
            )
            continue

        if m := re.search(r"^scheduler stream closed by validator conn=(\d+)", line):
            stream_closes.append({"conn": int(m.group(1))})

    return {
        "auth_challenge_count": sum(
            1 for line in lines if line.startswith("GetAuthChallenge:")
        ),
        "builder_config_count": sum(
            1 for line in lines if line.startswith("GetBuilderConfig:")
        ),
        "fee_config_observability_parsed": True,
        "builder_configs": builder_configs,
        "config_updates": config_updates,
        "scheduler_streams": scheduler_streams,
        "auth_proofs": auth_proofs,
        "leader_states": leader_states,
        "scripted_sent_batches": scripted_sent_batches,
        "batch_results": batch_results,
        "batch_result_txs": batch_result_txs,
        "wait_inbound": wait_inbound,
        "stream_closes": stream_closes,
    }


def count_by_seq_id(items: list[dict[str, Any]]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for item in items:
        seq_id = item.get("seq_id")
        if isinstance(seq_id, int):
            counts[seq_id] = counts.get(seq_id, 0) + 1
    return counts


def seq_count_projection(counts: dict[int, int]) -> list[dict[str, int]]:
    return [
        {"seq_id": seq_id, "count": count}
        for seq_id, count in sorted(counts.items())
    ]


def batch_result_completeness(
    scenario: dict[str, Any], bam: dict[str, Any]
) -> dict[str, Any]:
    scripted_sent_batches = bam.get("scripted_sent_batches", [])
    scenario_batches = scenario.get("batches", [])
    if scripted_sent_batches:
        expected_source = "bam_log"
        expected_batches = scripted_sent_batches
    else:
        expected_source = "scenario"
        expected_batches = scenario_batches

    expected_counts = count_by_seq_id(expected_batches)
    actual_counts = count_by_seq_id(bam.get("batch_results", []))
    checked = bool(expected_batches)

    missing: list[dict[str, int]] = []
    extra: list[dict[str, int]] = []
    for seq_id in sorted(set(expected_counts) | set(actual_counts)):
        expected = expected_counts.get(seq_id, 0)
        actual = actual_counts.get(seq_id, 0)
        if actual < expected:
            missing.append({"seq_id": seq_id, "expected": expected, "actual": actual})
        elif actual > expected:
            extra.append({"seq_id": seq_id, "expected": expected, "actual": actual})

    return {
        "checked": checked,
        "passed": not checked or (not missing and not extra),
        "expected_source": expected_source if checked else None,
        "expected_count": len(expected_batches),
        "actual_count": len(bam.get("batch_results", [])),
        "expected_seq_counts": seq_count_projection(expected_counts),
        "actual_seq_counts": seq_count_projection(actual_counts),
        "missing": missing,
        "extra": extra,
    }


def parse_fd_log(lines: list[str]) -> dict[str, Any]:
    decoded_bundles: list[dict[str, Any]] = []
    bam_ingress_slot_summaries: list[dict[str, Any]] = []
    bank_hashes: list[dict[str, Any]] = []
    startup_contact_info: dict[str, str] = {}
    default_tpu_addresses: list[dict[str, Any]] = []
    tpu_address_updates: list[dict[str, Any]] = []
    public_tpu_address_updates: list[dict[str, Any]] = []
    contact_info_client_ids: list[dict[str, Any]] = []
    stale_bam_result_drops: list[dict[str, Any]] = []

    for line_no, line in enumerate(lines, start=1):
        if m := re.search(
            r"Dropping stale BAM bundle result: seq_id=(\d+) result_gen=(\d+) current_gen=(\d+)",
            line,
        ):
            stale_bam_result_drops.append(
                {
                    "line": line_no,
                    "seq_id": int(m.group(1)),
                    "result_gen": int(m.group(2)),
                    "current_gen": int(m.group(3)),
                }
            )
            continue
        if m := re.search(r"^Gossip Address: (.+)$", line):
            startup_contact_info["gossip"] = m.group(1)
            continue
        if m := re.search(r"^TPU QUIC Address: (.+)$", line):
            startup_contact_info["tpu_quic"] = m.group(1)
            continue
        if m := re.search(
            r"Using configured default TPU addresses: tpu=([^ ]+) fwd=([^ ]+)",
            line,
        ):
            default_tpu_addresses.append(
                {
                    "tpu": m.group(1),
                    "fwd": m.group(2).rstrip("."),
                    "line": line_no,
                }
            )
            continue
        if m := re.search(
            r"Prepare to set TPU addresses: tpu=([^ ]+) fwd=([^,]+), use_bam: ([01])",
            line,
        ):
            tpu_address_updates.append(
                {
                    "stage": "prepare",
                    "tpu": m.group(1),
                    "fwd": m.group(2),
                    "use_bam": int(m.group(3)),
                    "line": line_no,
                }
            )
            continue
        if m := re.search(r"Updated TPU addresses: tpu=([^ ]+) fwd=([^ ]+)", line):
            tpu_address_updates.append(
                {
                    "stage": "updated",
                    "tpu": m.group(1),
                    "fwd": m.group(2),
                    "line": line_no,
                }
            )
            continue
        if m := re.search(
            r"Public TPU( Forwards)? addresses set to udp=Some\(([^)]+)\) quic=Some\(([^)]+)\)",
            line,
        ):
            public_tpu_address_updates.append(
                {
                    "kind": "forwards" if m.group(1) else "tpu",
                    "udp": m.group(2),
                    "quic": m.group(3),
                    "line": line_no,
                }
            )
            continue
        if m := re.search(r"ContactInfo client id set to (\d+)", line):
            contact_info_client_ids.append({"client_id": int(m.group(1)), "line": line_no})
            continue
        m = re.search(r"BAM rx bundle: seq_id=(\d+).*?txns=(\d+)", line)
        if m:
            decoded_bundles.append({"seq_id": int(m.group(1)), "txns": int(m.group(2))})
            continue
        if m := re.search(r"BAM rx bundle: seq_id=(\d+)", line):
            decoded_bundles.append({"seq_id": int(m.group(1))})
            continue
        m = re.search(
            r"BAM ingress vs Firedancer slot summary: "
            r"max_schedule_slot=(\d+) "
            r"first_rx_ns=(-?\d+) "
            r"first_rx_minus_slot_end_ns=(-?\d+) "
            r"first_rx_after_slot_end=(\d+) "
            r"txns_before_slot_end=(\d+) "
            r"txns_after_slot_end=(\d+) "
            r"txns_unknown_slot_end=(\d+) "
            r"current_leader_slot=(\d+)",
            line,
        )
        if m:
            max_schedule_slot = int(m.group(1))
            current_leader_slot = int(m.group(8))
            txns_after_slot_end = int(m.group(6))
            txns_unknown_slot_end = int(m.group(7))
            bam_ingress_slot_summaries.append(
                {
                    "line": line_no,
                    "max_schedule_slot": max_schedule_slot,
                    "first_rx_ns": int(m.group(2)),
                    "first_rx_minus_slot_end_ns": int(m.group(3)),
                    "first_rx_after_slot_end": bool(int(m.group(4))),
                    "txns_before_slot_end": int(m.group(5)),
                    "txns_after_slot_end": txns_after_slot_end,
                    "txns_unknown_slot_end": txns_unknown_slot_end,
                    "current_leader_slot": current_leader_slot,
                    "current_after_max_schedule_slot": current_leader_slot
                    > max_schedule_slot,
                    "has_after_or_unknown_slot_end_txns": (
                        txns_after_slot_end > 0 or txns_unknown_slot_end > 0
                    ),
                }
            )
            continue

    for line in lines:
        if m := re.search(r"bank frozen: (\d+) hash: ([1-9A-HJ-NP-Za-km-z]+)", line):
            bank_hashes.append({"slot": int(m.group(1)), "hash": m.group(2)})

    return {
        "bam_identity_update_count": sum(
            1 for line in lines if "BAM identity pubkey updated to" in line
        ),
        "updated_tpu_addresses_count": sum(
            1 for line in lines if "Updated TPU addresses:" in line
        ),
        "decoded_bundles": decoded_bundles,
        "stale_bam_result_drops": stale_bam_result_drops,
        "bam_ingress_slot_summaries": bam_ingress_slot_summaries,
        "bank_hashes": bank_hashes,
        "contact_info": {
            "startup": startup_contact_info,
            "default_tpu_addresses": default_tpu_addresses,
            "tpu_address_updates": tpu_address_updates,
            "public_tpu_address_updates": public_tpu_address_updates,
            "contact_info_client_ids": contact_info_client_ids,
        },
    }


def parse_rpc_blocks(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for item in items:
        slot = maybe_int(str(item.get("slot"))) if "slot" in item else None
        response = item.get("response", item)
        result = response.get("result") if isinstance(response, dict) else None
        error = response.get("error") if isinstance(response, dict) else None
        signatures: list[str] = []
        if isinstance(result, dict):
            raw_signatures = result.get("signatures")
            if isinstance(raw_signatures, list):
                signatures = [sig for sig in raw_signatures if isinstance(sig, str)]
        blocks.append(
            {
                "slot": slot,
                "signatures": signatures,
                "signature_count": len(signatures),
                "available": isinstance(result, dict),
                "error": error,
            }
        )
    return blocks


def parse_rpc_signature_statuses(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    for item in items:
        signature = item.get("signature")
        if not isinstance(signature, str):
            continue
        statuses.append(
            {
                "signature": signature,
                "found": item.get("found") is True,
                "slot": maybe_int(str(item.get("slot"))) if item.get("slot") is not None else None,
                "err": item.get("err"),
                "confirmation_status": item.get("confirmation_status"),
                "confirmations": item.get("confirmations"),
                "poll_count": item.get("poll_count"),
                "rpc_error": item.get("rpc_error"),
            }
        )
    return statuses


def expected_signature_occurrences(
    expected: list[dict[str, Any]],
    rpc_blocks: list[dict[str, Any]],
    rpc_signature_statuses: list[dict[str, Any]],
    start_slot: Any,
) -> list[dict[str, Any]]:
    expected_by_signature: dict[str, list[dict[str, Any]]] = {}
    for item in expected:
        signature = item.get("signature")
        if isinstance(signature, str):
            expected_by_signature.setdefault(signature, []).append(item)

    chain_occurrences: dict[str, list[dict[str, Any]]] = {}
    landing_ordinal = 0
    start_slot_int = start_slot if isinstance(start_slot, int) else None
    for block in sorted(rpc_blocks, key=lambda item: maybe_int(str(item.get("slot"))) or 0):
        slot = block.get("slot")
        slot_offset = None
        if isinstance(slot, int) and start_slot_int is not None:
            slot_offset = slot - start_slot_int
        for block_index, signature in enumerate(block.get("signatures", [])):
            if signature not in expected_by_signature:
                continue
            chain_occurrences.setdefault(signature, []).append(
                {
                    "slot": slot,
                    "slot_offset": slot_offset,
                    "block_index": block_index,
                    "landing_ordinal": landing_ordinal,
                    "source": "rpc_block",
                }
            )
            landing_ordinal += 1

    status_by_signature: dict[str, dict[str, Any]] = {}
    for status in rpc_signature_statuses:
        signature = status.get("signature")
        if not isinstance(signature, str):
            continue
        previous = status_by_signature.get(signature)
        if previous is None or (
            status.get("found") is True and previous.get("found") is not True
        ):
            status_by_signature[signature] = status

    statuses: list[dict[str, Any]] = []
    for item in expected:
        signature = item.get("signature")
        occurrences = chain_occurrences.get(signature, []) if isinstance(signature, str) else []
        signature_status = (
            status_by_signature.get(signature) if isinstance(signature, str) else None
        )
        if not occurrences and isinstance(signature_status, dict) and signature_status.get("found") is True:
            slot = signature_status.get("slot")
            slot_offset = None
            if isinstance(slot, int) and start_slot_int is not None:
                slot_offset = slot - start_slot_int
            occurrences = [
                {
                    "slot": slot,
                    "slot_offset": slot_offset,
                    "block_index": None,
                    "landing_ordinal": None,
                    "source": "signature_status",
                    "err": signature_status.get("err"),
                    "confirmation_status": signature_status.get("confirmation_status"),
                }
            ]
        duplicate_expected = (
            isinstance(signature, str)
            and len(expected_by_signature.get(signature, [])) > 1
        )
        statuses.append(
            {
                "seq_id": item.get("seq_id"),
                "tx_index": item.get("tx_index"),
                "batch_ordinal": item.get("batch_ordinal"),
                "signature": signature,
                "attributable": bool(signature) and not duplicate_expected,
                "duplicate_expected_signature": duplicate_expected,
                "occurrence_count": len(occurrences),
                "evidence": sorted(
                    {
                        occurrence.get("source")
                        for occurrence in occurrences
                        if occurrence.get("source")
                    }
                ),
                "signature_status_found": (
                    signature_status.get("found")
                    if isinstance(signature_status, dict)
                    else None
                ),
                "status": (
                    "ambiguous_duplicate_expected_signature"
                    if duplicate_expected
                    else "landed"
                    if occurrences
                    else "missing"
                ),
                "occurrences": occurrences,
            }
        )
    return statuses


def landed_expected_signatures(
    statuses: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    landed: list[dict[str, Any]] = []
    for status in statuses:
        if not status.get("attributable"):
            continue
        signature = status.get("signature")
        for occurrence in status.get("occurrences", []):
            landed.append(
                {
                    "slot": occurrence.get("slot"),
                    "slot_offset": occurrence.get("slot_offset"),
                    "block_index": occurrence.get("block_index"),
                    "landing_ordinal": occurrence.get("landing_ordinal"),
                    "source": occurrence.get("source"),
                    "seq_id": status.get("seq_id"),
                    "tx_index": status.get("tx_index"),
                    "batch_ordinal": status.get("batch_ordinal"),
                    "signature": signature,
                }
            )
    return sorted(
        landed,
        key=lambda item: (
            0 if isinstance(item.get("landing_ordinal"), int) else 1,
            item.get("landing_ordinal") if isinstance(item.get("landing_ordinal"), int) else 0,
            maybe_int(str(item.get("batch_ordinal"))) or 0,
            maybe_int(str(item.get("seq_id"))) or 0,
            maybe_int(str(item.get("tx_index"))) or 0,
        ),
    )


def normal_tpu_signature_statuses(
    summary: dict[str, str],
    rpc_signature_statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    packet_file = summary.get("normal_tpu_packet")
    if packet_file is None or packet_file in ("", "n/a", "none"):
        return {}

    expected_landed = summary.get("normal_tpu_expected_landed") in ("1", "true", "yes")
    signatures = [
        signature
        for signature in packet_file_signatures(Path(packet_file))
        if isinstance(signature, str)
    ]
    status_by_signature = {
        item.get("signature"): item
        for item in rpc_signature_statuses
        if isinstance(item.get("signature"), str)
    }
    statuses = []
    for ordinal, signature in enumerate(signatures):
        status = status_by_signature.get(signature)
        found = isinstance(status, dict) and status.get("found") is True
        statuses.append(
            {
                "ordinal": ordinal,
                "signature": signature,
                "found": found,
                "slot": status.get("slot") if isinstance(status, dict) else None,
                "err": status.get("err") if isinstance(status, dict) else None,
                "confirmation_status": status.get("confirmation_status")
                if isinstance(status, dict)
                else None,
                "rpc_error": status.get("rpc_error") if isinstance(status, dict) else None,
            }
        )

    return {
        "packet_file": packet_file,
        "dst": summary.get("normal_tpu_dst") or summary.get("normal_tpu_port"),
        "checked": bool(signatures),
        "expected_landed": expected_landed,
        "statuses": statuses,
        "passed": bool(signatures)
        and all(
            (item["found"] and item["err"] is None) if expected_landed else not item["found"]
            for item in statuses
        ),
    }


def parse_operator_log(lines: list[str]) -> dict[str, Any]:
    return {
        "events": [
            line[len("operator: ") :]
            for line in lines
            if line.startswith("operator: ")
        ],
    }


def state_from_summary(summary: dict[str, str]) -> dict[str, Any]:
    state: dict[str, Any] = {}
    if "payer" in summary:
        state["payer"] = {
            "pubkey": summary.get("payer"),
            "initial": maybe_int(summary.get("payer_initial")),
            "observed": maybe_int(summary.get("payer_observed")),
        }
    if noneish(summary.get("bam_fee_recipient")) is not None:
        state["bam_fee_recipient"] = {
            "pubkey": summary.get("bam_fee_recipient"),
            "initial": maybe_int(summary.get("bam_fee_recipient_initial")),
            "observed": maybe_int(summary.get("bam_fee_recipient_observed")),
            "expected_min_delta": maybe_int(summary.get("bam_fee_recipient_expected_min_delta")),
            "commission_bps": maybe_int(summary.get("bam_fee_commission_bps")),
            "builder_commission_pct": maybe_int(summary.get("bam_fee_builder_commission_pct")),
        }
    if noneish(summary.get("bam_fee_recipient_second")) is not None:
        state["bam_fee_recipient_second"] = {
            "pubkey": summary.get("bam_fee_recipient_second"),
            "initial": maybe_int(summary.get("bam_fee_recipient_second_initial")),
            "observed": maybe_int(summary.get("bam_fee_recipient_second_observed")),
            "expected_min_delta": maybe_int(summary.get("bam_fee_recipient_expected_min_delta")),
            "commission_bps": maybe_int(summary.get("bam_fee_commission_bps")),
            "builder_commission_pct": maybe_int(summary.get("bam_fee_builder_commission_pct")),
        }
    if "recipient_one" in summary:
        state["recipient_one"] = {
            "pubkey": summary.get("recipient_one"),
            "initial": maybe_int(summary.get("recipient_one_initial")),
            "expected": maybe_int(summary.get("recipient_one_expected")),
            "observed": maybe_int(summary.get("recipient_one_observed")),
            "owner_expected": noneish(summary.get("recipient_one_owner_expected")),
            "owner_observed": noneish(summary.get("recipient_one_owner_observed")),
            "space_expected": maybe_int(summary.get("recipient_one_space_expected")),
            "space_observed": maybe_int(summary.get("recipient_one_space_observed")),
        }
    if "recipient_two" in summary:
        state["recipient_two"] = {
            "pubkey": summary.get("recipient_two"),
            "initial": maybe_int(summary.get("recipient_two_initial")),
            "expected": maybe_int(summary.get("recipient_two_expected")),
            "observed": maybe_int(summary.get("recipient_two_observed")),
            "owner_expected": noneish(summary.get("recipient_two_owner_expected")),
            "owner_observed": noneish(summary.get("recipient_two_owner_observed")),
            "space_expected": maybe_int(summary.get("recipient_two_space_expected")),
            "space_observed": maybe_int(summary.get("recipient_two_space_observed")),
        }
    if "vote_account" in summary:
        state["vote_account"] = {
            "pubkey": summary.get("vote_account"),
            "pre_authorized_voter": summary.get("pre_authorized_voter"),
            "post_authorized_voter": summary.get("post_authorized_voter"),
            "pre_votes_len": maybe_int(summary.get("pre_votes_len")),
            "post_votes_len": maybe_int(summary.get("post_votes_len")),
            "pre_last_timestamp_slot": maybe_int(summary.get("pre_last_timestamp_slot")),
            "post_last_timestamp_slot": maybe_int(summary.get("post_last_timestamp_slot")),
        }
    return state


def noneish(value: str | None) -> str | None:
    if value is None or value == "" or value == "n/a" or value == "none":
        return None
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iter-dir", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--runner-kind", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--input-family", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    iter_dir = Path(args.iter_dir)
    output_path = Path(args.output) if args.output else iter_dir / "normalized_outcome.json"
    summary_path = iter_dir / "summary.txt"
    bam_log_path = iter_dir / "bam.log"
    fd_log_path = iter_dir / "fd.log"
    operator_log_path = iter_dir / "operator.log"
    rpc_blocks_path = iter_dir / "rpc_blocks.jsonl"
    rpc_signature_statuses_path = iter_dir / "rpc_signature_statuses.jsonl"
    bam_shred_capture_path = iter_dir / "bam_shred_capture.json"

    summary = read_summary(summary_path)
    bam_lines = read_lines(bam_log_path)
    fd_lines = read_lines(fd_log_path)
    operator_lines = read_lines(operator_log_path)
    scenario = parse_scripted_scenario(summary)
    rpc_blocks = parse_rpc_blocks(read_jsonl(rpc_blocks_path))
    rpc_signature_statuses = parse_rpc_signature_statuses(
        read_jsonl(rpc_signature_statuses_path)
    )
    signature_statuses = expected_signature_occurrences(
        scenario.get("expected_signatures", []),
        rpc_blocks,
        rpc_signature_statuses,
        maybe_int(summary.get("start_slot")),
    )
    bam = parse_bam_log(bam_lines)
    bam["batch_result_completeness"] = batch_result_completeness(scenario, bam)

    outcome = {
        "schema": "firebam.live_outcome.v1",
        "target": args.target,
        "runner_kind": args.runner_kind,
        "mode": summary.get("mode", args.mode),
        "input_family": summary.get("input_family", args.input_family),
        "seed": maybe_int(summary.get("seed")),
        "summary": summary,
        "bam": bam,
        "fd": parse_fd_log(fd_lines),
        "bam_shred_capture": read_json(bam_shred_capture_path),
        "scenario": scenario,
        "chain": {
            "rpc_blocks": rpc_blocks,
            "rpc_signature_statuses": rpc_signature_statuses,
            "expected_signature_statuses": signature_statuses,
            "landed_expected_signatures": landed_expected_signatures(signature_statuses),
            "normal_tpu_signature_statuses": normal_tpu_signature_statuses(
                summary,
                rpc_signature_statuses,
            ),
        },
        "operator": parse_operator_log(operator_lines),
        "state": state_from_summary(summary),
        "artifacts": {
            "summary": str(summary_path),
            "bam_log": str(bam_log_path),
            "fd_log": str(fd_log_path),
            "operator_log": str(operator_log_path),
            "rpc_blocks": str(rpc_blocks_path),
            "rpc_signature_statuses": str(rpc_signature_statuses_path),
            "bam_shred_capture": str(bam_shred_capture_path)
            if bam_shred_capture_path.exists()
            else None,
        },
    }

    output_path.write_text(json.dumps(outcome, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
