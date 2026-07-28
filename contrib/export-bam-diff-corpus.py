#!/usr/bin/env python3
"""Export live BAM differential iterations into a portable scenario corpus."""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback is not expected here.
    tomllib = None


def read_summary(path: Path) -> dict[str, str]:
    summary: dict[str, str] = {}
    if not path.exists():
        return summary
    for line in path.read_text(errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        summary[key] = value
    return summary


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        item = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    return item if isinstance(item, dict) else {}


def iter_dirs(root: Path) -> list[Path]:
    dirs = sorted(path for path in root.glob("iter_*") if path.is_dir())
    if dirs:
        return dirs
    if (root / "summary.txt").exists():
        return [root]
    return []


def safe_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    value = value.strip("-")
    return value or "scenario"


def scenario_path(summary: dict[str, str], outcome: dict[str, Any]) -> Path | None:
    scenario = outcome.get("scenario", {})
    source = scenario.get("source") if isinstance(scenario, dict) else None
    candidates = [
        source,
        summary.get("effective_scenario_file"),
        summary.get("scenario_file"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate and candidate != "n/a":
            path = Path(candidate)
            if path.exists():
                return path
    return None


def packet_file_values(data: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for event in data.get("events", []):
        if not isinstance(event, dict):
            continue
        if isinstance(event.get("packets_base64_file"), str):
            values.append(event["packets_base64_file"])
        for batch in event.get("batches", []):
            if isinstance(batch, dict) and isinstance(batch.get("packets_base64_file"), str):
                values.append(batch["packets_base64_file"])
    return values


def copy_scenario_with_packets(source: Path, dest_dir: Path) -> tuple[Path, list[str]]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    packets_dir = dest_dir / "packets"
    packets_dir.mkdir(exist_ok=True)
    scenario_text = source.read_text()

    copied_packets: list[str] = []
    packet_values: list[str] = []
    if tomllib is not None:
        try:
            data = tomllib.loads(scenario_text)
            packet_values = packet_file_values(data)
        except Exception:
            packet_values = []

    replacements: dict[str, str] = {}
    for idx, value in enumerate(packet_values):
        packet_path = Path(value)
        if not packet_path.is_absolute():
            packet_path = source.parent / packet_path
        if not packet_path.exists():
            continue
        dest_name = f"{idx:03d}-{packet_path.name}"
        dest_packet = packets_dir / dest_name
        shutil.copy2(packet_path, dest_packet)
        replacements[value] = f"packets/{dest_name}"
        copied_packets.append(str(dest_packet.relative_to(dest_dir)))

    for old, new in replacements.items():
        scenario_text = scenario_text.replace(old, new)

    dest_scenario = dest_dir / source.name
    dest_scenario.write_text(scenario_text)
    return dest_scenario, copied_packets


def export_iteration(iter_dir: Path, output_root: Path) -> dict[str, Any] | None:
    summary_path = iter_dir / "summary.txt"
    outcome_path = iter_dir / "normalized_outcome.json"
    summary = read_summary(summary_path)
    outcome = load_json(outcome_path)
    source_scenario = scenario_path(summary, outcome)
    if source_scenario is None:
        return None

    mode = summary.get("mode", outcome.get("mode", "unknown"))
    input_family = summary.get("input_family", outcome.get("input_family", "unknown"))
    entry_name = safe_name(f"{iter_dir.name}-{mode}-{input_family}")
    entry_dir = output_root / entry_name
    entry_dir.mkdir(parents=True, exist_ok=True)

    dest_scenario, copied_packets = copy_scenario_with_packets(source_scenario, entry_dir)
    if summary_path.exists():
        shutil.copy2(summary_path, entry_dir / "summary.txt")
    if outcome_path.exists():
        shutil.copy2(outcome_path, entry_dir / "normalized_outcome.json")
    rpc_blocks = iter_dir / "rpc_blocks.jsonl"
    if rpc_blocks.exists():
        shutil.copy2(rpc_blocks, entry_dir / "rpc_blocks.jsonl")

    scenario_info = outcome.get("scenario", {}) if isinstance(outcome.get("scenario"), dict) else {}
    return {
        "id": entry_name,
        "mode": mode,
        "input_family": input_family,
        "system_kind": summary.get("system_kind"),
        "seq_one": summary.get("seq_one"),
        "seq_two": summary.get("seq_two"),
        "source_iteration": str(iter_dir),
        "scenario": str(dest_scenario.relative_to(output_root)),
        "summary": str((entry_dir / "summary.txt").relative_to(output_root)),
        "normalized_outcome": str((entry_dir / "normalized_outcome.json").relative_to(output_root))
        if outcome_path.exists()
        else None,
        "rpc_blocks": str((entry_dir / "rpc_blocks.jsonl").relative_to(output_root))
        if rpc_blocks.exists()
        else None,
        "packet_files": copied_packets,
        "expected_signature_count": len(scenario_info.get("expected_signatures", [])),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-log-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for iter_dir in iter_dirs(args.source_log_dir):
        entry = export_iteration(iter_dir, args.output_dir)
        if entry is not None:
            entries.append(entry)

    manifest = {
        "schema": "firebam.diff_scenario_corpus.v1",
        "source_log_dir": str(args.source_log_dir),
        "entry_count": len(entries),
        "entries": entries,
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(f"exported {len(entries)} scenario(s) to {args.output_dir}")
    return 0 if entries else 2


if __name__ == "__main__":
    raise SystemExit(main())
