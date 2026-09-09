#!/usr/bin/env python3
"""Render a local BAM fddev config with selected network ports."""

from __future__ import annotations

import argparse
from pathlib import Path


def update_section(lines: list[str], section: str, values: dict[str, str]) -> list[str]:
    out: list[str] = []
    in_section = False
    seen = set()

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if in_section:
                for key, value in values.items():
                    if key not in seen:
                        out.append(f"{key} = {value}\n")
                seen.clear()
            in_section = stripped == f"[{section}]"

        if in_section and "=" in line and not stripped.startswith("#"):
            key = line.split("=", 1)[0].strip()
            if key in values:
                out.append(f"{key} = {values[key]}\n")
                seen.add(key)
                continue
        out.append(line)

    if in_section:
        for key, value in values.items():
            if key not in seen:
                out.append(f"{key} = {value}\n")
    return out


def update_or_append_section(
    lines: list[str], section: str, values: dict[str, str]
) -> list[str]:
    marker = f"[{section}]"
    if any(line.strip() == marker for line in lines):
        return update_section(lines, section, values)

    out = list(lines)
    if out and not out[-1].endswith("\n"):
        out[-1] += "\n"
    if out and out[-1].strip():
        out.append("\n")
    out.append(f"{marker}\n")
    for key, value in values.items():
        out.append(f"{key} = {value}\n")
    return out


def quoted(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bam-url", required=True)
    parser.add_argument("--bundle-url")
    parser.add_argument("--rpc-port", type=int, required=True)
    parser.add_argument("--gossip-port", type=int, required=True)
    parser.add_argument("--regular-tpu-port", type=int, required=True)
    parser.add_argument("--quic-tpu-port", type=int, required=True)
    parser.add_argument("--shred-listen-port", type=int, required=True)
    parser.add_argument("--gui-port", type=int)
    parser.add_argument("--layout-affinity")
    parser.add_argument("--agave-affinity")
    parser.add_argument("--genesis-hashes-per-tick", type=int)
    parser.add_argument("--genesis-target-tick-duration-micros", type=int)
    parser.add_argument("--genesis-ticks-per-slot", type=int)
    parser.add_argument("--paths-base")
    parser.add_argument("--paths-genesis")
    parser.add_argument("--local-snapshot-mode", action="store_true")
    parser.add_argument("--gossip-entrypoint")
    parser.add_argument("--expected-shred-version", type=int)
    args = parser.parse_args()

    if args.local_snapshot_mode and not args.gossip_entrypoint:
        parser.error("--local-snapshot-mode requires --gossip-entrypoint")
    if args.local_snapshot_mode and not args.expected_shred_version:
        parser.error("--local-snapshot-mode requires --expected-shred-version")

    lines = args.template.read_text().splitlines(keepends=True)
    lines = update_section(lines, "tiles.bam", {"url": quoted(args.bam_url)})
    if args.bundle_url:
        lines = update_section(lines, "tiles.bundle", {"url": quoted(args.bundle_url)})
    lines = update_section(lines, "rpc", {"port": str(args.rpc_port)})
    lines = update_section(lines, "tiles.rpc", {"rpc_listen_port": str(args.rpc_port)})
    gossip_values = {"port": str(args.gossip_port)}
    if args.gossip_entrypoint:
        gossip_values["entrypoints"] = f"[{quoted(args.gossip_entrypoint)}]"
    lines = update_section(lines, "gossip", gossip_values)
    lines = update_section(
        lines,
        "tiles.quic",
        {
            "regular_transaction_listen_port": str(args.regular_tpu_port),
            "quic_transaction_listen_port": str(args.quic_tpu_port),
        },
    )
    lines = update_section(
        lines, "tiles.shred", {"shred_listen_port": str(args.shred_listen_port)}
    )
    if args.gui_port is not None:
        lines = update_section(
            lines,
            "tiles.gui",
            {
                "enabled": "true",
                "gui_listen_address": quoted("127.0.0.1"),
                "gui_listen_port": str(args.gui_port),
            },
        )
    layout_values: dict[str, str] = {}
    if args.layout_affinity:
        layout_values["affinity"] = quoted(args.layout_affinity)
    if args.agave_affinity:
        layout_values["agave_affinity"] = quoted(args.agave_affinity)
    if layout_values:
        lines = update_section(lines, "layout", layout_values)
    path_values: dict[str, str] = {}
    if args.paths_base:
        path_values["base"] = quoted(args.paths_base)
    if args.paths_genesis:
        path_values["genesis"] = quoted(args.paths_genesis)
    if path_values:
        lines = update_section(lines, "paths", path_values)
    genesis_values: dict[str, str] = {}
    if args.genesis_hashes_per_tick is not None:
        genesis_values["hashes_per_tick"] = str(args.genesis_hashes_per_tick)
    if args.genesis_target_tick_duration_micros is not None:
        genesis_values["target_tick_duration_micros"] = str(
            args.genesis_target_tick_duration_micros
        )
    if args.genesis_ticks_per_slot is not None:
        genesis_values["ticks_per_slot"] = str(args.genesis_ticks_per_slot)
    if genesis_values:
        lines = update_section(lines, "development.genesis", genesis_values)
    if args.local_snapshot_mode:
        lines = update_or_append_section(
            lines,
            "snapshots",
            {
                "incremental_snapshots": "false",
                "genesis_download": "false",
            },
        )
        lines = update_or_append_section(
            lines,
            "snapshots.sources.gossip",
            {
                "allow_any": "false",
                "allow_list": "[]",
            },
        )
        lines = update_or_append_section(
            lines,
            "consensus",
            {
                "wait_for_vote_to_start_leader": "false",
                "expected_shred_version": str(args.expected_shred_version),
            },
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
