#!/usr/bin/env python3
"""Small UDP packet counter for local BAM shred delivery checks."""

from __future__ import annotations

import argparse
import json
import signal
import socket
import time
from pathlib import Path


stop_requested = False


def request_stop(_signum: int, _frame: object) -> None:
    global stop_requested
    stop_requested = True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path)
    parser.add_argument("--timeout-secs", type=float, default=0.0)
    parser.add_argument("--max-packets", type=int, default=0)
    parser.add_argument("--sample-limit", type=int, default=16)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.ready_file:
        args.ready_file.parent.mkdir(parents=True, exist_ok=True)

    started = time.time()
    last_packet_ts: float | None = None
    first_packet_ts: float | None = None
    packet_count = 0
    byte_count = 0
    min_size: int | None = None
    max_size = 0
    samples: list[dict[str, int | float | str]] = []
    bind_error: str | None = None

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((args.bind, args.port))
            sock.settimeout(0.2)
            if args.ready_file:
                args.ready_file.write_text("ready\n")

            while not stop_requested:
                if args.timeout_secs > 0 and time.time() - started >= args.timeout_secs:
                    break
                if args.max_packets > 0 and packet_count >= args.max_packets:
                    break
                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                now = time.time()
                size = len(data)
                packet_count += 1
                byte_count += size
                min_size = size if min_size is None else min(min_size, size)
                max_size = max(max_size, size)
                if first_packet_ts is None:
                    first_packet_ts = now
                last_packet_ts = now
                if len(samples) < args.sample_limit:
                    samples.append(
                        {
                            "index": packet_count,
                            "size": size,
                            "from": f"{addr[0]}:{addr[1]}",
                            "offset_secs": round(now - started, 6),
                        }
                    )
    except OSError as exc:
        bind_error = str(exc)
        if args.ready_file:
            args.ready_file.write_text(f"error: {exc}\n")

    finished = time.time()
    report = {
        "schema": "firebam.udp_packet_capture.v1",
        "bind": args.bind,
        "port": args.port,
        "started_unix": started,
        "finished_unix": finished,
        "duration_secs": round(finished - started, 6),
        "packet_count": packet_count,
        "byte_count": byte_count,
        "min_packet_size": min_size,
        "max_packet_size": max_size,
        "first_packet_offset_secs": None
        if first_packet_ts is None
        else round(first_packet_ts - started, 6),
        "last_packet_offset_secs": None
        if last_packet_ts is None
        else round(last_packet_ts - started, 6),
        "samples": samples,
        "bind_error": bind_error,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 1 if bind_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
