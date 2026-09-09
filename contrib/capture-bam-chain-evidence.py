#!/usr/bin/env python3
"""Capture expected BAM transaction signature status evidence from JSON RPC."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def load_normalizer() -> Any:
    path = Path(__file__).with_name("normalize-bam-outcome.py")
    spec = importlib.util.spec_from_file_location("bam_outcome_normalizer", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def rpc_call(url: str, method: str, params: list[Any]) -> dict[str, Any]:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=2) as response:
            data = response.read()
    except (OSError, urllib.error.URLError) as err:
        return {"error": {"message": str(err)}}
    try:
        decoded = json.loads(data)
    except json.JSONDecodeError as err:
        return {"error": {"message": f"invalid_json: {err}"}}
    return decoded if isinstance(decoded, dict) else {"error": {"message": "non_object"}}


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def status_record(
    signature: str,
    status: Any,
    poll_count: int,
    rpc_error: Any | None = None,
) -> dict[str, Any]:
    if isinstance(status, dict):
        return {
            "signature": signature,
            "found": True,
            "slot": status.get("slot"),
            "err": status.get("err"),
            "confirmation_status": status.get("confirmationStatus"),
            "confirmations": status.get("confirmations"),
            "poll_count": poll_count,
            "rpc_error": rpc_error,
        }
    return {
        "signature": signature,
        "found": False,
        "slot": None,
        "err": None,
        "confirmation_status": None,
        "confirmations": None,
        "poll_count": poll_count,
        "rpc_error": rpc_error,
    }


def capture_statuses(
    rpc_url: str,
    signatures: list[str],
    timeout_secs: float,
    batch_size: int,
) -> list[dict[str, Any]]:
    if not signatures:
        return []

    latest: dict[str, dict[str, Any]] = {
        signature: status_record(signature, None, 0) for signature in signatures
    }
    remaining = set(signatures)
    deadline = time.monotonic() + max(0.0, timeout_secs)
    poll_count = 0

    while True:
        poll_count += 1
        for batch in chunks(signatures, max(1, batch_size)):
            response = rpc_call(
                rpc_url,
                "getSignatureStatuses",
                [batch, {"searchTransactionHistory": True}],
            )
            error = response.get("error") if isinstance(response, dict) else None
            values = None
            result = response.get("result") if isinstance(response, dict) else None
            if isinstance(result, dict) and isinstance(result.get("value"), list):
                values = result["value"]

            if values is None:
                for signature in batch:
                    latest[signature] = status_record(
                        signature,
                        None,
                        poll_count,
                        error if error is not None else {"message": "missing_result_value"},
                    )
                continue

            for signature, status in zip(batch, values):
                latest[signature] = status_record(signature, status, poll_count, error)
                if isinstance(status, dict):
                    remaining.discard(signature)

        if not remaining or time.monotonic() >= deadline:
            break
        time.sleep(0.5)

    return [latest[signature] for signature in signatures]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario-file", required=True, type=Path)
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--extra-packet-file", action="append", default=[], type=Path)
    parser.add_argument("--timeout-secs", type=float, default=8.0)
    parser.add_argument("--batch-size", type=int, default=256)
    args = parser.parse_args()

    normalizer = load_normalizer()
    scenario = normalizer.parse_scripted_scenario(
        {"scenario_file": str(args.scenario_file.resolve())}
    )

    signatures: list[str] = []
    seen: set[str] = set()
    for item in scenario.get("expected_signatures", []):
        signature = item.get("signature")
        if isinstance(signature, str) and signature not in seen:
            seen.add(signature)
            signatures.append(signature)
    for packet_file in args.extra_packet_file:
        for signature in normalizer.packet_file_signatures(packet_file):
            if isinstance(signature, str) and signature not in seen:
                seen.add(signature)
                signatures.append(signature)

    records = capture_statuses(
        args.rpc_url,
        signatures,
        args.timeout_secs,
        args.batch_size,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records)
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(1)
    except Exception as err:
        print(f"error: {err}", file=sys.stderr)
        raise SystemExit(1)
