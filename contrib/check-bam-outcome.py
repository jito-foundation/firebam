#!/usr/bin/env python3
"""Check single-run BAM outcome invariants.

This is the source/solo counterpart to compare-bam-outcomes.py.  It reuses the
same projection helpers, but checks one normalized outcome instead of comparing
two validators.  The intent is to make fullfd-only queue/backpressure soaks fail
on the same production invariants that paired differential runs care about.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def load_compare_module() -> Any:
    path = Path(__file__).with_name("compare-bam-outcomes.py")
    spec = importlib.util.spec_from_file_location("compare_bam_outcomes", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_entry(passed: bool | None, details: Any = None) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "checked": passed is not None,
        "passed": passed,
    }
    if details is not None:
        entry["details"] = details
    return entry


def bool_pass(value: Any) -> bool:
    return value is not False


QUARANTINE_MODES = {
    "quarantine_disable_enable_queue_inflight",
    "quarantine_url_churn_queue_inflight",
}


def stale_generation_quarantine_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    mode = outcome.get("mode")
    if mode not in QUARANTINE_MODES:
        return {
            "checked": False,
            "passed": True,
            "reason": "not_a_stale_generation_quarantine_mode",
        }

    bam = outcome.get("bam", {})
    fd = outcome.get("fd", {})
    scenario = outcome.get("scenario", {})
    sent_batches = bam.get("scripted_sent_batches")
    if not isinstance(sent_batches, list) or not sent_batches:
        sent_batches = scenario.get("batches", [])
    if not isinstance(sent_batches, list):
        sent_batches = []

    sent_seq_ids = {
        item.get("seq_id")
        for item in sent_batches
        if isinstance(item, dict) and isinstance(item.get("seq_id"), int)
    }
    batch_results = bam.get("batch_results", [])
    if not isinstance(batch_results, list):
        batch_results = []
    scheduler_streams = bam.get("scheduler_streams", [])
    if not isinstance(scheduler_streams, list):
        scheduler_streams = []
    stale_drops = fd.get("stale_bam_result_drops", [])
    if not isinstance(stale_drops, list):
        stale_drops = []

    post_change_leaks: list[dict[str, Any]] = []
    for result in batch_results:
        if not isinstance(result, dict):
            continue
        seq_id = result.get("seq_id")
        conn = result.get("conn")
        if isinstance(seq_id, int) and seq_id in sent_seq_ids and isinstance(conn, int) and conn >= 2:
            post_change_leaks.append(
                {
                    "seq_id": seq_id,
                    "conn": conn,
                    "status": result.get("status"),
                    "line": result.get("line"),
                }
            )

    invalid_drops: list[dict[str, Any]] = []
    for drop in stale_drops:
        if not isinstance(drop, dict):
            continue
        seq_id = drop.get("seq_id")
        result_gen = drop.get("result_gen")
        current_gen = drop.get("current_gen")
        if seq_id not in sent_seq_ids or result_gen == current_gen:
            invalid_drops.append(drop)

    expected_count = len(sent_batches)
    actual_count = len(batch_results)
    terminal_results_cut_off = actual_count < expected_count
    terminal_results_completed_before_change = actual_count == expected_count
    no_extra_terminal_results = actual_count <= expected_count
    checks = {
        "sent_batches": expected_count > 0,
        "scheduler_reconnected": len(scheduler_streams) >= 2,
        "terminal_results_safe": terminal_results_cut_off or terminal_results_completed_before_change,
        "no_extra_terminal_results": no_extra_terminal_results,
        "no_post_change_leaks": not post_change_leaks,
        "stale_drop_generations_valid_when_present": not invalid_drops,
    }

    return {
        "checked": True,
        "passed": all(checks.values()),
        "mode": mode,
        "checks": checks,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "terminal_results_cut_off": terminal_results_cut_off,
        "terminal_results_completed_before_change": terminal_results_completed_before_change,
        "scheduler_stream_count": len(scheduler_streams),
        "stale_drop_count": len(stale_drops),
        "stale_drops": stale_drops,
        "post_change_leaks": post_change_leaks,
        "invalid_drops": invalid_drops,
    }


def build_report(
    outcome: dict[str, Any],
    compare: Any,
    *,
    require_chain_evidence: bool,
    allow_m42_duplicate_terminal: bool,
) -> dict[str, Any]:
    requires_chain = compare.requires_chain_oracle(outcome)
    has_chain = compare.chain_evidence_capture_present(outcome)
    chain_checks_enabled = require_chain_evidence and requires_chain and has_chain

    batch_result_completeness = compare.batch_result_completeness_projection(outcome)
    terminal_result_integrity = compare.terminal_result_integrity_projection(outcome)
    result_attribution_integrity = compare.result_attribution_integrity_projection(
        outcome
    )
    result_identity_integrity = compare.result_identity_integrity_projection(outcome)
    result_execution_semantics = compare.result_execution_semantics_projection(outcome)
    protocol = compare.protocol_projection(outcome)
    state_consistency = compare.state_consistency_projection(outcome)
    fee_config_observability = compare.fee_config_observability_projection(outcome)
    fee_config_snapshot = compare.fee_config_snapshot_projection(outcome)
    schedule_boundary_validity = compare.schedule_boundary_validity_projection(outcome)
    normal_tpu = compare.normal_tpu_signature_projection(outcome)
    contact_info = compare.contact_info_projection(outcome)
    bam_shred_capture = outcome.get("bam_shred_capture", {})
    if not isinstance(bam_shred_capture, dict):
        bam_shred_capture = {}
    bam_shred_capture_expected = bool(outcome.get("summary", {}).get("bam_shred_capture"))

    checks: dict[str, dict[str, Any]] = {
        "batch_result_completeness": check_entry(
            compare.batch_result_completeness_pass(outcome),
            batch_result_completeness,
        ),
        "terminal_result_integrity": check_entry(
            compare.terminal_result_integrity_pass(outcome),
            terminal_result_integrity,
        ),
        "result_attribution_integrity": check_entry(
            compare.result_attribution_integrity_pass(outcome),
            result_attribution_integrity,
        ),
        "result_identity_integrity": check_entry(
            compare.result_identity_integrity_pass(outcome),
            result_identity_integrity,
        ),
        "result_execution_semantics": check_entry(
            compare.result_execution_semantics_pass(outcome),
            result_execution_semantics,
        ),
        "protocol": check_entry(compare.protocol_validity_pass(outcome), protocol),
        "state_consistency": check_entry(
            compare.state_consistency_pass(outcome),
            state_consistency,
        ),
        "fee_config_observability": check_entry(
            compare.fee_config_observability_pass(outcome),
            fee_config_observability,
        ),
        "fee_config_snapshot": check_entry(
            compare.fee_config_snapshot_pass(outcome),
            fee_config_snapshot,
        ),
        "schedule_boundary_validity": check_entry(
            compare.schedule_boundary_validity_pass(outcome),
            schedule_boundary_validity,
        ),
        "normal_tpu_signature_consistency": check_entry(
            compare.normal_tpu_signature_pass(outcome),
            normal_tpu,
        ),
        "contact_info_consistency": check_entry(
            compare.contact_info_consistency_pass(outcome),
            contact_info,
        ),
        "bam_shred_delivery": check_entry(
            None
            if not bam_shred_capture_expected
            else int(bam_shred_capture.get("packet_count") or 0) > 0,
            bam_shred_capture or {"reason": "not_requested"},
        ),
    }

    stale_generation_quarantine = stale_generation_quarantine_projection(outcome)
    if stale_generation_quarantine.get("checked"):
        checks["stale_generation_quarantine"] = check_entry(
            stale_generation_quarantine.get("passed") is True,
            stale_generation_quarantine,
        )
        if stale_generation_quarantine.get("terminal_results_cut_off"):
            checks["batch_result_completeness"] = check_entry(
                True,
                {
                    **batch_result_completeness,
                    "passed": True,
                    "suppressed": "quarantine mode allows generation churn to cut off old-generation in-flight results",
                },
            )
            checks["state_consistency"] = check_entry(
                True,
                {
                    **state_consistency,
                    "passed": True,
                    "suppressed": "quarantine mode does not assert committed balance deltas for cut-off old-generation work",
                },
            )

    if allow_m42_duplicate_terminal:
        issues = terminal_result_integrity.get("issues", [])
        if (
            isinstance(issues, list)
            and issues
            and all(
                isinstance(issue, dict)
                and issue.get("reason")
                == "duplicate_terminal_result_without_expected_duplicate"
                for issue in issues
            )
        ):
            checks["terminal_result_integrity"] = check_entry(
                True,
                {
                    **terminal_result_integrity,
                    "passed": True,
                    "suppressed": "M-42 duplicate-seq terminal fixture allowed for this operator restart run",
                },
            )

    if require_chain_evidence and requires_chain:
        checks["chain_evidence_capture"] = check_entry(
            has_chain,
            {
                "requires_chain_oracle": requires_chain,
                "chain_evidence_present": has_chain,
            },
        )
    else:
        checks["chain_evidence_capture"] = check_entry(
            None,
            {
                "requires_chain_oracle": requires_chain,
                "chain_evidence_present": has_chain,
                "reason": "not_required_for_single_outcome_check",
            },
        )

    if chain_checks_enabled:
        checks["atomic_result_semantics"] = check_entry(
            compare.atomic_result_semantics_pass(outcome),
            compare.atomic_result_semantics_projection(outcome),
        )
        checks["bam_execution_order"] = check_entry(
            compare.bam_execution_order_pass(outcome),
            compare.bam_execution_order_projection(outcome),
        )
        checks["chain_consistency"] = check_entry(
            compare.chain_consistency_pass(outcome),
            compare.chain_consistency_projection(outcome),
        )
    else:
        checks["atomic_result_semantics"] = check_entry(
            None,
            {
                "requires_chain_oracle": requires_chain,
                "chain_evidence_present": has_chain,
                "reason": "chain_checks_not_required"
                if not require_chain_evidence
                else "chain_evidence_unavailable",
            },
        )
        checks["bam_execution_order"] = check_entry(
            None,
            {
                "requires_chain_oracle": requires_chain,
                "chain_evidence_present": has_chain,
                "reason": "chain_checks_not_required"
                if not require_chain_evidence
                else "chain_evidence_unavailable",
            },
        )
        checks["chain_consistency"] = check_entry(
            None,
            {
                "requires_chain_oracle": requires_chain,
                "chain_evidence_present": has_chain,
                "reason": "chain_checks_not_required"
                if not require_chain_evidence
                else "chain_evidence_unavailable",
            },
        )

    failed = [
        name
        for name, entry in checks.items()
        if entry.get("checked") is True and not bool_pass(entry.get("passed"))
    ]
    return {
        "schema": "firebam.live_outcome_self_check.v1",
        "target": outcome.get("target"),
        "runner_kind": outcome.get("runner_kind"),
        "mode": outcome.get("mode"),
        "input_family": outcome.get("input_family"),
        "passed": not failed,
        "failed_checks": failed,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("outcome", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--require-chain-evidence",
        action="store_true",
        help="Fail if a chain-aware outcome lacks RPC block/signature evidence.",
    )
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--allow-m42-duplicate-terminal",
        action="store_true",
        help="Allow duplicate terminal results caused by an explicit duplicate-seq fixture.",
    )
    args = parser.parse_args()

    compare = load_compare_module()
    outcome = load_json(args.outcome)
    report = build_report(
        outcome,
        compare,
        require_chain_evidence=args.require_chain_evidence,
        allow_m42_duplicate_terminal=args.allow_m42_duplicate_terminal,
    )

    output = args.output or args.outcome.with_name("outcome_self_check.json")
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if not args.quiet:
        status = "pass" if report["passed"] else "fail"
        print(
            f"outcome self-check {status}: "
            f"{report.get('target')} {report.get('mode')} -> {output}"
        )
        if not report["passed"]:
            print("failed checks: " + ",".join(report["failed_checks"]), file=sys.stderr)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
