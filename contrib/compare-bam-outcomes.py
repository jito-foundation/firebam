#!/usr/bin/env python3
"""Compare two normalized BAM live-run outcomes."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def batch_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for result in outcome.get("bam", {}).get("batch_results", []):
        projected.append(
            {
                "seq_id": result.get("seq_id"),
                "status": result.get("status"),
                "txns": result.get("txns"),
                "reason": result.get("reason"),
                "index": result.get("index"),
                "detail": result.get("detail"),
            }
        )
    return sorted(
        projected,
        key=lambda item: (
            _sort_value(item.get("seq_id")),
            _sort_value(item.get("txns")),
            _sort_value(item.get("index")),
            str(item.get("status")),
            str(item.get("reason")),
            str(item.get("detail")),
        ),
    )


def batch_result_order_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for result in sorted(
        outcome.get("bam", {}).get("batch_results", []),
        key=lambda item: (
            _sort_value(item.get("line")),
            _sort_value(item.get("seq_id")),
            str(item.get("status")),
        ),
    ):
        projected.append(
            {
                "seq_id": result.get("seq_id"),
                "status": result.get("status"),
                "txns": result.get("txns"),
                "reason": result.get("reason"),
                "index": result.get("index"),
                "detail": result.get("detail"),
            }
        )
    return projected


def tx_projection(
    outcome: dict[str, Any],
    include_loaded_accounts_data_size: bool = True,
    include_cus_consumed: bool = True,
    include_feepayer_balance_delta: bool = True,
) -> list[dict[str, Any]]:
    projected = []
    payer_initial = numeric(outcome.get("state", {}).get("payer", {}).get("initial"))
    for result in outcome.get("bam", {}).get("batch_result_txs", []):
        item = {
            "seq_id": result.get("seq_id"),
            "tx_index": result.get("tx_index"),
            "execution_success": result.get("execution_success"),
        }
        feepayer_balance = numeric(result.get("feepayer_balance_lamports"))
        if (
            include_feepayer_balance_delta
            and payer_initial is not None
            and feepayer_balance is not None
        ):
            item["feepayer_balance_delta_lamports"] = feepayer_balance - payer_initial
        if include_cus_consumed:
            item["cus_consumed"] = result.get("cus_consumed")
        if include_loaded_accounts_data_size:
            item["loaded_accounts_data_size"] = result.get("loaded_accounts_data_size")
        projected.append(item)
    return sorted(
        projected,
        key=lambda item: (
            _sort_value(item.get("seq_id")),
            _sort_value(item.get("tx_index")),
            str(item.get("execution_success")),
            _sort_value(item.get("feepayer_balance_delta_lamports")),
            _sort_value(item.get("cus_consumed")),
            _sort_value(item.get("loaded_accounts_data_size")),
        ),
    )


def _projection_excluding_seq_ids(
    projection: list[dict[str, Any]], excluded_seq_ids: set[Any]
) -> list[dict[str, Any]]:
    return [item for item in projection if item.get("seq_id") not in excluded_seq_ids]


def batch_projection_excluding(
    outcome: dict[str, Any], excluded_seq_ids: set[Any]
) -> list[dict[str, Any]]:
    return _projection_excluding_seq_ids(batch_projection(outcome), excluded_seq_ids)


def batch_result_order_projection_excluding(
    outcome: dict[str, Any], excluded_seq_ids: set[Any]
) -> list[dict[str, Any]]:
    return _projection_excluding_seq_ids(
        batch_result_order_projection(outcome), excluded_seq_ids
    )


def expected_signature_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("scenario", {}).get("expected_signatures", []):
        projected.append(
            {
                "seq_id": item.get("seq_id"),
                "tx_index": item.get("tx_index"),
                "batch_ordinal": item.get("batch_ordinal"),
            }
        )
    return projected


def landed_signature_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("chain", {}).get("landed_expected_signatures", []):
        projected.append(
            {
                "seq_id": item.get("seq_id"),
                "tx_index": item.get("tx_index"),
                "batch_ordinal": item.get("batch_ordinal"),
            }
        )
    return sorted(
        projected,
        key=lambda item: (
            _sort_value(item.get("batch_ordinal")),
            _sort_value(item.get("seq_id")),
            _sort_value(item.get("tx_index")),
        ),
    )


def block_landing_order_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("chain", {}).get("landed_expected_signatures", []):
        if item.get("source") != "rpc_block" or item.get("landing_ordinal") is None:
            continue
        projected.append(
            {
                "seq_id": item.get("seq_id"),
                "tx_index": item.get("tx_index"),
                "batch_ordinal": item.get("batch_ordinal"),
                "landing_ordinal": item.get("landing_ordinal"),
            }
        )
    return sorted(projected, key=lambda item: _sort_value(item.get("landing_ordinal")))


def bam_execution_order_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    observed = block_landing_order_projection(outcome)
    landed = landed_signature_projection(outcome)
    complete = len(observed) == len(landed)
    issues: list[dict[str, Any]] = []

    if len(observed) < 2:
        return {
            "checked": False,
            "passed": True,
            "reason": "insufficient_rpc_block_landed_signatures",
            "block_landing_order_complete": complete,
            "observed": observed,
        }

    previous: dict[str, Any] | None = None
    previous_key: tuple[int, int] | None = None
    for item in observed:
        batch_ordinal = numeric(item.get("batch_ordinal"))
        tx_index = numeric(item.get("tx_index"))
        if batch_ordinal is None or tx_index is None:
            issues.append(
                {
                    "entry": item,
                    "reason": "missing_batch_identity",
                }
            )
            continue

        key = (batch_ordinal, tx_index)
        if previous_key is not None and key < previous_key:
            issues.append(
                {
                    "previous": previous,
                    "current": item,
                    "reason": "landed_signature_order_inverts_bam_batch_order",
                }
            )
        previous = item
        previous_key = key

    return {
        "checked": True,
        "passed": not issues,
        "block_landing_order_complete": complete,
        "observed": observed,
        "issues": issues,
    }


def bam_execution_order_pass(outcome: dict[str, Any]) -> bool:
    return bam_execution_order_projection(outcome).get("passed") is not False


def signature_status_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("chain", {}).get("expected_signature_statuses", []):
        projected.append(
            {
                "seq_id": item.get("seq_id"),
                "tx_index": item.get("tx_index"),
                "batch_ordinal": item.get("batch_ordinal"),
                "status": item.get("status"),
                "attributable": item.get("attributable"),
                "duplicate_expected_signature": item.get("duplicate_expected_signature"),
                "occurrence_count": item.get("occurrence_count"),
            }
        )
    return projected


def normal_tpu_signature_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    evidence = outcome.get("chain", {}).get("normal_tpu_signature_statuses", {})
    if not isinstance(evidence, dict) or not evidence:
        return {}
    return {
        "checked": evidence.get("checked"),
        "expected_landed": evidence.get("expected_landed"),
        "passed": evidence.get("passed"),
        "statuses": [
            {
                "ordinal": item.get("ordinal"),
                "found": item.get("found"),
                "err": item.get("err"),
                "rpc_error": item.get("rpc_error"),
            }
            for item in evidence.get("statuses", [])
            if isinstance(item, dict)
        ],
    }


def normal_tpu_signature_pass(outcome: dict[str, Any]) -> bool:
    if outcome.get("mode") == "source_mix_duplicate_tpu_after_bam":
        return True
    evidence = outcome.get("chain", {}).get("normal_tpu_signature_statuses", {})
    if not isinstance(evidence, dict) or not evidence:
        return True
    if evidence.get("expected_landed") is True and evidence.get("passed") is False:
        recipient_two = outcome.get("state", {}).get("recipient_two", {})
        expected = numeric(recipient_two.get("expected"))
        observed = numeric(recipient_two.get("observed"))
        if expected is not None and observed == expected:
            return True
    return evidence.get("passed") is not False


def rpc_block_capture_present(outcome: dict[str, Any]) -> bool:
    return bool(outcome.get("chain", {}).get("rpc_blocks", []))


def chain_evidence_capture_present(outcome: dict[str, Any]) -> bool:
    chain = outcome.get("chain", {})
    return bool(chain.get("rpc_blocks", [])) or bool(chain.get("rpc_signature_statuses", []))


def requires_chain_oracle(outcome: dict[str, Any]) -> bool:
    return outcome.get("runner_kind") != "helper" and bool(expected_signature_projection(outcome))


def helper_comparison(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left.get("runner_kind") == "helper" or right.get("runner_kind") == "helper"


def is_queue_burst_mode_name(mode: Any) -> bool:
    return mode in {
        "queue_burst_reconnect",
        "queue_burst64_reconnect",
        "queue_burst64_leader_plus1_reconnect",
        "schedule_boundary_jitter",
        "queue_reconnect_timing_jitter",
        "queue_burst_multi_reconnect",
        "queue_burst128_reconnect",
        "queue_burst256_reconnect",
        "queue_burst512_reconnect",
        "queue_burst_leader_reconnect",
        "queue_burst64_leader_reconnect",
        "bam_fee_queue_burst_reconnect",
        "source_mix_queue_burst_reconnect",
        "source_mix_queue_burst_multi_reconnect",
        "disable_enable_queue_burst_reconnect",
        "bam_fee_source_mix_queue_burst_reconnect",
        "bam_fee_config_refresh_queue_burst",
        "bam_fee_config_refresh_source_mix_queue_burst",
        "bam_fee_config_midqueue_refresh",
        "bam_fee_config_midqueue_source_mix_queue_burst",
        "bam_fee_config_midqueue_source_mix_multi_reconnect",
    }


def duplicate_result_seq_ids(outcome: dict[str, Any]) -> set[Any]:
    counts: dict[Any, int] = {}
    for result in outcome.get("bam", {}).get("batch_results", []):
        seq_id = result.get("seq_id")
        counts[seq_id] = counts.get(seq_id, 0) + 1
    return {seq_id for seq_id, count in counts.items() if count > 1}


def batch_result_completeness_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    completeness = outcome.get("bam", {}).get("batch_result_completeness", {})
    if not isinstance(completeness, dict):
        return {"checked": False, "passed": True}
    return {
        "checked": completeness.get("checked"),
        "passed": completeness.get("passed"),
        "expected_source": completeness.get("expected_source"),
        "expected_count": completeness.get("expected_count"),
        "actual_count": completeness.get("actual_count"),
        "missing": completeness.get("missing", []),
        "extra": completeness.get("extra", []),
    }


def batch_result_completeness_pass(outcome: dict[str, Any]) -> bool:
    completeness = batch_result_completeness_projection(outcome)
    return completeness.get("passed") is not False


def _seq_counts(items: list[dict[str, Any]]) -> dict[Any, int]:
    counts: dict[Any, int] = {}
    for item in items:
        seq_id = item.get("seq_id")
        counts[seq_id] = counts.get(seq_id, 0) + 1
    return counts


def _expected_batch_counts(outcome: dict[str, Any]) -> dict[Any, int]:
    sent_batches = outcome.get("bam", {}).get("scripted_sent_batches", [])
    if sent_batches:
        return _seq_counts(sent_batches)
    return _seq_counts(scenario_batch_projection(outcome))


def terminal_result_integrity_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    batch_results = outcome.get("bam", {}).get("batch_results", [])
    batch_result_txs = outcome.get("bam", {}).get("batch_result_txs", [])
    result_counts = _seq_counts(batch_results)
    expected_counts = _expected_batch_counts(outcome)

    txs_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for tx in batch_result_txs:
        txs_by_seq.setdefault(tx.get("seq_id"), []).append(tx)
    ambiguous_result_seq_ids = {
        seq_id for seq_id, count in result_counts.items() if count > 1
    }
    committed_result_seq_ids = {
        result.get("seq_id")
        for result in batch_results
        if result.get("status") == "committed"
    }

    issues: list[dict[str, Any]] = []
    for seq_id, actual_count in sorted(result_counts.items(), key=lambda item: _sort_value(item[0])):
        expected_count = expected_counts.get(seq_id)
        if expected_count in (None, 0) and actual_count > 1:
            issues.append(
                {
                    "seq_id": seq_id,
                    "actual": actual_count,
                    "reason": "duplicate_terminal_result_without_expected_duplicate",
                }
            )
        elif expected_count == 1 and actual_count != 1:
            issues.append(
                {
                    "seq_id": seq_id,
                    "expected": expected_count,
                    "actual": actual_count,
                    "reason": "unexpected_terminal_result_count",
                }
            )

    unique_result_seq_ids = {seq_id for seq_id, count in result_counts.items() if count == 1}
    for result in batch_results:
        seq_id = result.get("seq_id")
        status = result.get("status")
        seq_txs = txs_by_seq.get(seq_id, [])

        if status == "committed":
            txns = numeric(result.get("txns"))
            if txns is None or txns <= 0:
                issues.append(
                    {
                        "seq_id": seq_id,
                        "status": status,
                        "txns": result.get("txns"),
                        "reason": "committed_result_missing_positive_txn_count",
                    }
                )
            if seq_id in unique_result_seq_ids and txns is not None and len(seq_txs) != txns:
                issues.append(
                    {
                        "seq_id": seq_id,
                        "status": status,
                        "txns": txns,
                        "tx_result_count": len(seq_txs),
                        "reason": "committed_result_tx_count_mismatch",
                    }
                )
            if result.get("reason") is not None or result.get("detail") is not None:
                issues.append(
                    {
                        "seq_id": seq_id,
                        "status": status,
                        "reason": "committed_result_has_failure_fields",
                    }
                )
        elif status == "not_committed":
            if (
                seq_txs
                and (
                    seq_id not in ambiguous_result_seq_ids
                    or seq_id not in committed_result_seq_ids
                )
            ):
                issues.append(
                    {
                        "seq_id": seq_id,
                        "status": status,
                        "tx_result_count": len(seq_txs),
                        "reason": "not_committed_result_has_tx_results",
                    }
                )
            if result.get("reason") in (None, ""):
                issues.append(
                    {
                        "seq_id": seq_id,
                        "status": status,
                        "reason": "not_committed_result_missing_reason",
                    }
                )
        elif status != "missing_result":
            issues.append(
                {
                    "seq_id": seq_id,
                    "status": status,
                    "reason": "unknown_terminal_result_status",
                }
            )

    return {
        "checked": bool(batch_results),
        "passed": not issues,
        "issues": issues,
    }


def terminal_result_integrity_pass(outcome: dict[str, Any]) -> bool:
    return terminal_result_integrity_projection(outcome).get("passed") is not False


def result_attribution_integrity_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    batch_results = outcome.get("bam", {}).get("batch_results", [])
    batch_result_txs = outcome.get("bam", {}).get("batch_result_txs", [])
    scenario_batches = scenario_batch_projection(outcome)
    batches_by_seq = _items_by_seq(scenario_batches)
    results_by_seq = _items_by_seq(batch_results)
    txs_by_seq = _items_by_seq(batch_result_txs)

    issues: list[dict[str, Any]] = []
    entries: dict[str, Any] = {}
    for seq_id in sorted(set(results_by_seq) | set(txs_by_seq), key=_sort_value):
        key = str(seq_id)
        batches = batches_by_seq.get(seq_id, [])
        results = results_by_seq.get(seq_id, [])
        txs = txs_by_seq.get(seq_id, [])
        entry = {
            "seq_id": seq_id,
            "checked": False,
            "scenario_batch_count": len(batches),
            "result_count": len(results),
            "tx_result_count": len(txs),
            "passed": True,
        }
        entries[key] = entry

        if len(batches) != 1 or len(results) != 1:
            entry["reason"] = "ambiguous_seq_id"
            continue

        batch = batches[0]
        result = results[0]
        packet_count = numeric(batch.get("packet_count"))
        txns = numeric(result.get("txns"))
        entry.update(
            {
                "checked": packet_count is not None,
                "batch_ordinal": batch.get("ordinal"),
                "packet_count": packet_count,
                "status": result.get("status"),
                "txns": txns,
            }
        )
        if packet_count is None:
            entry["reason"] = "missing_packet_count"
            continue

        seq_issues: list[dict[str, Any]] = []
        seen_tx_indexes: set[int] = set()
        for tx in txs:
            tx_index = numeric(tx.get("tx_index"))
            if tx_index is None:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "tx_result_missing_numeric_tx_index",
                        "tx_index": tx.get("tx_index"),
                    }
                )
                continue
            if tx_index in seen_tx_indexes:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "tx_index": tx_index,
                        "reason": "duplicate_tx_result_index",
                    }
                )
            seen_tx_indexes.add(tx_index)
            if tx_index < 0 or tx_index >= packet_count:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "tx_index": tx_index,
                        "packet_count": packet_count,
                        "reason": "tx_result_index_out_of_batch_bounds",
                    }
                )

        if result.get("status") == "committed":
            if txns is not None and txns > packet_count:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "txns": txns,
                        "packet_count": packet_count,
                        "reason": "committed_txn_count_exceeds_packet_count",
                    }
                )
        elif result.get("status") == "not_committed":
            failure_index = numeric(result.get("index"))
            if (
                failure_index is not None
                and packet_count > 0
                and (failure_index < 0 or failure_index >= packet_count)
            ):
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "index": failure_index,
                        "packet_count": packet_count,
                        "reason": "failure_index_out_of_batch_bounds",
                    }
                )

        if seq_issues:
            entry["passed"] = False
            entry["issues"] = seq_issues
            issues.extend(seq_issues)

    return {
        "checked": bool(entries),
        "passed": not issues,
        "issues": issues,
        "entries": entries,
    }


def result_attribution_integrity_pass(outcome: dict[str, Any]) -> bool:
    return result_attribution_integrity_projection(outcome).get("passed") is not False


def _ordered_batch_results(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        outcome.get("bam", {}).get("batch_results", []),
        key=lambda item: (
            _sort_value(item.get("line")),
            _sort_value(item.get("seq_id")),
            str(item.get("status")),
        ),
    )


def result_identity_integrity_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    scenario_batches = scenario_batch_projection(outcome)
    results = _ordered_batch_results(outcome)
    batches_by_seq = _items_by_seq(scenario_batches)
    results_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for result in results:
        results_by_seq.setdefault(result.get("seq_id"), []).append(result)

    issues: list[dict[str, Any]] = []
    entries: dict[str, Any] = {}
    checked_any = False
    for seq_id, batches in sorted(
        batches_by_seq.items(), key=lambda item: _sort_value(item[0])
    ):
        if len(batches) <= 1:
            continue

        key = str(seq_id)
        seq_results = results_by_seq.get(seq_id, [])
        expected_packet_counts = [
            numeric(batch.get("packet_count")) for batch in batches
        ]
        observed_txns = [numeric(result.get("txns")) for result in seq_results]
        entry = {
            "seq_id": seq_id,
            "checked": False,
            "scenario_batch_count": len(batches),
            "result_count": len(seq_results),
            "expected_packet_counts": expected_packet_counts,
            "observed_txns": observed_txns,
            "passed": True,
        }
        entries[key] = entry

        if len(seq_results) != len(batches):
            issue = {
                "seq_id": seq_id,
                "reason": "duplicate_seq_result_count_mismatch",
                "expected": len(batches),
                "actual": len(seq_results),
            }
            entry["checked"] = True
            checked_any = True
            entry["passed"] = False
            entry["issues"] = [issue]
            issues.append(issue)
            continue

        if any(count is None for count in expected_packet_counts):
            entry["reason"] = "missing_packet_count"
            continue
        if len(set(expected_packet_counts)) <= 1:
            entry["reason"] = "indistinguishable_duplicate_batch_shape"
            continue

        entry["checked"] = True
        checked_any = True
        seq_issues: list[dict[str, Any]] = []
        for index, (batch, result) in enumerate(zip(batches, seq_results)):
            packet_count = numeric(batch.get("packet_count"))
            txns = numeric(result.get("txns"))
            if result.get("status") == "committed" and txns != packet_count:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "batch_ordinal": batch.get("ordinal"),
                        "result_index": index,
                        "expected_txns": packet_count,
                        "observed_txns": txns,
                        "reason": "duplicate_seq_committed_result_identity_mismatch",
                    }
                )

        if seq_issues:
            entry["passed"] = False
            entry["issues"] = seq_issues
            issues.extend(seq_issues)

    return {
        "checked": checked_any,
        "passed": not issues,
        "issues": issues,
        "entries": entries,
    }


def result_identity_integrity_pass(outcome: dict[str, Any]) -> bool:
    return result_identity_integrity_projection(outcome).get("passed") is not False


NON_REVERT_PREFIX_MODES = {
    "non_atomic_first_overdraft",
    "non_atomic_mid_overdraft",
    "non_atomic_partial_overdraft",
    "non_atomic_partial_overdraft_reconnect",
    "non_atomic_partial_cu_fail",
}

ATOMIC_ROLLBACK_MODES = {
    "atomic_revert",
    "atomic_mid_fail",
    "atomic_blockhash_mid_fail",
    "atomic_resolver_mid_fail",
}

ATOMIC_INDEX_ONE_MODES = {
    "atomic_revert",
    "atomic_mid_fail",
    "atomic_blockhash_mid_fail",
    "atomic_resolver_mid_fail",
}


def _items_by_seq(items: list[dict[str, Any]]) -> dict[Any, list[dict[str, Any]]]:
    by_seq: dict[Any, list[dict[str, Any]]] = {}
    for item in items:
        by_seq.setdefault(item.get("seq_id"), []).append(item)
    return by_seq


def _tx_success_count(txs: list[dict[str, Any]]) -> int:
    return sum(1 for tx in txs if tx.get("execution_success") is True)


def _tx_failure_count(txs: list[dict[str, Any]]) -> int:
    return sum(1 for tx in txs if tx.get("execution_success") is False)


def result_execution_semantics_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    batch_results = outcome.get("bam", {}).get("batch_results", [])
    batch_result_txs = outcome.get("bam", {}).get("batch_result_txs", [])
    scenario_batches = scenario_batch_projection(outcome)
    batches_by_seq = _items_by_seq(scenario_batches)
    results_by_seq = _items_by_seq(batch_results)
    txs_by_seq = _items_by_seq(batch_result_txs)

    entries: dict[str, Any] = {}
    issues: list[dict[str, Any]] = []
    for seq_id, results in sorted(results_by_seq.items(), key=lambda item: _sort_value(item[0])):
        key = str(seq_id)
        batches = batches_by_seq.get(seq_id, [])
        txs = txs_by_seq.get(seq_id, [])
        entry = {
            "seq_id": seq_id,
            "checked": False,
            "result_count": len(results),
            "scenario_batch_count": len(batches),
            "tx_result_count": len(txs),
            "success_count": _tx_success_count(txs),
            "failure_count": _tx_failure_count(txs),
            "passed": True,
        }
        entries[key] = entry

        if len(results) != 1 or len(batches) != 1:
            entry["reason"] = "ambiguous_seq_id"
            continue

        result = results[0]
        batch = batches[0]
        revert_on_error = batch.get("revert_on_error")
        status = result.get("status")
        entry.update(
            {
                "checked": revert_on_error in (True, False),
                "revert_on_error": revert_on_error,
                "status": status,
                "txns": result.get("txns"),
            }
        )
        if revert_on_error not in (True, False):
            entry["reason"] = "missing_revert_on_error"
            continue

        seq_issues: list[dict[str, Any]] = []
        if revert_on_error is True:
            if status == "committed":
                txns = numeric(result.get("txns"))
                if txns is not None and len(txs) != txns:
                    seq_issues.append(
                        {
                            "seq_id": seq_id,
                            "reason": "committed_atomic_tx_result_count_mismatch",
                            "txns": txns,
                            "tx_result_count": len(txs),
                        }
                    )
                if any(tx.get("execution_success") is not True for tx in txs):
                    seq_issues.append(
                        {
                            "seq_id": seq_id,
                            "reason": "committed_atomic_has_failed_tx_result",
                        }
                    )
            elif status == "not_committed" and txs:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "reverted_atomic_has_tx_results",
                        "tx_result_count": len(txs),
                    }
                )
        if seq_issues:
            entry["passed"] = False
            entry["issues"] = seq_issues
            issues.extend(seq_issues)

    return {
        "checked": bool(batch_results),
        "passed": not issues,
        "issues": issues,
        "entries": entries,
    }


def result_execution_semantics_pass(outcome: dict[str, Any]) -> bool:
    return result_execution_semantics_projection(outcome).get("passed") is not False


def scenario_execution_semantics_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    mode = outcome.get("mode")
    if mode not in NON_REVERT_PREFIX_MODES and mode not in ATOMIC_ROLLBACK_MODES:
        return {"checked": False, "passed": True, "reason": "mode_has_no_scenario_intent_oracle"}

    batch_results = outcome.get("bam", {}).get("batch_results", [])
    batch_result_txs = outcome.get("bam", {}).get("batch_result_txs", [])
    scenario_batches = scenario_batch_projection(outcome)
    batches_by_seq = _items_by_seq(scenario_batches)
    results_by_seq = _items_by_seq(batch_results)
    txs_by_seq = _items_by_seq(batch_result_txs)

    issues: list[dict[str, Any]] = []
    entries: dict[str, Any] = {}
    for seq_id, batches in sorted(batches_by_seq.items(), key=lambda item: _sort_value(item[0])):
        key = str(seq_id)
        results = results_by_seq.get(seq_id, [])
        txs = txs_by_seq.get(seq_id, [])
        entry = {
            "seq_id": seq_id,
            "checked": False,
            "scenario_batch_count": len(batches),
            "result_count": len(results),
            "tx_result_count": len(txs),
            "success_count": _tx_success_count(txs),
            "failure_count": _tx_failure_count(txs),
            "passed": True,
        }
        entries[key] = entry

        if len(batches) != 1 or len(results) != 1:
            entry["reason"] = "ambiguous_seq_id"
            continue

        batch = batches[0]
        result = results[0]
        status = result.get("status")
        packet_count = numeric(batch.get("packet_count"))
        txns = numeric(result.get("txns"))
        success_count = _tx_success_count(txs)
        seq_issues: list[dict[str, Any]] = []
        entry.update(
            {
                "checked": True,
                "status": status,
                "packet_count": packet_count,
                "txns": txns,
                "revert_on_error": batch.get("revert_on_error"),
            }
        )

        if mode in ATOMIC_ROLLBACK_MODES:
            if status != "not_committed":
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "atomic_rollback_batch_not_rejected",
                        "status": status,
                    }
                )
            if txs:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "atomic_rollback_leaked_tx_results",
                        "tx_result_count": len(txs),
                    }
                )
            if mode in ATOMIC_INDEX_ONE_MODES and numeric(result.get("index")) != 1:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "atomic_rollback_failure_index_mismatch",
                        "expected_index": 1,
                        "actual_index": result.get("index"),
                    }
                )
        elif mode in NON_REVERT_PREFIX_MODES:
            if status != "committed":
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "non_revert_batch_rejected_instead_of_prefix_commit",
                        "status": status,
                        "detail": result.get("detail"),
                    }
                )
            elif txns is not None and txns <= 0:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "non_revert_commit_missing_positive_txn_count",
                        "txns": result.get("txns"),
                    }
                )
            elif txs and success_count == 0:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "non_revert_commit_without_successful_tx_result",
                    }
                )
            if mode == "non_atomic_valid_multi_packet" and packet_count is not None and txns is not None and txns != packet_count:
                seq_issues.append(
                    {
                        "seq_id": seq_id,
                        "reason": "valid_non_revert_multi_packet_not_fully_committed",
                        "expected_txns": packet_count,
                        "actual_txns": txns,
                    }
                )

        if seq_issues:
            entry["passed"] = False
            entry["issues"] = seq_issues
            issues.extend(seq_issues)

    return {
        "checked": bool(entries),
        "passed": not issues,
        "mode": mode,
        "issues": issues,
        "entries": entries,
    }


def scenario_execution_semantics_pass(outcome: dict[str, Any]) -> bool:
    return scenario_execution_semantics_projection(outcome).get("passed") is not False

def scenario_batch_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for batch in outcome.get("scenario", {}).get("batches", []):
        projected.append(
            {
                "ordinal": batch.get("ordinal"),
                "event_type": batch.get("event_type"),
                "batch_index": batch.get("batch_index"),
                "seq_id": batch.get("seq_id"),
                "packet_count": batch.get("packet_count"),
                "known_signature_count": batch.get("known_signature_count"),
                "max_schedule_slot": batch.get("max_schedule_slot"),
                "revert_on_error": batch.get("revert_on_error"),
            }
        )
    return projected


def schedule_boundary_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    sent_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for sent in outcome.get("bam", {}).get("scripted_sent_batches", []):
        sent_by_seq.setdefault(sent.get("seq_id"), []).append(sent)

    sent_cursor: dict[Any, int] = {}
    projected: list[dict[str, Any]] = []
    for result in sorted(
        outcome.get("bam", {}).get("batch_results", []),
        key=lambda item: (
            _sort_value(item.get("line")),
            _sort_value(item.get("seq_id")),
            str(item.get("status")),
        ),
    ):
        seq_id = result.get("seq_id")
        sent_items = sent_by_seq.get(seq_id, [])
        cursor = sent_cursor.get(seq_id, 0)
        sent = sent_items[cursor] if cursor < len(sent_items) else {}
        sent_cursor[seq_id] = cursor + 1

        max_schedule_slot = numeric(sent.get("max_schedule_slot"))
        leader_slot_at_send = numeric(sent.get("leader_slot_at_send"))
        leader_slot_at_result = numeric(result.get("leader_slot_at_result"))
        projected.append(
            {
                "seq_id": seq_id,
                "status": result.get("status"),
                "reason": result.get("reason"),
                "detail": result.get("detail"),
                "max_schedule_slot": max_schedule_slot
                if max_schedule_slot is not None
                else sent.get("max_schedule_slot"),
                "leader_slot_at_send": leader_slot_at_send,
                "leader_slot_at_result": leader_slot_at_result,
                "sent_after_max_schedule_slot": (
                    None
                    if max_schedule_slot is None or leader_slot_at_send is None
                    else leader_slot_at_send > max_schedule_slot
                ),
                "result_after_max_schedule_slot": (
                    None
                    if max_schedule_slot is None or leader_slot_at_result is None
                    else leader_slot_at_result > max_schedule_slot
                ),
            }
        )
    return projected


def schedule_boundary_validity_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    entries = schedule_boundary_projection(outcome)
    committed_max_slots = {
        numeric(item.get("max_schedule_slot"))
        for item in entries
        if item.get("status") == "committed"
        and numeric(item.get("max_schedule_slot")) is not None
    }
    issues = []
    for item in entries:
        max_schedule_slot = numeric(item.get("max_schedule_slot"))
        if max_schedule_slot is None or item.get("status") != "committed":
            continue
        if item.get("sent_after_max_schedule_slot") is True:
            issues.append(
                {
                    "seq_id": item.get("seq_id"),
                    "max_schedule_slot": max_schedule_slot,
                    "leader_slot_at_send": item.get("leader_slot_at_send"),
                    "reason": "committed_batch_sent_after_max_schedule_slot",
                }
            )

    for item in bam_ingress_slot_summary_projection(outcome):
        max_schedule_slot = numeric(item.get("max_schedule_slot"))
        if max_schedule_slot not in committed_max_slots:
            continue
        if item.get("first_rx_after_slot_end") is True or (
            numeric(item.get("txns_after_slot_end")) or 0
        ) > 0:
            issues.append(
                {
                    "max_schedule_slot": max_schedule_slot,
                    "current_leader_slot": item.get("current_leader_slot"),
                    "first_rx_minus_slot_end_ns": item.get(
                        "first_rx_minus_slot_end_ns"
                    ),
                    "txns_after_slot_end": item.get("txns_after_slot_end"),
                    "reason": "committed_batch_ingressed_after_slot_end",
                }
            )

    return {
        "checked": bool(entries),
        "passed": not issues,
        "issues": issues,
    }


def schedule_boundary_validity_pass(outcome: dict[str, Any]) -> bool:
    return schedule_boundary_validity_projection(outcome).get("passed") is not False


def bank_hash_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("fd", {}).get("bank_hashes", []):
        projected.append(
            {
                "slot": item.get("slot"),
                "hash": item.get("hash"),
            }
        )
    return projected


def bam_ingress_slot_summary_projection(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    projected = []
    for item in outcome.get("fd", {}).get("bam_ingress_slot_summaries", []):
        txn_cnt = sum(
            numeric(item.get(key)) or 0
            for key in (
                "txns_before_slot_end",
                "txns_after_slot_end",
                "txns_unknown_slot_end",
            )
        )
        if not txn_cnt:
            continue
        projected.append(
            {
                "max_schedule_slot": item.get("max_schedule_slot"),
                "current_leader_slot": item.get("current_leader_slot"),
                "current_after_max_schedule_slot": item.get(
                    "current_after_max_schedule_slot"
                ),
                "first_rx_minus_slot_end_ns": item.get(
                    "first_rx_minus_slot_end_ns"
                ),
                "first_rx_after_slot_end": item.get("first_rx_after_slot_end"),
                "txns_before_slot_end": item.get("txns_before_slot_end"),
                "txns_after_slot_end": item.get("txns_after_slot_end"),
                "txns_unknown_slot_end": item.get("txns_unknown_slot_end"),
            }
        )
    return projected


def bank_hashes_comparable(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return bool(bank_hash_projection(left)) and bool(bank_hash_projection(right))


def protocol_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    bam = outcome.get("bam", {})
    auth_proofs = []
    for proof in bam.get("auth_proofs", []):
        sig_len = proof.get("sig_len")
        signed = (
            isinstance(sig_len, int)
            and sig_len > 0
            or isinstance(sig_len, str)
            and sig_len.isdigit()
            and int(sig_len) > 0
        )
        auth_proofs.append(
            {
                "conn": proof.get("conn"),
                "challenge_len": proof.get("challenge_len"),
                "signed": signed,
            }
        )

    projected = {
        "auth_challenge_count": bam.get("auth_challenge_count"),
        "scheduler_streams": sorted(
            [stream.get("conn") for stream in bam.get("scheduler_streams", [])],
            key=_sort_value,
        ),
        "auth_proofs": sorted(
            auth_proofs,
            key=lambda item: (
                _sort_value(item.get("conn")),
                _sort_value(item.get("challenge_len")),
                _sort_value(item.get("signed")),
            ),
        ),
    }
    challenge_count = numeric(projected.get("auth_challenge_count")) or 0
    checks: dict[str, bool | None] = {
        "scheduler_stream_present": bool(projected["scheduler_streams"]),
        "auth_challenge_seen": challenge_count > 0,
        "auth_proof_seen": bool(projected["auth_proofs"]),
        "auth_proofs_signed": all(
            proof.get("signed") is True for proof in projected["auth_proofs"]
        ),
        "challenge_lengths_positive": all(
            (numeric(proof.get("challenge_len")) or 0) > 0
            for proof in projected["auth_proofs"]
        ),
    }
    projected["checks"] = checks
    projected["passed"] = _checks_pass(checks)
    return projected


def protocol_validity_pass(outcome: dict[str, Any]) -> bool:
    return protocol_projection(outcome).get("passed") is not False


def contact_info_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    fd = outcome.get("fd", {})
    contact_info = fd.get("contact_info", {})
    if not isinstance(contact_info, dict):
        contact_info = {}

    startup = contact_info.get("startup", {})
    if not isinstance(startup, dict):
        startup = {}
    tpu_updates = [
        item
        for item in contact_info.get("tpu_address_updates", [])
        if isinstance(item, dict)
    ]
    public_updates = [
        item
        for item in contact_info.get("public_tpu_address_updates", [])
        if isinstance(item, dict)
    ]
    client_ids = [
        item
        for item in contact_info.get("contact_info_client_ids", [])
        if isinstance(item, dict)
    ]
    default_addrs = [
        item
        for item in contact_info.get("default_tpu_addresses", [])
        if isinstance(item, dict)
    ]

    target = outcome.get("target")
    checks: dict[str, bool | None]
    if target == "fddev":
        checks = {
            "default_tpu_present": bool(default_addrs),
            "bam_enable_prepare": any(item.get("use_bam") == 1 for item in tpu_updates),
            "updated_tpu_present": any(item.get("stage") == "updated" for item in tpu_updates),
            "public_tpu_present": any(item.get("kind") == "tpu" for item in public_updates),
            "public_forwards_present": any(
                item.get("kind") == "forwards" for item in public_updates
            ),
            "contact_info_client_id_present": bool(client_ids),
        }
    elif target == "jito-agave":
        checks = {
            "startup_gossip_present": text_or_none(startup.get("gossip")) is not None,
            "startup_tpu_quic_present": text_or_none(startup.get("tpu_quic")) is not None,
        }
    else:
        checks = {}

    return {
        "target": target,
        "startup": {
            "gossip_present": text_or_none(startup.get("gossip")) is not None,
            "tpu_quic_present": text_or_none(startup.get("tpu_quic")) is not None,
        },
        "default_tpu_count": len(default_addrs),
        "tpu_address_update_count": len(tpu_updates),
        "bam_enable_update_count": sum(1 for item in tpu_updates if item.get("use_bam") == 1),
        "public_tpu_update_count": len(public_updates),
        "contact_info_client_id_count": len(client_ids),
        "checks": checks,
        "passed": _checks_pass(checks),
    }


def contact_info_consistency_pass(outcome: dict[str, Any]) -> bool:
    return contact_info_projection(outcome).get("passed") is not False


def _sort_value(value: Any) -> tuple[int, Any]:
    if isinstance(value, int):
        return (0, value)
    if isinstance(value, str) and value.isdigit():
        return (0, int(value))
    return (1, "" if value is None else str(value))


def numeric(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def text_or_none(value: Any) -> str | None:
    if not isinstance(value, str) or value in ("", "n/a", "none"):
        return None
    return value


def bam_fee_modes(outcome: dict[str, Any]) -> bool:
    return outcome.get("mode") in (
        "bam_fee_priority_commit",
        "bam_fee_priority_replay_after_reconnect",
        "bam_fee_queue_burst_reconnect",
        "bam_fee_source_mix_queue_burst_reconnect",
        "bam_fee_url_churn_priority_commit",
        "bam_fee_url_churn_same_slot_priority_commit",
        "bam_fee_config_refresh_priority_commit",
        "bam_fee_config_commission_refresh_priority_commit",
        "bam_fee_config_refresh_queue_burst",
        "bam_fee_config_refresh_source_mix_queue_burst",
        "bam_fee_config_midqueue_refresh",
        "bam_fee_config_midqueue_source_mix_queue_burst",
        "bam_fee_config_midqueue_source_mix_multi_reconnect",
    )


def bam_fee_config_refresh_modes(outcome: dict[str, Any]) -> bool:
    return outcome.get("mode") in (
        "bam_fee_config_refresh_priority_commit",
        "bam_fee_config_commission_refresh_priority_commit",
        "bam_fee_config_refresh_queue_burst",
        "bam_fee_config_refresh_source_mix_queue_burst",
        "bam_fee_config_midqueue_refresh",
        "bam_fee_config_midqueue_source_mix_queue_burst",
        "bam_fee_config_midqueue_source_mix_multi_reconnect",
    )


def bam_fee_config_commission_only_refresh_modes(outcome: dict[str, Any]) -> bool:
    return outcome.get("mode") == "bam_fee_config_commission_refresh_priority_commit"


def _fee_config_pubkey(item: dict[str, Any]) -> str | None:
    return text_or_none(item.get("prio_fee_recipient_pubkey"))


def _fee_config_line(item: dict[str, Any]) -> int:
    return numeric(item.get("line")) or 0


def _fee_config_summary(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    return {
        "line": item.get("line"),
        "conn": item.get("conn"),
        "prio_fee_recipient_pubkey": item.get("prio_fee_recipient_pubkey"),
        "commission_bps": item.get("commission_bps"),
    }


def fee_config_observability_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    if not bam_fee_config_refresh_modes(outcome):
        return {
            "checked": False,
            "passed": True,
            "reason": "not_fee_config_refresh_mode",
        }

    bam = outcome.get("bam", {})
    if not isinstance(bam, dict) or bam.get("fee_config_observability_parsed") is not True:
        return {
            "checked": False,
            "passed": True,
            "reason": "normalized_outcome_without_fee_config_events",
        }

    builder_configs = [
        item for item in bam.get("builder_configs", []) if isinstance(item, dict)
    ]
    config_updates = [
        item for item in bam.get("config_updates", []) if isinstance(item, dict)
    ]
    issues: list[dict[str, Any]] = []
    matched_updates: list[dict[str, Any]] = []

    if not config_updates:
        issues.append({"reason": "missing_scripted_config_update"})
    if not builder_configs:
        issues.append({"reason": "missing_builder_config_response"})

    for update in config_updates:
        update_pubkey = _fee_config_pubkey(update)
        update_commission = numeric(update.get("commission_bps"))
        update_line = _fee_config_line(update)
        if update_pubkey is None:
            issues.append(
                {
                    "reason": "config_update_missing_prio_fee_recipient",
                    "update": _fee_config_summary(update),
                }
            )
            continue
        matches = []
        for builder in builder_configs:
            if _fee_config_line(builder) <= update_line:
                continue
            if _fee_config_pubkey(builder) != update_pubkey:
                continue
            if update_commission is not None and numeric(builder.get("commission_bps")) != update_commission:
                continue
            matches.append(builder)
        if not matches:
            issues.append(
                {
                    "reason": "refreshed_builder_config_not_observed_after_update",
                    "update": _fee_config_summary(update),
                }
            )
        else:
            matched_updates.append(
                {
                    "update": _fee_config_summary(update),
                    "first_match": _fee_config_summary(matches[0]),
                }
            )

    state = outcome.get("state", {})
    if not isinstance(state, dict):
        state = {}
    initial_recipient = state.get("bam_fee_recipient")
    refreshed_recipient = state.get("bam_fee_recipient_second")
    initial_pubkey = (
        text_or_none(initial_recipient.get("pubkey"))
        if isinstance(initial_recipient, dict)
        else None
    )
    refreshed_pubkey = (
        text_or_none(refreshed_recipient.get("pubkey"))
        if isinstance(refreshed_recipient, dict)
        else None
    )

    if config_updates:
        first_update_line = min(_fee_config_line(update) for update in config_updates)
        pre_update_builders = [
            builder for builder in builder_configs if _fee_config_line(builder) < first_update_line
        ]
        post_update_builders = [
            builder for builder in builder_configs if _fee_config_line(builder) > first_update_line
        ]
        if initial_pubkey is not None and not any(
            _fee_config_pubkey(builder) == initial_pubkey for builder in pre_update_builders
        ):
            issues.append(
                {
                    "reason": "initial_builder_config_not_observed_before_update",
                    "expected_pubkey": initial_pubkey,
                }
            )
        if refreshed_pubkey is not None and not any(
            _fee_config_pubkey(builder) == refreshed_pubkey for builder in post_update_builders
        ):
            issues.append(
                {
                    "reason": "refreshed_builder_config_not_observed_after_update",
                    "expected_pubkey": refreshed_pubkey,
                }
            )

    return {
        "checked": True,
        "passed": not issues,
        "builder_config_count": len(builder_configs),
        "config_update_count": len(config_updates),
        "initial_expected_pubkey": initial_pubkey,
        "refreshed_expected_pubkey": refreshed_pubkey,
        "builder_configs": [_fee_config_summary(item) for item in builder_configs],
        "config_updates": [_fee_config_summary(item) for item in config_updates],
        "matched_updates": matched_updates,
        "issues": issues,
    }


def fee_config_observability_pass(outcome: dict[str, Any]) -> bool:
    return fee_config_observability_projection(outcome).get("passed") is not False


def _fee_config_snapshot_entry(
    state: dict[str, Any],
    key: str,
    *,
    require_positive_delta: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    item = state.get(key)
    entry: dict[str, Any] = {
        "key": key,
        "checked": False,
        "passed": True,
    }
    issues: list[dict[str, Any]] = []
    if not isinstance(item, dict):
        issues.append({"key": key, "reason": "missing_fee_recipient_state"})
        entry["passed"] = False
        entry["reason"] = "missing_fee_recipient_state"
        return entry, issues

    initial = numeric(item.get("initial"))
    observed = numeric(item.get("observed"))
    observed_delta = None if initial is None or observed is None else observed - initial
    expected_min_delta = numeric(item.get("expected_min_delta"))
    minimum_delta = (
        expected_min_delta
        if expected_min_delta is not None
        else (1 if require_positive_delta else None)
    )
    entry.update(
        {
            "checked": True,
            "pubkey": item.get("pubkey"),
            "initial": initial,
            "observed": observed,
            "observed_delta": observed_delta,
            "expected_min_delta": expected_min_delta,
            "commission_bps": numeric(item.get("commission_bps")),
            "builder_commission_pct": numeric(item.get("builder_commission_pct")),
            "checks": {
                "pubkey": text_or_none(item.get("pubkey")) is not None,
                "observed_delta": observed_delta is not None,
                "positive_delta": (
                    None
                    if not require_positive_delta
                    else observed_delta is not None and observed_delta > 0
                ),
                "minimum_delta": (
                    None
                    if minimum_delta is None
                    else observed_delta is not None and observed_delta >= minimum_delta
                ),
            },
        }
    )
    checks = entry["checks"]
    if checks["pubkey"] is not True:
        issues.append({"key": key, "reason": "fee_recipient_pubkey_missing"})
    if checks["observed_delta"] is not True:
        issues.append({"key": key, "reason": "fee_recipient_delta_missing"})
    if checks["minimum_delta"] is False:
        issues.append(
            {
                "key": key,
                "reason": "fee_recipient_delta_below_minimum",
                "observed_delta": observed_delta,
                "expected_min_delta": minimum_delta,
            }
        )
    entry["passed"] = not issues
    if issues:
        entry["issues"] = issues
    return entry, issues


def fee_config_snapshot_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    if not bam_fee_modes(outcome):
        return {
            "checked": False,
            "passed": True,
            "reason": "not_fee_mode",
        }

    state = outcome.get("state", {})
    if not isinstance(state, dict):
        state = {}
    if not any(
        isinstance(state.get(key), dict)
        for key in ("bam_fee_recipient", "bam_fee_recipient_second")
    ):
        return {
            "checked": False,
            "passed": True,
            "reason": "normalized_outcome_without_fee_state",
        }

    required_keys = ["bam_fee_recipient"]
    refresh_mode = bam_fee_config_refresh_modes(outcome)
    commission_only_refresh = bam_fee_config_commission_only_refresh_modes(outcome)
    if refresh_mode and not commission_only_refresh:
        required_keys.append("bam_fee_recipient_second")

    issues: list[dict[str, Any]] = []
    entries: dict[str, Any] = {}
    for key in required_keys:
        entry, entry_issues = _fee_config_snapshot_entry(
            state,
            key,
            require_positive_delta=True,
        )
        entries[key] = entry
        issues.extend(entry_issues)

    if refresh_mode and not commission_only_refresh:
        first = entries.get("bam_fee_recipient", {})
        second = entries.get("bam_fee_recipient_second", {})
        first_pubkey = text_or_none(first.get("pubkey"))
        second_pubkey = text_or_none(second.get("pubkey"))
        if first_pubkey is not None and second_pubkey is not None:
            distinct = first_pubkey != second_pubkey
            if not distinct:
                issues.append(
                    {
                        "reason": "refreshed_fee_recipient_not_distinct",
                        "pubkey": first_pubkey,
                    }
                )
            entries["recipient_pubkeys_distinct"] = {
                "checked": True,
                "passed": distinct,
                "initial_pubkey": first_pubkey,
                "refreshed_pubkey": second_pubkey,
            }

    return {
        "checked": True,
        "passed": not issues,
        "mode": outcome.get("mode"),
        "required_keys": required_keys,
        "entries": entries,
        "issues": issues,
    }


def fee_config_snapshot_pass(outcome: dict[str, Any]) -> bool:
    return fee_config_snapshot_projection(outcome).get("passed") is not False


def state_delta_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    state = outcome.get("state", {})
    projected: dict[str, Any] = {}
    fee_recipient = state.get("bam_fee_recipient")
    if bam_fee_modes(outcome) and isinstance(fee_recipient, dict):
        initial = numeric(fee_recipient.get("initial"))
        observed = numeric(fee_recipient.get("observed"))
        if (
            text_or_none(fee_recipient.get("pubkey")) is not None
            and initial is not None
            and observed is not None
        ):
            projected["bam_fee_recipient"] = {
                "pubkey": fee_recipient.get("pubkey"),
                "observed_delta": observed - initial,
                "expected_min_delta": numeric(fee_recipient.get("expected_min_delta")),
                "commission_bps": numeric(fee_recipient.get("commission_bps")),
                "builder_commission_pct": numeric(
                    fee_recipient.get("builder_commission_pct")
                ),
            }
    fee_recipient_second = state.get("bam_fee_recipient_second")
    if outcome.get("mode") in ("bam_fee_url_churn_priority_commit", "bam_fee_url_churn_same_slot_priority_commit", "bam_fee_config_refresh_priority_commit", "bam_fee_config_refresh_queue_burst", "bam_fee_config_refresh_source_mix_queue_burst", "bam_fee_config_midqueue_refresh", "bam_fee_config_midqueue_source_mix_queue_burst", "bam_fee_config_midqueue_source_mix_multi_reconnect") and isinstance(fee_recipient_second, dict):
        initial = numeric(fee_recipient_second.get("initial"))
        observed = numeric(fee_recipient_second.get("observed"))
        if (
            text_or_none(fee_recipient_second.get("pubkey")) is not None
            and initial is not None
            and observed is not None
        ):
            projected["bam_fee_recipient_second"] = {
                "pubkey": fee_recipient_second.get("pubkey"),
                "observed_delta": observed - initial,
                "expected_min_delta": numeric(fee_recipient_second.get("expected_min_delta")),
                "commission_bps": numeric(fee_recipient_second.get("commission_bps")),
                "builder_commission_pct": numeric(
                    fee_recipient_second.get("builder_commission_pct")
                ),
            }

    for key in ("recipient_one", "recipient_two"):
        item = state.get(key)
        if not isinstance(item, dict):
            continue
        initial = numeric(item.get("initial"))
        observed = numeric(item.get("observed"))
        expected = numeric(item.get("expected"))
        if (
            outcome.get("mode") == "external_scenario"
            or is_queue_burst_mode_name(outcome.get("mode"))
        ) and expected is None:
            continue
        if text_or_none(item.get("pubkey")) is None and all(
            value is None for value in (initial, observed, expected)
        ):
            continue
        owner_expected = text_or_none(item.get("owner_expected"))
        owner_observed = text_or_none(item.get("owner_observed"))
        space_expected = numeric(item.get("space_expected"))
        space_observed = numeric(item.get("space_observed"))
        observed_delta = None if initial is None or observed is None else observed - initial
        expected_delta = None if initial is None or expected is None else expected - initial
        if (
            observed_delta is None
            and expected_delta == 0
            and owner_expected is None
            and space_expected in (None, 0)
        ):
            observed_delta = 0
        projected[key] = {
            "pubkey": item.get("pubkey"),
            "observed_delta": observed_delta,
            "expected_delta": expected_delta,
        }
        if owner_expected is not None or space_expected not in (None, 0):
            projected[key]["owner_expected"] = owner_expected
            projected[key]["owner_observed"] = owner_observed
            projected[key]["owner_matches"] = owner_expected == owner_observed
            projected[key]["space_expected"] = space_expected
            projected[key]["space_observed"] = space_observed
            projected[key]["space_matches"] = space_expected == space_observed
    if "vote_account" in state:
        vote = state["vote_account"]
        projected["vote_account"] = {
            "pubkey": vote.get("pubkey"),
            "authorized_voter_changed": vote.get("pre_authorized_voter")
            != vote.get("post_authorized_voter"),
            "votes_len_delta": _delta(vote.get("pre_votes_len"), vote.get("post_votes_len")),
            "last_timestamp_slot_delta": _delta(
                vote.get("pre_last_timestamp_slot"), vote.get("post_last_timestamp_slot")
            ),
        }
    return projected


def _explicit_zero_only_state_deltas(projection: dict[str, Any]) -> bool:
    if not projection:
        return True
    for item in projection.values():
        if not isinstance(item, dict):
            return False
        if item.get("observed_delta") not in (None, 0):
            return False
        if item.get("expected_delta") not in (None, 0):
            return False
        for key, value in item.items():
            if key in ("pubkey", "observed_delta", "expected_delta"):
                continue
            if key.endswith("_matches"):
                if value is not True:
                    return False
                continue
            if value not in (None, 0):
                return False
    return True


def state_delta_projections_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if left == right:
        return True
    return (
        (not left and _explicit_zero_only_state_deltas(right))
        or (not right and _explicit_zero_only_state_deltas(left))
    )


def is_vote_reject_mode(outcome: dict[str, Any]) -> bool:
    return outcome.get("mode") in ("vote_reject_once", "vote_reject_reconnect")


def helper_state_delta_projection_match(
    left: dict[str, Any], right: dict[str, Any]
) -> bool:
    left_state = state_delta_projection(left)
    right_state = state_delta_projection(right)
    if left_state and right_state:
        return False
    if not (left_state or right_state):
        return True
    if not helper_comparison(left, right):
        return False

    populated = right if left.get("runner_kind") == "helper" else left
    return state_consistency_pass(populated)


def state_consistency_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    state = outcome.get("state", {})
    projected: dict[str, Any] = {}
    for key in ("recipient_one", "recipient_two"):
        item = state.get(key)
        if not isinstance(item, dict):
            continue
        initial = numeric(item.get("initial"))
        observed = numeric(item.get("observed"))
        expected = numeric(item.get("expected"))
        if text_or_none(item.get("pubkey")) is None and all(
            value is None for value in (initial, observed, expected)
        ):
            continue

        checks: dict[str, bool | None] = {
            "balance": None,
            "owner": None,
            "space": None,
        }
        observed_delta = None if initial is None or observed is None else observed - initial
        expected_delta = None if initial is None or expected is None else expected - initial
        if expected_delta is not None and observed_delta is not None:
            checks["balance"] = expected_delta == observed_delta

        owner_expected = text_or_none(item.get("owner_expected"))
        owner_observed = text_or_none(item.get("owner_observed"))
        if owner_expected is not None:
            checks["owner"] = owner_expected == owner_observed

        space_expected = numeric(item.get("space_expected"))
        space_observed = numeric(item.get("space_observed"))
        if space_expected is not None:
            checks["space"] = space_expected == space_observed

        projected[key] = {
            "pubkey": item.get("pubkey"),
            "expected_delta": expected_delta,
            "observed_delta": observed_delta,
            "checks": checks,
            "passed": _checks_pass(checks),
        }

    if is_vote_reject_mode(outcome) and "vote_account" in state:
        vote = state["vote_account"]
        votes_len_delta = _delta(vote.get("pre_votes_len"), vote.get("post_votes_len"))
        last_timestamp_slot_delta = _delta(
            vote.get("pre_last_timestamp_slot"), vote.get("post_last_timestamp_slot")
        )
        checks = {
            "authorized_voter": vote.get("pre_authorized_voter")
            == vote.get("post_authorized_voter"),
        }
        projected["vote_account"] = {
            "pubkey": vote.get("pubkey"),
            "checks": checks,
            "observed": {
                "votes_len_delta": votes_len_delta,
                "last_timestamp_slot_delta": last_timestamp_slot_delta,
            },
            "note": "fddev normal tower voting may advance the validator vote account during this rejected-vote scenario",
            "passed": _checks_pass(checks),
        }

    if bam_fee_modes(outcome):
        fee_keys = ["bam_fee_recipient"]
        if outcome.get("mode") in ("bam_fee_url_churn_priority_commit", "bam_fee_url_churn_same_slot_priority_commit", "bam_fee_config_refresh_priority_commit", "bam_fee_config_refresh_queue_burst", "bam_fee_config_refresh_source_mix_queue_burst", "bam_fee_config_midqueue_refresh", "bam_fee_config_midqueue_source_mix_queue_burst", "bam_fee_config_midqueue_source_mix_multi_reconnect"):
            fee_keys.append("bam_fee_recipient_second")
        for fee_key in fee_keys:
            fee_recipient = state.get(fee_key)
            if not isinstance(fee_recipient, dict):
                continue
            initial = numeric(fee_recipient.get("initial"))
            observed = numeric(fee_recipient.get("observed"))
            expected_min_delta = numeric(fee_recipient.get("expected_min_delta"))
            if expected_min_delta is None:
                expected_min_delta = 0
            observed_delta = (
                None if initial is None or observed is None else observed - initial
            )
            checks = {
                "pubkey": text_or_none(fee_recipient.get("pubkey")) is not None,
                "observed": observed_delta is not None,
                "min_delta": (
                    None
                    if observed_delta is None
                    else observed_delta >= expected_min_delta
                ),
            }
            projected[fee_key] = {
                "pubkey": fee_recipient.get("pubkey"),
                "expected_min_delta": expected_min_delta,
                "observed_delta": observed_delta,
                "checks": checks,
                "passed": _checks_pass(checks),
            }
        return projected

    return projected


def state_consistency_pass(outcome: dict[str, Any]) -> bool:
    return all(
        item.get("passed", True)
        for item in state_consistency_projection(outcome).values()
        if isinstance(item, dict)
    )


def atomic_result_semantics_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    if not requires_chain_oracle(outcome) or not chain_evidence_capture_present(outcome):
        return {}

    atomic_batches_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for batch in scenario_batch_projection(outcome):
        if batch.get("revert_on_error") is True:
            atomic_batches_by_seq.setdefault(batch.get("seq_id"), []).append(batch)

    statuses_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for item in outcome.get("chain", {}).get("expected_signature_statuses", []):
        statuses_by_seq.setdefault(item.get("seq_id"), []).append(item)

    ambiguous_result_seq_ids = duplicate_result_seq_ids(outcome)
    projected: dict[str, Any] = {}
    for result in batch_projection(outcome):
        seq_id = result.get("seq_id")
        atomic_batches = atomic_batches_by_seq.get(seq_id, [])
        if not atomic_batches:
            continue

        key = str(seq_id)
        if seq_id in ambiguous_result_seq_ids or len(atomic_batches) != 1:
            projected[key] = {
                "result_status": result.get("status"),
                "checked": False,
                "reason": "ambiguous_seq_id",
                "passed": True,
            }
            continue

        signatures = [
            item
            for item in statuses_by_seq.get(seq_id, [])
            if item.get("attributable") is True
        ]
        if not signatures:
            projected[key] = {
                "result_status": result.get("status"),
                "checked": False,
                "reason": "no_attributable_signatures",
                "passed": True,
            }
            continue

        occurrence_counts = [numeric(item.get("occurrence_count")) or 0 for item in signatures]
        block_occurrence_counts = [
            sum(
                1
                for occurrence in item.get("occurrences", [])
                if isinstance(occurrence, dict) and occurrence.get("source") == "rpc_block"
            )
            for item in signatures
        ]
        result_status = result.get("status")
        failure_reason = None
        if result_status == "committed":
            passed = all(count > 0 for count in occurrence_counts)
            if not passed:
                failure_reason = "committed_atomic_batch_missing_landed_signature"
        elif result_status == "not_committed":
            passed = all(count == 0 for count in block_occurrence_counts)
            if not passed:
                failure_reason = "not_committed_atomic_batch_landed_signature"
        else:
            passed = False
            failure_reason = "atomic_batch_missing_terminal_result"

        projected[key] = {
            "result_status": result_status,
            "checked": True,
            "signature_count": len(signatures),
            "landed_count": sum(1 for count in occurrence_counts if count > 0),
            "block_landed_count": sum(1 for count in block_occurrence_counts if count > 0),
            "occurrence_counts": occurrence_counts,
            "block_occurrence_counts": block_occurrence_counts,
            "passed": passed,
        }
        if failure_reason is not None:
            projected[key]["reason"] = failure_reason
    return projected


def atomic_result_semantics_pass(outcome: dict[str, Any]) -> bool:
    return all(
        item.get("passed", True)
        for item in atomic_result_semantics_projection(outcome).values()
        if isinstance(item, dict)
    )


def chain_consistency_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    statuses_by_seq: dict[Any, list[dict[str, Any]]] = {}
    for item in outcome.get("chain", {}).get("expected_signature_statuses", []):
        statuses_by_seq.setdefault(item.get("seq_id"), []).append(item)

    ambiguous_result_seq_ids = duplicate_result_seq_ids(outcome)
    projected: dict[str, Any] = {}
    for result in batch_projection(outcome):
        seq_id = result.get("seq_id")
        result_status = result.get("status")
        if seq_id in ambiguous_result_seq_ids:
            projected[str(seq_id)] = {
                "result_status": result_status,
                "checked": False,
                "reason": "duplicate_result_seq_id",
                "passed": True,
            }
            continue
        signatures = [
            item
            for item in statuses_by_seq.get(seq_id, [])
            if item.get("attributable") is True
        ]
        ambiguous = [
            item
            for item in statuses_by_seq.get(seq_id, [])
            if item.get("duplicate_expected_signature") is True
        ]
        if not signatures and ambiguous:
            projected[str(seq_id)] = {
                "result_status": result_status,
                "checked": False,
                "reason": "duplicate_expected_signature",
                "passed": True,
            }
            continue
        if not signatures:
            continue

        occurrence_counts = [numeric(item.get("occurrence_count")) or 0 for item in signatures]
        block_occurrence_counts = [
            sum(
                1
                for occurrence in item.get("occurrences", [])
                if isinstance(occurrence, dict) and occurrence.get("source") == "rpc_block"
            )
            for item in signatures
        ]
        if result_status == "committed":
            passed = all(count > 0 for count in occurrence_counts)
        elif result_status == "not_committed":
            passed = all(count == 0 for count in block_occurrence_counts)
        else:
            passed = True

        projected[str(seq_id)] = {
            "result_status": result_status,
            "checked": result_status in ("committed", "not_committed"),
            "signature_count": len(signatures),
            "landed_count": sum(1 for count in occurrence_counts if count > 0),
            "block_landed_count": sum(1 for count in block_occurrence_counts if count > 0),
            "occurrence_counts": occurrence_counts,
            "block_occurrence_counts": block_occurrence_counts,
            "passed": passed,
        }
    return projected


def chain_consistency_pass(outcome: dict[str, Any]) -> bool:
    return all(
        item.get("passed", True)
        for item in chain_consistency_projection(outcome).values()
        if isinstance(item, dict)
    )


def _checks_pass(checks: dict[str, bool | None]) -> bool:
    return all(value is not False for value in checks.values())


def _delta(before: Any, after: Any) -> int | None:
    before_i = numeric(before)
    after_i = numeric(after)
    if before_i is None or after_i is None:
        return None
    return after_i - before_i


def _unique_projection_by_seq(
    projection: list[dict[str, Any]],
) -> tuple[dict[Any, dict[str, Any]], list[Any]]:
    by_seq: dict[Any, dict[str, Any]] = {}
    duplicates: list[Any] = []
    for item in projection:
        seq_id = item.get("seq_id")
        if seq_id in by_seq:
            duplicates.append(seq_id)
            continue
        by_seq[seq_id] = item
    return by_seq, duplicates


def _committed_result(item: dict[str, Any]) -> bool:
    return item.get("status") == "committed"


def _outside_leader_slot_result(item: dict[str, Any]) -> bool:
    return (
        item.get("status") == "not_committed"
        and item.get("reason") == "scheduling_error"
        and item.get("detail") == "OUTSIDE_LEADER_SLOT"
    )


def _committed_vs_outside_leader_slot(
    left: dict[str, Any], right: dict[str, Any]
) -> bool:
    return (
        (_committed_result(left) and _outside_leader_slot_result(right))
        or (_committed_result(right) and _outside_leader_slot_result(left))
    )


def _tight_schedule_window(entry: dict[str, Any]) -> bool:
    max_schedule_slot = numeric(entry.get("max_schedule_slot"))
    leader_slot_at_send = numeric(entry.get("leader_slot_at_send"))
    if max_schedule_slot is None or leader_slot_at_send is None:
        return False
    return 0 <= max_schedule_slot - leader_slot_at_send <= 1


def _committed_inside_schedule_window(entry: dict[str, Any]) -> bool:
    if entry.get("status") != "committed":
        return True
    return (
        entry.get("sent_after_max_schedule_slot") is not True
        and entry.get("result_after_max_schedule_slot") is not True
    )


def _sent_seq_order(outcome: dict[str, Any]) -> tuple[list[Any], list[Any]]:
    ordered: list[tuple[int, int, Any]] = []
    duplicates: list[Any] = []
    seen: set[Any] = set()
    for index, sent in enumerate(
        outcome.get("bam", {}).get("scripted_sent_batches", [])
    ):
        seq_id = sent.get("seq_id")
        if seq_id in seen:
            duplicates.append(seq_id)
        seen.add(seq_id)
        order = numeric(sent.get("ordinal"))
        if order is None:
            order = numeric(sent.get("line"))
        if order is None:
            order = index
        ordered.append((order, index, seq_id))
    ordered.sort()
    return [seq_id for _, _, seq_id in ordered], duplicates


def _seq_ids_are_sent_suffix(
    outcome: dict[str, Any], candidate_seq_ids: set[Any]
) -> bool:
    order, duplicates = _sent_seq_order(outcome)
    if duplicates or not order:
        return False
    positions = {seq_id: index for index, seq_id in enumerate(order)}
    if any(seq_id not in positions for seq_id in candidate_seq_ids):
        return False
    first_candidate_index = min(positions[seq_id] for seq_id in candidate_seq_ids)
    suffix = set(order[first_candidate_index:])
    return suffix == candidate_seq_ids


def _seq_ids_are_sent_prefix(
    outcome: dict[str, Any], candidate_seq_ids: set[Any]
) -> bool:
    order, duplicates = _sent_seq_order(outcome)
    if duplicates or not order:
        return False
    positions = {seq_id: index for index, seq_id in enumerate(order)}
    if any(seq_id not in positions for seq_id in candidate_seq_ids):
        return False
    last_candidate_index = max(positions[seq_id] for seq_id in candidate_seq_ids)
    prefix = set(order[: last_candidate_index + 1])
    return prefix == candidate_seq_ids


def _outside_rejecting_side(
    candidate_seq_ids: set[Any],
    left_by_seq: dict[Any, dict[str, Any]],
    right_by_seq: dict[Any, dict[str, Any]],
) -> str | None:
    rejecting_side: str | None = None
    for seq_id in candidate_seq_ids:
        left_result = left_by_seq[seq_id]
        right_result = right_by_seq[seq_id]
        if _committed_result(left_result) and _outside_leader_slot_result(right_result):
            current = "right"
        elif _committed_result(right_result) and _outside_leader_slot_result(left_result):
            current = "left"
        else:
            return None
        if rejecting_side is None:
            rejecting_side = current
        elif rejecting_side != current:
            return None
    return rejecting_side


def _prefix_reconnect_slot_transition(
    outcome: dict[str, Any],
    candidate_seq_ids: set[Any],
    schedule_by_seq: dict[Any, dict[str, Any]],
) -> dict[str, Any]:
    evidence: dict[str, Any] = {"passed": False}
    order, duplicates = _sent_seq_order(outcome)
    if duplicates or not order:
        evidence["reason"] = "sent_order_unavailable"
        return evidence
    positions = {seq_id: index for index, seq_id in enumerate(order)}
    if any(seq_id not in positions for seq_id in candidate_seq_ids):
        evidence["reason"] = "candidate_not_in_sent_order"
        return evidence
    last_candidate_index = max(positions[seq_id] for seq_id in candidate_seq_ids)
    remaining_seq_ids = order[last_candidate_index + 1 :]
    if not remaining_seq_ids:
        evidence["reason"] = "no_later_sent_batch"
        return evidence

    candidate_send_slots = []
    for seq_id in candidate_seq_ids:
        entry = schedule_by_seq.get(seq_id)
        leader_slot_at_send = numeric(entry.get("leader_slot_at_send")) if entry else None
        if leader_slot_at_send is None:
            evidence["reason"] = "missing_candidate_send_slot"
            return evidence
        candidate_send_slots.append(leader_slot_at_send)
    max_candidate_send_slot = max(candidate_send_slots)

    for seq_id in remaining_seq_ids:
        entry = schedule_by_seq.get(seq_id)
        leader_slot_at_send = numeric(entry.get("leader_slot_at_send")) if entry else None
        if leader_slot_at_send is not None and leader_slot_at_send > max_candidate_send_slot:
            evidence.update(
                {
                    "passed": True,
                    "last_candidate_index": last_candidate_index,
                    "max_candidate_leader_slot_at_send": max_candidate_send_slot,
                    "crossing_seq_id": seq_id,
                    "crossing_leader_slot_at_send": leader_slot_at_send,
                }
            )
            return evidence

    evidence.update(
        {
            "reason": "no_later_batch_crossed_send_slot",
            "last_candidate_index": last_candidate_index,
            "max_candidate_leader_slot_at_send": max_candidate_send_slot,
        }
    )
    return evidence


def timing_differential_projection(
    left: dict[str, Any],
    right: dict[str, Any],
    left_txs: list[dict[str, Any]],
    right_txs: list[dict[str, Any]],
    checks: dict[str, bool],
    *,
    ignore_batch_result_order: bool,
) -> dict[str, Any]:
    failed_checks = {name for name, passed in checks.items() if passed is False}
    allowed_failed_checks = {
        "batch_results",
        "batch_result_order",
        "batch_result_txs",
    }
    projection: dict[str, Any] = {
        "checked": bool(failed_checks),
        "passed": False,
        "classification": None,
        "reason": None,
        "seq_ids": [],
    }
    if not failed_checks:
        projection["reason"] = "no_failed_checks"
        return projection
    if failed_checks - allowed_failed_checks:
        projection["reason"] = "non_result_check_failed"
        projection["failed_checks"] = sorted(failed_checks)
        return projection

    left_results = batch_projection(left)
    right_results = batch_projection(right)
    left_by_seq, left_duplicates = _unique_projection_by_seq(left_results)
    right_by_seq, right_duplicates = _unique_projection_by_seq(right_results)
    if left_duplicates or right_duplicates:
        projection["reason"] = "duplicate_result_seq_id"
        projection["left_duplicate_seq_ids"] = sorted(left_duplicates, key=_sort_value)
        projection["right_duplicate_seq_ids"] = sorted(right_duplicates, key=_sort_value)
        return projection
    if set(left_by_seq) != set(right_by_seq):
        projection["reason"] = "result_seq_id_set_mismatch"
        projection["left_only_seq_ids"] = sorted(
            set(left_by_seq) - set(right_by_seq), key=_sort_value
        )
        projection["right_only_seq_ids"] = sorted(
            set(right_by_seq) - set(left_by_seq), key=_sort_value
        )
        return projection

    candidate_seq_ids = {
        seq_id
        for seq_id in left_by_seq
        if left_by_seq[seq_id] != right_by_seq[seq_id]
    }
    if not candidate_seq_ids:
        projection["reason"] = "no_batch_result_difference"
        return projection
    if not all(
        _committed_vs_outside_leader_slot(left_by_seq[seq_id], right_by_seq[seq_id])
        for seq_id in candidate_seq_ids
    ):
        projection["reason"] = "non_boundary_result_difference"
        projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
        return projection

    left_schedule_by_seq, left_schedule_duplicates = _unique_projection_by_seq(
        schedule_boundary_projection(left)
    )
    right_schedule_by_seq, right_schedule_duplicates = _unique_projection_by_seq(
        schedule_boundary_projection(right)
    )
    if left_schedule_duplicates or right_schedule_duplicates:
        projection["reason"] = "duplicate_schedule_seq_id"
        projection["left_duplicate_seq_ids"] = sorted(left_schedule_duplicates, key=_sort_value)
        projection["right_duplicate_seq_ids"] = sorted(right_schedule_duplicates, key=_sort_value)
        return projection

    suffix_timing = _seq_ids_are_sent_suffix(
        left, candidate_seq_ids
    ) and _seq_ids_are_sent_suffix(right, candidate_seq_ids)
    prefix_timing = False
    prefix_rejecting_side: str | None = None
    prefix_transition_evidence: dict[str, Any] | None = None
    if not suffix_timing:
        prefix_rejecting_side = _outside_rejecting_side(
            candidate_seq_ids, left_by_seq, right_by_seq
        )
        if (
            prefix_rejecting_side
            and _seq_ids_are_sent_prefix(left, candidate_seq_ids)
            and _seq_ids_are_sent_prefix(right, candidate_seq_ids)
        ):
            rejecting_schedule_by_seq = (
                left_schedule_by_seq
                if prefix_rejecting_side == "left"
                else right_schedule_by_seq
            )
            rejecting_outcome = left if prefix_rejecting_side == "left" else right
            prefix_transition_evidence = _prefix_reconnect_slot_transition(
                rejecting_outcome, candidate_seq_ids, rejecting_schedule_by_seq
            )
            prefix_timing = prefix_transition_evidence.get("passed") is True
    if not suffix_timing and not prefix_timing:
        projection["reason"] = "result_difference_is_not_recognized_timing_boundary"
        projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
        if prefix_transition_evidence is not None:
            projection["prefix_transition_evidence"] = prefix_transition_evidence
        return projection

    schedule_evidence = []
    for seq_id in sorted(candidate_seq_ids, key=_sort_value):
        left_entry = left_schedule_by_seq.get(seq_id)
        right_entry = right_schedule_by_seq.get(seq_id)
        if left_entry is None or right_entry is None:
            projection["reason"] = "missing_schedule_boundary_evidence"
            projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
            return projection
        if not _tight_schedule_window(left_entry) or not _tight_schedule_window(
            right_entry
        ):
            projection["reason"] = "schedule_window_not_tight"
            projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
            return projection
        if not _committed_inside_schedule_window(
            left_entry
        ) or not _committed_inside_schedule_window(
            right_entry
        ):
            projection["reason"] = "committed_result_outside_schedule_window"
            projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
            return projection
        if prefix_timing:
            rejecting_entry = (
                left_entry if prefix_rejecting_side == "left" else right_entry
            )
            if (
                rejecting_entry.get("sent_after_max_schedule_slot") is True
                or rejecting_entry.get("result_after_max_schedule_slot") is not True
            ):
                projection["reason"] = "prefix_reject_not_after_schedule_boundary"
                projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
                return projection
        schedule_evidence.append(
            {
                "seq_id": seq_id,
                "left": left_entry,
                "right": right_entry,
            }
        )

    filtered_checks = {
        "batch_results": batch_projection_excluding(left, candidate_seq_ids)
        == batch_projection_excluding(right, candidate_seq_ids),
        "batch_result_order": ignore_batch_result_order
        or batch_result_order_projection_excluding(left, candidate_seq_ids)
        == batch_result_order_projection_excluding(right, candidate_seq_ids),
        "batch_result_txs": _projection_excluding_seq_ids(left_txs, candidate_seq_ids)
        == _projection_excluding_seq_ids(right_txs, candidate_seq_ids),
    }
    if not all(filtered_checks.values()):
        projection["reason"] = "filtered_results_still_differ"
        projection["seq_ids"] = sorted(candidate_seq_ids, key=_sort_value)
        projection["filtered_checks"] = filtered_checks
        return projection

    projection.update(
        {
            "passed": True,
            "classification": (
                "tail_schedule_boundary_timing_differential"
                if suffix_timing
                else "reconnect_prefix_schedule_boundary_timing_differential"
            ),
            "reason": (
                "committed_vs_outside_leader_slot_tail_with_tight_schedule_window"
                if suffix_timing
                else "committed_vs_outside_leader_slot_prefix_across_reconnect_slot_transition"
            ),
            "seq_ids": sorted(candidate_seq_ids, key=_sort_value),
            "filtered_checks": filtered_checks,
            "schedule_evidence": schedule_evidence,
        }
    )
    if prefix_timing:
        projection["prefix_rejecting_side"] = prefix_rejecting_side
        projection["prefix_transition_evidence"] = prefix_transition_evidence
    return projection


def compare(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    ignore_cus_consumed: bool = False,
    ignore_batch_result_order: bool = False,
    ignore_loaded_accounts_data_size: bool = False,
    ignore_chain_evidence: bool = False,
) -> dict[str, Any]:
    state_deltas_match = state_delta_projections_match(
        state_delta_projection(left), state_delta_projection(right)
    )
    if not state_deltas_match and is_vote_reject_terminal_match(left, right):
        state_deltas_match = True
    if not state_deltas_match and helper_state_delta_projection_match(left, right):
        state_deltas_match = True

    include_loaded_accounts_data_size = (
        not helper_comparison(left, right)
        and not ignore_loaded_accounts_data_size
    )
    include_feepayer_balance_delta = (
        numeric(left.get("state", {}).get("payer", {}).get("initial")) is not None
        and numeric(right.get("state", {}).get("payer", {}).get("initial")) is not None
    )
    left_txs = tx_projection(
        left,
        include_loaded_accounts_data_size,
        include_cus_consumed=not ignore_cus_consumed,
        include_feepayer_balance_delta=include_feepayer_balance_delta,
    )
    right_txs = tx_projection(
        right,
        include_loaded_accounts_data_size,
        include_cus_consumed=not ignore_cus_consumed,
        include_feepayer_balance_delta=include_feepayer_balance_delta,
    )

    bank_hashes_match = True
    if bank_hashes_comparable(left, right):
        bank_hashes_match = bank_hash_projection(left) == bank_hash_projection(right)

    block_landing_order_match = True
    left_block_landing_order = block_landing_order_projection(left)
    right_block_landing_order = block_landing_order_projection(right)
    left_landed_signatures = landed_signature_projection(left)
    right_landed_signatures = landed_signature_projection(right)
    left_block_landing_complete = len(left_block_landing_order) == len(left_landed_signatures)
    right_block_landing_complete = len(right_block_landing_order) == len(right_landed_signatures)
    if (
        left_block_landing_order
        and right_block_landing_order
        and left_block_landing_complete
        and right_block_landing_complete
    ):
        block_landing_order_match = left_block_landing_order == right_block_landing_order

    chain_comparison_enabled = (
        not ignore_chain_evidence
        and requires_chain_oracle(left)
        and requires_chain_oracle(right)
    )
    batch_result_order_match = (
        batch_result_order_projection(left) == batch_result_order_projection(right)
    )

    checks = {
        "mode": left.get("mode") == right.get("mode"),
        "input_family": left.get("input_family") == right.get("input_family"),
        "batch_results": batch_projection(left) == batch_projection(right),
        "batch_result_order": ignore_batch_result_order or batch_result_order_match,
        "batch_result_txs": left_txs == right_txs,
        "left_batch_result_completeness": batch_result_completeness_pass(left),
        "right_batch_result_completeness": batch_result_completeness_pass(right),
        "left_terminal_result_integrity": terminal_result_integrity_pass(left),
        "right_terminal_result_integrity": terminal_result_integrity_pass(right),
        "left_result_attribution_integrity": result_attribution_integrity_pass(left),
        "right_result_attribution_integrity": result_attribution_integrity_pass(right),
        "left_result_identity_integrity": result_identity_integrity_pass(left),
        "right_result_identity_integrity": result_identity_integrity_pass(right),
        "left_result_execution_semantics": result_execution_semantics_pass(left),
        "right_result_execution_semantics": result_execution_semantics_pass(right),
        "left_scenario_execution_semantics": scenario_execution_semantics_pass(left),
        "right_scenario_execution_semantics": scenario_execution_semantics_pass(right),
        "left_fee_config_observability": fee_config_observability_pass(left),
        "right_fee_config_observability": fee_config_observability_pass(right),
        "left_fee_config_snapshot": fee_config_snapshot_pass(left),
        "right_fee_config_snapshot": fee_config_snapshot_pass(right),
        "chain_evidence_capture": (
            not chain_comparison_enabled
            or (chain_evidence_capture_present(left) and chain_evidence_capture_present(right))
        ),
        "landed_signatures": (
            not chain_comparison_enabled
            or landed_signature_projection(left) == landed_signature_projection(right)
        ),
        "block_landing_order": not chain_comparison_enabled or block_landing_order_match,
        "signature_statuses": (
            not chain_comparison_enabled
            or signature_status_projection(left) == signature_status_projection(right)
        ),
        "bank_hashes": bank_hashes_match,
        "protocol": protocol_validity_pass(left) and protocol_validity_pass(right),
        "state_deltas": state_deltas_match,
        "left_state_consistency": state_consistency_pass(left),
        "right_state_consistency": state_consistency_pass(right),
        "left_schedule_boundary_validity": schedule_boundary_validity_pass(left),
        "right_schedule_boundary_validity": schedule_boundary_validity_pass(right),
        "left_atomic_result_semantics": (
            ignore_chain_evidence
            or not requires_chain_oracle(left)
            or atomic_result_semantics_pass(left)
        ),
        "right_atomic_result_semantics": (
            ignore_chain_evidence
            or not requires_chain_oracle(right)
            or atomic_result_semantics_pass(right)
        ),
        "left_bam_execution_order": (
            ignore_chain_evidence
            or not requires_chain_oracle(left)
            or bam_execution_order_pass(left)
        ),
        "right_bam_execution_order": (
            ignore_chain_evidence
            or not requires_chain_oracle(right)
            or bam_execution_order_pass(right)
        ),
        "left_chain_consistency": (
            ignore_chain_evidence
            or not requires_chain_oracle(left)
            or chain_consistency_pass(left)
        ),
        "right_chain_consistency": (
            ignore_chain_evidence
            or not requires_chain_oracle(right)
            or chain_consistency_pass(right)
        ),
        "left_normal_tpu_signature_consistency": normal_tpu_signature_pass(left),
        "right_normal_tpu_signature_consistency": normal_tpu_signature_pass(right),
        "left_contact_info_consistency": contact_info_consistency_pass(left),
        "right_contact_info_consistency": contact_info_consistency_pass(right),
    }
    status = classify_status(checks)
    timing_differential = timing_differential_projection(
        left,
        right,
        left_txs,
        right_txs,
        checks,
        ignore_batch_result_order=ignore_batch_result_order,
    )
    if status != "PASS" and timing_differential.get("passed") is True:
        status = "TIMING_DIFFERENTIAL"
    return {
        "schema": "firebam.live_outcome_comparison.v1",
        "left_target": left.get("target"),
        "right_target": right.get("target"),
        "status": status,
        "passed": status in ("PASS", "TIMING_DIFFERENTIAL"),
        "failure_classes": failure_classes(checks),
        "comparison_options": {
            "ignore_cus_consumed": ignore_cus_consumed,
            "ignore_batch_result_order": ignore_batch_result_order,
            "ignore_loaded_accounts_data_size": ignore_loaded_accounts_data_size,
            "ignore_chain_evidence": ignore_chain_evidence,
        },
        "timing_differential": timing_differential,
        "checks": checks,
        "left": {
            "mode": left.get("mode"),
            "input_family": left.get("input_family"),
            "batch_results": batch_projection(left),
            "batch_result_order": batch_result_order_projection(left),
            "batch_result_txs": left_txs,
            "batch_result_completeness": batch_result_completeness_projection(left),
            "terminal_result_integrity": terminal_result_integrity_projection(left),
            "result_attribution_integrity": result_attribution_integrity_projection(left),
            "result_identity_integrity": result_identity_integrity_projection(left),
            "result_execution_semantics": result_execution_semantics_projection(left),
            "scenario_execution_semantics": scenario_execution_semantics_projection(left),
            "scenario_batches": scenario_batch_projection(left),
            "schedule_boundaries": schedule_boundary_projection(left),
            "schedule_boundary_validity": schedule_boundary_validity_projection(left),
            "expected_signatures": expected_signature_projection(left),
            "landed_signatures": left_landed_signatures,
            "block_landing_order": left_block_landing_order,
            "block_landing_order_complete": left_block_landing_complete,
            "signature_statuses": signature_status_projection(left),
            "normal_tpu_signature_statuses": normal_tpu_signature_projection(left),
            "contact_info": contact_info_projection(left),
            "bam_ingress_slot_summaries": bam_ingress_slot_summary_projection(left),
            "bank_hashes": bank_hash_projection(left),
            "protocol": protocol_projection(left),
            "state_deltas": state_delta_projection(left),
            "state_consistency": state_consistency_projection(left),
            "fee_config_observability": fee_config_observability_projection(left),
            "fee_config_snapshot": fee_config_snapshot_projection(left),
            "atomic_result_semantics": atomic_result_semantics_projection(left),
            "bam_execution_order": bam_execution_order_projection(left),
            "chain_consistency": chain_consistency_projection(left),
        },
        "right": {
            "mode": right.get("mode"),
            "input_family": right.get("input_family"),
            "batch_results": batch_projection(right),
            "batch_result_order": batch_result_order_projection(right),
            "batch_result_txs": right_txs,
            "batch_result_completeness": batch_result_completeness_projection(right),
            "terminal_result_integrity": terminal_result_integrity_projection(right),
            "result_attribution_integrity": result_attribution_integrity_projection(right),
            "result_identity_integrity": result_identity_integrity_projection(right),
            "result_execution_semantics": result_execution_semantics_projection(right),
            "scenario_execution_semantics": scenario_execution_semantics_projection(right),
            "scenario_batches": scenario_batch_projection(right),
            "schedule_boundaries": schedule_boundary_projection(right),
            "schedule_boundary_validity": schedule_boundary_validity_projection(right),
            "expected_signatures": expected_signature_projection(right),
            "landed_signatures": right_landed_signatures,
            "block_landing_order": right_block_landing_order,
            "block_landing_order_complete": right_block_landing_complete,
            "signature_statuses": signature_status_projection(right),
            "normal_tpu_signature_statuses": normal_tpu_signature_projection(right),
            "contact_info": contact_info_projection(right),
            "bam_ingress_slot_summaries": bam_ingress_slot_summary_projection(right),
            "bank_hashes": bank_hash_projection(right),
            "protocol": protocol_projection(right),
            "state_deltas": state_delta_projection(right),
            "state_consistency": state_consistency_projection(right),
            "fee_config_observability": fee_config_observability_projection(right),
            "fee_config_snapshot": fee_config_snapshot_projection(right),
            "atomic_result_semantics": atomic_result_semantics_projection(right),
            "bam_execution_order": bam_execution_order_projection(right),
            "chain_consistency": chain_consistency_projection(right),
        },
    }


def failure_classes(checks: dict[str, bool]) -> list[str]:
    return [name for name, passed in checks.items() if not passed]


def classify_status(checks: dict[str, bool]) -> str:
    if all(checks.values()):
        return "PASS"
    priority = [
        ("mode", "HARNESS_MODE_MISMATCH"),
        ("input_family", "HARNESS_INPUT_MISMATCH"),
        ("chain_evidence_capture", "CHAIN_ORACLE_MISSING"),
        ("left_batch_result_completeness", "LEFT_RESULT_INCOMPLETE"),
        ("right_batch_result_completeness", "RIGHT_RESULT_INCOMPLETE"),
        ("left_terminal_result_integrity", "LEFT_TERMINAL_RESULT_INVALID"),
        ("right_terminal_result_integrity", "RIGHT_TERMINAL_RESULT_INVALID"),
        ("left_result_attribution_integrity", "LEFT_RESULT_ATTRIBUTION_INVALID"),
        ("right_result_attribution_integrity", "RIGHT_RESULT_ATTRIBUTION_INVALID"),
        ("left_result_identity_integrity", "LEFT_RESULT_IDENTITY_INVALID"),
        ("right_result_identity_integrity", "RIGHT_RESULT_IDENTITY_INVALID"),
        ("left_result_execution_semantics", "LEFT_RESULT_EXECUTION_SEMANTICS_INVALID"),
        ("right_result_execution_semantics", "RIGHT_RESULT_EXECUTION_SEMANTICS_INVALID"),
        ("batch_results", "RESULT_DIFF"),
        ("batch_result_order", "RESULT_ORDER_DIFF"),
        ("batch_result_txs", "TX_RESULT_DIFF"),
        ("left_scenario_execution_semantics", "LEFT_SCENARIO_EXECUTION_SEMANTICS_INVALID"),
        ("right_scenario_execution_semantics", "RIGHT_SCENARIO_EXECUTION_SEMANTICS_INVALID"),
        ("left_fee_config_observability", "LEFT_FEE_CONFIG_OBSERVABILITY_INVALID"),
        ("right_fee_config_observability", "RIGHT_FEE_CONFIG_OBSERVABILITY_INVALID"),
        ("left_fee_config_snapshot", "LEFT_FEE_CONFIG_SNAPSHOT_INVALID"),
        ("right_fee_config_snapshot", "RIGHT_FEE_CONFIG_SNAPSHOT_INVALID"),
        ("landed_signatures", "BLOCK_SIGNATURE_DIFF"),
        ("block_landing_order", "BLOCK_SIGNATURE_ORDER_DIFF"),
        ("signature_statuses", "BLOCK_SIGNATURE_DIFF"),
        ("bank_hashes", "BANK_HASH_DIFF"),
        ("state_deltas", "STATE_DIFF"),
        ("left_state_consistency", "LEFT_STATE_INCONSISTENT"),
        ("right_state_consistency", "RIGHT_STATE_INCONSISTENT"),
        ("left_schedule_boundary_validity", "LEFT_SCHEDULE_BOUNDARY_INVALID"),
        ("right_schedule_boundary_validity", "RIGHT_SCHEDULE_BOUNDARY_INVALID"),
        ("left_atomic_result_semantics", "LEFT_ATOMIC_RESULT_SEMANTICS_INVALID"),
        ("right_atomic_result_semantics", "RIGHT_ATOMIC_RESULT_SEMANTICS_INVALID"),
        ("left_bam_execution_order", "LEFT_BAM_EXECUTION_ORDER_INVALID"),
        ("right_bam_execution_order", "RIGHT_BAM_EXECUTION_ORDER_INVALID"),
        ("left_chain_consistency", "LEFT_CHAIN_INCONSISTENT"),
        ("right_chain_consistency", "RIGHT_CHAIN_INCONSISTENT"),
        ("left_normal_tpu_signature_consistency", "LEFT_NORMAL_TPU_LANDED"),
        ("right_normal_tpu_signature_consistency", "RIGHT_NORMAL_TPU_LANDED"),
        ("left_contact_info_consistency", "LEFT_CONTACT_INFO_INCONSISTENT"),
        ("right_contact_info_consistency", "RIGHT_CONTACT_INFO_INCONSISTENT"),
        ("protocol", "PROTOCOL_DIFF"),
    ]
    for check, status in priority:
        if not checks.get(check, True):
            return status
    return "DIFF"


def is_vote_reject_terminal_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if not is_vote_reject_mode(left) or not is_vote_reject_mode(right):
        return False
    if left.get("mode") != right.get("mode"):
        return False
    left_batches = batch_projection(left)
    right_batches = batch_projection(right)
    if len(left_batches) != 1 or len(right_batches) != 1:
        return False
    if left_batches != right_batches:
        return False
    expected = {
        "seq_id": left_batches[0].get("seq_id"),
        "status": "not_committed",
        "txns": None,
        "reason": "deserialization_error",
        "index": 0,
        "detail": "VOTE_TRANSACTION_FAILURE",
    }
    return left_batches[0] == expected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--ignore-cus-consumed",
        action="store_true",
        help="Ignore TransactionCommittedResult.cus_consumed differences, useful after documenting M-41.",
    )
    parser.add_argument(
        "--ignore-batch-result-order",
        action="store_true",
        help="Ignore ordering differences between otherwise equivalent AtomicTxnBatchResult sets.",
    )
    parser.add_argument(
        "--ignore-loaded-accounts-data-size",
        action="store_true",
        help="Ignore TransactionCommittedResult.loaded_accounts_data_size differences while still comparing per-tx success and payer deltas.",
    )
    parser.add_argument(
        "--ignore-chain-evidence",
        action="store_true",
        help="Ignore RPC block/signature landing evidence checks for targets without a comparable chain oracle.",
    )
    args = parser.parse_args()

    result = compare(
        load(args.left),
        load(args.right),
        ignore_cus_consumed=args.ignore_cus_consumed,
        ignore_batch_result_order=args.ignore_batch_result_order,
        ignore_loaded_accounts_data_size=args.ignore_loaded_accounts_data_size,
        ignore_chain_evidence=args.ignore_chain_evidence,
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        sys.stdout.write(text)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
