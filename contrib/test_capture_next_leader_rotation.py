#!/usr/bin/env python3

import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import capture_next_leader_rotation as capture


def minimal_slot_response(
    key: str = "query_detailed",
    slot: int = 100,
    response_id: int = 1000,
    completed_time_nanos: str = "200",
) -> dict:
    return {
        "topic": "slot",
        "key": key,
        "id": response_id,
        "value": {
            "publish": {
                "slot": slot,
                "completed_time_nanos": completed_time_nanos,
                "success_nonvote_transaction_cnt": 1,
                "failed_nonvote_transaction_cnt": 0,
                "success_vote_transaction_cnt": 2,
                "failed_vote_transaction_cnt": 0,
                "compute_units": 123,
                "transaction_fee": 5000,
                "priority_fee": 6000,
                "tips": 7000,
            },
            "waterfall": {
                "in": {
                    "quic": 10,
                    "udp": 2,
                    "gossip": 1,
                    "block_engine": 3,
                },
                "out": {},
            },
        },
    }


class FakeWebsocketJsonSession:
    scripts: list[list[dict]] = []
    sessions: list["FakeWebsocketJsonSession"] = []

    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self.rows = list(self.scripts.pop(0))
        self.sent_payloads: list[dict] = []
        self.sessions.append(self)

    def __enter__(self) -> "FakeWebsocketJsonSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def send_json(self, payload: dict) -> None:
        self.sent_payloads.append(payload)

    def read_json(self, timeout_sec: float):
        del timeout_sec
        if self.rows:
            return self.rows.pop(0)
        return None


class CaptureNextLeaderRotationTest(unittest.TestCase):
    def test_get_next_leader_rotation_falls_back_to_public_mainnet_schedule(self) -> None:
        calls: list[tuple[str, str, object]] = []

        def fake_rpc_call(rpc_url: str, method: str, params=None):
            calls.append((rpc_url, method, params))
            if method == "getIdentity":
                return {"identity": "validator"}
            if method == "getEpochInfo":
                return {
                    "absoluteSlot": 100,
                    "slotIndex": 10,
                    "slotsInEpoch": 100,
                }
            if method == "getGenesisHash":
                return capture.MAINNET_GENESIS_HASH
            if method == "getLeaderSchedule":
                if rpc_url == "http://local-rpc":
                    raise RuntimeError("local getLeaderSchedule failed")
                return {"validator": [20, 21, 22]}
            raise AssertionError(f"unexpected RPC method {method}")

        stderr = io.StringIO()
        with mock.patch.object(capture, "rpc_call", fake_rpc_call):
            with contextlib.redirect_stderr(stderr):
                result = capture.get_next_leader_rotation("http://local-rpc")

        self.assertEqual(result, ("validator", 100, 110, 112))
        self.assertIn("retrying via https://api.mainnet-beta.solana.com", stderr.getvalue())
        self.assertIn(("http://local-rpc", "getLeaderSchedule", [100, {"identity": "validator"}]), calls)
        self.assertIn(
            (
                capture.PUBLIC_MAINNET_RPC_URL,
                "getLeaderSchedule",
                [100, {"identity": "validator"}],
            ),
            calls,
        )

    def test_get_next_leader_rotation_uses_explicit_schedule_rpc_url(self) -> None:
        calls: list[tuple[str, str, object]] = []

        def fake_rpc_call(rpc_url: str, method: str, params=None):
            calls.append((rpc_url, method, params))
            if method == "getIdentity":
                return {"identity": "validator"}
            if method == "getEpochInfo":
                return {
                    "absoluteSlot": 100,
                    "slotIndex": 10,
                    "slotsInEpoch": 100,
                }
            if method == "getLeaderSchedule":
                if rpc_url == "http://local-rpc":
                    raise RuntimeError("local getLeaderSchedule failed")
                return {"validator": [30, 31]}
            raise AssertionError(f"unexpected RPC method {method}")

        stderr = io.StringIO()
        with mock.patch.object(capture, "rpc_call", fake_rpc_call):
            with contextlib.redirect_stderr(stderr):
                result = capture.get_next_leader_rotation(
                    "http://local-rpc",
                    leader_schedule_rpc_url="http://schedule-rpc",
                )

        self.assertEqual(result, ("validator", 100, 120, 121))
        self.assertIn("retrying via http://schedule-rpc", stderr.getvalue())
        self.assertNotIn(("http://local-rpc", "getGenesisHash", None), calls)
        self.assertIn(("http://schedule-rpc", "getLeaderSchedule", [100, {"identity": "validator"}]), calls)

    def test_get_slot_seconds_falls_back_to_secondary_rpc(self) -> None:
        calls: list[tuple[str, str, object]] = []

        def fake_rpc_call(rpc_url: str, method: str, params=None):
            calls.append((rpc_url, method, params))
            if method == "getRecentPerformanceSamples":
                if rpc_url == "http://local-rpc":
                    raise RuntimeError("local samples failed")
                return [{"numSlots": 100, "samplePeriodSecs": 40}]
            raise AssertionError(f"unexpected RPC method {method}")

        stderr = io.StringIO()
        with mock.patch.object(capture, "rpc_call", fake_rpc_call):
            with contextlib.redirect_stderr(stderr):
                slot_seconds = capture.get_slot_seconds(["http://local-rpc", "http://fallback-rpc"])

        self.assertEqual(slot_seconds, 0.4)
        self.assertIn("getRecentPerformanceSamples failed via http://local-rpc", stderr.getvalue())
        self.assertEqual(
            calls,
            [
                ("http://local-rpc", "getRecentPerformanceSamples", [1]),
                ("http://fallback-rpc", "getRecentPerformanceSamples", [1]),
            ],
        )

    def test_parse_slot_result_row_accepts_query_detailed_response(self) -> None:
        parsed = capture.parse_slot_result_row(minimal_slot_response("query_detailed", slot=101))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["slot"], 101)
        self.assertEqual(parsed["slot_metrics"]["non_vote_success"], 1)

    def test_parse_slot_result_row_accepts_legacy_query_response(self) -> None:
        parsed = capture.parse_slot_result_row(minimal_slot_response("query", slot=102))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["slot"], 102)
        self.assertEqual(parsed["sankey_nodes"]["block_engine"], 3)

    def test_parse_slot_result_row_ignores_non_slot_and_null_value_rows(self) -> None:
        self.assertIsNone(
            capture.parse_slot_result_row(
                {
                    "topic": "summary",
                    "key": "query_detailed",
                    "value": minimal_slot_response()["value"],
                }
            )
        )
        self.assertIsNone(capture.parse_slot_result_row({"topic": "slot", "key": "query_detailed", "value": None}))

    def test_scrape_websocket_slots_accepts_all_response_keys_and_modes(self) -> None:
        cases = (
            {"response_key": "query_detailed", "mode": "recent", "startup_time_nanos": "1"},
            {"response_key": "query", "mode": "recent", "startup_time_nanos": "1"},
            {"response_key": "query_detailed", "mode": "since-startup", "startup_time_nanos": "100"},
            {"response_key": "query", "mode": "since-startup", "startup_time_nanos": "100"},
        )

        for case in cases:
            with self.subTest(**case):
                FakeWebsocketJsonSession.scripts = [
                    [
                        {"topic": "summary", "key": "identity_key", "value": "validator"},
                        {
                            "topic": "summary",
                            "key": "startup_time_nanos",
                            "value": case["startup_time_nanos"],
                        },
                        {"topic": "summary", "key": "completed_slot", "value": 103},
                        {
                            "topic": "epoch",
                            "key": "new",
                            "value": {
                                "start_slot": 100,
                                "end_slot": 103,
                                "staked_pubkeys": ["validator"],
                                "leader_slots": [0],
                            },
                        },
                    ],
                    [minimal_slot_response(case["response_key"], slot=100, response_id=1000)],
                ]
                FakeWebsocketJsonSession.sessions = []

                stderr = io.StringIO()
                with mock.patch.object(capture, "WebsocketJsonSession", FakeWebsocketJsonSession):
                    with contextlib.redirect_stderr(stderr):
                        rows = capture.scrape_websocket_slots(
                            websocket_url="ws://example.test/websocket",
                            mode=case["mode"],
                            recent_count=1,
                            snapshot_secs=1,
                            query_wait_secs=0,
                            detail_timeout_secs=1,
                        )

                slot_rows = [row for row in rows if row.get("record_type") == "slot"]
                self.assertEqual(len(slot_rows), 1)
                self.assertEqual(slot_rows[0]["slot"], 100)
                self.assertEqual(stderr.getvalue(), "")
                self.assertEqual(FakeWebsocketJsonSession.sessions[1].sent_payloads[0]["key"], "query_detailed")


if __name__ == "__main__":
    unittest.main()
