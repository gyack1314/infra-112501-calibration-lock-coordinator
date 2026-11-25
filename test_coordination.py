"""Behavior tests for fair hierarchical leases, takeover and persistence."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from calcoord import Coordinator, Lab, ReplayRunner, SimulationExecutor
from calcoord.http_api import CoordinatorClient, serve_coordinator, serve_executor


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def resource(coordinator: Coordinator, name: str, parent: str | None = None, at: int = 0):
    return coordinator.register_resource(at, name, parent)


class CoordinatorFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.coord = Coordinator(self.dir / "service.json")
        for name, parent in [
            ("hall", None),
            ("line-a", "hall"),
            ("line-b", "hall"),
            ("a1", "line-a"),
            ("a2", "line-a"),
            ("b1", "line-b"),
            ("kit", None),
        ]:
            resource(self.coord, name, parent)

    def tearDown(self):
        self.tmp.cleanup()

    def test_hierarchy_read_sharing_and_exclusive_write(self):
        first = self.coord.acquire(1, "read-1", "r1", {"a1": "read"}, 10)
        second = self.coord.acquire(2, "read-2", "r2", {"line-a": "read"}, 10)
        self.assertEqual(first["status"], "granted")
        self.assertEqual(second["status"], "granted")

        writer = self.coord.acquire(3, "baseline", "w", {"line-a": "write"}, 10)
        self.assertEqual(writer["status"], "queued")
        self.assertEqual(writer["blockers"][0]["code"], "resource_busy")
        self.assertEqual(writer["blockers"][0]["conflict_resources"], ["a1"])
        self.assertEqual(writer["blockers"][1]["conflict_resources"], ["a1", "a2", "line-a"])

        self.coord.apply_stop_ack(4, first["permit_id"], "read-1", "r1", ["a1"])
        self.coord.apply_stop_ack(4, second["permit_id"], "read-2", "r2", ["a1", "a2", "line-a"])
        view = self.coord.inspect(job="baseline")
        self.assertEqual(view["requests"][0]["status"], "granted")
        self.assertEqual(view["requests"][0]["permit_id"], "permit:3")

    def test_fair_queue_unrelated_line_is_served_and_local_cannot_jump(self):
        spot = self.coord.acquire(1, "spot", "w1", {"a1": "write", "kit": "write"}, 6)
        self.coord.mark_started(
            2, spot["permit_id"], "spot", "w1", ["a1", "kit"], {"epoch": 1}
        )
        entire = self.coord.acquire(3, "entire-a", "w2", {"line-a": "write"}, 9)
        local_read = self.coord.acquire(4, "late-a", "w3", {"a2": "read"}, 3)
        other = self.coord.acquire(4, "other", "w4", {"line-b": "read"}, 3)

        self.assertEqual(spot["permit_id"], "permit:1")
        self.assertEqual(entire["status"], "queued")
        self.assertEqual(local_read["status"], "queued")
        self.assertEqual(other["status"], "granted")
        self.assertEqual(other["permit_id"], "permit:2")

        late_view = self.coord.inspect(job="late-a")
        self.assertEqual(late_view["requests"][0]["blockers"][0]["code"], "queued_ahead")
        self.assertEqual(late_view["requests"][0]["blockers"][0]["job"], "entire-a")

        renewal = self.coord.renew(5, "spot", "permit:1", 20)
        self.assertFalse(renewal["accepted"])
        self.assertEqual(renewal["code"], "queued_conflict")
        self.assertEqual(renewal["queued_blockers"][0]["job"], "entire-a")
        self.assertEqual(self.coord.state["permits"]["permit:1"]["expires_at"], 7)

        self.coord.tick(8)
        entire_view = self.coord.inspect(job="entire-a")
        blocker = entire_view["requests"][0]["blockers"][0]
        self.assertEqual(blocker["code"], "stop_not_acknowledged")
        self.assertEqual(blocker["unstopped_resources"], ["a1", "kit"])
        self.assertEqual(blocker["conflict_resources"], ["a1"])

        stop = self.coord.apply_stop_ack(9, "permit:1", "spot", "w1", ["a1", "kit"])
        self.assertTrue(stop["accepted"])
        self.assertIn("promoted", stop)
        self.assertEqual(stop["promoted"][0]["permit_id"], "permit:3")
        entire_view = self.coord.inspect(job="entire-a")
        self.assertEqual(entire_view["requests"][0]["status"], "granted")

        late_view = self.coord.inspect(job="late-a")
        self.assertEqual(late_view["requests"][0]["blockers"][0]["code"], "resource_busy")
        self.assertEqual(late_view["requests"][0]["blockers"][0]["permit_id"], "permit:3")

    def test_multi_resource_request_is_atomic_and_holds_no_partial_lock(self):
        self.coord.acquire(1, "kit-job", "kit-worker", {"kit": "write"}, 5)
        bridge = self.coord.acquire(
            2, "two-stations-kit", "bridge", {"a1": "write", "a2": "write", "kit": "write"}, 8
        )
        solo = self.coord.acquire(3, "solo-a1", "solo", {"a1": "write"}, 8)

        self.assertEqual(bridge["status"], "queued")
        self.assertTrue(all(b["conflict_resources"] == ["kit"] for b in bridge["blockers"]))
        self.assertEqual(solo["status"], "queued")
        self.assertEqual(solo["blockers"][0]["code"], "queued_ahead")
        self.assertEqual(solo["blockers"][0]["job"], "two-stations-kit")

        # Kit lease expires without a started operation; queue promotion is atomic.
        self.coord.tick(6)
        bridge_view = self.coord.inspect(job="two-stations-kit")
        self.assertEqual(bridge_view["requests"][0]["status"], "granted")
        permit = bridge_view["permits"][0]
        self.assertEqual(set(permit["covered_resources"]), {"a1", "a2", "kit"})

        solo_view = self.coord.inspect(job="solo-a1")
        self.assertEqual(solo_view["requests"][0]["status"], "queued")
        self.assertEqual(solo_view["requests"][0]["blockers"][0]["code"], "resource_busy")

    def test_partial_stop_ack_can_return_one_resource(self):
        permit = self.coord.acquire(
            1, "bundle", "w1", {"a1": "write", "kit": "write"}, 20
        )["permit_id"]
        self.coord.mark_started(2, permit, "bundle", "w1", ["a1", "kit"], {"epoch": 1})
        waiting = self.coord.acquire(3, "a1-only", "w2", {"a1": "write"}, 5)
        self.assertEqual(waiting["status"], "queued")

        stop = self.coord.apply_stop_ack(4, permit, "bundle", "w1", ["a1"])
        self.assertEqual(stop["unstopped_resources"], ["kit"])
        self.assertEqual(stop["promoted"][0]["job"], "a1-only")
        a1_view = self.coord.inspect(job="a1-only")
        self.assertEqual(a1_view["requests"][0]["status"], "granted")

        restart = self.coord.authorize(5, permit, "bundle", "w1", ["a1"], "begin")
        self.assertEqual(restart["code"], "resource_stopped")
        kit_request = self.coord.acquire(5, "kit-only", "w3", {"kit": "write"}, 5)
        self.assertEqual(kit_request["status"], "queued")

    def test_cannot_combine_partial_permits_into_line_exclusive(self):
        p1 = self.coord.acquire(1, "one", "w1", {"a1": "write"}, 10)["permit_id"]
        p2 = self.coord.acquire(1, "two", "w2", {"a2": "write"}, 10)["permit_id"]
        auth = self.coord.authorize(2, p1, "one", "w1", ["a1", "a2"], "begin")
        self.assertFalse(auth["allowed"])
        self.assertEqual(auth["code"], "resource_not_covered")

        line = self.coord.acquire(2, "line", "w3", {"line-a": "write"}, 10)
        self.assertEqual(line["status"], "queued")
        self.coord.apply_stop_ack(3, p1, "one", "w1", ["a1"])
        self.coord.apply_stop_ack(3, p2, "two", "w2", ["a2"])
        line_view = self.coord.inspect(job="line")
        self.assertEqual(line_view["requests"][0]["status"], "granted")
        line_pid = line_view["requests"][0]["permit_id"]
        auth = self.coord.authorize(4, line_pid, "line", "w3", ["a1", "a2"], "begin")
        self.assertTrue(auth["allowed"])


class TakeoverAndPersistenceTests(unittest.TestCase):
    def test_supplied_scenarios_and_restart(self):
        with TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / "state"
            outputs = ReplayRunner(
                DATA / "overlapping-and-unrelated.jsonl", state_dir
            ).run(restart_each_event=True)
            inspections = [o["result"] for o in outputs if o["type"] == "inspect"]
            self.assertEqual(inspections[0]["requests"][0]["wait_reasons"], ["stop_not_acknowledged"])
            self.assertEqual(inspections[1]["requests"][0]["status"], "granted")
            self.assertEqual(inspections[1]["requests"][0]["permit_id"], "permit:3")

            # A fresh process/object reloads the durable queue and permits.
            reloaded = Coordinator(state_dir / "coordinator-state.json")
            self.assertEqual(reloaded.state["permit_seq"], 3)
            self.assertEqual(reloaded.state["queue"], ["req:3"])
            self.assertTrue((state_dir / "executor-state.json").exists())

        with TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / "takeover"
            outputs = ReplayRunner(
                DATA / "takeover-and-delayed-record.jsonl", state_dir
            ).run(restart_each_event=True)
            inspection = next(o["result"] for o in outputs if o["type"] == "inspect")
            permit = inspection["permits"][0]
            self.assertEqual(permit["current_worker"], "w2")
            self.assertEqual(permit["epoch"], 2)
            self.assertTrue(permit["can_backup_begin"])
            self.assertEqual([w["write_id"] for w in inspection["recorded_writes"]], ["delta-1"])
            self.assertEqual(inspection["recorded_writes"][0]["accepted_at"], 3)
            self.assertEqual(inspection["recorded_writes"][0]["admitted_at"], 6)
            self.assertEqual(
                inspection["simulation_executor"]["calibration_table"][0]["write_id"], "delta-1"
            )

            lab = Lab(state_dir)
            before_stop = lab.coordinator.authorize(
                8, "permit:1", "shift", "w2", ["a1"], "begin"
            )
            # Stop already happened at 7 in trace, so after restart backup may start.
            self.assertTrue(before_stop["allowed"])
            begin = lab.handle_event(
                {
                    "timestamp": 9,
                    "type": "begin",
                    "data": {
                        "job": "shift",
                        "worker": "w2",
                        "permit": "permit:1",
                        "resources": ["a1"],
                    },
                }
            )
            self.assertTrue(begin["ok"], begin)
            record = lab.handle_event(
                {
                    "timestamp": 10,
                    "type": "record",
                    "data": {
                        "job": "shift",
                        "worker": "w2",
                        "permit": "permit:1",
                        "resource": "a1",
                        "write_id": "delta-3",
                        "value": 21,
                    },
                }
            )
            self.assertTrue(record["result"]["accepted"])

            old_record = lab.handle_event(
                {
                    "timestamp": 11,
                    "type": "record",
                    "data": {
                        "job": "shift",
                        "worker": "w1",
                        "permit": "permit:1",
                        "resource": "a1",
                        "write_id": "delta-4",
                        "value": 99,
                    },
                }
            )
            self.assertFalse(old_record["result"]["accepted"])
            self.assertEqual(old_record["result"]["reason"]["code"], "old_owner_fenced")

    def test_backup_cannot_start_before_stop_and_unconfirmed_write_is_rejected(self):
        with TemporaryDirectory() as temporary:
            lab = Lab(Path(temporary) / "lab")
            coord = lab.coordinator
            coord.register_resource(0, "a1", None)
            coord.acquire(1, "job", "w1", {"a1": "write"}, 15)
            lab.handle_event(
                {"timestamp": 2, "type": "begin", "data": {
                    "job": "job", "worker": "w1", "permit": "permit:1", "resources": ["a1"]
                }}
            )
            handover = lab.handle_event(
                {"timestamp": 3, "type": "handover", "data": {
                    "job": "job", "worker": "w2", "permit": "permit:1"
                }}
            )
            self.assertTrue(handover["ok"])

            denied_begin = coord.authorize(4, "permit:1", "job", "w2", ["a1"], "begin")
            self.assertFalse(denied_begin["allowed"])
            self.assertEqual(denied_begin["code"], "takeover_stop_pending")

            unknown_ack = lab.handle_event(
                {"timestamp": 5, "type": "write_ack", "data": {
                    "job": "job", "worker": "w1", "permit": "permit:1",
                    "resource": "a1", "write_id": "never-formed"
                }}
            )
            self.assertFalse(unknown_ack["result"]["accepted"])
            self.assertEqual(unknown_ack["result"]["code"], "executor_receipt_missing")

            lab.handle_event(
                {"timestamp": 6, "type": "stop_ack", "data": {
                    "job": "job", "worker": "w1", "permit": "permit:1", "resources": ["a1"]
                }}
            )
            allowed = coord.authorize(7, "permit:1", "job", "w2", ["a1"], "begin")
            self.assertTrue(allowed["allowed"])

    def test_expired_handover_requires_new_permit_after_stop(self):
        with TemporaryDirectory() as temporary:
            lab = Lab(Path(temporary) / "expired-handover")
            coord = lab.coordinator
            coord.register_resource(0, "a1", None)
            coord.acquire(1, "job", "w1", {"a1": "write"}, 3)
            lab.handle_event(
                {"timestamp": 2, "type": "begin", "data": {
                    "job": "job", "worker": "w1", "permit": "permit:1", "resources": ["a1"]
                }}
            )
            coord.register_handover(3, "permit:1", "job", "w2")
            queued = coord.acquire(3, "next-writer", "w3", {"a1": "write"}, 5)
            self.assertEqual(queued["blockers"][0]["code"], "handover_stop_pending")
            coord.tick(4)
            stop = coord.apply_stop_ack(5, "permit:1", "job", "w1", ["a1"])
            self.assertTrue(stop["new_acquire_required"])
            self.assertEqual(stop["promoted"][0]["job"], "next-writer")
            denied = coord.authorize(6, "permit:1", "job", "w2", ["a1"], "begin")
            self.assertEqual(denied["code"], "permit_expired")

    def test_staged_replay_resumes_from_persisted_offset(self):
        with TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / "staged"
            runner = ReplayRunner(DATA / "takeover-and-delayed-record.jsonl", state_dir)
            first = runner.run(max_events=5)
            self.assertEqual(len(first), 5)
            progress = json.loads((state_dir / "replay-state.json").read_text(encoding="utf-8"))
            self.assertEqual(progress["processed"], 5)

            # New runner/process resumes rather than replaying acquire.
            second_runner = ReplayRunner(DATA / "takeover-and-delayed-record.jsonl", state_dir)
            rest = second_runner.run()
            self.assertEqual(len(rest), 5)  # 10 total events, first five processed.
            coord = Coordinator(state_dir / "coordinator-state.json")
            self.assertEqual(coord.state["permits"]["permit:1"]["epoch"], 2)


class ScenarioMatrixTests(unittest.TestCase):
    def test_every_trace_replays_with_reload_after_each_event(self):
        traces = sorted(DATA.glob("*.jsonl"))
        self.assertGreaterEqual(len(traces), 4)
        for trace in traces:
            with self.subTest(trace=trace.name), TemporaryDirectory() as temporary:
                outputs = ReplayRunner(trace, Path(temporary) / "state").run(
                    restart_each_event=True
                )
                self.assertTrue(outputs)
                unexpected = [row for row in outputs if not row["ok"]]
                self.assertEqual(unexpected, [])

    def test_atomic_kit_trace_keeps_fair_reservation(self):
        with TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / "atomic"
            ReplayRunner(DATA / "atomic-kit-and-fair-reservation.jsonl", state_dir).run(
                restart_each_event=True
            )
            coord = Coordinator(state_dir / "coordinator-state.json")
            self.assertEqual(coord.state["queue"], ["req:3"])
            bridge = coord.state["requests"][1]
            self.assertEqual(bridge["status"], "granted")
            self.assertEqual(set(bridge["scopes"][i]["root"] for i in range(3)), {"a1", "a2", "kit"})

    def test_backup_trace_records_only_confirmed_writes(self):
        with TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / "backup"
            ReplayRunner(DATA / "backup-completes-after-restart.jsonl", state_dir).run(
                restart_each_event=True
            )
            coord = Coordinator(state_dir / "coordinator-state.json")
            self.assertEqual({row["write_id"] for row in coord.state["ledger"]}, {"delta-1", "delta-3"})
            executor = SimulationExecutor(state_dir / "executor-state.json")
            self.assertEqual(
                {row["write_id"] for row in executor.state["calibration_table"]},
                {"delta-1", "delta-3"},
            )
            self.assertTrue(
                any(item["reason"]["code"] == "old_owner_fenced" for item in executor.state["rejected"])
            )


class HttpEntryTests(unittest.TestCase):
    def setUp(self):
        import threading

        self.tmp = TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self.coord_httpd = serve_coordinator(self.state_dir, "127.0.0.1", 0, verbose=False)
        self.coord_port = self.coord_httpd.server_address[1]
        self.coord_thread = threading.Thread(target=self.coord_httpd.serve_forever, daemon=True)
        self.coord_thread.start()
        self.service_url = f"http://127.0.0.1:{self.coord_port}"
        self.client = CoordinatorClient(self.service_url)
        self.exec_httpd = serve_executor(
            self.state_dir, self.service_url, "127.0.0.1", 0, verbose=False
        )
        self.exec_port = self.exec_httpd.server_address[1]
        self.exec_thread = threading.Thread(target=self.exec_httpd.serve_forever, daemon=True)
        self.exec_thread.start()

    def tearDown(self):
        self.coord_httpd.shutdown()
        self.exec_httpd.shutdown()
        self.coord_httpd.server_close()
        self.exec_httpd.server_close()
        self.tmp.cleanup()

    def post_executor(self, path, payload):
        import urllib.request

        request = urllib.request.Request(
            f"http://127.0.0.1:{self.exec_port}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_http_begin_record_restart_executor_then_ack(self):
        self.client.post("register_resource", now=0, name="a1", parent=None)
        granted = self.client.post(
            "acquire", now=1, job="http-job", worker="w1", uses={"a1": "write"}, ttl=10
        )
        self.assertEqual(granted["status"], "granted")
        begun = self.post_executor(
            "/v1/executor/begin",
            {"now": 2, "job": "http-job", "worker": "w1", "permit_id": "permit:1", "resources": ["a1"]},
        )
        self.assertTrue(begun["accepted"])
        recorded = self.post_executor(
            "/v1/executor/record",
            {
                "now": 3,
                "job": "http-job",
                "worker": "w1",
                "permit_id": "permit:1",
                "resource": "a1",
                "write_id": "http-write",
                "value": 7,
            },
        )
        self.assertTrue(recorded["accepted"])

        # Restart only the simulator process/object from executor-state.json.
        self.exec_httpd.shutdown()
        self.exec_httpd.server_close()
        from calcoord.http_api import serve_executor

        restarted = serve_executor(self.state_dir, self.service_url, "127.0.0.1", 0, verbose=False)
        self.exec_port = restarted.server_address[1]
        import threading

        self.exec_httpd = restarted
        self.exec_thread = threading.Thread(target=restarted.serve_forever, daemon=True)
        self.exec_thread.start()

        ack = self.post_executor(
            "/v1/executor/write-ack",
            {"now": 4, "job": "http-job", "permit_id": "permit:1", "write_id": "http-write"},
        )
        self.assertTrue(ack["accepted"])
        view = self.client.get_inspect(job="http-job")
        self.assertEqual([row["write_id"] for row in view["recorded_writes"]], ["http-write"])


if __name__ == "__main__":
    unittest.main()
