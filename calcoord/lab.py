"""In-process wiring between the coordinator and simulated executor."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .executor import SimulationExecutor
from .service import Coordinator
from .store import CalCoordError, atomic_write_json, load_json


class Lab:
    """A state directory containing the two independently restartable roles."""

    def __init__(self, state_dir: str | Path) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.coordinator = Coordinator(self.state_dir / "coordinator-state.json")
        self.executor = SimulationExecutor(self.state_dir / "executor-state.json")

    @classmethod
    def load(cls, state_dir: str | Path) -> "Lab":
        return cls(state_dir)

    def save(self) -> None:
        self.coordinator.save()
        self.executor.save()

    def handle_event(self, event: dict[str, Any], index: int | None = None) -> dict[str, Any]:
        event_type = event["type"]
        data = event.get("data", {})
        now = int(event["timestamp"])
        # Logical time is advanced for every ordered event, not only "advance".
        self.coordinator.tick(now)
        try:
            handler = getattr(self, f"_event_{event_type}", None)
            if handler is None:
                raise CalCoordError(f"未实现事件类型: {event_type}")
            result = handler(now, data, index)
            self.save()
            return {"ok": True, "type": event_type, "timestamp": now, "index": index, "result": result}
        except CalCoordError as error:
            self.save()
            return {
                "ok": False,
                "type": event_type,
                "timestamp": now,
                "index": index,
                "error": str(error),
                "code": getattr(error, "code", "validation_error"),
                "details": getattr(error, "details", {}),
            }

    def _event_resource(self, now, data, index):
        return self.coordinator.register_resource(
            now, data["resource"], data.get("parent"), event_index=index
        )

    def _event_acquire(self, now, data, index):
        return self.coordinator.acquire(
            now,
            data["job"],
            data["worker"],
            data["uses"],
            int(data["ttl"]),
            event_index=index,
        )

    def _event_renew(self, now, data, index):
        return self.coordinator.renew(now, data["job"], data["permit"], int(data["value"]))

    def _event_handover(self, now, data, index):
        permit_id = data["permit"]
        job = data["job"]
        new_worker = data["worker"]
        decision = self.coordinator.register_handover(
            now, permit_id, job, new_worker, event_index=index
        )
        if decision.get("accepted") and decision.get("handover"):
            handover = decision["handover"]
            self.executor.apply_handover(
                now,
                permit_id,
                job,
                handover["from_worker"],
                handover["to_worker"],
                handover["epoch"],
            )
        return decision

    def _event_begin(self, now, data, index):
        result = self.executor.begin(
            now,
            data["job"],
            data["worker"],
            data["permit"],
            list(data["resources"]),
            self.coordinator,
        )
        if result.get("accepted"):
            auth = result["authorization"]
            self.coordinator.mark_started(
                now,
                data["permit"],
                data["job"],
                data["worker"],
                list(data["resources"]),
                auth,
                event_index=index,
            )
            result["coordinator_started"] = True
        return result

    def _event_record(self, now, data, index):
        payload = {
            "job": data["job"],
            "worker": data["worker"],
            "permit_id": data["permit"],
            "resource": data["resource"],
            "write_id": data["write_id"],
            "value": data.get("value"),
        }
        result = self.executor.receive_record(
            now=now,
            job=payload["job"],
            worker=payload["worker"],
            permit_id=payload["permit_id"],
            resource=payload["resource"],
            write_id=payload["write_id"],
            value=payload["value"],
            authorizer=self.coordinator,
            event_index=index,
        )
        if not result.get("accepted"):
            self.coordinator.record_write_denial(
                now,
                payload,
                result.get("reason", {"code": "record_rejected"}),
            )
        return result

    def _event_write_ack(self, now, data, index):
        write_id = data["write_id"]
        receipt = self.executor.prepare_write_ack(
            write_id, job=data.get("job"), permit_id=data.get("permit")
        )
        if receipt is None:
            reason = {
                "code": "executor_receipt_missing",
                "message": "执行端没有实际形成并持久化该 write_id 的接纳回执",
            }
            self.coordinator.record_write_denial(now, data, reason)
            return {"accepted": False, "write_id": write_id, **reason}

        decision = self.coordinator.accept_write_ack(
            now, receipt, verified_executor_receipt=True
        )
        if decision.get("accepted"):
            self.executor.mark_ack_delivered(write_id, now)
        return decision

    def _stop_epoch_for_worker(self, permit_id: str, worker: str) -> int | None:
        permit = self.coordinator.state["permits"].get(permit_id)
        if not permit:
            return None
        for ownership in reversed(permit.get("ownership", [])):
            if ownership["worker"] == worker:
                return int(ownership["epoch"])
        return int(permit["epoch"])

    def _event_stop_ack(self, now, data, index):
        decision = self.coordinator.apply_stop_ack(
            now,
            data["permit"],
            data["job"],
            data["worker"],
            list(data["resources"]),
            event_index=index,
        )
        epoch = self._stop_epoch_for_worker(data["permit"], data["worker"])
        self.executor.apply_stop_fact(
            now,
            data["permit"],
            data["job"],
            data["worker"],
            list(data["resources"]),
            epoch=epoch,
        )
        return decision

    def _event_advance(self, now, data, index):
        return {"advanced_to": now}

    def _event_inspect(self, now, data, index):
        view = self.coordinator.inspect(job=data.get("job"), resource=data.get("resource"))
        if data.get("job"):
            view["simulation_executor"] = self.executor.inspection(job=data["job"])
        return view


class ReplayRunner:
    def __init__(self, trace_path: str | Path, state_dir: str | Path) -> None:
        self.trace_path = Path(trace_path).resolve()
        self.state_dir = Path(state_dir)
        self.progress_path = self.state_dir / "replay-state.json"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.progress = load_json(
            self.progress_path,
            {"trace": str(self.trace_path), "processed": 0},
        )
        if self.progress.get("trace") != str(self.trace_path):
            raise CalCoordError(
                "state directory was used for another trace; choose a new --state-dir or --reset"
            )

    def _read_events(self) -> list[dict[str, Any]]:
        # Reuse the project's trace envelope checker.
        project_root = Path(__file__).resolve().parent.parent
        import sys

        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from trace_io import read_trace

        allowed_path = project_root / "data" / "event_types.json"
        allowed = set(load_json(allowed_path, []))
        return list(read_trace(self.trace_path, allowed))

    def run(
        self, max_events: int | None = None, restart_each_event: bool = False
    ) -> list[dict[str, Any]]:
        events = self._read_events()
        start = int(self.progress.get("processed", 0))
        if start > len(events):
            raise CalCoordError("replay state is ahead of trace file")
        stop = len(events) if max_events is None else min(len(events), start + max_events)
        outputs: list[dict[str, Any]] = []
        lab = Lab(self.state_dir)
        for index in range(start, stop):
            if restart_each_event and index > start:
                # Reconstruct both role objects from their durable JSON files.
                lab = Lab(self.state_dir)
            result = lab.handle_event(events[index], index=index)
            outputs.append(result)
            self.progress["processed"] = index + 1
            atomic_write_json(self.progress_path, self.progress)
            if restart_each_event:
                lab.save()
        return outputs
