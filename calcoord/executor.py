"""Persistent simulated field executor.

The executor fails closed: before a begin or a new calibration write it asks the
coordinator whether the exact permit/worker/epoch/resources are still valid.
Accepted write receipts and stop facts are durable so a restarted simulator can
continue fencing old owners and resend only genuinely formed receipts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from .store import atomic_write_json, load_json


class Authorizer(Protocol):
    def authorize(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str],
        action: str,
    ) -> dict[str, Any]: ...


class SimulationExecutor:
    def __init__(
        self,
        path: str | Path | None = None,
        executor_id: str = "simulation-executor",
    ) -> None:
        self.path = Path(path) if path is not None else None
        if self.path is not None and self.path.exists():
            self.state = load_json(self.path, self._default_state(executor_id))
        else:
            self.state = self._default_state(executor_id)

    @staticmethod
    def _default_state(executor_id: str) -> dict[str, Any]:
        return {
            "version": 1,
            "executor_id": executor_id,
            "permits": {},
            "write_index": {},
            "calibration_table": [],
            "rejected": [],
        }

    def save(self) -> None:
        if self.path is not None:
            atomic_write_json(self.path, self.state)

    def reset(self) -> None:
        self.state = self._default_state(self.state.get("executor_id", "simulation-executor"))
        self.save()

    def _permit(self, permit_id: str, job: str | None = None) -> dict[str, Any]:
        permit = self.state["permits"].setdefault(
            permit_id,
            {
                "permit_id": permit_id,
                "job": job,
                "current_worker": None,
                "epoch": 1,
                "handover": None,
                "begins": [],
                "stops": [],
                "receipts": {},
            },
        )
        if job and not permit.get("job"):
            permit["job"] = job
        return permit

    # ------------------------------------------------------------------ begin

    def begin(
        self,
        now: int,
        job: str,
        worker: str,
        permit_id: str,
        resources: list[str],
        authorizer: Authorizer,
    ) -> dict[str, Any]:
        resources = sorted(set(resources))
        auth = authorizer.authorize(now, permit_id, job, worker, resources, "begin")
        permit = self._permit(permit_id, job)

        if not auth.get("allowed"):
            rejection = {
                "at": now,
                "kind": "begin",
                "job": job,
                "worker": worker,
                "permit_id": permit_id,
                "resources": resources,
                "reason": auth,
            }
            self.state["rejected"].append(rejection)
            permit.setdefault("rejected", []).append(rejection)
            self.save()
            return {"accepted": False, "authorization": auth}

        epoch = int(auth["epoch"])
        # Idempotent restart/redelivery: same durable begin fact is not duplicated.
        for begin in permit["begins"]:
            if (
                begin["worker"] == worker
                and begin["epoch"] == epoch
                and set(begin["resources"]) == set(resources)
            ):
                return {
                    "accepted": True,
                    "idempotent": True,
                    "begin": begin,
                    "authorization": auth,
                }

        fact = {
            "at": now,
            "job": job,
            "worker": worker,
            "epoch": epoch,
            "permit_id": permit_id,
            "resources": resources,
            "authorization_expires_at": auth.get("expires_at"),
        }
        permit["begins"].append(fact)
        permit["current_worker"] = worker
        permit["epoch"] = epoch
        self.save()
        return {"accepted": True, "begin": fact, "authorization": auth}

    def has_local_active_begin(
        self, permit_id: str, worker: str, epoch: int, resource: str, at: int
    ) -> bool:
        permit = self.state["permits"].get(permit_id)
        if not permit:
            return False
        for begin in permit["begins"]:
            if (
                begin["worker"] == worker
                and begin["epoch"] == epoch
                and resource in begin["resources"]
                and begin["at"] <= at
            ):
                stopped_at = self._resource_stopped_at(permit, worker, resource, epoch)
                if stopped_at is None or at < stopped_at:
                    return True
        return False

    def _resource_stopped_at(
        self, permit: dict[str, Any], worker: str, resource: str, epoch: int | None = None
    ) -> int | None:
        stopped_at: int | None = None
        for stop in permit.get("stops", []):
            if stop["worker"] != worker:
                continue
            if epoch is not None and stop.get("epoch") not in (None, epoch):
                continue
            if resource in stop.get("resources", []):
                candidate = int(stop["at"])
                stopped_at = candidate if stopped_at is None else min(stopped_at, candidate)
        return stopped_at

    # ------------------------------------------------------------------ writes

    def receive_record(
        self,
        now: int,
        job: str,
        worker: str,
        permit_id: str,
        resource: str,
        write_id: str,
        value: Any,
        authorizer: Authorizer,
        event_index: int | None = None,
    ) -> dict[str, Any]:
        if not isinstance(write_id, str) or not write_id:
            raise ValueError("write_id 必须是非空字符串")

        existing_receipt = self.find_receipt(write_id)
        if existing_receipt is not None:
            if (
                existing_receipt["permit_id"] != permit_id
                or existing_receipt["job"] != job
                or existing_receipt["resource"] != resource
                or existing_receipt["value"] != value
            ):
                reason = {"allowed": False, "code": "write_id_conflict"}
                self.state["rejected"].append(
                    {
                        "at": now,
                        "kind": "record",
                        "write_id": write_id,
                        "payload": {"job": job, "worker": worker, "permit_id": permit_id, "resource": resource, "value": value},
                        "reason": reason,
                    }
                )
                self.save()
                return {"accepted": False, "reason": reason}
            return {"accepted": True, "idempotent": True, "receipt": existing_receipt}

        auth = authorizer.authorize(now, permit_id, job, worker, [resource], "record")
        permit = self._permit(permit_id, job)
        if not auth.get("allowed"):
            rejection = {
                "at": now,
                "kind": "record",
                "job": job,
                "worker": worker,
                "permit_id": permit_id,
                "resource": resource,
                "write_id": write_id,
                "value": value,
                "reason": auth,
            }
            self.state["rejected"].append(rejection)
            permit.setdefault("rejected", []).append(rejection)
            self.save()
            return {"accepted": False, "reason": auth}

        epoch = int(auth["epoch"])
        if not self.has_local_active_begin(permit_id, worker, epoch, resource, now):
            reason = {
                "allowed": False,
                "code": "executor_start_fact_missing",
                "message": "执行端重启后没有找到匹配且未停止的开始事实",
            }
            rejection = {
                "at": now,
                "kind": "record",
                "write_id": write_id,
                "job": job,
                "worker": worker,
                "permit_id": permit_id,
                "resource": resource,
                "reason": reason,
            }
            self.state["rejected"].append(rejection)
            permit.setdefault("rejected", []).append(rejection)
            self.save()
            return {"accepted": False, "reason": reason}

        receipt = {
            "receipt_id": f"rcpt:{write_id}",
            "write_id": write_id,
            "job": job,
            "worker": worker,
            "permit_id": permit_id,
            "resource": resource,
            "value": value,
            "accepted_at": now,
            "accepted_index": event_index,
            "epoch": epoch,
            "valid_until": auth["expires_at"],
            "executor_id": self.state["executor_id"],
            "confirmed": True,
            "delivered": False,
            "delivered_at": None,
        }
        permit["receipts"][write_id] = receipt
        self.state["write_index"][write_id] = permit_id
        self.state["calibration_table"].append(
            {
                "write_id": write_id,
                "job": job,
                "permit_id": permit_id,
                "resource": resource,
                "value": value,
                "committed_at": now,
                "epoch": epoch,
            }
        )
        self.save()
        return {"accepted": True, "receipt": receipt}

    def find_receipt(self, write_id: str) -> dict[str, Any] | None:
        permit_id = self.state["write_index"].get(write_id)
        if permit_id:
            return self.state["permits"].get(permit_id, {}).get("receipts", {}).get(write_id)
        for permit in self.state["permits"].values():
            receipt = permit.get("receipts", {}).get(write_id)
            if receipt:
                return receipt
        return None

    def prepare_write_ack(
        self, write_id: str, job: str | None = None, permit_id: str | None = None
    ) -> dict[str, Any] | None:
        receipt = self.find_receipt(write_id)
        if receipt is None:
            return None
        if job is not None and receipt["job"] != job:
            return None
        if permit_id is not None and receipt["permit_id"] != permit_id:
            return None
        receipt["ack_requests"] = int(receipt.get("ack_requests", 0)) + 1
        self.save()
        return dict(receipt)

    def mark_ack_delivered(self, write_id: str, admitted_at: int) -> None:
        receipt = self.find_receipt(write_id)
        if receipt is not None:
            receipt["delivered"] = True
            receipt["delivered_at"] = admitted_at
            self.save()

    # ------------------------------------------------------------- handover/stop

    def apply_handover(
        self,
        now: int,
        permit_id: str,
        job: str,
        old_worker: str,
        new_worker: str,
        epoch: int,
    ) -> dict[str, Any]:
        permit = self._permit(permit_id, job)
        permit["handover"] = {
            "at": now,
            "from_worker": old_worker,
            "to_worker": new_worker,
            "epoch": epoch,
        }
        permit["current_worker"] = new_worker
        permit["epoch"] = epoch
        self.save()
        return permit["handover"]

    def apply_stop_fact(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str],
        epoch: int | None = None,
    ) -> dict[str, Any]:
        permit = self._permit(permit_id, job)
        fact = {
            "at": now,
            "job": job,
            "worker": worker,
            "epoch": epoch if epoch is not None else permit.get("epoch"),
            "resources": sorted(set(resources)),
        }
        permit["stops"].append(fact)
        self.save()
        return fact

    def inspection(self, job: str | None = None) -> dict[str, Any]:
        if job:
            permits = {
                pid: permit
                for pid, permit in self.state["permits"].items()
                if permit.get("job") == job
            }
            rejected = [item for item in self.state["rejected"] if item.get("job") == job]
            table = [item for item in self.state["calibration_table"] if item.get("job") == job]
            return {
                "executor_id": self.state["executor_id"],
                "permits": permits,
                "calibration_table": table,
                "rejected": rejected,
            }
        return self.state
