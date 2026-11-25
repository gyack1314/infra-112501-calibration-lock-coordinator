"""Resource hierarchy, fair queueing, leases, fencing and coordinator ledger.

The coordinator is intentionally persistence-only and uses logical integer time.
The event replay and HTTP layer are thin adapters over this class.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .store import (
    PermissionDenied,
    ValidationError,
    atomic_write_json,
    load_json,
)


MODES = ("read", "write")


def _conflict_resources(scopes_a: list[dict[str, Any]], scopes_b: list[dict[str, Any]]):
    """Return mode-conflicting scope intersections.

    Read/read overlaps are allowed.  Any overlap touching a write scope is
    exclusive and therefore conflicts.
    """

    conflicts: list[dict[str, Any]] = []
    all_resources: set[str] = set()
    for left in scopes_a:
        left_resources = set(left["resources"])
        for right in scopes_b:
            intersection = sorted(left_resources & set(right["resources"]))
            if intersection and (left["mode"] == "write" or right["mode"] == "write"):
                conflicts.append(
                    {
                        "resources": intersection,
                        "left_root": left["root"],
                        "left_mode": left["mode"],
                        "right_root": right["root"],
                        "right_mode": right["mode"],
                    }
                )
                all_resources.update(intersection)
    if not conflicts:
        return None
    return {"pairs": conflicts, "resources": sorted(all_resources)}


def _raw_intersection(scopes_a, scopes_b):
    resources: set[str] = set()
    for left in scopes_a:
        for right in scopes_b:
            resources.update(set(left["resources"]) & set(right["resources"]))
    return sorted(resources)


class Coordinator:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else None
        if self.path is not None and self.path.exists():
            self.state = load_json(self.path, self._default_state())
        else:
            self.state = self._default_state()

    @staticmethod
    def _default_state() -> dict[str, Any]:
        return {
            "version": 1,
            "clock": 0,
            "resources": {},
            "request_seq": 0,
            "permit_seq": 0,
            "requests": [],
            "queue": [],
            "permits": {},
            "ledger": [],
            "denied_writes": [],
            "audit": [],
        }

    def save(self) -> None:
        if self.path is not None:
            atomic_write_json(self.path, self.state)

    def reset(self) -> None:
        self.state = self._default_state()
        self.save()

    def _audit(self, now: int | None, kind: str, **details: Any) -> None:
        self.state.setdefault("audit", []).append(
            {"at": self.state["clock"] if now is None else now, "kind": kind, **details}
        )

    # ------------------------------------------------------------------ resources

    def register_resource(
        self, now: int, name: str, parent: str | None = None, event_index: int | None = None
    ) -> dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise ValidationError("resource 名称必须是非空字符串")
        if parent is not None and (not isinstance(parent, str) or not parent):
            raise ValidationError("parent 必须是 null 或非空字符串")
        existing = self.state["resources"].get(name)
        if existing is not None:
            if existing.get("parent") != parent:
                raise ValidationError(f"resource {name} 的 parent 与已登记拓扑不一致")
            return {"resource": name, "parent": parent, "idempotent": True}
        self.state["resources"][name] = {"name": name, "parent": parent}
        self._audit(now, "resource_registered", resource=name, parent=parent, event_index=event_index)
        self.save()
        return {"resource": name, "parent": parent}

    def scope_resources(self, root: str) -> set[str]:
        """Return root and every currently registered descendant."""
        resources = self.state["resources"]
        result = {root} if root in resources else set()
        for name in resources:
            current: str | None = name
            seen: set[str] = set()
            while current is not None:
                if current == root:
                    result.add(name)
                    break
                if current in seen:  # Defensive against a malformed cycle.
                    break
                seen.add(current)
                record = resources.get(current)
                current = record.get("parent") if record else None
        return result

    def _make_scopes(self, uses: dict[str, str]) -> list[dict[str, Any]]:
        if not isinstance(uses, dict) or not uses:
            raise ValidationError("uses 必须是非空对象")
        scopes: list[dict[str, Any]] = []
        for root, mode in uses.items():
            if root not in self.state["resources"]:
                raise ValidationError(f"使用了未登记资源: {root}")
            if mode not in MODES:
                raise ValidationError(f"{root} 的模式必须是 read 或 write")
            members = sorted(self.scope_resources(root))
            scopes.append({"root": root, "mode": mode, "resources": members})
        return scopes

    def coverage_set(self, scopes: list[dict[str, Any]]) -> set[str]:
        result: set[str] = set()
        for scope in scopes:
            result.update(scope["resources"])
        return result

    def mode_for_resource(self, permit: dict[str, Any], resource: str) -> str | None:
        mode: str | None = None
        for scope in permit["scopes"]:
            if resource in scope["resources"]:
                if scope["mode"] == "write":
                    return "write"
                mode = "read"
        return mode

    # --------------------------------------------------------------------- time

    def tick(self, now: int) -> dict[str, Any]:
        if type(now) is not int or now < 0:
            raise ValidationError("逻辑时间必须是非负整数")
        if now < self.state["clock"]:
            raise ValidationError(
                f"逻辑时间倒退: {now} < 当前服务时钟 {self.state['clock']}"
            )
        self.state["clock"] = now
        expired: list[str] = []
        for permit in self.state["permits"].values():
            if permit["lease_state"] == "active" and now >= int(permit["expires_at"]):
                permit["lease_state"] = "expired"
                permit["expired_at"] = now
                expired.append(permit["id"])
                unstopped = self._unstopped_resources(permit)
                if not unstopped:
                    permit["occupancy_released"] = True
                    permit["released_at"] = now
                    permit["release_reason"] = permit.get("release_reason") or (
                        "expired_without_operation"
                        if not permit.get("runs")
                        else "expired_after_stop_ack"
                    )
                else:
                    permit["release_reason"] = "awaiting_stop_ack"
                self._audit(
                    now,
                    "permit_expired",
                    permit_id=permit["id"],
                    unstopped_resources=unstopped,
                )
        promoted = self._promote_queue(now)
        self.save()
        return {"now": now, "expired": expired, "promoted": promoted}

    # ------------------------------------------------------------ runs and stops

    def _unstopped_resources(self, permit: dict[str, Any]) -> list[str]:
        unstopped: set[str] = set()
        for run in permit.get("runs", []):
            stopped = set(run.get("stopped_resources", []))
            for resource in run.get("resources", []):
                if resource not in stopped:
                    unstopped.add(resource)
        return sorted(unstopped)

    def _lingering_scopes(self, permit: dict[str, Any]) -> list[dict[str, Any]]:
        scopes: list[dict[str, Any]] = []
        for resource in self._unstopped_resources(permit):
            mode = self.mode_for_resource(permit, resource) or "write"
            scopes.append({"root": f"operation:{resource}", "mode": mode, "resources": [resource]})
        return scopes

    def _occupancy_scopes(self, permit: dict[str, Any]) -> list[dict[str, Any]]:
        if permit.get("occupancy_released"):
            return []
        if permit.get("lease_state") == "active":
            scopes = permit["scopes"]
            handover = permit.get("handover")
            # A resource-level stop_ack can return individual resources while
            # another operation under the same multi-resource permit continues.
            # During a handover the whole permit remains reserved for the backup.
            stopped = set(permit.get("stopped_resources", []))
            if stopped and not handover:
                filtered: list[dict[str, Any]] = []
                for scope in scopes:
                    resources = [r for r in scope["resources"] if r not in stopped]
                    if resources:
                        filtered.append({**scope, "resources": resources})
                scopes = filtered
            return scopes
        return self._lingering_scopes(permit)

    def _has_active_run(
        self, permit: dict[str, Any], worker: str, epoch: int, resources: list[str]
    ) -> bool:
        wanted = set(resources)
        for run in permit.get("runs", []):
            if run.get("worker") != worker or run.get("epoch") != epoch:
                continue
            stopped = set(run.get("stopped_resources", []))
            active_resources = set(run.get("resources", [])) - stopped
            if wanted.issubset(active_resources):
                return True
        return False

    # ------------------------------------------------------------------ queue

    def _find_request(self, request_id: str) -> dict[str, Any] | None:
        for request in self.state["requests"]:
            if request["id"] == request_id:
                return request
        return None

    def _ahead_blockers(
        self, scopes: list[dict[str, Any]], seq: int, request_id: str | None
    ) -> list[dict[str, Any]]:
        blockers: list[dict[str, Any]] = []
        for earlier_id in self.state["queue"]:
            earlier = self._find_request(earlier_id)
            if earlier is None or earlier.get("status") != "queued":
                continue
            if earlier["seq"] >= seq or earlier["id"] == request_id:
                continue
            conflict = _conflict_resources(scopes, earlier["scopes"])
            if conflict is None:
                continue
            blockers.append(
                {
                    "type": "queued_ahead",
                    "code": "queued_ahead",
                    "request_id": earlier["id"],
                    "job": earlier["job"],
                    "worker": earlier["worker"],
                    "submitted_at": earlier["submitted_at"],
                    "conflict_resources": conflict["resources"],
                    "message": f"前面还有相交排队请求 {earlier['id']}（job={earlier['job']}）",
                }
            )
        return blockers

    def _occupancy_blockers(
        self, scopes: list[dict[str, Any]], now: int
    ) -> list[dict[str, Any]]:
        blockers: list[dict[str, Any]] = []
        for permit in self.state["permits"].values():
            occupancy = self._occupancy_scopes(permit)
            if not occupancy:
                continue
            conflict = _conflict_resources(scopes, occupancy)
            if conflict is None:
                continue
            unstopped = self._unstopped_resources(permit)
            handover = permit.get("handover")
            codes = ["resource_busy"]
            if unstopped:
                if permit["lease_state"] == "expired":
                    codes.append("stop_not_acknowledged")
                else:
                    codes.append("field_running")
            if handover and not handover.get("ready"):
                codes.append("handover_stop_pending")
            if permit["lease_state"] == "expired" and unstopped:
                primary = "stop_not_acknowledged"
                message = (
                    f"{permit['id']} 已到期且旧现场操作未确认停下: "
                    + ",".join(unstopped)
                )
            elif handover and not handover.get("ready") and unstopped:
                primary = "handover_stop_pending"
                message = (
                    f"{permit['id']} 已登记接替，但原执行端仍需确认停止: "
                    + ",".join(handover.get("waiting_for_resources", unstopped))
                )
            else:
                primary = "resource_busy"
                message = f"与现行许可 {permit['id']}（job={permit['job']}）冲突"
            blockers.append(
                {
                    "type": "permit",
                    "code": primary,
                    "codes": codes,
                    "permit_id": permit["id"],
                    "job": permit["job"],
                    "worker": permit.get("current_worker", permit.get("worker")),
                    "lease_state": permit["lease_state"],
                    "expires_at": permit["expires_at"],
                    "epoch": permit["epoch"],
                    "handover_state": (handover or {}).get("state"),
                    "handover_ready": bool(handover and handover.get("ready")),
                    "unstopped_resources": unstopped,
                    "conflict_resources": conflict["resources"],
                    "message": message,
                }
            )
        return blockers

    def evaluate(
        self,
        scopes: list[dict[str, Any]],
        seq: int = 10**12,
        request_id: str | None = None,
        now: int | None = None,
    ) -> dict[str, Any]:
        now = self.state["clock"] if now is None else now
        ahead = self._ahead_blockers(scopes, seq, request_id)
        busy = self._occupancy_blockers(scopes, now)
        blockers = ahead + busy
        return {"grantable": not blockers, "blockers": blockers}

    # ---------------------------------------------------------------- acquire

    def acquire(
        self,
        now: int,
        job: str,
        worker: str,
        uses: dict[str, str],
        ttl: int,
        event_index: int | None = None,
    ) -> dict[str, Any]:
        if not isinstance(job, str) or not job:
            raise ValidationError("job 必须是非空字符串")
        if not isinstance(worker, str) or not worker:
            raise ValidationError("worker 必须是非空字符串")
        if type(ttl) is not int or ttl <= 0:
            raise ValidationError("ttl 必须是正整数秒")
        self.tick(now)
        if event_index is not None:
            for existing_request in self.state["requests"]:
                if existing_request.get("event_index") == event_index:
                    decision = self.evaluate(
                        existing_request["scopes"],
                        existing_request["seq"],
                        existing_request["id"],
                        now,
                    )
                    return {
                        "request_id": existing_request["id"],
                        "status": existing_request["status"],
                        "permit_id": existing_request.get("permit_id"),
                        "expires_at": (
                            self.state["permits"][existing_request["permit_id"]]["expires_at"]
                            if existing_request.get("permit_id")
                            else None
                        ),
                        "blockers": [] if existing_request["status"] == "granted" else decision["blockers"],
                        "idempotent": True,
                    }
        scopes = self._make_scopes(uses)
        self.state["request_seq"] += 1
        seq = self.state["request_seq"]
        request_id = f"req:{seq}"
        request = {
            "id": request_id,
            "seq": seq,
            "job": job,
            "worker": worker,
            "uses": uses,
            "scopes": scopes,
            "ttl": ttl,
            "submitted_at": now,
            "status": "new",
            "permit_id": None,
            "event_index": event_index,
        }
        self.state["requests"].append(request)
        decision = self.evaluate(scopes, seq, request_id, now)
        if decision["grantable"]:
            permit = self._issue_permit(request, now)
            request["status"] = "granted"
            request["permit_id"] = permit["id"]
            result = {
                "request_id": request_id,
                "status": "granted",
                "permit_id": permit["id"],
                "expires_at": permit["expires_at"],
                "blockers": [],
            }
        else:
            request["status"] = "queued"
            self.state["queue"].append(request_id)
            request["queued_at"] = now
            result = {
                "request_id": request_id,
                "status": "queued",
                "permit_id": None,
                "blockers": decision["blockers"],
            }
        self._audit(now, "acquire_" + result["status"], **{k: v for k, v in result.items() if k != "blockers"}, job=job)
        self.save()
        return result

    def _issue_permit(self, request: dict[str, Any], now: int) -> dict[str, Any]:
        self.state["permit_seq"] += 1
        seq = self.state["permit_seq"]
        permit_id = f"permit:{seq}"
        permit = {
            "id": permit_id,
            "seq": seq,
            "request_id": request["id"],
            "job": request["job"],
            "original_worker": request["worker"],
            "current_worker": request["worker"],
            "uses": request["uses"],
            "scopes": request["scopes"],
            "issued_at": now,
            "ttl": request["ttl"],
            "expires_at": now + int(request["ttl"]),
            "lease_state": "active",
            "occupancy_released": False,
            "released_at": None,
            "release_reason": None,
            "epoch": 1,
            "ownership": [
                {"epoch": 1, "worker": request["worker"], "from_at": now, "until_at": None}
            ],
            "handover": None,
            "runs": [],
            "stop_acks": [],
            "stopped_resources": [],
            "renewals": [],
            "ledger_write_ids": [],
            "event_index": request.get("event_index"),
        }
        self.state["permits"][permit_id] = permit
        return permit

    def _promote_queue(self, now: int) -> list[dict[str, Any]]:
        promoted: list[dict[str, Any]] = []
        for request_id in list(self.state["queue"]):
            request = self._find_request(request_id)
            if request is None or request.get("status") != "queued":
                if request_id in self.state["queue"]:
                    self.state["queue"].remove(request_id)
                continue
            decision = self.evaluate(request["scopes"], request["seq"], request["id"], now)
            if decision["grantable"]:
                permit = self._issue_permit(request, now)
                request["status"] = "granted"
                request["permit_id"] = permit["id"]
                request["granted_at"] = now
                self.state["queue"].remove(request_id)
                promoted.append(
                    {"request_id": request_id, "job": request["job"], "permit_id": permit["id"]}
                )
                self._audit(
                    now,
                    "queue_promoted",
                    request_id=request_id,
                    job=request["job"],
                    permit_id=permit["id"],
                )
        return promoted

    # ----------------------------------------------------------------- renew

    def renew(
        self, now: int, job: str, permit_id: str, value: int
    ) -> dict[str, Any]:
        self.tick(now)
        permit = self.state["permits"].get(permit_id)
        if permit is None:
            raise ValidationError(f"未知许可 {permit_id}")
        if permit["job"] != job:
            raise ValidationError(f"{permit_id} 不属于 job {job}")
        if type(value) is not int or value <= now:
            decision = {
                "accepted": False,
                "code": "invalid_expiry",
                "message": "续期目标必须是大于当前逻辑时间的整数秒点",
                "permit_id": permit_id,
                "expires_at": permit["expires_at"],
            }
            permit["renewals"].append({"at": now, "requested_expires_at": value, **decision})
            self.save()
            return decision

        queued_blockers: list[dict[str, Any]] = []
        for request_id in self.state["queue"]:
            request = self._find_request(request_id)
            if request is None or request["job"] == permit["job"]:
                continue
            raw_resources = _raw_intersection(permit["scopes"], request["scopes"])
            if raw_resources:
                mode_conflict = _conflict_resources(permit["scopes"], request["scopes"])
                queued_blockers.append(
                    {
                        "request_id": request["id"],
                        "job": request["job"],
                        "worker": request["worker"],
                        "intersection_resources": raw_resources,
                        "conflict_resources": (
                            mode_conflict["resources"] if mode_conflict else []
                        ),
                        "mode_conflict": bool(mode_conflict),
                        "message": f"相交排队请求 {request['id']} 已先进入队列",
                    }
                )

        if permit["lease_state"] == "expired" or permit.get("occupancy_released"):
            decision = {
                "accepted": False,
                "code": "permit_closed",
                "message": "许可已到期或已停止释放，不能续期；请重新 acquire",
                "permit_id": permit_id,
                "expires_at": permit["expires_at"],
                "queued_blockers": queued_blockers,
            }
        elif queued_blockers and value > int(permit["expires_at"]):
            decision = {
                "accepted": False,
                "code": "queued_conflict",
                "message": "已有相交排队请求，不能借续期延长并越过队列",
                "permit_id": permit_id,
                "expires_at": permit["expires_at"],
                "requested_expires_at": value,
                "queued_blockers": queued_blockers,
            }
        else:
            old_expiry = permit["expires_at"]
            permit["expires_at"] = value
            decision = {
                "accepted": True,
                "code": "renewed",
                "message": "许可已续期",
                "permit_id": permit_id,
                "old_expires_at": old_expiry,
                "expires_at": value,
                "queued_blockers": queued_blockers,
            }
        permit["renewals"].append(
            {
                "at": now,
                "requested_expires_at": value,
                "accepted": decision["accepted"],
                "code": decision["code"],
                "blockers": queued_blockers,
            }
        )
        self._audit(now, "renew_" + decision["code"], permit_id=permit_id, job=job)
        self.save()
        return decision

    # ------------------------------------------------------------- handover

    def register_handover(
        self,
        now: int,
        permit_id: str,
        job: str,
        new_worker: str,
        event_index: int | None = None,
    ) -> dict[str, Any]:
        self.tick(now)
        permit = self.state["permits"].get(permit_id)
        if permit is None:
            raise ValidationError(f"未知许可 {permit_id}")
        if permit["job"] != job:
            raise ValidationError(f"{permit_id} 不属于 job {job}")
        if not isinstance(new_worker, str) or not new_worker:
            raise ValidationError("替补 worker 必须是非空字符串")
        if permit["lease_state"] == "expired" or permit.get("occupancy_released"):
            result = {
                "accepted": False,
                "code": "permit_expired",
                "message": "许可已失效；替补必须重新申请当前资源的确切许可",
                "permit_id": permit_id,
            }
            self._audit(now, "handover_rejected", **result)
            self.save()
            return result

        existing = permit.get("handover")
        if existing:
            if existing["to_worker"] != new_worker:
                raise ValidationError(f"{permit_id} 已登记由 {existing['to_worker']} 接替")
            result = {
                "accepted": True,
                "idempotent": True,
                "permit_id": permit_id,
                "epoch": permit["epoch"],
                "handover": existing,
            }
            return result

        old_worker = permit["current_worker"]
        if new_worker == old_worker:
            return {
                "accepted": True,
                "idempotent": True,
                "permit_id": permit_id,
                "epoch": permit["epoch"],
                "handover": None,
            }

        for ownership in reversed(permit["ownership"]):
            if ownership["until_at"] is None:
                ownership["until_at"] = now
                ownership["until_event"] = event_index
                break
        permit["epoch"] += 1
        permit["current_worker"] = new_worker
        permit["ownership"].append(
            {
                "epoch": permit["epoch"],
                "worker": new_worker,
                "from_at": now,
                "from_event": event_index,
                "until_at": None,
                "until_event": None,
            }
        )
        unstopped = self._unstopped_resources(permit)
        handover = {
            "state": "registered",
            "from_worker": old_worker,
            "to_worker": new_worker,
            "epoch": permit["epoch"],
            "registered_at": now,
            "ready": False,
            "ready_at": None,
            "waiting_for_resources": unstopped,
            "stop_acks": [],
        }
        permit["handover"] = handover
        result = {
            "accepted": True,
            "permit_id": permit_id,
            "epoch": permit["epoch"],
            "handover": handover,
            "message": "接替已登记；新执行端开始前必须等待原执行端 stop_ack",
        }
        self._audit(now, "handover_registered", permit_id=permit_id, **{k: handover[k] for k in ("from_worker", "to_worker", "epoch")})
        self.save()
        return result

    # ------------------------------------------------------------ authorize

    def authorize(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str] | tuple[str, ...] | set[str],
        action: str,
    ) -> dict[str, Any]:
        self.tick(now)
        resource_list = sorted(set(resources))
        if not resource_list:
            return PermissionDenied("empty_resources", "必须声明至少一个资源").to_dict()
        permit = self.state["permits"].get(permit_id)
        if permit is None:
            return PermissionDenied("unknown_permit", f"未知许可 {permit_id}").to_dict()
        if permit["job"] != job:
            return PermissionDenied("job_mismatch", f"许可属于 {permit['job']}").to_dict()

        handover = permit.get("handover")
        current_worker = permit["current_worker"]

        if handover:
            if worker == handover.get("from_worker"):
                return PermissionDenied(
                    "old_owner_fenced",
                    "许可已经转交；旧执行端不得再发起新的开始或写入",
                    permit_id=permit_id,
                    current_worker=current_worker,
                    epoch=permit["epoch"],
                ).to_dict()
            if worker != handover.get("to_worker"):
                return PermissionDenied(
                    "worker_mismatch",
                    f"许可接替给 {handover.get('to_worker')}",
                    permit_id=permit_id,
                    current_worker=current_worker,
                ).to_dict()
            if permit["lease_state"] == "expired" or now >= int(permit["expires_at"]):
                return PermissionDenied(
                    "permit_expired",
                    "等待停止期间许可已到期；原操作停止后须重新申请确切的新许可",
                    permit_id=permit_id,
                    new_acquire_required=True,
                ).to_dict()
            if not handover.get("ready"):
                return PermissionDenied(
                    "takeover_stop_pending",
                    "替补已登记，但原执行端尚未确认停止相交动作",
                    permit_id=permit_id,
                    waiting_for_resources=handover.get("waiting_for_resources", []),
                ).to_dict()
        elif worker != current_worker:
            return PermissionDenied(
                "worker_mismatch",
                f"许可当前属于 {current_worker}",
                permit_id=permit_id,
                current_worker=current_worker,
            ).to_dict()

        if permit["lease_state"] == "expired" or now >= int(permit["expires_at"]):
            return PermissionDenied(
                "permit_expired",
                "许可已到期，不能发起新动作；已开始操作等待 stop_ack",
                permit_id=permit_id,
                expires_at=permit["expires_at"],
                unstopped_resources=self._unstopped_resources(permit),
            ).to_dict()
        if permit.get("occupancy_released"):
            return PermissionDenied(
                "permit_released", "许可已经停止/释放，不能继续发起动作", permit_id=permit_id
            ).to_dict()

        coverage = self.coverage_set(permit["scopes"])
        outside = [resource for resource in resource_list if resource not in coverage]
        if outside:
            return PermissionDenied(
                "resource_not_covered",
                "声明资源不在该确切许可覆盖范围内；不能用多个局部许可拼出全线独占",
                permit_id=permit_id,
                outside_resources=outside,
                covered_resources=sorted(coverage),
            ).to_dict()

        stopped_resources = set(permit.get("stopped_resources", []))
        stopped_requested = sorted(r for r in resource_list if r in stopped_resources)
        if stopped_requested and not handover:
            return PermissionDenied(
                "resource_stopped",
                "该资源已经 stop_ack 并交回；不能在原许可上重新发起动作",
                permit_id=permit_id,
                stopped_resources=stopped_requested,
            ).to_dict()

        epoch = permit["epoch"]
        if action in ("record", "write"):
            if self.mode_for_resource(permit, resource_list[0]) != "write":
                return PermissionDenied(
                    "write_mode_required", "该资源的许可模式不是 write", permit_id=permit_id
                ).to_dict()
            if not self._has_active_run(permit, worker, epoch, resource_list):
                return PermissionDenied(
                    "start_required",
                    "写入前没有找到同一 worker/epoch 的有效开始事实，或该资源已停止",
                    permit_id=permit_id,
                    worker=worker,
                    epoch=epoch,
                ).to_dict()

        return {
            "allowed": True,
            "code": "ok",
            "permit_id": permit_id,
            "job": job,
            "worker": worker,
            "epoch": epoch,
            "authorized_at": now,
            "expires_at": permit["expires_at"],
            "resources": resource_list,
        }

    def mark_started(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str],
        authorization: dict[str, Any] | None = None,
        event_index: int | None = None,
    ) -> dict[str, Any]:
        permit = self.state["permits"].get(permit_id)
        if permit is None or permit["job"] != job:
            raise ValidationError(f"开始事实引用了无效许可 {permit_id}/{job}")
        epoch = (authorization or {}).get("epoch", permit["epoch"])
        resource_list = sorted(set(resources))
        # Idempotent redelivery of the same start fact.
        for run in permit["runs"]:
            if (
                run["worker"] == worker
                and run["epoch"] == epoch
                and set(run["resources"]) == set(resource_list)
                and not run.get("stopped_resources")
            ):
                return {"recorded": True, "idempotent": True, "run_id": run["id"]}
        run_id = f"{permit_id}-run:{len(permit['runs']) + 1}"
        run = {
            "id": run_id,
            "worker": worker,
            "epoch": epoch,
            "resources": resource_list,
            "begun_at": now,
            "begun_event": event_index,
            "stopped_resources": [],
            "stops": [],
            "state": "running",
        }
        permit["runs"].append(run)
        self._audit(now, "execution_started", permit_id=permit_id, worker=worker, resources=resource_list)
        self.save()
        return {"recorded": True, "run_id": run_id, "epoch": epoch}

    # --------------------------------------------------------------- stop ack

    def apply_stop_ack(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str],
        event_index: int | None = None,
    ) -> dict[str, Any]:
        self.tick(now)
        permit = self.state["permits"].get(permit_id)
        if permit is None:
            raise ValidationError(f"未知许可 {permit_id}")
        if permit["job"] != job:
            raise ValidationError(f"{permit_id} 不属于 job {job}")
        resource_list = sorted(set(resources or []))
        if not resource_list:
            raise ValidationError("stop_ack 必须明确列出已停止资源")
        coverage = self.coverage_set(permit["scopes"])
        outside = [r for r in resource_list if r not in coverage]
        if outside:
            raise ValidationError(f"stop_ack 资源不在许可范围内: {outside}")

        handover = permit.get("handover")
        known_workers = {permit["original_worker"], permit["current_worker"]}
        if handover:
            known_workers.update({handover["from_worker"], handover["to_worker"]})
        for run in permit["runs"]:
            known_workers.add(run["worker"])
        if worker not in known_workers:
            raise PermissionDenied(
                "worker_mismatch", f"停止回执 worker {worker} 与许可/开始事实不符"
            )

        ack = {"at": now, "worker": worker, "resources": resource_list, "event_index": event_index}
        permit["stop_acks"].append(ack)

        stopped_set: set[str] = set()
        for resource in resource_list:
            stopped_set.update(self.scope_resources(resource))
        stopped_set &= coverage
        permit["stopped_resources"] = sorted(
            set(permit.get("stopped_resources", [])) | stopped_set
        )

        stopped_now: list[str] = []
        for run in permit["runs"]:
            if run["worker"] != worker:
                continue
            for resource in run.get("resources", []):
                if resource in stopped_set and resource not in run["stopped_resources"]:
                    run["stopped_resources"].append(resource)
                    stopped_now.append(resource)
                    run["stops"].append(
                        {"at": now, "event_index": event_index, "worker": worker, "resource": resource}
                    )
            if set(run["stopped_resources"]) >= set(run["resources"]):
                run["state"] = "stopped"
                run["stopped_at"] = now

        unstopped = self._unstopped_resources(permit)
        result: dict[str, Any] = {
            "accepted": True,
            "permit_id": permit_id,
            "stopped_now": sorted(set(stopped_now)),
            "unstopped_resources": unstopped,
        }

        if handover and not handover.get("ready"):
            handover["stop_acks"].append(ack)
            handover["waiting_for_resources"] = unstopped
            if not unstopped:
                if permit["lease_state"] == "expired" or now >= int(permit["expires_at"]):
                    handover["state"] = "lapsed_expired"
                    handover["lapsed_at"] = now
                    handover["ready"] = False
                    handover["can_begin"] = False
                    result["handover_ready"] = False
                    result["new_acquire_required"] = True
                    permit["occupancy_released"] = True
                    permit["released_at"] = now
                    permit["release_reason"] = "expired_then_old_stop_ack"
                    result["released"] = True
                else:
                    handover["state"] = "ready"
                    handover["ready"] = True
                    handover["ready_at"] = now
                    handover["can_begin"] = True
                    result["handover_ready"] = True
                    result["can_begin_worker"] = handover["to_worker"]
            else:
                result["handover_ready"] = False
        elif not handover:
            # A full stop confirmation ends this job's occupancy even before the
            # TTL.  Partial acks narrow the active occupancy via stopped_resources.
            result["all_operations_stopped"] = not unstopped
            if not unstopped:
                permit["occupancy_released"] = True
                permit["released_at"] = now
                permit["released_event"] = event_index
                permit["release_reason"] = "stop_ack"
                result["released"] = True
            else:
                result["permit_remains_active"] = permit["lease_state"] == "active"
                result["released_resources"] = sorted(stopped_set)

        if (
            not permit.get("occupancy_released")
            and permit["lease_state"] == "expired"
            and not unstopped
        ):
            permit["occupancy_released"] = True
            permit["released_at"] = now
            permit["released_event"] = event_index
            permit["release_reason"] = "expired_then_stop_ack"
            result["released"] = True

        # A partial ack can also make enough scope disjoint to promote a waiter
        # (for example a1 stopped while kit remains busy).
        promoted = self._promote_queue(now)
        if promoted:
            result["promoted"] = promoted
        self._audit(now, "stop_ack", permit_id=permit_id, worker=worker, resources=resource_list, unstopped=unstopped)
        self.save()
        return result

    def release_permit(
        self, now: int, permit_id: str, job: str | None = None, reason: str = "explicit_release"
    ) -> dict[str, Any]:
        """Explicitly surrender a lease when the job is known to be finished."""
        self.tick(now)
        permit = self.state["permits"].get(permit_id)
        if permit is None:
            raise ValidationError(f"未知许可 {permit_id}")
        if job is not None and permit["job"] != job:
            raise ValidationError(f"{permit_id} 不属于 job {job}")
        if permit.get("occupancy_released"):
            return {"accepted": True, "idempotent": True, "permit_id": permit_id}
        permit["occupancy_released"] = True
        permit["released_at"] = now
        permit["release_reason"] = reason
        unstopped = self._unstopped_resources(permit)
        result = {"accepted": True, "permit_id": permit_id, "unstopped_resources": unstopped}
        if unstopped:
            result["warning"] = "许可已释放，但仍有未确认停止的仿真操作"
        promoted = self._promote_queue(now)
        if promoted:
            result["promoted"] = promoted
        self._audit(now, "permit_released", permit_id=permit_id, reason=reason)
        self.save()
        return result

    # ------------------------------------------------------------- write ledger

    def record_write_denial(self, now: int, payload: dict[str, Any], reason: dict[str, Any]) -> None:
        entry = {"at": now, "payload": payload, "reason": reason}
        self.state["denied_writes"].append(entry)
        self._audit(now, "write_denied", write_id=payload.get("write_id"), code=reason.get("code"))
        self.save()

    def accept_write_ack(
        self, now: int, receipt: dict[str, Any], verified_executor_receipt: bool = False
    ) -> dict[str, Any]:
        required = ["write_id", "job", "worker", "permit_id", "resource", "accepted_at", "epoch", "value"]
        missing = [key for key in required if key not in receipt]
        if missing:
            result = {"accepted": False, "code": "malformed_receipt", "missing": missing}
            self.state["denied_writes"].append({"at": now, "receipt": receipt, "reason": result})
            self.save()
            return result

        write_id = receipt["write_id"]
        for existing in self.state["ledger"]:
            if existing["write_id"] == write_id:
                same = all(existing.get(k) == receipt.get(k) for k in ("job", "worker", "permit_id", "resource", "value", "accepted_at", "epoch"))
                if not same:
                    result = {"accepted": False, "code": "write_id_conflict", "write_id": write_id}
                    self.state["denied_writes"].append({"at": now, "receipt": receipt, "reason": result})
                    self.save()
                    return result
                return {"accepted": True, "idempotent": True, "entry": existing}

        if not verified_executor_receipt:
            result = {
                "accepted": False,
                "code": "unconfirmed_write",
                "message": "协调服务没有从仿真执行端核实到先前形成的 receipt",
                "write_id": write_id,
            }
            self.state["denied_writes"].append({"at": now, "receipt": receipt, "reason": result})
            self.save()
            return result

        permit = self.state["permits"].get(receipt["permit_id"])
        historical_error = self._verify_historical_receipt(permit, receipt)
        if historical_error:
            result = {"accepted": False, "write_id": write_id, **historical_error}
            self.state["denied_writes"].append({"at": now, "receipt": receipt, "reason": result})
            self._audit(now, "write_ack_rejected", write_id=write_id, code=historical_error["code"])
            self.save()
            return result

        entry = {
            "write_id": write_id,
            "job": receipt["job"],
            "worker": receipt["worker"],
            "permit_id": receipt["permit_id"],
            "epoch": receipt["epoch"],
            "resource": receipt["resource"],
            "value": receipt["value"],
            "accepted_at": receipt["accepted_at"],
            "admitted_at": now,
            "status": "recorded_late" if now != receipt["accepted_at"] or receipt["epoch"] != permit["epoch"] else "recorded",
            "verification": "executor_receipt+historical_permit",
        }
        self.state["ledger"].append(entry)
        permit.setdefault("ledger_write_ids", []).append(write_id)
        self._audit(now, "write_recorded", write_id=write_id, permit_id=permit["id"], late=entry["status"] == "recorded_late")
        self.save()
        return {"accepted": True, "entry": entry}

    def _verify_historical_receipt(
        self, permit: dict[str, Any] | None, receipt: dict[str, Any]
    ) -> dict[str, Any] | None:
        if permit is None:
            return {"code": "unknown_permit", "message": "receipt 引用未知许可"}
        if receipt["job"] != permit["job"]:
            return {"code": "job_mismatch", "message": "receipt job 与许可不符"}
        accepted_at = receipt["accepted_at"]
        if type(accepted_at) is not int or accepted_at < permit["issued_at"]:
            return {"code": "before_permit_issued", "message": "写入接纳时间早于许可签发"}
        valid_until = receipt.get("valid_until", permit["expires_at"])
        if accepted_at >= int(valid_until):
            return {"code": "permit_not_valid_then", "message": "写入被执行端接纳时许可已到期"}
        if self.mode_for_resource(permit, receipt["resource"]) != "write":
            return {"code": "resource_not_writable", "message": "receipt 资源不在 write 范围内"}

        accepted_index = receipt.get("accepted_index")

        def before_boundary(boundary_at: Any, boundary_event: Any) -> bool:
            if boundary_at is None:
                return True
            if accepted_at < int(boundary_at):
                return True
            if accepted_at > int(boundary_at):
                return False
            if (
                accepted_index is not None
                and boundary_event is not None
                and type(accepted_index) is int
                and type(boundary_event) is int
            ):
                return accepted_index < boundary_event
            return False

        ownership_ok = any(
            ownership["epoch"] == receipt["epoch"]
            and ownership["worker"] == receipt["worker"]
            and ownership["from_at"] <= accepted_at
            and before_boundary(ownership.get("until_at"), ownership.get("until_event"))
            for ownership in permit["ownership"]
        )
        if not ownership_ok:
            return {"code": "not_owner_then", "message": "写入接纳时 worker/epoch 不是许可持有方"}

        run_ok = False
        for run in permit.get("runs", []):
            if not (
                run["worker"] == receipt["worker"]
                and run["epoch"] == receipt["epoch"]
                and receipt["resource"] in run["resources"]
                and run["begun_at"] <= accepted_at
            ):
                continue
            begun_event = run.get("begun_event")
            if (
                run["begun_at"] == accepted_at
                and accepted_index is not None
                and begun_event is not None
                and begun_event > accepted_index
            ):
                continue
            stopped_before = False
            for stop in run.get("stops", []):
                if stop.get("resource") != receipt["resource"]:
                    continue
                if stop["at"] < accepted_at:
                    stopped_before = True
                    break
                if stop["at"] == accepted_at:
                    stop_event = stop.get("event_index")
                    if (
                        accepted_index is not None
                        and stop_event is not None
                        and stop_event > accepted_index
                    ):
                        continue
                    stopped_before = True
                    break
            if not stopped_before:
                run_ok = True
                break
        if not run_ok:
            return {"code": "no_active_begin_then", "message": "写入接纳时没有匹配的有效开始事实"}

        if permit.get("released_at") is not None and not before_boundary(
            permit.get("released_at"), permit.get("released_event")
        ):
            return {"code": "permit_released_then", "message": "写入发生在许可释放之后"}
        return None

    # --------------------------------------------------------------- inspection

    def inspect(self, job: str | None = None, resource: str | None = None) -> dict[str, Any]:
        now = self.state["clock"]
        if job:
            return self._inspect_job(job, now)
        if resource:
            return self._inspect_resource(resource, now)
        return self._overview(now)

    def _request_view(self, request: dict[str, Any], now: int) -> dict[str, Any]:
        view = {key: value for key, value in request.items() if key != "scopes"}
        view["scope_resources"] = sorted(self.coverage_set(request["scopes"]))
        if request.get("status") == "queued":
            decision = self.evaluate(request["scopes"], request["seq"], request["id"], now)
            view["blockers"] = decision["blockers"]
            view["wait_reasons"] = [blocker["code"] for blocker in decision["blockers"]]
            view["can_grant_now"] = decision["grantable"]
            if request["id"] in self.state["queue"]:
                view["queue_position"] = self.state["queue"].index(request["id"]) + 1
        return view

    def _permit_view(self, permit: dict[str, Any], now: int) -> dict[str, Any]:
        unstopped = self._unstopped_resources(permit)
        handover = permit.get("handover")
        can_backup_begin = bool(
            handover
            and handover.get("ready")
            and permit["lease_state"] == "active"
            and not permit.get("occupancy_released")
            and now < int(permit["expires_at"])
        )
        current_queue_conflicts = []
        for request_id in self.state["queue"]:
            request = self._find_request(request_id)
            if request and request.get("job") != permit["job"]:
                conflict = _conflict_resources(permit["scopes"], request["scopes"])
                raw = _raw_intersection(permit["scopes"], request["scopes"])
                if conflict or raw:
                    current_queue_conflicts.append(
                        {
                            "request_id": request_id,
                            "job": request["job"],
                            "mode_conflict": bool(conflict),
                            "intersection_resources": raw,
                        }
                    )
        if permit.get("occupancy_released"):
            status = "released"
        elif permit["lease_state"] == "expired":
            status = "expired_stop_pending" if unstopped else "expired"
        elif handover:
            if handover.get("state") == "lapsed_expired":
                status = "handover_lapsed_expired"
            elif handover.get("ready"):
                status = "backup_active" if unstopped else "handover_ready"
            else:
                status = "handover_pending"
        else:
            status = "active"

        return {
            "permit_id": permit["id"],
            "status": status,
            "job": permit["job"],
            "original_worker": permit["original_worker"],
            "current_worker": permit["current_worker"],
            "epoch": permit["epoch"],
            "uses": permit["uses"],
            "covered_resources": sorted(self.coverage_set(permit["scopes"])),
            "issued_at": permit["issued_at"],
            "expires_at": permit["expires_at"],
            "clock": now,
            "lease_state": permit["lease_state"],
            "valid_for_new_actions": (
                permit["lease_state"] == "active"
                and not permit.get("occupancy_released")
                and now < int(permit["expires_at"])
            ),
            "occupying": not permit.get("occupancy_released")
            and (permit["lease_state"] == "active" or bool(unstopped)),
            "occupancy_released": permit.get("occupancy_released", False),
            "release_reason": permit.get("release_reason"),
            "stopped_resources": permit.get("stopped_resources", []),
            "unstopped_resources": unstopped,
            "runs": permit.get("runs", []),
            "handover": handover,
            "can_backup_begin": can_backup_begin,
            "renewals": permit.get("renewals", []),
            "intersecting_queue": current_queue_conflicts,
            "recorded_write_ids": permit.get("ledger_write_ids", []),
        }

    def _inspect_job(self, job: str, now: int) -> dict[str, Any]:
        requests = [r for r in self.state["requests"] if r["job"] == job]
        permits = [p for p in self.state["permits"].values() if p["job"] == job]
        entries = [entry for entry in self.state["ledger"] if entry["job"] == job]
        denied = [entry for entry in self.state["denied_writes"] if entry.get("payload", {}).get("job") == job or entry.get("receipt", {}).get("job") == job]
        if not requests and not permits:
            return {"clock": now, "job": job, "found": False}
        return {
            "clock": now,
            "job": job,
            "found": True,
            "requests": [self._request_view(r, now) for r in requests],
            "permits": [self._permit_view(p, now) for p in permits],
            "recorded_writes": entries,
            "denied_write_acks": denied,
        }

    def _inspect_resource(self, resource: str, now: int) -> dict[str, Any]:
        if resource not in self.state["resources"]:
            return {"clock": now, "resource": resource, "found": False}
        members = sorted(self.scope_resources(resource))
        occupancies = []
        for permit in self.state["permits"].values():
            scopes = self._occupancy_scopes(permit)
            if _raw_intersection([{"resources": members}], scopes):
                occupancies.append(self._permit_view(permit, now))
        waiting = []
        for request_id in self.state["queue"]:
            request = self._find_request(request_id)
            if request and _raw_intersection([{"resources": members}], request["scopes"]):
                waiting.append(self._request_view(request, now))
        writes = [entry for entry in self.state["ledger"] if entry["resource"] in members]
        return {
            "clock": now,
            "resource": resource,
            "found": True,
            "scope_resources": members,
            "current_occupancies": occupancies,
            "waiting_requests": waiting,
            "recorded_writes": writes,
        }

    def _overview(self, now: int) -> dict[str, Any]:
        queue_views = []
        for request_id in self.state["queue"]:
            request = self._find_request(request_id)
            if request:
                queue_views.append(self._request_view(request, now))
        return {
            "clock": now,
            "resources": self.state["resources"],
            "queue": queue_views,
            "permits": [self._permit_view(p, now) for p in self.state["permits"].values()],
            "ledger": self.state["ledger"],
            "denied_writes": self.state["denied_writes"],
        }
