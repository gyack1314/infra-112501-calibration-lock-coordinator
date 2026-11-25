"""Optional local JSON/HTTP entry points for coordinator and simulator roles.

The core classes do not require HTTP; this adapter lets both processes be
started independently and restarted while recovering their JSON state files.
Only loopback/local simulation is intended.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from .executor import SimulationExecutor
from .service import Coordinator
from .store import CalCoordError


class CoordinatorClient:
    def __init__(self, base_url: str, timeout: float = 3.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def post(self, method: str, **payload: Any) -> dict[str, Any]:
        url = f"{self.base_url}/v1/method/{method}"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {"code": "http_error", "message": body}
            parsed.setdefault("ok", False)
            parsed.setdefault("http_status", exc.code)
            return parsed
        except error.URLError as exc:
            return {"allowed": False, "ok": False, "code": "coordinator_unavailable", "message": str(exc)}

    def get_inspect(self, job: str | None = None, resource: str | None = None) -> dict[str, Any]:
        query = parse.urlencode({k: v for k, v in {"job": job, "resource": resource}.items() if v})
        url = f"{self.base_url}/v1/inspect" + (f"?{query}" if query else "")
        with request.urlopen(url, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def authorize(
        self,
        now: int,
        permit_id: str,
        job: str,
        worker: str,
        resources: list[str],
        action: str,
    ) -> dict[str, Any]:
        return self.post(
            "authorize",
            now=now,
            permit_id=permit_id,
            job=job,
            worker=worker,
            resources=list(resources),
            action=action,
        )


def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise CalCoordError("HTTP 请求体必须是 JSON 对象")
    return value


_COORDINATOR_METHODS = {
    "tick",
    "register_resource",
    "acquire",
    "renew",
    "register_handover",
    "authorize",
    "mark_started",
    "apply_stop_ack",
    "release_permit",
    "accept_write_ack",
    "record_write_denial",
}


def make_coordinator_handler(coordinator: Coordinator) -> type[BaseHTTPRequestHandler]:
    class CoordinatorHandler(BaseHTTPRequestHandler):
        server_version = "CalCoordCoordinator/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            # CLI process can still show concise errors if desired; tests stay quiet.
            if getattr(self.server, "verbose", False):
                super().log_message(fmt, *args)

        def do_GET(self) -> None:  # noqa: N802
            parsed = parse.urlparse(self.path)
            if parsed.path == "/healthz":
                _send_json(self, 200, {"ok": True, "role": "coordinator", "clock": coordinator.state["clock"]})
                return
            if parsed.path == "/v1/inspect":
                query = parse.parse_qs(parsed.query)
                job = query.get("job", [None])[0]
                resource = query.get("resource", [None])[0]
                _send_json(self, 200, coordinator.inspect(job=job, resource=resource))
                return
            _send_json(self, 404, {"ok": False, "code": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = parse.urlparse(self.path)
            prefix = "/v1/method/"
            if not parsed.path.startswith(prefix):
                _send_json(self, 404, {"ok": False, "code": "not_found"})
                return
            method_name = parsed.path[len(prefix) :]
            if method_name not in _COORDINATOR_METHODS:
                _send_json(self, 404, {"ok": False, "code": "unknown_method"})
                return
            try:
                payload = _read_body(self)
                method = getattr(coordinator, method_name)
                result = method(**payload)
                coordinator.save()
                if isinstance(result, dict):
                    result.setdefault("ok", bool(result.get("allowed", True)))
                    response = result
                else:
                    response = {"ok": True, "result": result}
                _send_json(self, 200, response)
            except (CalCoordError, TypeError, KeyError, json.JSONDecodeError) as exc:
                coordinator.save()
                _send_json(self, 400, {"ok": False, "code": getattr(exc, "code", "bad_request"), "message": str(exc)})

    return CoordinatorHandler


def make_executor_handler(
    executor: SimulationExecutor, client: CoordinatorClient
) -> type[BaseHTTPRequestHandler]:
    class ExecutorHandler(BaseHTTPRequestHandler):
        server_version = "CalCoordSimulator/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            if getattr(self.server, "verbose", False):
                super().log_message(fmt, *args)

        def do_GET(self) -> None:  # noqa: N802
            parsed = parse.urlparse(self.path)
            if parsed.path == "/healthz":
                _send_json(self, 200, {"ok": True, "role": "simulation-executor"})
                return
            if parsed.path == "/v1/executor/state":
                _send_json(self, 200, executor.state)
                return
            _send_json(self, 404, {"ok": False, "code": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = parse.urlparse(self.path)
            try:
                payload = _read_body(self)
                if parsed.path == "/v1/executor/begin":
                    result = executor.begin(
                        now=int(payload["now"]),
                        job=payload["job"],
                        worker=payload["worker"],
                        permit_id=payload["permit_id"],
                        resources=list(payload["resources"]),
                        authorizer=client,
                    )
                    if result.get("accepted"):
                        started = client.post(
                            "mark_started",
                            now=payload["now"],
                            permit_id=payload["permit_id"],
                            job=payload["job"],
                            worker=payload["worker"],
                            resources=list(payload["resources"]),
                            authorization=result["authorization"],
                            event_index=payload.get("event_index"),
                        )
                        result["coordinator_started"] = started
                    _send_json(self, 200, result)
                    return

                if parsed.path == "/v1/executor/record":
                    result = executor.receive_record(
                        now=int(payload["now"]),
                        job=payload["job"],
                        worker=payload["worker"],
                        permit_id=payload["permit_id"],
                        resource=payload["resource"],
                        write_id=payload["write_id"],
                        value=payload.get("value"),
                        authorizer=client,
                        event_index=payload.get("event_index"),
                    )
                    if not result.get("accepted"):
                        client.post(
                            "record_write_denial",
                            now=int(payload["now"]),
                            payload=payload,
                            reason=result.get("reason", {"code": "record_rejected"}),
                        )
                    _send_json(self, 200, result)
                    return

                if parsed.path == "/v1/executor/handover":
                    decision = client.post("register_handover", **payload)
                    if decision.get("accepted") and decision.get("handover"):
                        handover = decision["handover"]
                        executor.apply_handover(
                            now=int(payload["now"]),
                            permit_id=payload["permit_id"],
                            job=payload["job"],
                            old_worker=handover["from_worker"],
                            new_worker=handover["to_worker"],
                            epoch=handover["epoch"],
                        )
                    _send_json(self, 200, decision)
                    return

                if parsed.path == "/v1/executor/stop":
                    coordinator_payload = {
                        key: value for key, value in payload.items() if key != "epoch"
                    }
                    decision = client.post("apply_stop_ack", **coordinator_payload)
                    if decision.get("accepted"):
                        # Epoch is recovered durably by the coordinator state; the
                        # executor can also accept an explicit epoch from the caller.
                        executor.apply_stop_fact(
                            now=int(payload["now"]),
                            permit_id=payload["permit_id"],
                            job=payload["job"],
                            worker=payload["worker"],
                            resources=list(payload["resources"]),
                            epoch=payload.get("epoch"),
                        )
                    _send_json(self, 200, decision)
                    return

                if parsed.path == "/v1/executor/write-ack":
                    receipt = executor.prepare_write_ack(
                        payload["write_id"],
                        job=payload.get("job"),
                        permit_id=payload.get("permit_id"),
                    )
                    if receipt is None:
                        reason = {
                            "code": "executor_receipt_missing",
                            "message": "执行端没有实际形成该 write_id 的 receipt",
                        }
                        client.post(
                            "record_write_denial",
                            now=int(payload.get("now", 0)),
                            payload=payload,
                            reason=reason,
                        )
                        _send_json(self, 200, {"accepted": False, **reason})
                        return
                    decision = client.post(
                        "accept_write_ack",
                        now=int(payload["now"]),
                        receipt=receipt,
                        verified_executor_receipt=True,
                    )
                    if decision.get("accepted"):
                        executor.mark_ack_delivered(payload["write_id"], int(payload["now"]))
                    _send_json(self, 200, decision)
                    return

                _send_json(self, 404, {"ok": False, "code": "not_found"})
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                _send_json(self, 400, {"ok": False, "code": "bad_request", "message": str(exc)})

    return ExecutorHandler


def serve_coordinator(
    state_dir: str | Path, host: str = "127.0.0.1", port: int = 8765, verbose: bool = True
) -> ThreadingHTTPServer:
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    coordinator = Coordinator(state_dir / "coordinator-state.json")
    server = ThreadingHTTPServer((host, port), make_coordinator_handler(coordinator))
    server.verbose = verbose  # type: ignore[attr-defined]
    return server


def serve_executor(
    state_dir: str | Path,
    service_url: str,
    host: str = "127.0.0.1",
    port: int = 8766,
    verbose: bool = True,
) -> ThreadingHTTPServer:
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    executor = SimulationExecutor(state_dir / "executor-state.json")
    client = CoordinatorClient(service_url)
    server = ThreadingHTTPServer((host, port), make_executor_handler(executor, client))
    server.verbose = verbose  # type: ignore[attr-defined]
    return server
