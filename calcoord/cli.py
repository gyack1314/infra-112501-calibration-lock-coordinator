"""Command line entry points for replay, inspection and local HTTP services."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Any

from .http_api import CoordinatorClient, serve_coordinator, serve_executor
from .lab import Lab, ReplayRunner
from .service import Coordinator
from .store import CalCoordError


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def replay_command(args: argparse.Namespace) -> int:
    state_dir = Path(args.state_dir)
    if args.reset:
        _reset_dir(state_dir)
    runner = ReplayRunner(args.trace, state_dir)
    outputs = runner.run(max_events=args.max_events, restart_each_event=args.restart_each_event)
    if args.json:
        _print(outputs)
        return 0

    for output in outputs:
        index = output.get("index")
        event_type = output["type"]
        timestamp = output["timestamp"]
        if not output["ok"]:
            print(f"[{index} t={timestamp}] {event_type} 错误: {output.get('error')}", file=sys.stderr)
            continue
        result = output["result"]
        if event_type == "inspect":
            print(f"\n=== inspect @ t={timestamp} (event {index}) ===")
            _print(result)
        elif event_type == "acquire":
            if result["status"] == "granted":
                print(f"[{index} t={timestamp}] acquire -> {result['permit_id']} (expires {result['expires_at']})")
            else:
                reasons = "; ".join(blocker["message"] for blocker in result["blockers"])
                print(f"[{index} t={timestamp}] acquire -> queued {result['request_id']}: {reasons}")
        elif event_type == "renew":
            mark = "接受" if result.get("accepted") else "拒绝"
            print(f"[{index} t={timestamp}] renew -> {mark} ({result.get('code')})")
        elif event_type == "handover":
            print(f"[{index} t={timestamp}] handover -> epoch {result.get('epoch')} {result.get('code', 'registered')}")
        elif event_type in {"begin", "record", "write_ack"}:
            accepted = result.get("accepted")
            reason = result.get("reason") or result.get("authorization") or {}
            code = result.get("code") or reason.get("code")
            print(f"[{index} t={timestamp}] {event_type} -> accepted={accepted} {code or ''}")
        elif event_type == "stop_ack":
            extra = "，接替可开始" if result.get("handover_ready") else ""
            extra += "，已释放并提升队列" if result.get("promoted") or result.get("released") else ""
            print(f"[{index} t={timestamp}] stop_ack -> stopped={result.get('stopped_now')} unstopped={result.get('unstopped_resources')}{extra}")
        else:
            print(f"[{index} t={timestamp}] {event_type} ok")
    return 0


def inspect_command(args: argparse.Namespace) -> int:
    state_dir = Path(args.state_dir)
    coordinator_path = state_dir / "coordinator-state.json"
    if not coordinator_path.exists():
        raise CalCoordError(f"找不到协调服务状态: {coordinator_path}")
    coordinator = Coordinator(coordinator_path)
    view = coordinator.inspect(job=args.job, resource=args.resource)
    if args.job and (state_dir / "executor-state.json").exists() and not args.service_only:
        executor_state = json.loads((state_dir / "executor-state.json").read_text(encoding="utf-8"))
        permits = {
            pid: permit
            for pid, permit in executor_state.get("permits", {}).items()
            if permit.get("job") == args.job
        }
        view["simulation_executor"] = {
            "permits": permits,
            "calibration_table": [
                row for row in executor_state.get("calibration_table", []) if row.get("job") == args.job
            ],
            "rejected": [row for row in executor_state.get("rejected", []) if row.get("job") == args.job],
        }
    _print(view)
    return 0


def service_command(args: argparse.Namespace) -> int:
    httpd = serve_coordinator(args.state_dir, args.host, args.port, verbose=not args.quiet)
    actual_port = httpd.server_address[1]
    print(f"coordinator listening on http://{args.host}:{actual_port}; state={Path(args.state_dir).resolve()}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping coordinator")
    finally:
        httpd.server_close()
    return 0


def executor_command(args: argparse.Namespace) -> int:
    httpd = serve_executor(
        args.state_dir, args.service_url, args.host, args.port, verbose=not args.quiet
    )
    actual_port = httpd.server_address[1]
    print(
        f"simulation executor listening on http://{args.host}:{actual_port}; "
        f"coordinator={args.service_url}; state={Path(args.state_dir).resolve()}"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping simulation executor")
    finally:
        httpd.server_close()
    return 0


def client_command(args: argparse.Namespace) -> int:
    client = CoordinatorClient(args.service_url)
    if args.remote_command == "inspect":
        _print(client.get_inspect(job=args.job, resource=args.resource))
        return 0
    payload = json.loads(args.payload) if args.payload else {}
    _print(client.post(args.remote_command, **payload))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="calcoord",
        description="共享标定资源的层级许可、公平排队、接替与持久化仿真执行端",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    replay = sub.add_parser("replay", help="按 data 事件契约重放 JSONL 演练")
    replay.add_argument("trace", type=Path)
    replay.add_argument("--state-dir", required=True)
    replay.add_argument("--reset", action="store_true", help="清空状态目录后重新演练")
    replay.add_argument("--max-events", type=int, default=None, help="本次进程最多处理多少条新事件")
    replay.add_argument(
        "--restart-each-event",
        action="store_true",
        help="每条事件后从 JSON 状态重建服务与执行端，用于复验重启恢复",
    )
    replay.add_argument("--json", action="store_true", help="输出每条事件的完整 JSON 结果")
    replay.set_defaults(func=replay_command)

    inspect = sub.add_parser("inspect", help="从持久化状态查看排队、冲突、接替和入账")
    inspect.add_argument("--state-dir", default=".calcoord-state")
    inspect.add_argument("--job")
    inspect.add_argument("--resource")
    inspect.add_argument("--service-only", action="store_true")
    inspect.set_defaults(func=inspect_command)

    service = sub.add_parser("service", help="启动本地 JSON/HTTP 协调服务")
    service.add_argument("--state-dir", default=".calcoord-state")
    service.add_argument("--host", default="127.0.0.1")
    service.add_argument("--port", type=int, default=8765)
    service.add_argument("--quiet", action="store_true")
    service.set_defaults(func=service_command)

    executor = sub.add_parser("executor", help="启动持久化仿真执行端 HTTP 入口")
    executor.add_argument("--state-dir", default=".calcoord-state")
    executor.add_argument("--service-url", default="http://127.0.0.1:8765")
    executor.add_argument("--host", default="127.0.0.1")
    executor.add_argument("--port", type=int, default=8766)
    executor.add_argument("--quiet", action="store_true")
    executor.set_defaults(func=executor_command)

    client = sub.add_parser("remote", help="调用协调服务 HTTP 方法或 inspect")
    client.add_argument("remote_command", help="inspect 或 /v1/method 后的方法名")
    client.add_argument("--service-url", default="http://127.0.0.1:8765")
    client.add_argument("--payload", help="JSON 对象形式的命名参数")
    client.add_argument("--job")
    client.add_argument("--resource")
    client.set_defaults(func=client_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except CalCoordError as error:
        print(f"错误: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
