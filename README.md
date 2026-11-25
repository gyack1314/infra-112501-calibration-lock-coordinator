# 共享标定资源的层级租约与接替授权

几个训练工位共用移动标定架/便携标定件时，由 `calcoord` 维护：

- 车间 → 产线 → 工位的资源从属覆盖；
- `read` 共享、`write` 独占的冲突判定；
- 多资源申请的原子授权与公平排队；
- 短期许可 TTL、续期防插队、到期后的现场停止确认；
- 同 job 替补执行端登记、epoch  fencing、旧端迟来写入的入账判定；
- 协调服务与仿真执行端各自的 JSON 持久化和进程重启恢复。

这里只有本地仿真，不连接任何实体设备。业务事件契约仍由根目录 `trace_io.py` 校验。

## 快速验证

需要 Python 3.11，仅使用标准库。

```sh
# 检查 data/ 中所有 JSONL 事件封装
python3 trace_io.py data

# 运行资料测试和协调语义/HTTP 重启测试
python3 -m unittest -v

# 完整重放两个原始场景；每条事件后都从 JSON 重建两个角色
python3 -m calcoord replay data/overlapping-and-unrelated.jsonl \
  --state-dir .calcoord-state/overlap --reset --restart-each-event

python3 -m calcoord replay data/takeover-and-delayed-record.jsonl \
  --state-dir .calcoord-state/takeover --reset --restart-each-event

# 查看某个 job 的排队原因、许可、接替状态和已入账结果
python3 -m calcoord inspect \
  --state-dir .calcoord-state/overlap --job entire-a
```

分阶段重启重放：第一次进程只处理前 5 条，第二次进程从 `replay-state.json` 记录的位置继续。

```sh
python3 -m calcoord replay data/takeover-and-delayed-record.jsonl \
  --state-dir .calcoord-state/staged --reset --max-events 5
python3 -m calcoord inspect --state-dir .calcoord-state/staged --job shift
python3 -m calcoord replay data/takeover-and-delayed-record.jsonl \
  --state-dir .calcoord-state/staged
```

## 服务和执行端入口

两个角色都有项目根目录入口，也都可以用 `python -m calcoord ...` 启动。

```sh
# 终端 1：协调服务
python3 coordination_service.py \
  --state-dir .calcoord-state/http \
  --host 127.0.0.1 --port 8765

# 终端 2：持久化仿真执行端
python3 simulation_executor.py \
  --state-dir .calcoord-state/http \
  --service-url http://127.0.0.1:8765 \
  --host 127.0.0.1 --port 8766
```

服务重启时重新读取 `coordinator-state.json`，因此资源拓扑、等待顺序、`permit:N`
发号、许可和校准台账都不丢。执行端重启时读取 `executor-state.json`，按此前的
begin、stop、handover 和已形成 receipt 判断。

也可以不启动 HTTP，直接用 `Lab`/`ReplayRunner` 把事件接到两个持久化对象；
JSONL 演练和测试就是这样运行的。

### 常用 HTTP 调用

```sh
curl -s http://127.0.0.1:8765/healthz

curl -s -X POST http://127.0.0.1:8765/v1/method/register_resource \
  -H 'Content-Type: application/json' \
  -d '{"now":0,"name":"line-a","parent":null}'

curl -s -X POST http://127.0.0.1:8765/v1/method/acquire \
  -H 'Content-Type: application/json' \
  -d '{"now":1,"job":"entire-a","worker":"w2",
       "uses":{"line-a":"write"},"ttl":9}'

curl -s 'http://127.0.0.1:8765/v1/inspect?job=entire-a'
```

执行端 HTTP 入口为 `/v1/executor/begin`、`/record`、`/handover`、`/stop`、
`/write-ack` 和 `GET /v1/executor/state`。

## 关键语义

### 1. 层级覆盖与读写冲突

`uses` 中的每个资源根会覆盖它的全部已登记后代。

- `read/read` 覆盖相交时允许共享；
- 只要任一侧是 `write` 且资源集合相交，就冲突；
- `line-a: write` 与 `a1: read/write`、`a2: read/write` 都冲突；
- `line-a` 与 `line-b` 是兄弟子树，不冲突；
- 便携件 `kit` 通常是独立根，只覆盖自身。

### 2. 多资源作业原子取得

`uses` 可以同时包含两个工位和同一套 `kit`。协调器只有在所有 scope 都可授权时
才发出一个许可；不会先发工位、再让作业方无限等待便携件。

请求进入队列后，后续与它模式冲突的局部申请会得到 `queued_ahead`，不能越过它。
与它无覆盖交集的产线/工位仍可立即授权。

### 3. 公平排队和等待原因

队列按申请进入顺序保存。提升队列时按顺序扫描，但不会被无关请求队头阻塞：
与当前不能授权的较早请求不相交的后续请求仍可服务。

`inspect` 中每个等待请求会列出全部 blocker：

| code | 含义 |
| --- | --- |
| `queued_ahead` | 前面已有模式冲突的排队请求，不能插队 |
| `resource_busy` | 与仍有效的现行许可冲突 |
| `stop_not_acknowledged` | 许可已到期，但已开始的仿真操作还没有 stop_ack |
| `handover_stop_pending` | 替补已登记，原执行端尚未确认停止 |
| `takeover_stop_pending` | 替补提前 begin/record 时的拒绝原因 |
| `old_owner_fenced` | 转交后旧 worker 的新 begin/record 被 fencing |

blocker 同时给出 `permit_id`、request/job/worker、冲突资源和未停止资源，调度员
能看到“在等谁、冲突在哪里”。

### 4. TTL、到期和停止回执

许可的初始到期点是 `签发逻辑时间 + ttl`。逻辑时间到达 `expires_at` 后：

1. 许可立即变为 `lease_state=expired`，不能再 begin/record/renew；
2. 没有已开始操作，或操作均已停止：许可占用直接释放并提升队列；
3. 有未停止的已开始仿真操作：只保留这些实际资源的 lingering occupancy，
   inspector 显示 `stop_not_acknowledged`；
4. 收到覆盖相应资源的 `stop_ack` 后才清除占用并提升队列。

`stop_ack` 是现场停止事实：没有 handover 时，若所有已知运行都已停止，会立即
结束该许可占用并提升队列，不必干等 TTL；部分资源的 ack 只交回列出的资源
（资源名按层级展开），其余资源仍占用，且旧 worker 不能在已交回资源上重新
begin。handover 过程中整个 scope 仍保留给替补，直到旧端停止且替补可开始；若
等待期间 TTL 到期，旧许可不能重新激活，stop 后资源按队列分配，替补需重新
acquire。显式完成作业也可调用 `release_permit`。这样不会把仍在运行的工位直接
交给下一个写者，而已经确认停下的资源可以尽早复用。

### 5. 续期尊重已排队请求

`renew` 到新的绝对逻辑秒点时，如果已存在与该许可 scope 相交的排队请求，超过当前
`expires_at` 的续期会被拒绝，返回 `queued_conflict` 和具体等待者（其中
`mode_conflict` 区分读写独占冲突与可共享读相交）。原到期时间不变，因此全线独占
不会被一次又一次续期挤掉。

### 6. 接替、fencing 和迟来写入

`handover` 把同一 job 的 `permit:N` 登记给替补 worker，同时 epoch 加一：

- 旧 worker 随后的新 begin/record 一律 `old_owner_fenced`；
- 新 worker 可以先处于 `takeover_stop_pending`，但不能真正开始；
- 旧 worker 的 `stop_ack` 是必要例外；收齐后 `handover.ready=true`、
  `can_backup_begin=true`；
- 若等待期间许可已到期，旧许可不能重新激活；stop 后资源进入队列，替补需要重新
  acquire 当前资源的确切许可。

写入分两段：

1. `record` 到达执行端时，执行端实时调用协调器 `authorize(... action="record")`；
   只有许可、job、worker/epoch、资源、begin 事实和有效期都正确，才形成并持久化
   唯一 `write_id` receipt 和本地仿真校准值；
2. 网络恢复后的 `write_ack` 只负责重传执行端已有的 receipt。协调器核实 receipt
   存在，并验证它在 `accepted_at` 当时属于有效 owner/epoch、许可和 begin 事实，
   才写入服务台账。

因此：

- 转交前已接纳、receipt 真实存在的 `delta-1`，即使 ack 迟来也保留；
- 转交后旧端的 `delta-2` 不会形成 receipt，之后伪造或重发 ack 会得到
  `executor_receipt_missing` / `unconfirmed_write`；
- 重复 `write_id`/`write_ack` 幂等，不重复入账。

### 7. 不能拼局部许可冒充全线独占

一次 begin/record 只接受一个 `permit`。如果该许可的 scope 不覆盖声明资源，返回
`resource_not_covered`。即使同一 job 持有几个仍有效的 `a1`、`a2` 局部许可，也不
能拿其中一个发出 `line-a` 全线开始；必须取得一个覆盖 `line-a` 全部范围的新许可。

## 持久化文件

一个 `--state-dir` 中包含：

- `coordinator-state.json`：资源树、请求顺序、等待队列、permit 发号器、许可/
  epoch/runs/handover、续期记录、校准台账和拒绝记录；
- `executor-state.json`：执行端见过的 owner/epoch、begin、stop、本地 receipt、
  仿真校准表和拒绝记录；
- `replay-state.json`：JSONL 重放偏移，仅 replay 入口使用。

写入采用同目录临时文件 + `os.replace` 原子替换，避免进程被终止时留下半 JSON。

## 复验场景

`data/` 中除原始两份资料外，还加入了三种组合：

| 文件 | 拓扑/组合 | 复验点 |
| --- | --- | --- |
| `overlapping-and-unrelated.jsonl` | hall/line-a/line-b/a1/a2 + kit | 全线排队、局部不能插队、无冲突 line-b 授权、续期被拒、到期后等待 stop_ack |
| `takeover-and-delayed-record.jsonl` | line-a/a1 | handover、旧端 fencing、迟来 receipt 入账、stop 后可接替 |
| `atomic-kit-and-fair-reservation.jsonl` | 两工位 + 共享 kit + 兄弟线 | 多资源原子申请、队列预留、无部分占用、无关工位服务 |
| `deep-hierarchy-read-sharing.jsonl` | workshop/line/cell/station 深层树 | 后代覆盖、read/read 共享、write 独占、兄弟产线不冲突 |
| `backup-completes-after-restart.jsonl` | line-a/a1 | 替补提前开始被拒、停止后开始并写入、重复 ack 幂等、逐条事件重启 |

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 `timestamp`、事件名称 `type`
和对象 `data`。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于
建立输入之间的引用。

- **resource**　登记资源 `resource` 与可空的 `parent`。
- **acquire**　提交 `job`、`worker`、`uses` 和 `ttl`；`uses` 值为 `read`/`write`。
- **renew**　续期指定 `permit`，`value` 是拟延长到的逻辑秒点。
- **begin**　现场端收到作业开始，声明实用资源与 permit。
- **record**　现场端接到写入请求；校验通过后形成唯一 `write_id` receipt。
- **handover**　同一 job 由替补 worker 接替 `permit:N`。
- **stop_ack**　确认 job 操作停下；多资源时明确列出已停止资源。
- **advance**　仅推进逻辑时间。
- **inspect**　按 job 或 resource 查询排队与执行状态。
- **write_ack**　重传执行端先前实际形成的 receipt；不是“写入请求已成功”的口头声明。

资料检查命令：

```sh
python3 trace_io.py data
python3 -m unittest -v
```
