# 共享标定资源的层级租约与接替授权

资源由 parent 建立层级，便携件可独立于产线。作业方接收短期许可，现场端按许可开展仿真校准。任何记录均不对应真实设备。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **resource**　登记资源 resource 与可空的 parent。
- **acquire**　提交 job 与 worker 以及 uses。uses 键是资源名，值为 read 或 write，申请覆盖该资源及其后代。ttl 为申请有效秒数。
- **renew**　续期指定 permit，value 表示拟延长到的逻辑秒点。
- **begin**　现场端收到作业开始，声明实用资源与 permit。
- **record**　现场端接到写入请求。若它已确认接纳，会形成唯一 write_id 的 receipt。重复出示先前已经形成的 receipt 可以重发原事实。样例通过 write_ack 传送已有 receipt。
- **handover**　同一 job 由替补 worker 接替。permit:N 引用本次运行发出的第 N 个确认许可。
- **stop_ack**　现场端确认指定 job 的操作已经停下。许可覆盖不同资源时回执明确列出已停止的资源。
- **advance**　仅推进仿真时间，载荷为空对象。
- **inspect**　按 job 或 resource 查询排队与执行状态。
- **write_ack**　现场执行端传送先前实际形成的写入接纳回执，样例以 write_id 引用本地已形成的写入，不是写入请求成功的声明。

## 资料文件

- overlapping-and-unrelated.jsonl
- takeover-and-delayed-record.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。
