# Distributed KV

基于MIT 6.5840编写的分布式KV服务。固定成员的 Raft KV，支持 Get、带版本条件的 Put、请求去重、持久化和快照。唯一入口为 `cmd/raftkv`。使用 Go 1.27 或更高版本。

## 启动

```sh
make check
make build
```

单节点：

```sh
./bin/raftkv -id 0 -peers 127.0.0.1:9000 -http 127.0.0.1:8080 -data data/node-0 -snapshot-every 100
```

三个终端分别运行：

```sh
./bin/raftkv -id 0 -peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 -http 127.0.0.1:8080 -data data/node-0 -snapshot-every 0
./bin/raftkv -id 1 -peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 -http 127.0.0.1:8081 -data data/node-1 -snapshot-every 0
./bin/raftkv -id 2 -peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 -http 127.0.0.1:8082 -data data/node-2 -snapshot-every 0
```

所有进程使用同一份有序 `-peers`，`-id` 是自己在表中的下标。各节点独占一个数据目录，重启必须沿用原 ID、地址和目录。初始启动要求全部成员可达，默认期限 15 秒，可通过 `-startup-timeout 30s` 调整。单节点目录不要直接复用于三节点部署。

`-snapshot-every` 指每应用多少条日志生成快照，默认 0 表示禁用。Ctrl+C 正常关闭节点。跨机器部署时把回环地址换成彼此可达的地址。

## 请求

写入新键（version=0）；更新已有键时使用上次响应的版本：

```sh
curl -i -X PUT http://127.0.0.1:8080/kv \
  -H 'Content-Type: application/json' \
  -H 'X-Client-ID: demo-1' -H 'X-Request-Seq: 1' \
  -d '{"key":"name","value":"Alice","version":0}'
```

```sh
curl -i 'http://127.0.0.1:8080/kv?key=name' \
  -H 'X-Client-ID: demo-1' -H 'X-Request-Seq: 2'
```

每个新请求递增 Seq，重试同一个请求保持 Client-ID、Seq 和参数不变。新的客户端会话使用新的 Client-ID。Leader 才处理读写；收到 503 时尝试其他节点，超时后也用原身份重试。200 表示成功，404 表示键不存在，409 表示版本冲突，400 表示请求无效。

## 简要代码分析

| 位置 | 职责 |
| --- | --- |
| cmd/raftkv | 参数、信号与启动入口 |
| internal/cluster | 节点生命周期、RPC 监听与调用、HTTP 启停 |
| internal/raft | 选举、复制、提交、按序交付、快照安装与恢复 |
| internal/server | HTTP 转换、等待提交、执行 KV、请求去重和应用快照 |
| internal/kv | 键值、版本条件与快照数据 |
| internal/storage | 临时文件写入、Sync、Rename，原子替换 raft.state |

请求流程：`HTTP → Raft.Propose → 多数复制并提交 → Applied → KV 执行/去重 → HTTP 响应`。Get 也经过日志。取消或超时不代表写入没有发生，因此重试必须保持请求身份。

应用快照保存 KV 与完整去重结果，和 Raft 任期、投票、剩余日志一起持久化。压缩后日志仍用逻辑索引；落后节点通过 InstallSnapshot 恢复，再复制后缀。每次 RPC 独立建连，整个调用期限为 500ms。

这是固定成员的学习实现：不包含动态成员、分片、客户端自动发现 Leader、TLS 或鉴权；仅用于受信任网络。去重结果尚无淘汰，快照整块发送，大快照可能超过 RPC 期限。`make check` 进行格式、静态分析和构建检查，不等于完整分布式正确性验证。
