# Labflow

Labflow 是由 artifact DAG 驱动的持续实验室 supervisor。设计规范见
[`rfc/0001-mvp.md`](rfc/0001-mvp.md)、
[`rfc/0002-benchmark.md`](rfc/0002-benchmark.md) 和
[`rfc/0003-artifact-kinds.md`](rfc/0003-artifact-kinds.md)。后者定义当前计划表面。

## 构建

```sh
cargo build --release
```

运行 supervisor 需要 Linux；artifact watcher 直接使用异步 inotify。

## 初始化与控制

```sh
labflow init --port 4096
labflow plan check
labflow publish system-supervisor system-backend system-active query-request
labflow publish '!query-request'
labflow host-tasks --poll 60
labflow status
```

启动 wrapper：

```sh
.labflow/run
```

`.labflow/run` 持有 supervisor，supervisor 持有 OpenCode backend。也可以直接
运行一次 supervisor：

```sh
labflow supervisor
```

`system-supervisor` 和 `system-backend` 的创建、touch、删除分别控制对应进程
的启动、重启和停止。修改计划后 publish `system-plan` 才会使新计划生效；
加载失败时 `host-tasks` 会请求 Host 再次 publish `system-plan`。

## 计划示例

```toml
version = 1

[roles.researcher]
permissions = ["webfetch"]

[roles.evaluator]
permissions = []

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
requires = ["system-active", "learn-domain.researcher", "query-request"]
goal = "goal.md"
assets = ["answer.md"]
check = ["answer.md"]
permissions = ["webfetch"]

[artifacts."learn-domain.researcher"]
kind = "learn"
goal = "goals/learn-domain.md"
inputs = ["knowledge/domain/"]

[artifacts."bench-answer.evaluator"]
kind = "bench"
requires = ["answer.researcher"]

[artifacts."bench-answer.evaluator".bench]
name = "answer"
source = "benchmark/questions.jsonl"
qlist = "benchmark/current.ids"
public-knowledge = ["benchmark/public/"]

[artifacts."bench-answer.evaluator".bench.permissions]
write = ["benchmark/workspace/"]
commands = ["just verify"]
```

artifact 的角色始终是后缀，例如 `answer.researcher`。Host 可以通过 publish
普通名称或 `!名称` 操作非 `_` 的实体 artifact；Learn artifact 和 `_blocked`
等虚拟名称仅由 supervisor 控制。角色每次建立新 session 时，其 Learn artifact
自动失效并重新学习。Bench 的被测 Agent profile 由配置自动生成。

文件访问由 artifact 自动确定：`assets` 可读写和删除，`inputs` 只读，`goal`
只读；Glob 允许发现路径，Grep 禁用。手写 `permissions` 只声明其他 OpenCode
能力，`read`、`edit`、`glob`、`grep` 是保留名称。
