# Labflow

Labflow 是由 artifact DAG 驱动的持续实验室 supervisor。设计规范见
[`rfc/0001-mvp.md`](rfc/0001-mvp.md)。

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

[backend]
command = ["opencode", "serve"]
hostname = "127.0.0.1"

[roles.researcher]
kind = "lab-worker"
permissions = ["webfetch"]

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
depends-on = ["system-active", "_ready.researcher", "query-request"]
goal = "goal.md"
assets = ["answer.md"]
check = ["answer.md"]
permissions = ["webfetch"]
```

artifact 的角色始终是后缀，例如 `answer.researcher`。Host 可以通过 publish
普通名称或 `!名称` 操作任意非 `_` artifact；`_ready.researcher`、`_blocked`
等名称仅由 supervisor 控制。

文件访问由 artifact 自动确定：`assets` 可读写和删除，`inputs` 只读，`goal`
只读；Glob 允许发现路径，Grep 禁用。手写 `permissions` 只声明其他 OpenCode
能力，`read`、`edit`、`glob`、`grep` 是保留名称。
