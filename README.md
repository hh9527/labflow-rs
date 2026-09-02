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
labflow init
labflow plan check
labflow publish query-request
labflow publish system-active
labflow status
labflow unpublish query-request
```

启动 wrapper：

```sh
.labflow/run-supervisor
```

或者直接运行一次 supervisor：

```sh
labflow supervisor
```

`system-supervisor` 用于请求 wrapper 重启 supervisor，`system-backend` 用于
请求 supervisor 重启 OpenCode 无头服务器子进程。

## 计划示例

```toml
version = 1

[backend]
command = ["opencode", "serve"]
hostname = "127.0.0.1"
port = 4096

[roles.researcher]
kind = "lab-worker"
permissions = ["read", "edit"]

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
depends-on = ["system-active", "_ready.researcher", "query-request"]
goal = "goal.md"
assets = ["answer.md"]
check = ["answer.md"]
```

artifact 的角色始终是后缀，例如 `answer.researcher`。Host 可以 publish 或
unpublish 任意非 `_` artifact；`_ready.researcher`、`_blocked` 等名称仅由
supervisor 控制。
