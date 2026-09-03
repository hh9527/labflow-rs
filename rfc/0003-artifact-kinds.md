# RFC 0003：统一的 Artifact 类型

- 状态：已接受
- 创建日期：2026-09-03

## 摘要

本 RFC 将普通任务、会话学习和 Agent 评测统一建模为 artifact。计划不再包含
独立的 backend、角色类型或 benchmark 顶层配置；artifact 的 `kind` 决定其构建
协议，而名称后缀仍唯一决定承担任务的 DAG role。

该设计删除 `_ready.<role>` 特例。角色的新会话通过重新构建其 Learn artifact
获得工作所需认知。Bench artifact 则由配置自动派生一次性被测 Agent R 的身份，
无需在计划中声明 respondent。

## 完整示例

```toml
version = 1

[roles.a1]
permissions = ["bash"]

[roles.a2]
permissions = []

[artifacts."learn-domain.a1"]
kind = "learn"
goal = "goals/learn-domain.md"
inputs = ["knowledge/domain/"]

[artifacts."answer.a1"]
requires = ["system-active", "request", "learn-domain.a1"]
goal = "goals/answer.md"
inputs = ["requests/current.md", "knowledge/domain/"]
assets = ["outputs/answer.md"]
check = ["outputs/answer.md"]

[artifacts."review.a2"]
requires = ["answer.a1", "feedback?"]
goal = "goals/review.md"
assets = ["outputs/review.md"]

[artifacts."bench-solver.a2"]
kind = "bench"
requires = ["system-active", "answer.a1"]

[artifacts."bench-solver.a2".bench]
name = "solver"
source = "benchmark/questions.jsonl"
public-knowledge = ["benchmark/public/"]

[artifacts."bench-solver.a2".bench.permissions]
read = ["benchmark/reference/"]
write = ["benchmark/workspace/"]
commands = ["just verify"]
```

`kind` 可取 `task`、`learn` 和 `bench`，缺省为 `task`。所有 artifact 仍使用
`part.role` 命名；后缀 role 必须存在于 `[roles]`。`requires` 保持既有的必需和
`?` 可选依赖语义。

## 计划简化

计划删除 `[backend]`。生成的 Bash `.labflow/run` 从 `.labflow/config` 读取端口，
并在 `127.0.0.1` 上运行 `opencode serve`；supervisor 只连接该服务并根据健康状态
暂停或恢复调度，不拥有 OpenCode 进程。

实验室 OpenCode 以仓库根为工作目录，但 Bash launcher 设置
`OPENCODE_CONFIG_DIR=<root>/.labflow/opencode` 和
`OPENCODE_DISABLE_PROJECT_CONFIG=1`。生成的 agent profile 位于
`.labflow/opencode/agents/`，与同一仓库中 Host OpenCode 使用的 `.opencode/`
完全隔离。

`roles.<role>.kind` 被删除。所有显式 role 都是 DAG worker，固定基础提示词为：

```text
你是实验员 <role>，请按照指令要求完成任务。
```

role 的 `permissions` 是默认工具权限，artifact 可以通过自身 `permissions`
覆盖它。

只要 artifact 配置了 `goal`，该文件就自动加入其 effective inputs，无需也不应
在 `inputs` 中重复。完整规则为：

```text
effective-inputs = { goal } ∪ configured-or-derived-inputs
```

其中 configured-or-derived-inputs 是显式 `inputs`，或在未配置时由直接依赖的
`assets` 取并集。权限计算和任务提示中的文件清单都使用 effective inputs。

## Task Artifact

Task 是默认类型，保持 RFC 0001 的普通构建协议：必须配置 `goal`；`assets`
定义可读写和删除范围；`check` 是成功回答后必须存在的文件清单。成功后通过
touch `.labflow/artifacts/<name>` 发布实体 artifact。

## Learn Artifact

Learn 用于让 role 的当前 session 学习已有资产：

- 必须配置 `goal`；可以配置 `requires`、`inputs` 和 `permissions`；
- 不输出或更新资产，因此禁止配置 `assets` 和 `check`；
- 使用与 Task 相同的任务回答协议；
- 成功后由 supervisor 在内存 State 和 `states.sqlite` 中发布虚拟 artifact，
  不创建 `.labflow/artifacts/` 下的文件；
- 每当该 role 建立新 session，reducer 使该 role 的全部 Learn artifact 失效，
  持久化删除其状态，再由 DAG 自然调度重新学习；
- Host 不能 publish 或 unpublish 计划中的 Learn artifact。

因此依赖 `learn-domain.a1` 的任务只会在 a1 的当前 session 已经完成相应学习后
运行，不再需要 `_ready.a1`。计划解析和调度均不再识别 `_ready.<role>`。

Learn 的失效、重建和持久化决策全部由 reducer 完成。SQLite 删除和写入都是
携带完整参数的 Effect；Effect 不读取 State。

## Bench Artifact

Bench 将 RFC 0002 的顶层 `[benchmark.*]` 收入 artifact：

- artifact 名称后缀表示挑战者 C 的 DAG role；
- `goal` 被禁止，C 使用 Labflow 内建的挑战者任务提示；
- `bench.name` 必须符合 artifact part 命名规则，并且在整个计划中唯一；
- records 固定派生为 `.labflow/benchmarks/<bench.name>.sqlite`；
- `assets` 和 `check` 均不可配置，records 是 Labflow 管理的内部数据库；
- `bench.source` 必须是一个 JSONL 文件，其非空行按顺序构成本轮完整题集；
- `bench.public-knowledge` 可以包含文件或以 `/` 结尾的目录；
- `bench.permissions` 包含 `read`、`write` 和 `commands`。

CLI 使用完整 artifact 名定位评测：

```text
labflow bench start bench-solver.a2
labflow challenge next bench-solver.a2
labflow challenge clarify bench-solver.a2 '<text>'
labflow challenge archive bench-solver.a2
labflow bench finish bench-solver.a2
```

Host 可以按稳定的 `bench.name` 对 records 执行只读分析查询：

```text
labflow query-bench solver -e 'SELECT * FROM bench_round'
labflow query-bench solver -f analysis.sql
printf 'SELECT * FROM question' | labflow query-bench solver -f -
```

`-e/--execute` 与 `-f/--file` 必须且只能指定一个；相对 SQL 文件名以实验室根
目录解析，`-f -` 从 stdin 读取到 EOF。命令使用 SQLite read-only connection，
数据库不存在或 SQL 尝试写入
时返回非零。成功结果为 `{"columns":[...],"rows":[...]}` JSON，SQL NULL、整数、
浮点数和文本映射为对应 JSON 类型，BLOB 映射为 `{"base64":"..."}`。

`.labflow/artifacts/bench-solver.a2` 仍是表示该次构建完成的名义制品文件，
与追加保存所有历史轮次的 `.labflow/benchmarks/solver.sqlite` 相互独立。

每行题目格式为 `{ id, Q, K?, R?, tags? }`。`id` 必须非空且在文件内唯一，
`Q` 是题面，`K` 是可选澄清知识，`R` 是可选参考答案，`tags` 是默认空数组的
非空、无重复字符串集合。`bench start` 按文件行序将整份题集快照到新 round；
参考答案不由 challenge CLI 返回，也绝不发送给被测 Agent。数据库将它保存为
`question.reference_answer`，并将标签规范化保存到 `question_tag`，供后续分析。

题目、轮次、澄清和 records schema 继续遵循 RFC 0002。`source` 和隐藏
知识只对 C 可见；R 只能通过对话取得 Q 和 C 基于公开背景及 K 编写的澄清。

### 自动派生 R

计划不再包含 `bench.respondent`。Labflow 根据以下内容生成 R 的不可变、
content-addressed OpenCode agent profile：

- 固定的 R 提示词和 Labflow 模板版本；
- `public-knowledge`；
- `bench.permissions`。

ID 形如 `bench-solver-a2.<sha256>`，对应
`.labflow/opencode/agents/<id>.md`。profile 只创建、不修改；相同内容复用同一身份，配置
变化产生新身份。每次 `bench start` 创建全新的顶层 R session，每次消息显式
指定该 profile，`bench finish` 必须删除 session 后才能转正轮次。

R 的 Read 范围是 `public-knowledge + permissions.read`，Edit/Write 范围是
`permissions.write`；Glob 允许，Grep 禁止；命令只允许 `commands` 的精确规则。
R 不加入 `[roles]`、DAG State、states.sqlite 或 timeline.sqlite。

## 校验与发布

`Plan` 继续直接通过 serde 和经过校验的 newtype 反序列化，再调用
`normalize(self) -> Result<Self>` 完成跨字段约束、输入派生和 Bench 名称唯一性校验。
未知字段全部拒绝，不为旧的 `[backend]`、`roles.kind`、`[benchmark]` 或
`_ready.<role>` 提供兼容别名。

`labflow publish` 在加载计划后拒绝 Learn artifact；Bench 和 Task 的实体 artifact
仍由正常构建成功路径发布。`system-active`、`system-plan`、`system-supervisor` 和
`system-backend` 等 Host 特殊 artifact 的行为不变。

## Reducer/Effect 约束

本 RFC 不引入任何异步协调捷径。session 建立、Learn 失效、任务完成、虚拟或
实体 artifact 发布、R session 生命周期、HTTP 和 SQLite 操作都先形成 Event，
由单线程纯 reducer 决定 State 转换和后续 Effect。Effect 携带执行所需的完整
不可变数据，不能读取 State，只能执行外部作用并回投新的 Event。
