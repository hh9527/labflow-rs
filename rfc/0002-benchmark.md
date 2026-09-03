# RFC 0002：被测 Agent 评测协议

- 状态：草案
- 创建日期：2026-09-02

## 摘要

本 RFC 定义评测实验中的被测 Agent 协议。评测角色 C 控制题目推进，被测
OpenCode agent R 在每轮全新的顶层会话中作答。Labflow 负责题目投递、轮次状态、
对话归档、结果转正和会话清理，不负责判断答案质量。

## 计划表面

```toml
[benchmark."a.b1"]
records = "benchmarks/a-b1.sqlite"
requires = ["system-active", "_ready.b1"]
public-knowledge = ["knowledge/public/"]

challenge.source = "datasets/questions.jsonl"
challenge.questions = "datasets/round-1.ids"

[benchmark."a.b1".respondent]
read = []
write = ["b1-ws/a.json"]
commands = ["just verify"]
```

名称 `<respondent>.<challenger-role>` 由两个合法的 artifact part 组成。上例中
`a` 是被测 OpenCode agent R，`b1` 是承担 C 的计划角色。benchmark 自动声明
worker artifact `bench-a.b1`，其唯一输出和检查项为 `records`。

以下字段都是实验室根目录下的规范化相对路径：

- `records` 必须是文件，表示该 benchmark 独立的追加式 SQLite 数据库；
- `public-knowledge`、`respondent.read` 和 `respondent.write` 可以是文件或以
  `/` 结尾的目录；
- `challenge.source` 和 `challenge.questions` 必须是文件。

自动生成的 `bench-<respondent>.<challenger-role>` 不能与显式 artifact 重名。
`requires` 使用与普通 artifact 相同的依赖语义。

## 题目输入

`challenge.source` 是 UTF-8 JSONL，每个非空行具有唯一的 `id`：

```json
{"id":"q1","Q":"问题一","K":"仅供 C 使用的澄清知识"}
```

`challenge.questions` 是 UTF-8 文本，每个非空行包含一个 ID。行首尾空白被
移除，空行被忽略，不支持注释，不允许重复 ID。文件顺序是本轮题目顺序；
每个 ID 必须在 source 中唯一存在。

`bench start` 将本轮 ID 及对应 Q、K 快照导入 records。后续命令不重新读取
source，因此源文件在轮次运行期间变化也不会改变本轮内容。

## 信息与能力边界

C 可以读取题目 source、questions、公开背景和 benchmark 的 records。R：

- 只能通过 `challenge next` 的对话消息取得当前 Q；
- 不能直接读取 source 或 questions；
- 不能直接取得 K；
- 只能通过 `challenge clarify` 取得 C 编写的澄清文本；
- 可以通过 Read 查看 `public-knowledge + respondent.read`；
- 可以读写或删除 `respondent.write`；
- 可以使用 Glob 发现路径，不能使用 Grep；
- 只能执行 `respondent.commands` 中逐字匹配的固定命令。

R session 的 permission 位于被测 agent 自身 permission 之后，以 OpenCode 的
最后匹配规则收窄能力。命令不自动添加通配符；`just verify` 不等于
`just verify *`。Host 对固定命令及其 recipe 不泄漏隐藏信息负责。

## CLI

### 开始轮次

```text
labflow bench start <name>
```

定位 `<name>` 的 records，创建状态为 `current` 的新 round，在同一事务中导入
questions 文件列出的全部题目快照，并创建全新的 R session。成功返回：

```json
{"round":"<round-id>"}
```

同一 benchmark 同时只能存在一个 current round。R session 不设置 OpenCode
`parentID`；它与 C 的归属是 records 中 current round 所表达的逻辑关系。
因此 R 可称为 C 的逻辑子会话，但在 OpenCode 中直接使用顶层 session；只有 C 的
会话由 DAG 管理，R 的会话过程不进入 timeline.sqlite 或 states.sqlite。

### 下一题

```text
labflow challenge next <name>
```

当前不得有尚未归档的问题。命令从当前 round 的剩余 ID 中按顺序选择下一题，
将其记录为 current question，只向 R 投递 Q，保存首轮回答，并向 C 返回：

```json
{"Q":"...","K":"...","reply":"..."}
```

没有剩余题目时返回严格的 JSON `null`。

### 澄清

```text
labflow challenge clarify <name> '<text>'
```

当前必须存在问题，且最多允许三次澄清。Labflow 先记录 C 的文本，再将其投递
给同一 R session，保存回答并返回：

```json
{"reply":"..."}
```

### 归档

```text
labflow challenge archive <name>
```

将当前题的 Q、K、首轮回答和全部澄清轮次机械归档，将题目状态改为
`archived`，并清除 current question。该命令不评价答案质量。

### 完成轮次

```text
labflow bench finish <name>
```

只有不存在 pending/current question 时才可完成。Labflow 完成机械汇总，删除
本轮 R session并等待 OpenCode 确认，然后在 SQLite 事务中将 round 从
`current` 转为 `committed`。命令成功后 C 才可回答“完成任务。”，随后由
supervisor check 并 publish `bench-<name>`。

异常中断的轮次和对话记录不删除，可以标记为 `abandoned` 或 `failed`，但分析
正式结果时只读取 `committed` round。

## Records

records 是每个 benchmark 独立、持续追加的 SQLite 数据库。它是自包含的评测
过程记录，不依赖回查 OpenCode session、message 或 event：

```text
bench_round
  id, status, respondent, session_id, started_at, finished_at,
  configuration_revision

question
  bench_round_id, question_id, ordinal, k, status, archived_at

turn
  bench_round_id, question_id, turn_index, is_last_turn,
  q, a, started_at, finished_at

action
  bench_round_id, question_id, turn_index, action_index,
  kind, subject, started_at, finished_at, result
```

一个 turn 是一次完整的 C 到 R 问答。首轮 q 是原始 Q，后续 q 是 C 实际发送
的澄清文本；a 始终保存 R 的完整原文。K 留在 question 输入快照中，因为它
不一定被发送给 R。archive 将当前 turn 标为 `is_last_turn`。

action 的 kind 至少包括 reasoning、text、read、edit、write、glob、bash 和
other-tool。只保存过程元数据；不保存 reasoning 内容、文件内容、命令输出或
完整 OpenCode payload。运行中的 R session ID 仅是 current round 的恢复字段，
finish 后清空，不作为分析溯源。

每次 start 创建新 round；所有插入均携带 round ID；finish 只转正当前批次，
永不覆盖既有 committed 数据。artifact touch file 表示该数据库新增了一批已经
转正的结果，而不是表示数据库首次创建。

## Reducer/Effect

协议严格遵守 RFC 0001 的 reducer/effect 原则，但 bench CLI 运行独立的短生命
周期 reducer runtime，不通过 supervisor 转发命令。每次 CLI 从该 benchmark
的 records 恢复状态，把命令转换为初始 Event，并串行执行 reducer 产生的
SQLite/HTTP Effect。CLI 命令处理代码本身不能绕过 Effect 直接修改 records
或调用 OpenCode。

R session 不进入 DAG 的 `State.sessions`，也不写 `states.sqlite`；其 session ID
只记录在独立 records 的 current round 中。supervisor 仍只管理 C 的 DAG
session，并通过现有 HTTP response/event 感知 C 的任务结束。R 是顶层 session，
但必须在对应 round 转正前由 bench CLI 确认删除。

reducer 是以下决策的唯一所有者：

- 当前 round、current question 和剩余题目；
- 何时创建、使用和删除 R session；
- 澄清次数、命令前置条件和状态转换；
- 接受或拒绝异步完成 Event；
- records Effect、HTTP Effect 和 CLI 响应 Effect 的先后关系；
- 失败、重试、abandon 和恢复。

Effect 携带执行所需的完整不可变参数，只执行文件读取、SQLite 事务、OpenCode
HTTP 请求或 CLI 响应，并回投事实 Event。Effect 不能读取 State，executor、
actor 和 CLI 都不能自行选择题目、推进轮次、重试、切换 session 或判断响应
是否过期。
