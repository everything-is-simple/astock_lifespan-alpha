# 批次 60 记录：MALF day live formal rebuild 与 Stage 19 重验收

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/60-malf-day-live-formal-regate-card-20260423.md`

## 1. 本轮执行

本轮严格按 Card 60 边界推进：

- 先做 live formal DB preflight
- 复跑 MALF 本地门
- 受控发起 `--no-resume` full-universe rebuild
- 仅在 run 明确 stale 后做 bookkeeping closeout

本轮没有改任何 MALF 代码、schema 或审计规则。

## 2. 关键偏差

### 2.1 本轮没有走 `.building.duckdb`

原计划接受 runner 自动新建新的 `.building.duckdb` 并最终 promotion。

实际现场不是这个路径：

- 新 run `day-107059a919fc` 直接往 `malf_day.duckdb` 写入
- 目录没有出现新的 `.building.duckdb`
- 最终在 target 上留下 interrupted run 的局部 ledger rows 与混合 checkpoint

### 2.2 stale running 判定

本轮将 `day-107059a919fc` 判定为 stale `running`，依据不是单次慢，而是：

- progress sidecar 长时间固定在同一组数值
- target / WAL 的 mtime 长时间停在同一时间窗
- 原高 CPU worker 消失
- 同命令行空挂进程不再产生任何推进

### 2.3 bookkeeping closeout

本轮没有删除历史 run，也没有删除审计痕迹。

只做了最小治理：

- 将 `day-107059a919fc` 标记为 `interrupted`
- 将其遗留的 `25` 条 `running queue` 改成 `interrupted`

历史 stale run `day-d696fdcd4774` 与 `day-3343b24d0f0b` 保持原样，等待后续统一治理。

## 3. 为什么本轮不能继续 forced audit

Card 60 预期是：

- rebuild 成功
- 得到新的 `completed` run
- 再用新 run 强制复跑 audit

但本轮失败后出现的是：

- `day-107059a919fc = interrupted`
- formal target 已混入 interrupted rows
- checkpoint 也已部分切到 `day-107059a919fc`

在这种状态下继续跑 forced audit，只会把“不完整 target 写入”误当成新 formal ledger，因此不符合 Card 60 验收口径。

## 4. 本轮正式登记

- Card 60 本地门已通过
- Card 60 live rebuild 已正式发起，但未完成
- 本轮 blocker 不在 `engine` 语义，而在 `day` formal rebuild 的 target 直写/停滞路径
- `Stage 19` live formal gate 本轮没有通过，也没有形成可接受的新 formal run
- 当前 MALF formal target 已需要后续专卡做恢复或重建治理
