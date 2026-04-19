# 批次 37 记录：阶段十一 MALF day repair 工程收口

## 1. 已实施修复

- `stock_daily_adjusted` 的 `Timeframe.DAY` 读取固定为 `adjust_method = backward`
- diagnostics 会记录过滤前重复事实与过滤后 contract 状态
- 正式 runner 对过滤后仍重复的 `symbol + trade_date` 直接 fail-fast
- `run_malf_engine()` 会拒绝重复 `bar_dt` 输入
- `_rank_snapshots()` / `_build_profiles()` 改为 sample pool 复用
- `run_malf_day_build()` 改成逐 symbol source 读取，避免全表一次性装入 Python

## 2. 已验证结果

- 单测确认 `backward` 口径只读
- 单测确认 duplicate `backward` 输入 fail-fast
- 单测确认缓存后的 rank/profile 语义与旧逻辑一致
- 真实诊断窗口下 `engine_seconds` 从 `6.789267` 降到 `1.419344`

## 3. 剩余偏差

- 同一真实诊断窗口下，新主瓶颈已从 `engine_timing` 转到 `write_timing`
- `python scripts/malf/run_malf_day_build.py` 在真实全量库上 10 分钟内未完成，本轮记录为长时间执行偏差而非 source OOM 回归
