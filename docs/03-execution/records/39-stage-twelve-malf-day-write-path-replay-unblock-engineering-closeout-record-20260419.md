# 批次 39 记录：阶段十二 MALF day 写路径重演 unblock 工程收口

## 1. 已实施修复

- `MalfRunSummary` 新增 `write_timing_summary`
- diagnostics 新增总体与逐 symbol 的 write timing breakdown
- runner 写路径按小批次聚合 symbol 结果
- ledger 插入改为 DuckDB registered relation 写入，优先 Arrow，缺失时使用 pandas，最后 fallback 到 `executemany`
- runner 对旧真实库遗留 `running` 状态启用 building 库重建与旧库 backup promotion

## 2. 已验证结果

- MALF 单元测试通过
- 模块边界与 runner 合同测试通过
- 全量 `pytest` 通过
- 优化后真实诊断窗口：
  - `write_seconds = 1.491133`
  - `insert_ledgers_seconds = 1.329266`
  - `checkpoint_seconds = 0.066266`
  - `queue_update_seconds = 0.066398`
- 安装 `pyarrow 23.0.1` 后真实诊断窗口：
  - `write_seconds = 0.911749`
  - `insert_ledgers_seconds = 0.755103`
  - `checkpoint_seconds = 0.05889`
  - `queue_update_seconds = 0.066466`

## 3. 剩余偏差

- 真实全量 build 仍超过 60 分钟观察窗
- building 库持续增长，说明进程在推进而非卡死
- 阶段九真实重演尚未重新打通，不能登记为完成
- 下一轮应在超过 60 分钟的更长执行窗口下验证完成性，或进一步设计分段落库与更粗粒度 materialization
