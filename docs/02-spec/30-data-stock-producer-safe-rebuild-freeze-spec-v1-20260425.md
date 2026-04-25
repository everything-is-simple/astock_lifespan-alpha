# Card 66 data stock producer 安全重建规格 v1

日期：`2026-04-25`
状态：`冻结`
文档标识：`card66-data-stock-producer-safe-rebuild`

## 1. 定位

本规格冻结新版 `data` 模块首轮 producer 恢复范围。

首轮只恢复 stock-only 的本地生产闭环：

```text
TDX offline -> isolated raw_market -> isolated market_base
```

本轮不改变现有主链路消费合同：

```text
data(source fact) -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

`pipeline` 继续保持现有 13 step 编排，不默认触发 data producer。

## 2. 老库安全边界

已有 `H:\Lifespan-data` 老库是正式 source fact 输入。

新构建的 data producer 默认不得对老库执行写入、覆盖、删除或原地迁移。

允许的唯一老库访问是：

- 使用 DuckDB `read_only=True` 做 source fact 审计
- 读取覆盖日期、表结构、行数、code 数、adjust_method 分布与 backward 唯一性

producer 写入目标必须是隔离 `target_data_root`。

如果未来需要替换正式老库，必须另开卡，并先生成完整备份数据库、校验报告和可回滚切换路径。

## 3. 数据源原则

本轮不依赖网络数据源。

正式优先级：

1. 本地 TDX 官方离线数据目录
2. 本地可控的第三方 `pytdx / mootdx` 扩展，后续另开卡
3. 网络 API 不进入本轮

本轮不恢复 Tushare、TdxQuant 实时日更、index、block 或 objective profile。

## 4. Runner 合同

新增 runner：

- `run_tdx_stock_raw_ingest`
- `run_market_base_build`
- `audit_data_source_fact_freeze`

新增 CLI：

- `scripts/data/run_tdx_stock_raw_ingest.py --target-data-root <isolated-root>`
- `scripts/data/run_market_base_build.py --target-data-root <isolated-root>`
- `scripts/data/audit_data_source_fact_freeze.py`

`run_tdx_stock_raw_ingest` 输出 isolated raw ledger，并记录：

- candidate / processed / skipped / failed file count
- inserted / reused / rematerialized bar count
- dirty queue

`run_market_base_build` 从 isolated raw ledger 构建 isolated base ledger，并记录：

- source row count
- inserted / reused / rematerialized count
- consumed dirty count

`audit_data_source_fact_freeze` 只读审计现有六个 source fact DB。

## 5. 验收标准

工程实现必须满足：

1. producer 默认拒绝写入 `WorkspaceRoots.data_root` 对应 source fact root。
2. audit 对老库连接必须使用 `read_only=True`。
3. TDX parser 支持 `Backward-Adjusted / Forward-Adjusted / Non-Adjusted`。
4. raw ingest 可以跳过未变化文件，并对变化标的挂 dirty。
5. market_base build 可以消费 dirty queue，并不得删除范围外已有行。
6. `pipeline` step count 和 `stage8_pipeline_v1` 合同不变。
7. 全量测试与现有 pipeline 回归通过后，Card 66 才允许登记工程收口。
