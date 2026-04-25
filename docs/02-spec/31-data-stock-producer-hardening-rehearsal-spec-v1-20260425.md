# Card 68 data stock producer 硬化与复演规格 v1

日期：`2026-04-25`
状态：`冻结`
文档标识：`card68-data-stock-producer-hardening-rehearsal`

## 1. 定位

本规格沿 Card 66 边界继续加强新版 `data` producer。

目标不是接管正式 `H:\Lifespan-data` 老库，而是把 stock-only isolated producer 从“可跑”推进到“可复演、可对账、可登记为切换准备态”。

本轮继续固定：

```text
TDX offline -> isolated raw_market -> isolated market_base -> isolated target audit
```

默认 `data -> system` pipeline 仍保持 13 step，不把 producer 作为前置步骤。

## 2. 范围

本轮纳入：

- stock-only 资产门禁。
- isolated target audit。
- isolated producer rehearsal runner。
- CLI 入口与单元测试。
- Card 68 中文治理文档。

本轮不纳入：

- 写入、替换、修补或删除正式 `H:\Lifespan-data` 老库。
- index / block producer。
- Tushare / TdxQuant 网络日更。
- pipeline 默认步骤变更。

## 3. stock-only 门禁

TDX stock ingest 默认只接受 A 股 stock code。

允许范围：

- `600 / 601 / 603 / 605 / 688 / 689` 上交所 stock。
- `000 / 001 / 002 / 003 / 300 / 301` 深交所 stock。

不符合上述范围的代码必须被排除，并记录到：

- `excluded_file_count`
- `excluded_non_stock_file_count`
- `excluded_non_stock_codes`

被排除文件不得写入 isolated `stock_daily_bar`，也不得进入 dirty queue。

`510300.SH` 是现有老库只读 audit anomaly；本轮只通过新 producer 门禁避免复现，不原地修复老库。

## 4. target audit 与 rehearsal

新增 `audit_stock_producer_target`，对 isolated `target_data_root` 下 day/week/month raw/base ledger 做只读审计，输出：

- row count
- symbol count
- date range
- adjust_method 分布
- raw/base code delta
- backward duplicate groups
- excluded non-stock codes
- gate failures

新增 `run_data_stock_producer_rehearsal`，串起：

```text
run_tdx_stock_raw_ingest -> run_market_base_build -> audit_stock_producer_target
```

默认 scope：

- `day:backward`
- `day:forward`
- `day:none`
- `week:backward`
- `month:backward`

所有写入只能发生在 isolated `target_data_root`。

## 5. 验收标准

工程实现必须满足：

1. `510300.SH` 等非 stock code 不得写入 isolated stock raw ledger。
2. 非 stock code 不得挂 dirty queue。
3. target audit 能识别 raw/base code delta，并在 delta 非空时返回 failed。
4. raw/base 构建完成后，target audit gate 可以返回 completed。
5. rehearsal summary 必须包含 raw summaries、base summaries、target audit summary 与 gate status。
6. 新 CLI 必须要求显式 `--target-data-root`。
7. 默认 pipeline step count 仍为 13。
8. 全量测试通过后，Card 68 才允许登记工程收口。
