# Card 68 data stock producer 硬化与复演记录

日期：`2026-04-25`

## 做了什么

1. 新增 A 股 stock-only code classifier。
2. `run_tdx_stock_raw_ingest` 新增非 stock 文件排除统计。
3. 新增 isolated target audit，用于检查 raw/base code delta 与 backward duplicate groups。
4. 新增 isolated producer rehearsal runner，串联 raw ingest、market_base build 与 target audit。
5. 新增 target audit 与 rehearsal CLI。
6. 补充 Card 68 单元测试与中文治理文档。

## 边界裁决

- `510300.SH` 继续解释为老库只读 audit anomaly。
- 本卡不原地修复、删除或迁移正式 `H:\Lifespan-data` 老库数据。
- 本卡不扩展 index / block，也不引入网络日更。
- `pipeline` 默认 13 step 合同不变。

## 备注

后续若需要把 isolated producer 产物提升为正式 source fact，必须另开 promotion / cutover 卡，并先定义备份、校验报告与回滚路径。
