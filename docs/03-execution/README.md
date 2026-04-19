# 执行区说明

`docs/03-execution/` 不是普通笔记目录，而是正式执行闭环目录。

本目录默认闭环为：

```text
card -> evidence -> record -> conclusion
```

## 阅读顺序

建议先读：

1. `00-card-execution-discipline-20260419.md`
2. `00-conclusion-catalog-20260419.md`
3. 当前批次对应的 `card`
4. 对应 `evidence`
5. 对应 `record`
6. 对应 `conclusion`

## 目录规则

根目录只放：

- `card`
- `conclusion`
- 模板
- 执行纪律
- 结论目录
- `README`

子目录规则：

- 证据放在 `docs/03-execution/evidence/`
- 记录放在 `docs/03-execution/records/`

如果把 `record` 或 `evidence` 直接放到根目录，属于治理违规，需要先回收整理再继续。

## 当前批次

当前已补齐批次：

- `01` 阶段一基础重构
- `02` MALF 文本规格冻结
- `03` MALF 图版规格冻结
- `04` MALF 契约与 Schema
- `05` MALF 语义引擎与 Runner
- `06` MALF 面向 Alpha 输出
- `07` 阶段二本地收口
- `08` Alpha PAS 触发器规格冻结
- `09` alpha_signal 汇总规格冻结
- `10` 阶段三文档收口
- `11` Alpha 契约与 Schema
- `12` Alpha 输入适配与共享骨架
- `13` Alpha 五触发器与 alpha_signal
- `14` 阶段三本地收口
- `15` alpha_signal -> position 桥接规格冻结
- `16` position 最小账本与 runner 规格冻结
- `17` portfolio_plan 最小桥接规格冻结
- `18` 阶段四文档总收口
- `19` position 契约、Schema 与 runner
- `20` position 物化与最小 portfolio_plan bridge
- `21` 阶段四本地收口
- `22` trade 最小执行账本与 runner 规格冻结
- `23` portfolio_plan -> trade 桥接规格冻结
- `24` 阶段五文档总收口
- `25` 阶段五 trade 工程收口
- `26` 阶段六 system 读出规格冻结
- `27` 阶段六 system 读出工程收口
- `28` 阶段七 data 源事实契约规格冻结
- `29` 阶段七 data 源事实契约工程收口
- `30` 阶段八 data -> system pipeline 编排规格冻结

Stage-five implementation defaults are frozen for engineering:
- `execution_price_line` is backed by `PathConfig.source_databases.market_base`.
- The replay work unit is `portfolio_id + symbol`.
- Valid `open` rows use 次日开盘执行 and materialize `filled`.
- `accepted` remains a reserved status and is not materialized by the first runner.

当前状态：
- 阶段五完成。
- `reconstruction-plan-part2` 已落档。
- `stage-six-system` 已冻结。
- 阶段六完成。
- `stage-seven-data-source-contract` 已冻结。
- 阶段七完成。
- `stage-eight-pipeline` 已冻结。
- 阶段八规格冻结，工程待实施。
