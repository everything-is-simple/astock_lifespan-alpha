# 批次 43 卡片：阶段十五 MALF day schema backfill 兼容修复

批次编号：`43`
日期：`2026-04-19`
文档标识：`stage-fifteen-malf-day-schema-backfill-compatibility`

## 1. 任务

解除阶段十四真实 proof 在第一步暴露的 `malf_run` 旧 schema 兼容性 blocker，并重新发起 `100 / 500 / 1000 symbol` proof。

## 2. 范围

本批次只处理：

- `initialize_malf_schema()` 对旧版 `malf_run` 的 DuckDB 兼容 backfill
- `repair_malf_day_schema` 显式 repair/probe 入口
- `scripts/malf/repair_malf_day_schema.py` 薄 CLI
- 阶段十五中文治理文档与测试

本批次不处理：

- MALF 语义状态机调整
- full-universe promote
- building artifact archive/remove
- 阶段九 replay 完成登记

## 3. 执行命令

计划执行：

```text
pytest
python scripts/malf/repair_malf_day_schema.py
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
```

若 `100 symbol` 通过，再继续阶段十四既定的 `500 / resume / 1000 symbol` proof。

## 4. 验收

- 旧版 `malf_run` backfill 单元测试通过
- repair/probe 幂等单元测试通过
- 全量 `pytest` 通过
- 真实 repair CLI 输出 `status = completed`
- 真实 `100 symbol` proof 重新启动并生成 summary/progress/artifact
