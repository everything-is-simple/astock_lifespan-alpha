# 阶段十批次 35 MALF day 真实库诊断工程收口卡

卡片编号：`35`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段十 `stage-ten-malf-day-diagnosis` 规格已冻结，需要完成脚本入口修正、真实库诊断实现与结果裁决。
- 目标：确认 `run_malf_day_build` 真实库瓶颈位置，并把仓库状态切为“阶段十完成，阶段九重演待重新发起”。
- 为什么现在做：阶段九 blocker 已经存在，必须先把诊断链路和结论做成正式闭环，再决定下一步是最小修复还是重演。

## 2. 规格输入

- `docs/02-spec/15-malf-day-real-data-diagnosis-spec-v1-20260419.md`
- `docs/03-execution/34-malf-day-real-data-diagnosis-spec-freeze-conclusion-20260419.md`

## 3. 执行摘要

1. 为阶段九演练链路的 14 个脚本入口补齐共享 bootstrap。
2. 新增 `profile_malf_day_real_data` 只读诊断模块与 CLI。
3. 完成 bootstrap 单测、diagnostics 单测、contracts 测试与全量测试。
4. 完成真实库诊断验收并生成 JSON/Markdown 报告。
5. 正式裁决阶段十完成，阶段九重演待重新发起。

## 4. 收口标准

1. `python scripts/malf/run_malf_day_build.py` 不再依赖手动 `PYTHONPATH`。
2. `python scripts/malf/profile_malf_day_real_data.py` 能产出真实库诊断报告。
3. 报告能确认瓶颈落点。
4. README、docs 索引与结论目录已切换到阶段十完成状态。
