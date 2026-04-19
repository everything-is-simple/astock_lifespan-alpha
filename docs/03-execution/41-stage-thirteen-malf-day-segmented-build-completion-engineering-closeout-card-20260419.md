# 阶段十三批次 41 MALF day segmented build completion 工程收口执行卡

卡片编号：`41`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：真实全量 `run_malf_day_build` 仍缺少 segmented build、resume、progress 与 artifact 隔离
- 目标：完成 day runner 的分段、续跑、进度侧写与历史 building 库登记
- 为什么现在做：阶段九 replay 继续盲跑不会继续降低不确定性

## 2. 规格输入

- `docs/02-spec/18-stage-thirteen-malf-day-segmented-build-completion-spec-v1-20260419.md`
- `docs/03-execution/40-stage-thirteen-malf-day-segmented-build-completion-spec-freeze-conclusion-20260419.md`

## 3. 工程输出

- `src/astock_lifespan_alpha/malf/contracts.py`
- `src/astock_lifespan_alpha/malf/source.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `scripts/malf/run_malf_day_build.py`
- `tests/unit/malf/test_runner.py`

## 4. 任务切片

1. 为 `run_malf_day_build` 增加 symbol 分段参数
2. 为 day build 增加 checkpoint-based resume
3. 增加 `progress_summary`、sidecar progress 与 `artifact_summary`
4. 增加 abandoned build artifacts 登记
5. 更新中文治理文档与测试

## 5. 收口标准

1. day runner 支持 symbol range / symbol limit
2. segmented build 可复用 checkpoint resume
3. `progress_summary` 与 sidecar progress 正式输出
4. `artifact_summary` 可识别 abandoned build artifacts
5. 阶段九 replay 继续保持待阶段十三完成后重新发起
