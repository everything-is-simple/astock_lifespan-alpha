# 阶段十一批次 37 MALF day repair 工程收口执行卡

卡片编号：`37`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段十一规格已冻结，但 source uniqueness、engine 重复扫描、真实脚本验证尚未工程落地
- 目标：完成 `MALF day repair` 的代码、测试、真实诊断复核与文档收口

## 2. 规格输入

- `docs/02-spec/16-stage-eleven-malf-day-repair-spec-v1-20260419.md`
- `docs/03-execution/36-stage-eleven-malf-day-repair-spec-freeze-conclusion-20260419.md`

## 3. 工程输出

- `src/astock_lifespan_alpha/malf/source.py`
- `src/astock_lifespan_alpha/malf/engine.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `src/astock_lifespan_alpha/malf/diagnostics.py`
- `tests/unit/malf/*`
- `tests/unit/docs/test_malf_day_repair_specs.py`

## 4. 任务切片

1. 冻结 `backward` source 读取并补重复日期 fail-fast
2. 改造 engine 的 sample pool 复用
3. 让 `run_malf_day_build` 以逐 symbol 方式读取真实 source，消除全量 source OOM
4. 跑 `pytest`、真实诊断脚本与真实 build 手动验证

## 5. 收口标准

1. docs tests、MALF tests 与全量 `pytest` 通过
2. 真实诊断报告写明 `selected_adjust_method = backward`
3. 同一诊断窗口下 `engine_seconds` 明确低于 `6.789267`
4. `snapshot_nk / pivot_nk` 不再因 `adjust_method` 重复输入而撞主键
