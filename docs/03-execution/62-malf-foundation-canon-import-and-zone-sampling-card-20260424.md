# 批次 62 卡片：MALF 地基 Canon、包入口修复与 zone sampling 收口

- 日期：`2026-04-24`
- 对应规格：`docs/02-spec/26-malf-foundation-canon-v1-20260424.md`

## 1. 目标

把 MALF 作为系统地基的核心概念正式收口，同时修复 `astock_lifespan_alpha.malf` 包入口被 audit 绘图依赖污染的问题，并把 Card 61 剩余的 `zone_coverage = 3` 收敛为 sample 选择问题。

## 2. 本卡边界

本卡允许：

- 新增 MALF Canon 中文规格
- 调整 `malf/__init__.py` 的轻量 public import
- 在 dev extra 中补齐 audit 所需绘图依赖
- 修改 audit sample 选择逻辑，使 sample 优先覆盖四个 `wave_position_zone`
- 补齐 public import、sample coverage、文档契约测试
- 用真实五根目录与 `D:\miniconda\py310` 执行验证

本卡明确排除：

- 修改 MALF public ledger schema
- 修改 `engine.py` 状态机
- 修改下游模块契约
- 把 `zone_coverage` 解释为交易或概率语义
- 重新发起 full-universe rebuild

## 3. 执行命令

```powershell
D:\miniconda\py310\python.exe -m pip install -e ".[dev]"
D:\miniconda\py310\python.exe -m pytest tests/unit/malf/test_public_imports.py tests/unit/malf/test_audit_sampling.py tests/unit/docs/test_malf_specs.py -q
D:\miniconda\py310\python.exe -m pytest tests/unit/malf -q
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
D:\miniconda\py310\python.exe scripts/malf/audit_malf_day_semantics.py --run-id day-e687a8277f61 --sample-count 12
```

运行真实 audit 时必须显式使用：

- `LIFESPAN_REPO_ROOT=H:\astock_lifespan-alpha`
- `LIFESPAN_DATA_ROOT=H:\Lifespan-data`
- `LIFESPAN_REPORT_ROOT=H:\Lifespan-report`
- `LIFESPAN_TEMP_ROOT=H:\Lifespan-temp`
- `LIFESPAN_VALIDATED_ROOT=H:\Lifespan-Validated`

## 4. 验收

本卡通过条件：

- `import astock_lifespan_alpha.malf` 不加载 `astock_lifespan_alpha.malf.audit`
- 核心 runner 仍可从 `astock_lifespan_alpha.malf` 导入
- audit/dev 依赖显式包含 `pandas / matplotlib`
- `state_snapshot_sample` 优先覆盖四个 `wave_position_zone`
- MALF Canon 文档契约通过
- 模块边界测试通过
- forced audit 直接命中 `day-e687a8277f61`
- `zone_coverage` 不再因 sample 选择触发 flag
