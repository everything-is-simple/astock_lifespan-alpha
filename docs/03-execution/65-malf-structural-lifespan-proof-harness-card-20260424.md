# 批次 65 卡片：MALF 结构寿命语义证明夹具

- 日期：`2026-04-24`
- 对应规格：`docs/02-spec/29-malf-structural-lifespan-proof-harness-v1-20260424.md`

## 1. 目标

把 MALF Canon 中最关键的结构寿命语义落成可回归证明资产。

本卡不重开 `malf 放行`，只证明并锁住：

- `break != confirmation`
- `reborn -> alive` 必须由新方向正式 `HH / LL` 确认
- `new-count × no-new-span × life-state` 共同定义波段寿命
- `WavePosition` 只表达历史生命位置

## 2. 本卡边界

本卡允许：

- 新增 `02-spec` 结构寿命语义证明规格
- 新增 Card 65 evidence、record、conclusion
- 扩展 `tests/unit/malf/test_engine.py`
- 扩展 `tests/unit/docs/test_malf_specs.py`
- 只读核对 live formal `malf_day.duckdb`

本卡明确排除：

- 修改 public schema
- 修改 runner 名称
- 修改 alpha / position / downstream 合同
- 引入交易、概率、均线或评分语义
- 重做 data -> system 或 system -> data 放行

## 3. 任务切片

1. 只读回查 `day-e687a8277f61` formal MALF run。
2. 增补 engine 对称证明夹具。
3. 增补文档契约，锁住 Card 65 规格短语。
4. 写入 `02-spec`、Card 65 evidence / record / conclusion。
5. 运行 MALF 文档契约、MALF 单测与模块边界回归。

## 4. 验收

本卡通过条件：

- 新规格明确 MALF 是结构事实账本
- 新规格明确唯一输入仍是 `price bars`
- 新规格明确 `break != confirmation`
- 新规格明确 `reborn -> alive` 依赖新方向 `HH / LL`
- 新测试覆盖上升破坏进入下降 `reborn` 并等待 `LL`
- 新测试覆盖下降破坏进入上升 `reborn` 并等待 `HH`
- 文档契约、MALF 单测、模块边界测试通过

## 5. 执行命令

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
D:\miniconda\py310\python.exe -m pytest tests/unit/malf -q
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```
