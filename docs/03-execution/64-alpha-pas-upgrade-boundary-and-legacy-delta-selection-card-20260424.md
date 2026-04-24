# 批次 64 卡片：Alpha(PAS) 核心升级边界与 legacy delta selection

- 日期：`2026-04-24`
- 对应规格：`docs/02-spec/28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md`

## 1. 目标

把当前已放行的 `alpha` producer 与历史 PAS 可继承能力之间的差距正式收口，明确下一轮真正值得做的最小升级目标。

## 2. 本卡边界

本卡允许：

- 只读回查当前 `alpha` live formal DB
- 对照 Card 57 的 legacy delta register
- 对照 Card 63 的 lineage lessons
- 新增 `02-spec` 升级边界规格
- 新增 Card 64 evidence、record、conclusion
- 更新 `docs/02-spec/README.md`
- 扩展 `tests/unit/docs/test_alpha_specs.py`

本卡明确排除：

- 重做 `alpha` freeze audit
- 修改 `alpha` 运行时代码
- 修改 `alpha` schema
- 引入新的 PAS scoring producer
- 恢复 `16-cell`
- 把 trade / 收益 / quality filter 写回核心合同

## 3. 任务切片

1. 只读回查 live formal `alpha` 与 `position` 消费面。
2. 明确当前正式 `alpha` 的角色边界。
3. 明确历史 PAS 能力的三类去向。
4. 明确下一轮最小治理升级目标。
5. 写入 `02-spec`、Card 64 evidence / record / conclusion。
6. 更新阅读路径与文档契约。

## 4. 验收

本卡通过条件：

- 新规格明确 `alpha` 当前是 `trigger ledger producer`
- 新规格明确 `alpha` 不是完整 `PAS scoring engine`
- 新规格明确 `alpha(PAS)` 只消费 MALF 正式字段
- 新规格明确 `16-cell` 当前系统不存在，不进入下一轮治理候选
- 新规格明确下一轮方向是治理升级，不是评分引擎升级
- 文档契约测试通过
- `alpha` 单测通过
- 模块边界测试通过

## 5. 执行命令

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_alpha_specs.py -q
D:\miniconda\py310\python.exe -m pytest tests/unit/alpha -q
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```
