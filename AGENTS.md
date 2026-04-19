# AGENTS

本文件是仓库级协作说明，面向进入本仓库工作的自动代理与工程协作者。

## 1. 仓库定位

`astock-lifespan-alpha` 是从 `lifespan-0.01` 重构出的本地优先历史账本系统。当前主链路固定为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

当前仓库已经完成阶段八最小闭环：`data -> system` pipeline orchestration。

技术栈以 Python + DuckDB + Arrow 为主。

## 2. 目录职责

- `src/astock_lifespan_alpha/`
  - 正式业务代码。
  - 当前正式模块：`core`、`data`、`malf`、`alpha`、`position`、`portfolio_plan`、`trade`、`system`、`pipeline`。
- `scripts/`
  - 各阶段 CLI 入口。
  - 只做薄封装，通常打印 runner summary JSON。
- `tests/`
  - 单元测试与边界契约测试。
  - 修改模块边界、runner 名称、文档口径时，先看这里。
- `docs/`
  - 正式中文治理文档入口。
  - 阅读顺序从 [docs/README.md](/H:/astock_lifespan-alpha/docs/README.md) 开始。

## 3. 必守边界

这些约束已经被测试覆盖，改动前先理解，不要逆着写。

1. 不要重新引入 `structure` 或 `filter` 模块引用。
   - 约束来源：`tests/unit/contracts/test_module_boundaries.py`
2. `business modules` 不得反向依赖 `pipeline`。
   - `malf / alpha / position / portfolio_plan / trade / system`
   - 这些模块内部不应 import `astock_lifespan_alpha.pipeline`
3. `system` 只能消费正式 `trade` 输出，不得回读上游模块，也不得触发上游 runner。
4. `pipeline` 只能编排 public runner，并写自己的 `pipeline_run / pipeline_step_run`。
   - 不要在 `pipeline` 中直接写业务表。
5. 现有 foundation runner 名称是稳定契约，除非明确升级契约，否则不要改名。

## 4. 工作区与路径约定

路径契约定义在 [src/astock_lifespan_alpha/core/paths.py](/H:/astock_lifespan-alpha/src/astock_lifespan_alpha/core/paths.py)。

仓库默认使用五根目录：

- `LIFESPAN_REPO_ROOT`
- `LIFESPAN_DATA_ROOT`
- `LIFESPAN_REPORT_ROOT`
- `LIFESPAN_TEMP_ROOT`
- `LIFESPAN_VALIDATED_ROOT`

未显式设置时，会从仓库上级目录推导默认路径。

新系统数据命名空间固定为：

```text
astock_lifespan_alpha
```

不要擅自改动命名空间、DuckDB 文件命名规则或正式模块名。

## 5. 常用命令

在仓库根目录执行：

```powershell
pytest
```

运行全链路最小 pipeline：

```powershell
python scripts/pipeline/run_data_to_system_pipeline.py
```

单独运行下游阶段示例：

```powershell
python scripts/trade/run_trade_from_portfolio_plan.py
python scripts/system/run_system_from_trade.py
```

## 6. 修改原则

1. 优先沿用现有 dataclass、contracts、schema、runner 分层。
2. 修改要尽量局部，避免跨模块顺手重构。
3. 触及业务语义、表结构、runner summary、模块边界时，同步补测试。
4. 触及正式口径时，同步补 `docs/`，且继续使用中文文档治理。
5. 除非任务明确要求，不要引入新的基础设施、ORM、Web 框架或额外持久化层。

## 7. 文档治理

`docs/` 是正式治理入口，当前采用中文文档治理，结构固定为：

```text
01-design -> 02-spec -> 03-execution
```

执行层闭环固定为：

```text
card -> evidence -> record -> conclusion
```

其中：

- `evidence` 放在 `docs/03-execution/evidence/`
- `record` 放在 `docs/03-execution/records/`

如果工程改动改变了正式边界、默认值、执行口径或阶段结论，不要只改代码，文档也要一起补齐。

## 8. 提交前检查

最少完成以下检查：

1. `pytest`
2. 受影响模块的 runner 或脚本至少手动跑一次（如果改动涉及运行路径）
3. 确认没有破坏模块边界测试
4. 确认新增文档与现有中文口径一致

## 9. 对后续代理的直接要求

- 先读代码和测试，再做结构判断。
- 把测试当成正式契约的一部分，不要把它们当作可随手绕开的实现细节。
- 如果用户只给目标、不限定实现方式，默认选择最保守、最贴近现有代码风格的实现。
- 不要因为“看起来更通用”就扩大设计边界。

