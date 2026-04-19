# 阶段七批次 29 data 源事实契约工程收口证据

证据编号：`29`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/core
pytest -q tests/unit/malf
pytest -q tests/unit/alpha
pytest -q tests/unit/position
pytest -q tests/unit/trade
pytest -q tests/unit/docs
pytest -q
```

## 2. 关键结果

- 6 个 source fact 路径已进入路径契约。
- `malf` 已支持真实 day/week/month stock adjusted 表。
- `alpha / position / trade` 已支持真实 day stock adjusted 表。
- 单元测试不再读取真实 `H:\Lifespan-data` 大库，runner contract 测试使用临时 workspace。
- 阶段七完成结论已登记。

## 3. 产物

- `src/astock_lifespan_alpha/core/paths.py`
- `src/astock_lifespan_alpha/malf/source.py`
- `src/astock_lifespan_alpha/alpha/source.py`
- `src/astock_lifespan_alpha/position/source.py`
- `src/astock_lifespan_alpha/trade/source.py`
- `docs/03-execution/29-data-source-fact-contract-alignment-engineering-closeout-conclusion-20260419.md`

