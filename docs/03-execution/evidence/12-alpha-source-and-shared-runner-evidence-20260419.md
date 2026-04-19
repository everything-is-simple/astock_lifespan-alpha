# 阶段三批次 12 Alpha 输入适配与共享骨架证据

证据编号：`12`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
```

## 2. 关键结果

- Alpha 共用输入适配层已固定读取 `market_base_day + malf_day.malf_wave_scale_snapshot`。
- 五个 trigger runner 共用同一 queue / checkpoint / replay 骨架。

## 3. 产物

- `src/astock_lifespan_alpha/alpha/source.py`
- `src/astock_lifespan_alpha/alpha/engine.py`
- `src/astock_lifespan_alpha/alpha/runner.py`
