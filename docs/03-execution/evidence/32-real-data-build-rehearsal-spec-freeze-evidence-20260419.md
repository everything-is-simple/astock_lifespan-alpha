# 阶段九批次 32 真实建库演练规格冻结证据

证据编号：`32`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/docs
pytest -q
```

## 2. 关键结果

- `stage-nine-real-data-build` 已冻结。
- 真实 `H:\Lifespan-data` 输入输出边界已登记。
- `module-by-module build` 与 `pipeline replay` 顺序已写入正式规格。
- `run_data_to_system_pipeline` 已登记为最后 replay 入口。
- `Go+DuckDB deferred` 已写入阶段九边界。

## 3. 产物

- `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`
- `docs/03-execution/32-real-data-build-rehearsal-spec-freeze-conclusion-20260419.md`
