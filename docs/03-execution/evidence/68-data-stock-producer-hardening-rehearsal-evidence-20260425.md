# Card 68 data stock producer 硬化与复演证据

日期：`2026-04-25`

## 命令

```powershell
pytest tests/unit/data -q
pytest tests/unit/data tests/unit/contracts/test_run_id_key_boundaries.py tests/unit/pipeline/test_pipeline_runner.py -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-targeted
pytest tests/unit/docs tests/unit/contracts -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-docs
pytest -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-full
```

## 关键结果

- `pytest tests/unit/data -q`：`13 passed`
- `pytest tests/unit/data tests/unit/contracts/test_run_id_key_boundaries.py tests/unit/pipeline/test_pipeline_runner.py -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-targeted`：`22 passed`
- `pytest tests/unit/docs tests/unit/contracts -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-docs`：`59 passed`
- `pytest -q --basetemp H:\Lifespan-temp\pytest-tmp-card68-full`：`154 passed`

## 覆盖点

- `is_a_share_stock_code` 接受正式 A 股 stock code，并拒绝 `510300.SH`。
- `run_tdx_stock_raw_ingest` 对非 stock 文件记录 `excluded_non_stock_codes`，不写 raw ledger，不挂 dirty queue。
- `audit_stock_producer_target` 可识别 day raw/base code delta，并在 market_base build 后返回 completed。
- `run_data_stock_producer_rehearsal` 可串联 raw ingest、market_base build 与 target audit。
- `test_pipeline_runner.py` 继续验证默认 pipeline step count 为 `13`。

## 偏差说明

- Windows 下并行 pytest 共享默认 `H:\Lifespan-temp\pytest-tmp` 会触发临时目录锁与 DuckDB WAL 冲突；最终验收改为串行命令并显式指定独立 `--basetemp`。

## 产物

- `src/astock_lifespan_alpha/data/target_audit.py`
- `src/astock_lifespan_alpha/data/rehearsal_runner.py`
- `scripts/data/audit_stock_producer_target.py`
- `scripts/data/run_stock_producer_rehearsal.py`
- `docs/02-spec/31-data-stock-producer-hardening-rehearsal-spec-v1-20260425.md`
- `docs/03-execution/68-data-stock-producer-hardening-rehearsal-card-20260425.md`
- `docs/03-execution/records/68-data-stock-producer-hardening-rehearsal-record-20260425.md`
