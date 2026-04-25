# Card 67 run_id key boundary contract 证据

日期：`2026-04-25`

## 命令

```powershell
pytest tests/unit/contracts/test_run_id_key_boundaries.py -q
pytest tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py -q
pytest tests/unit/docs tests/unit/contracts -q
pytest tests/unit/docs tests/unit/contracts -q --basetemp H:\Lifespan-temp\pytest-tmp-card67-docs
pytest
pytest -q --basetemp H:\Lifespan-temp\pytest-tmp-card67
```

## 关键结果

- `pytest tests/unit/contracts/test_run_id_key_boundaries.py -q`：`4 passed`
- `pytest tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py -q`：`5 passed`
- `pytest tests/unit/docs tests/unit/contracts -q`：`59 passed`
- `pytest tests/unit/docs tests/unit/contracts -q --basetemp H:\Lifespan-temp\pytest-tmp-card67-docs`：`59 passed`
- `pytest -q --basetemp H:\Lifespan-temp\pytest-tmp-card67`：`150 passed`
- `test_run_id_key_boundaries.py` 初始化 data/malf/alpha/position/portfolio_plan/trade/system/pipeline 临时 DuckDB schema。
- 契约测试读取 `PRAGMA table_info`，从真实 schema 抽取 primary key 列。
- 表分类固定为 `ledger / checkpoint / queue / run_audit`。
- `ledger / checkpoint` 主键不得包含 run lineage 字段。
- `queue` 分类只表示 per-run 执行痕迹，不承担断点续传账本职责。
- 首次全量 `pytest` 在前一次 10 分钟超时后受 `H:\Lifespan-temp\pytest-tmp` 遗留 Windows 文件锁影响，出现临时目录 cleanup / DuckDB open 错误；最终全量验证使用独立 `--basetemp` 完成。

## 产物

- `tests/unit/contracts/test_run_id_key_boundaries.py`
- `docs/03-execution/67-run-id-key-boundary-contract-card-20260425.md`
- `docs/03-execution/records/67-run-id-key-boundary-contract-record-20260425.md`
- `docs/03-execution/67-run-id-key-boundary-contract-conclusion-20260425.md`
