# Quant Stock Selector

This repository contains the A-share industry trend selector.

## Script

### `a_share_trend_selector.py`

Industry-first trend selection strategy.

- Selects strong industries first
- Selects stocks from chosen industries
- Uses Tushare as the primary data source
- Uses AkShare as a fallback when Tushare does not return usable data
- Keeps local caches under `.hist_cache/` to speed up repeated runs
- Outputs Excel reports with `industry`, `stock`, and `diagnostics` sheets

Run:

```bash
.venv/bin/python a_share_trend_selector.py
```

## Environment

Use the project virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export TUSHARE_TOKEN=your_tushare_token_here
```

## Data Source Auth

The repository uses:

- `TUSHARE_TOKEN` for the main data path
- AkShare as the fallback data source from `requirements.txt`
- `CODE_LIMIT` for small-scope debugging runs
- `ALLOW_COMPONENT_SHEET_FALLBACK=1` only if you explicitly want to reuse the last Excel result as a temporary component fallback

## Ignored Runtime Files

Generated files are intentionally excluded from Git:

- `.hist_cache/`
- `trend_selector_results_run*.xlsx`
- `run_full.log`
- `.DS_Store`
- `__pycache__/`

If your entire `ifind` workspace is synced through iCloud, the `.hist_cache/` directory will be synced with it as well, so another machine using the same synced folder can reuse the cache directly.
