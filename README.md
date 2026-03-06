# Quant Stock Selector

This repository contains the A-share industry trend selector.

## Script

### `a_share_trend_selector.py`

Industry-first trend selection strategy.

- Selects strong industries first
- Selects stocks from chosen industries
- Uses AkShare data
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
```

## Ignored Runtime Files

Generated files are intentionally excluded from Git:

- `.hist_cache/`
- `trend_selector_results_run*.xlsx`
- `run_full.log`
- `.DS_Store`
- `__pycache__/`
