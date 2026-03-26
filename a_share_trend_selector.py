#!/usr/bin/env python3
"""行业先筛选、行业内选股（按 RPS 与加速度）。"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import os
import time
import tempfile
import threading
from dataclasses import dataclass
import fcntl
from datetime import datetime, timedelta
import math

import pandas as pd
import tushare as ts
from ifind_http import build_client

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IFIND_STOCK_META_CACHE: dict[str, pd.DataFrame] = {}
_TUSHARE_STOCK_META_CACHE: dict[str, pd.DataFrame] = {}
_IFIND_CLIENT = None
_IFIND_CLIENT_LOCK = threading.Lock()
_TS_PRO_CLIENT = None
_TS_PRO_LOCK = threading.Lock()
_IFIND_HIST_LOCK = threading.Lock()
_TS_SW_DAILY_LOCK = threading.Lock()
_TS_SW_DAILY_LAST_CALL = 0.0
_TS_META_LOCK = threading.Lock()
_TS_META_LAST_CALL = 0.0


@dataclass
class Config:
    start_date: str
    end_date: str
    output_dir: str = SCRIPT_DIR
    hist_cache_dir: str = ".hist_cache"
    industry_top_n: int = 7
    industry_rps50_min: float = 60.0
    industry_rps20_min: float = 60.0
    industry_delta_rps20_min: float = 15.0
    stock_rps20_min: float = 60.0
    stock_per_industry: int = 5
    stock_min_mkt_cap_yi: float = 0.0
    stock_data_max_staleness_days: int = 1
    component_min_coverage_ratio: float = 0.8
    component_cache_max_age_days: int = 5
    io_workers: int = 8
    io_batch_size: int = 80


def _strategy_fetch_start_date(start_date: str, end_date: str, calendar_lookback_days: int = 400) -> str:
    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(end_dt):
        return start_date
    effective = end_dt - timedelta(days=calendar_lookback_days)
    if pd.isna(start_dt):
        return effective.strftime("%Y-%m-%d")
    return max(start_dt, effective).strftime("%Y-%m-%d")


def _run_with_retry(func, max_retries: int = 3, base_sleep: float = 1.0, timeout: float | None = 20.0):
    """Run `func` with retries and an optional timeout (seconds).

    Running in a thread lets us enforce a hard timeout and retry.
    """
    last_err: Exception | None = None
    for i in range(max_retries):
        try:
            if timeout is None:
                return func()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(func)
                return fut.result(timeout=timeout)
        except Exception as err:
            last_err = err
            if i < max_retries - 1:
                time.sleep(base_sleep * (i + 1))
    raise RuntimeError(f"调用失败(重试{max_retries}次): {last_err}")


def _normalize_ts_code(code: str) -> str:
    num = str(code).strip()
    if "." in num:
        return num.upper()
    if not num.isdigit() or len(num) != 6:
        return num
    if num.startswith(("6", "9")):
        return f"{num}.SH"
    if num.startswith(("4", "8")):
        return f"{num}.BJ"
    return f"{num}.SZ"


def _get_ifind_client():
    global _IFIND_CLIENT
    if _IFIND_CLIENT is not None:
        return _IFIND_CLIENT
    with _IFIND_CLIENT_LOCK:
        if _IFIND_CLIENT is None:
            _IFIND_CLIENT = build_client()
    return _IFIND_CLIENT


def _get_tushare_pro():
    global _TS_PRO_CLIENT
    if _TS_PRO_CLIENT is not None:
        return _TS_PRO_CLIENT
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("缺少 TUSHARE_TOKEN 环境变量")
    with _TS_PRO_LOCK:
        if _TS_PRO_CLIENT is None:
            ts.set_token(token)
            _TS_PRO_CLIENT = ts.pro_api(token)
    return _TS_PRO_CLIENT


def _ifind_stock_meta_cache_path(cache_dir: str, as_of_date: str) -> str:
    d = pd.to_datetime(as_of_date, errors="coerce")
    tag = d.strftime("%Y%m%d") if pd.notna(d) else "unknown"
    return os.path.join(cache_dir, f"ifind_stock_meta_{tag}.csv")


def _tushare_stock_meta_cache_path(cache_dir: str, as_of_date: str) -> str:
    d = pd.to_datetime(as_of_date, errors="coerce")
    tag = d.strftime("%Y%m%d") if pd.notna(d) else "unknown"
    return os.path.join(cache_dir, f"tushare_stock_meta_{tag}.csv")


def _load_stock_meta_cache_latest(cache_dir: str) -> pd.DataFrame:
    pattern = os.path.join(cache_dir, "ifind_stock_meta_*.csv")
    for path in sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True):
        try:
            df = pd.read_csv(path, dtype={"ts_code": str, "industry_code": str, "ifind_industry_code": str})
            required = {"ts_code", "stock_name", "industry_name", "industry_code"}
            if not required.issubset(df.columns):
                continue
            df["ts_code"] = df["ts_code"].astype(str).map(_normalize_ts_code)
            df["industry_code"] = df["industry_code"].astype(str)
            df["ifind_industry_code"] = df.get("ifind_industry_code", pd.Series(dtype=str)).astype(str)
            df["stock_name"] = df["stock_name"].astype(str)
            df["industry_name"] = df["industry_name"].astype(str)
            df = df.dropna(subset=["ts_code", "industry_name", "industry_code"])
            if not df.empty:
                return df.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)
        except Exception:
            continue
    return pd.DataFrame(
        columns=["ts_code", "stock_name", "industry_name", "industry_code", "ifind_industry_code"]
    )


def _load_tushare_stock_meta_cache_latest(cache_dir: str) -> pd.DataFrame:
    pattern = os.path.join(cache_dir, "tushare_stock_meta_*.csv")
    for path in sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True):
        try:
            df = pd.read_csv(path, dtype={"ts_code": str, "industry_code": str})
            required = {"ts_code", "stock_name", "industry_name", "industry_code"}
            if not required.issubset(df.columns):
                continue
            df["ts_code"] = df["ts_code"].astype(str).map(_normalize_ts_code)
            df["industry_code"] = df["industry_code"].astype(str).str.extract(r"(\d{6})", expand=False)
            df["stock_name"] = df["stock_name"].astype(str)
            df["industry_name"] = df["industry_name"].astype(str)
            df["ifind_industry_code"] = df.get("ifind_industry_code", pd.Series(dtype=str)).astype(str)
            df = df.dropna(subset=["ts_code", "industry_name", "industry_code"])
            if not df.empty:
                return df.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)
        except Exception:
            continue
    return pd.DataFrame(
        columns=["ts_code", "stock_name", "industry_name", "industry_code", "ifind_industry_code"]
    )


def _throttle_tushare_meta(min_interval_seconds: float = 0.35) -> None:
    global _TS_META_LAST_CALL
    with _TS_META_LOCK:
        now = time.time()
        wait_seconds = min_interval_seconds - (now - _TS_META_LAST_CALL)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _TS_META_LAST_CALL = time.time()


def _fetch_tushare_stock_metadata(as_of_date: str, cache_dir: str) -> pd.DataFrame:
    cached = _TUSHARE_STOCK_META_CACHE.get(as_of_date)
    if cached is not None:
        return cached.copy()

    cache_path = _tushare_stock_meta_cache_path(cache_dir, as_of_date)
    if os.path.exists(cache_path):
        try:
            cached_df = _load_tushare_stock_meta_cache_latest(cache_dir)
            if not cached_df.empty:
                _TUSHARE_STOCK_META_CACHE[as_of_date] = cached_df
                return cached_df.copy()
        except Exception:
            pass

    pro = _get_tushare_pro()
    try:
        _throttle_tushare_meta()
        universe = _run_with_retry(
            lambda: pro.index_classify(
                level="L2",
                src="SW2021",
                fields="index_code,industry_name,level",
            ),
            max_retries=3,
            timeout=20.0,
        )
    except Exception:
        universe = pd.DataFrame(columns=["index_code", "industry_name", "level"])

    if universe is None or universe.empty:
        fallback = _load_tushare_stock_meta_cache_latest(cache_dir)
        if not fallback.empty:
            _TUSHARE_STOCK_META_CACHE[as_of_date] = fallback
            return fallback.copy()
        raise RuntimeError("未获取到 Tushare 申万二级行业列表")

    universe = universe.copy()
    universe["industry_code"] = universe["index_code"].astype(str).str.extract(r"(\d{6})", expand=False)
    universe["industry_name"] = universe["industry_name"].astype(str).str.strip()
    universe = universe.dropna(subset=["industry_code", "industry_name"]).drop_duplicates(subset=["industry_code"])
    if universe.empty:
        raise RuntimeError("Tushare 申万二级行业列表为空")

    frames: list[pd.DataFrame] = []
    missing_codes: list[str] = []
    for industry_code, industry_name in universe[["industry_code", "industry_name"]].itertuples(index=False, name=None):
        try:
            _throttle_tushare_meta()
            comp = _run_with_retry(
                lambda ic=industry_code: pro.index_member_all(
                    l2_code=f"{ic}.SI",
                    is_new="Y",
                    fields="l2_code,l2_name,ts_code,name,is_new",
                ),
                max_retries=3,
                timeout=25.0,
            )
        except Exception:
            comp = pd.DataFrame()
        if comp is None or comp.empty:
            missing_codes.append(str(industry_code))
            continue
        out = comp.rename(
            columns={
                "l2_code": "industry_code",
                "l2_name": "industry_name",
                "name": "stock_name",
            }
        ).copy()
        out["industry_code"] = out["industry_code"].astype(str).str.extract(r"(\d{6})", expand=False)
        out["industry_name"] = out["industry_name"].fillna(industry_name).astype(str).str.strip()
        out["ts_code"] = out["ts_code"].astype(str).map(_normalize_ts_code)
        out["stock_name"] = out["stock_name"].astype(str).str.strip()
        out["ifind_industry_code"] = ""
        out = out[["ts_code", "stock_name", "industry_name", "industry_code", "ifind_industry_code"]]
        out = out.dropna(subset=["ts_code", "industry_name", "industry_code"])
        if not out.empty:
            frames.append(out.drop_duplicates(subset=["ts_code"]))

    if missing_codes:
        try:
            ifind_meta = _fetch_ifind_stock_metadata(as_of_date, cache_dir)
            fallback = ifind_meta[ifind_meta["industry_code"].astype(str).isin(set(missing_codes))].copy()
            if not fallback.empty:
                frames.append(fallback[["ts_code", "stock_name", "industry_name", "industry_code", "ifind_industry_code"]])
        except Exception:
            pass

    if not frames:
        fallback = _load_tushare_stock_meta_cache_latest(cache_dir)
        if not fallback.empty:
            _TUSHARE_STOCK_META_CACHE[as_of_date] = fallback
            return fallback.copy()
        raise RuntimeError("未获取到 Tushare 股票元数据")

    meta = pd.concat(frames, ignore_index=True)
    meta["ts_code"] = meta["ts_code"].astype(str).map(_normalize_ts_code)
    meta["industry_code"] = meta["industry_code"].astype(str).str.extract(r"(\d{6})", expand=False)
    meta["stock_name"] = meta["stock_name"].astype(str).str.strip()
    meta["industry_name"] = meta["industry_name"].astype(str).str.strip()
    meta["ifind_industry_code"] = meta["ifind_industry_code"].astype(str).str.strip()
    meta = meta.dropna(subset=["ts_code", "industry_name", "industry_code"])
    meta = meta.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)
    os.makedirs(cache_dir, exist_ok=True)
    meta.to_csv(cache_path, index=False, encoding="utf-8-sig")
    _TUSHARE_STOCK_META_CACHE[as_of_date] = meta
    return meta.copy()


def _fetch_ifind_stock_metadata(as_of_date: str, cache_dir: str) -> pd.DataFrame:
    cached = _IFIND_STOCK_META_CACHE.get(as_of_date)
    if cached is not None:
        return cached.copy()

    cache_path = _ifind_stock_meta_cache_path(cache_dir, as_of_date)
    def _normalize_table(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "ts_code",
                    "stock_name",
                    "industry_name",
                    "industry_code",
                    "ifind_industry_code",
                ]
            )
        rename_map = {}
        for col in df.columns:
            col_str = str(col)
            if col_str == "股票代码":
                rename_map[col] = "ts_code"
            elif col_str == "股票简称":
                rename_map[col] = "stock_name"
            elif col_str == "所属二级申万行业指数代码":
                rename_map[col] = "industry_code"
            elif col_str == "所属二级申万行业":
                rename_map[col] = "industry_name"
            elif col_str == "所属二级申万行业代码":
                rename_map[col] = "ifind_industry_code"
        out = df.rename(columns=rename_map).copy()
        required = {"ts_code", "stock_name", "industry_code", "industry_name", "ifind_industry_code"}
        if not required.issubset(out.columns):
            missing = ",".join(sorted(required - set(out.columns)))
            raise RuntimeError(f"iFind 股票元数据缺少字段: {missing}")
        out = out[["ts_code", "stock_name", "industry_code", "industry_name", "ifind_industry_code"]].copy()
        out["ts_code"] = out["ts_code"].astype(str).map(_normalize_ts_code)
        out["stock_name"] = out["stock_name"].astype(str).str.strip()
        out["industry_code"] = out["industry_code"].astype(str).str.strip()
        out["industry_name"] = out["industry_name"].astype(str).str.strip()
        out["ifind_industry_code"] = out["ifind_industry_code"].astype(str).str.strip()
        out = out.dropna(subset=["ts_code", "industry_name", "industry_code"]).copy()
        return out.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)

    if os.path.exists(cache_path):
        try:
            cached_df = _normalize_table(pd.read_csv(cache_path, dtype=str))
            if not cached_df.empty:
                _IFIND_STOCK_META_CACHE[as_of_date] = cached_df
                return cached_df.copy()
        except Exception:
            pass

    client = _get_ifind_client()
    payload = {
        "searchstring": "全部A股的股票代码,股票简称,所属二级申万行业指数代码,所属二级申万行业,所属二级申万行业代码",
        "searchtype": "stock",
    }
    try:
        data = _run_with_retry(
            lambda: client.post("smart_stock_picking", payload),
            max_retries=3,
            base_sleep=1.0,
            timeout=40.0,
        )
        table = (data.get("tables") or [{}])[0].get("table", {})
        api_df = _normalize_table(pd.DataFrame(table))
        if not api_df.empty:
            os.makedirs(cache_dir, exist_ok=True)
            api_df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            _IFIND_STOCK_META_CACHE[as_of_date] = api_df
            return api_df.copy()
    except Exception:
        pass

    fallback = _load_stock_meta_cache_latest(cache_dir)
    if not fallback.empty:
        _IFIND_STOCK_META_CACHE[as_of_date] = fallback
        return fallback.copy()
    raise RuntimeError("未获取到 iFind 股票元数据")


def _load_sw2_universe(as_of_date: str, cache_dir: str) -> pd.DataFrame:
    try:
        meta = _fetch_tushare_stock_metadata(as_of_date, cache_dir)
    except Exception:
        meta = _fetch_ifind_stock_metadata(as_of_date, cache_dir)
    out = meta[["industry_code", "industry_name"]].drop_duplicates(subset=["industry_code"]).copy()
    out["industry_code"] = out["industry_code"].astype(str).str.extract(r"(\d{6})", expand=False)
    out = out.dropna(subset=["industry_code", "industry_name"])
    if out.empty:
        raise RuntimeError("申万二级行业列表为空")
    return out.sort_values("industry_code").reset_index(drop=True)


def _get_ifind_industry_code(industry_code: str, as_of_date: str, cache_dir: str) -> str | None:
    meta = _fetch_ifind_stock_metadata(as_of_date, cache_dir)
    rel = meta.loc[meta["industry_code"].astype(str) == str(industry_code), "ifind_industry_code"].dropna()
    if rel.empty:
        return None
    rel = rel.astype(str).str.strip()
    rel = rel[rel != ""]
    if rel.empty:
        return None
    return rel.mode().iloc[0]


def _normalize_ifind_hist_df(raw_table: dict[str, object], code_col: str, with_volume: bool) -> pd.DataFrame:
    times = raw_table.get("time") or []
    table = raw_table.get("table") or {}
    closes = table.get("close")
    if not times or closes is None:
        cols = [code_col, "trade_date", "close"]
        if with_volume:
            cols.append("volume")
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame({"trade_date": times, "close": closes})
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    if with_volume:
        out["volume"] = pd.to_numeric(table.get("volume"), errors="coerce")
    out = out.dropna(subset=["trade_date", "close"])
    return out


def _fetch_ifind_stock_hist(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    client = _get_ifind_client()
    payload = {
        "codes": ts_code,
        "indicators": "close,volume,amount",
        "startdate": pd.to_datetime(start_date).strftime("%Y-%m-%d"),
        "enddate": pd.to_datetime(end_date).strftime("%Y-%m-%d"),
        "functionpara": {"Fill": "Blank"},
    }
    try:
        with _IFIND_HIST_LOCK:
            data = _run_with_retry(
                lambda: client.post("cmd_history_quotation", payload),
                max_retries=3,
                base_sleep=1.0,
                timeout=40.0,
            )
        table = (data.get("tables") or [{}])[0]
        out = _normalize_ifind_hist_df(table, "ts_code", with_volume=True)
        if out.empty:
            return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])
        out["trade_date"] = out["trade_date"].dt.strftime("%Y%m%d")
        out["ts_code"] = ts_code
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
        return out.dropna(subset=["trade_date", "close", "volume"])[["ts_code", "trade_date", "close", "volume"]]
    except Exception:
        return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])


def _fetch_tushare_stock_hist(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    pro = _get_tushare_pro()

    def _fetch() -> pd.DataFrame:
        raw = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            asset="E",
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
        )
        if raw is None or raw.empty:
            return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])
        out = raw[["trade_date", "close", "vol"]].copy()
        out.columns = ["trade_date", "close", "volume"]
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
        out["ts_code"] = ts_code
        return out.dropna(subset=["trade_date", "close", "volume"])

    with _TS_PRO_LOCK:
        out = _run_with_retry(_fetch, max_retries=3, base_sleep=1.0, timeout=30.0)
    return out if out is not None else pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])


def _throttle_tushare_sw_daily(min_interval_seconds: float = 6.5) -> None:
    global _TS_SW_DAILY_LAST_CALL
    with _TS_SW_DAILY_LOCK:
        now = time.time()
        wait_seconds = min_interval_seconds - (now - _TS_SW_DAILY_LAST_CALL)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _TS_SW_DAILY_LAST_CALL = time.time()


def _fetch_ifind_sw2_hist(industry_code: str, start_date: str, end_date: str, cache_dir: str) -> pd.DataFrame:
    ifind_code = _get_ifind_industry_code(industry_code, end_date, cache_dir)
    if not ifind_code:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    client = _get_ifind_client()
    payload = {
        "codes": ifind_code,
        "indicators": "close",
        "startdate": pd.to_datetime(start_date).strftime("%Y-%m-%d"),
        "enddate": pd.to_datetime(end_date).strftime("%Y-%m-%d"),
        "functionpara": {"Fill": "Blank"},
    }
    try:
        with _IFIND_HIST_LOCK:
            data = _run_with_retry(
                lambda: client.post("cmd_history_quotation", payload),
                max_retries=2,
                base_sleep=1.0,
                timeout=40.0,
            )
        table = (data.get("tables") or [{}])[0]
        out = _normalize_ifind_hist_df(table, "industry_code", with_volume=False)
        if out.empty:
            return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
        out["industry_code"] = str(industry_code)
        return out[["industry_code", "trade_date", "close"]]
    except Exception:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])


def _fetch_tushare_sw2_hist(industry_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    pro = _get_tushare_pro()
    _throttle_tushare_sw_daily()
    raw = _run_with_retry(
        lambda: pro.sw_daily(
            ts_code=f"{industry_code}.SI",
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
            fields="ts_code,trade_date,close",
        ),
        max_retries=3,
        timeout=20.0,
    )
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    out = raw[["trade_date", "close"]].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["trade_date", "close"])
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    out = out[(out["trade_date"] >= s) & (out["trade_date"] <= e)].copy()
    out["industry_code"] = industry_code
    return out[["industry_code", "trade_date", "close"]]


def _fetch_tushare_sw2_hist_batch(industry_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    if not industry_codes:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    pro = _get_tushare_pro()
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    if pd.isna(s) or pd.isna(e) or s > e:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])

    frames: list[pd.DataFrame] = []
    window_start = s
    while window_start <= e:
        window_end = min(window_start + timedelta(days=12), e)
        _throttle_tushare_sw_daily()
        raw = _run_with_retry(
            lambda ws=window_start, we=window_end: pro.sw_daily(
                start_date=ws.strftime("%Y%m%d"),
                end_date=we.strftime("%Y%m%d"),
                fields="ts_code,trade_date,close",
            ),
            max_retries=3,
            timeout=30.0,
        )
        if raw is not None and not raw.empty:
            out = raw[["ts_code", "trade_date", "close"]].copy()
            out["industry_code"] = out["ts_code"].astype(str).str.extract(r"(\d{6})", expand=False)
            out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
            out["close"] = pd.to_numeric(out["close"], errors="coerce")
            out = out.dropna(subset=["industry_code", "trade_date", "close"])
            out = out[out["industry_code"].astype(str).isin({str(x) for x in industry_codes})].copy()
            if not out.empty:
                frames.append(out[["industry_code", "trade_date", "close"]])
        window_start = window_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["industry_code", "trade_date"], keep="last")
    out = out[(out["trade_date"] >= s) & (out["trade_date"] <= e)].copy()
    return out[["industry_code", "trade_date", "close"]]


def _fetch_sw2_hist(industry_code: str, start_date: str, end_date: str, cache_dir: str) -> pd.DataFrame:
    try:
        out = _fetch_tushare_sw2_hist(industry_code, start_date, end_date)
        if not out.empty:
            return out
    except Exception:
        pass
    try:
        return _fetch_ifind_sw2_hist(industry_code, start_date, end_date, cache_dir)
    except Exception:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])


def _compute_industry_factors(ind_df: pd.DataFrame) -> pd.DataFrame:
    out = ind_df.copy().sort_values(["industry_code", "trade_date"]).reset_index(drop=True)
    g = out.groupby("industry_code")

    out["ret_20_ind"] = g["close"].transform(lambda x: x / x.shift(20) - 1)
    out["ret_50_ind"] = g["close"].transform(lambda x: x / x.shift(50) - 1)
    out["ret_120_ind"] = g["close"].transform(lambda x: x / x.shift(120) - 1)

    out["RPS20_ind"] = out.groupby("trade_date")["ret_20_ind"].rank(pct=True, ascending=True) * 100
    out["RPS50_ind"] = out.groupby("trade_date")["ret_50_ind"].rank(pct=True, ascending=True) * 100
    out["RPS120_ind"] = out.groupby("trade_date")["ret_120_ind"].rank(pct=True, ascending=True) * 100

    out["delta_RPS20_ind"] = g["RPS20_ind"].transform(lambda x: x - x.shift(10))
    return out


def _industry_hist_cache_dir(cache_dir: str) -> str:
    return os.path.join(cache_dir, "industry_hist_cache")


def _industry_hist_cache_path(cache_dir: str, industry_code: str) -> str:
    return os.path.join(_industry_hist_cache_dir(cache_dir), f"{industry_code}.csv")


def _load_industry_hist_cache(industry_code: str, start_date: str, end_date: str, cache_dir: str) -> pd.DataFrame:
    path = _industry_hist_cache_path(cache_dir, industry_code)
    if not os.path.exists(path):
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    try:
        df = pd.read_csv(path, dtype={"industry_code": str, "trade_date": str})
    except Exception:
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    if not {"industry_code", "trade_date", "close"}.issubset(df.columns):
        return pd.DataFrame(columns=["industry_code", "trade_date", "close"])
    df["industry_code"] = df["industry_code"].astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["industry_code", "trade_date", "close"])
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    df = df[(df["industry_code"] == str(industry_code)) & (df["trade_date"] >= s) & (df["trade_date"] <= e)].copy()
    return df[["industry_code", "trade_date", "close"]]


def _save_industry_hist_cache(df: pd.DataFrame, cache_dir: str) -> None:
    if df.empty:
        return
    os.makedirs(_industry_hist_cache_dir(cache_dir), exist_ok=True)
    for industry_code, grp in df.groupby("industry_code"):
        out = grp[["industry_code", "trade_date", "close"]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
        out = out.dropna(subset=["industry_code", "trade_date", "close"]).drop_duplicates(subset=["industry_code", "trade_date"])
        if out.empty:
            continue
        path = _industry_hist_cache_path(cache_dir, str(industry_code))
        out.to_csv(path, index=False, encoding="utf-8-sig")


def _load_industry_data(config: Config, industry_codes: list[str], cache_dir: str) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    missing_codes: list[str] = []
    cache_hit = 0
    ifind_hit = 0
    fetch_start_date = _strategy_fetch_start_date(config.start_date, config.end_date)
    min_history_rows = 80

    for industry_code in industry_codes:
        cached = _load_industry_hist_cache(industry_code, fetch_start_date, config.end_date, cache_dir)
        if (
            not cached.empty
            and len(cached) >= min_history_rows
            and not _is_hist_cache_stale(cached, config.end_date, max_lag_days=1)
        ):
            frames.append(cached)
            cache_hit += 1
            continue

        missing_codes.append(str(industry_code))

    print(
        f"fetching industry hist... cache_hit={cache_hit} tushare_batch_pending={len(missing_codes)} "
        f"ifind_fallback_ready={len(missing_codes)}"
    )

    if missing_codes:
        try:
            batch = _fetch_tushare_sw2_hist_batch(missing_codes, fetch_start_date, config.end_date)
        except Exception as err:
            print(f"warning: Tushare 批量行业历史拉取失败: {err}")
            batch = pd.DataFrame(columns=["industry_code", "trade_date", "close"])

        if not batch.empty:
            _save_industry_hist_cache(batch, cache_dir)
            by_code = {str(code): grp.copy() for code, grp in batch.groupby("industry_code")}
        else:
            by_code = {}

        for idx, industry_code in enumerate(missing_codes, start=1):
            hist = by_code.get(str(industry_code), pd.DataFrame(columns=["industry_code", "trade_date", "close"]))
            if not hist.empty:
                frames.append(hist)
            elif idx <= 10 or idx == len(missing_codes):
                print(f"warning: 行业 {industry_code} 历史仍为空")

            if idx % 20 == 0 or idx == len(missing_codes):
                print(f"fetching industry hist... tushare_batch_done={idx}/{len(missing_codes)} valid={len(frames)}")

        still_missing = sorted(set(str(x) for x in missing_codes) - set(by_code.keys()))
        for idx, industry_code in enumerate(still_missing, start=1):
            hist = _fetch_ifind_sw2_hist(industry_code, fetch_start_date, config.end_date, cache_dir)
            if not hist.empty:
                frames.append(hist)
                _save_industry_hist_cache(hist, cache_dir)
                ifind_hit += 1
            elif idx <= 10 or idx == len(still_missing):
                print(f"warning: 行业 {industry_code} iFind 备用历史仍为空")

        if still_missing:
            print(f"fetching industry hist... ifind_fallback_done={ifind_hit}/{len(still_missing)}")

    return frames


def _select_top_industries(
    ind_factors: pd.DataFrame,
    top_n: int,
    rps50_min: float,
    rps20_min: float,
    delta_rps20_min: float,
) -> pd.DataFrame:
    latest_date = ind_factors["trade_date"].max()
    latest = ind_factors[ind_factors["trade_date"] == latest_date].copy()
    latest = latest.dropna(subset=["RPS20_ind", "RPS50_ind", "delta_RPS20_ind"]).copy()
    # 轻门槛防噪声，核心按综合分排序
    mask = (
        (latest["RPS50_ind"] >= rps50_min)
        & (latest["RPS20_ind"] > rps20_min)
        & (latest["delta_RPS20_ind"] >= delta_rps20_min)
    )
    selected = latest[mask].copy()
    selected["IndustryScore"] = (
        0.5 * selected["RPS20_ind"] + 0.3 * selected["RPS50_ind"] + 0.2 * selected["delta_RPS20_ind"]
    )
    selected = selected.sort_values("IndustryScore", ascending=False).head(top_n)

    return selected[
        ["industry_code", "RPS20_ind", "RPS50_ind", "delta_RPS20_ind", "IndustryScore"]
    ].reset_index(drop=True)


def _fetch_sw2_components(industry_code: str, as_of_date: str, cache_dir: str) -> pd.DataFrame:
    try:
        meta = _fetch_tushare_stock_metadata(as_of_date, cache_dir)
    except Exception:
        meta = _fetch_ifind_stock_metadata(as_of_date, cache_dir)
    out = meta[meta["industry_code"].astype(str) == str(industry_code)][["industry_code", "ts_code", "stock_name"]].copy()
    if out.empty:
        return pd.DataFrame(columns=["industry_code", "ts_code", "stock_name"])
    out["industry_code"] = out["industry_code"].astype(str)
    out["ts_code"] = out["ts_code"].astype(str).map(_normalize_ts_code)
    out["stock_name"] = out["stock_name"].astype(str)
    return out.drop_duplicates(subset=["industry_code", "ts_code"]).reset_index(drop=True)


def _hist_cache_path(cache_dir: str, ts_code: str) -> str:
    return os.path.join(cache_dir, f"{ts_code.replace('.', '_')}.csv")


def _component_cache_dir(cache_dir: str) -> str:
    return os.path.join(cache_dir, "component_cache")


def _component_cache_path(cache_dir: str, industry_code: str, as_of_date: str) -> str:
    d = pd.to_datetime(as_of_date, errors="coerce")
    tag = d.strftime("%Y%m%d") if pd.notna(d) else "unknown"
    return os.path.join(_component_cache_dir(cache_dir), f"{industry_code}_{tag}.csv")


def _is_component_cache_fresh(path: str, as_of_date: str, max_age_days: int = 1) -> bool:
    try:
        d = pd.to_datetime(as_of_date, errors="coerce")
        if pd.isna(d):
            return False
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        age = (d.normalize() - pd.to_datetime(mtime).normalize()).days
        return age <= max_age_days
    except Exception:
        return False

def _load_component_cache_latest(cache_dir: str, industry_code: str, as_of_date: str, max_age_days: int = 1) -> pd.DataFrame:
    pattern = os.path.join(_component_cache_dir(cache_dir), f"{industry_code}_*.csv")
    paths = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    for path in paths:
        if not _is_component_cache_fresh(path, as_of_date, max_age_days=max_age_days):
            continue
        try:
            df = pd.read_csv(path, dtype={"industry_code": str, "ts_code": str, "stock_name": str})
            if not {"industry_code", "ts_code", "stock_name"}.issubset(df.columns):
                continue
            df["industry_code"] = df["industry_code"].astype(str)
            df["ts_code"] = df["ts_code"].astype(str).map(_normalize_ts_code)
            df = df[df["industry_code"] == str(industry_code)]
            if not df.empty:
                return df[["industry_code", "ts_code", "stock_name"]].drop_duplicates(subset=["industry_code", "ts_code"])
        except Exception:
            continue
    return pd.DataFrame(columns=["industry_code", "ts_code", "stock_name"])


def _load_component_from_last_stock_sheet(industry_code: str) -> pd.DataFrame:
    paths = sorted(
        glob.glob(os.path.join(SCRIPT_DIR, "trend_selector_results_run*_close*.xlsx")),
        key=os.path.getmtime,
        reverse=True,
    )
    for fp in paths:
        try:
            xls = pd.ExcelFile(fp)
            if "stock" not in xls.sheet_names:
                continue
            df = pd.read_excel(fp, sheet_name="stock")
            need = {"industry_code", "ts_code", "stock_name"}
            if not need.issubset(df.columns):
                continue
            df["industry_code"] = df["industry_code"].astype(str)
            df["ts_code"] = df["ts_code"].astype(str).map(_normalize_ts_code)
            out = df[df["industry_code"] == str(industry_code)][["industry_code", "ts_code", "stock_name"]].copy()
            if not out.empty:
                return out.drop_duplicates(subset=["industry_code", "ts_code"])
        except Exception:
            continue
    return pd.DataFrame(columns=["industry_code", "ts_code", "stock_name"])


def _fetch_components_with_cache(
    industry_code: str,
    industry_name: str | None,
    as_of_date: str,
    cache_dir: str,
    cache_max_age_days: int = 5,
) -> tuple[pd.DataFrame, str]:
    os.makedirs(_component_cache_dir(cache_dir), exist_ok=True)

    try:
        fresh = _fetch_sw2_components(industry_code, as_of_date=as_of_date, cache_dir=cache_dir)
        if not fresh.empty:
            fresh = fresh.copy()
            fresh["industry_code"] = fresh["industry_code"].astype(str)
            fresh.to_csv(_component_cache_path(cache_dir, industry_code, as_of_date), index=False, encoding="utf-8-sig")
            return fresh, "api"
    except Exception:
        pass
    cached = _load_component_cache_latest(cache_dir, industry_code, as_of_date, max_age_days=cache_max_age_days)
    if not cached.empty:
        return cached, "cache"
    allow_sheet_fallback = os.getenv("ALLOW_COMPONENT_SHEET_FALLBACK", "0") == "1"
    if allow_sheet_fallback:
        sheet_fallback = _load_component_from_last_stock_sheet(industry_code)
        if not sheet_fallback.empty:
            return sheet_fallback, "sheet_fallback"
    return pd.DataFrame(columns=["industry_code", "ts_code", "stock_name"]), "none"


def _resolve_cache_dir(hist_cache_dir: str) -> str:
    cache_dir = hist_cache_dir
    if not os.path.isabs(cache_dir):
        cache_dir = os.path.join(SCRIPT_DIR, cache_dir)
    return cache_dir


def _load_hist_cache(ts_code: str, start_date: str, end_date: str, cache_dir: str) -> pd.DataFrame:
    path = _hist_cache_path(cache_dir, ts_code)
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])
    try:
        df = pd.read_csv(path, dtype={"ts_code": str, "trade_date": str})
    except Exception:
        return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])

    if not {"ts_code", "trade_date", "close"}.issubset(df.columns):
        return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    else:
        df["volume"] = pd.NA
    df = df.dropna(subset=["trade_date", "close"])
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    df = df[(df["trade_date"] >= s) & (df["trade_date"] <= e)].copy()
    if df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])
    df["trade_date"] = df["trade_date"].dt.strftime("%Y%m%d")
    df["ts_code"] = ts_code
    return df[["ts_code", "trade_date", "close", "volume"]]


def _save_hist_cache(df: pd.DataFrame, cache_dir: str) -> None:
    if df.empty:
        return
    ts_code = str(df.iloc[0]["ts_code"])
    out = df.copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
    if "volume" not in out.columns:
        out["volume"] = pd.NA
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out = out[["ts_code", "trade_date", "close", "volume"]].dropna(subset=["trade_date", "close"])

    target = _hist_cache_path(cache_dir, ts_code)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    lock_path = target + '.lock'

    # 原子写入 + 文件锁，避免并发写导致 deadlock/损坏
    with open(lock_path, 'w') as lockf:
        fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
        fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(target)+'.', suffix='.tmp', dir=os.path.dirname(target))
        os.close(fd)
        try:
            out.to_csv(tmp_path, index=False, encoding='utf-8-sig')
            os.replace(tmp_path, target)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)


def _is_hist_cache_stale(hist: pd.DataFrame, end_date: str, max_lag_days: int = 0) -> bool:
    if hist.empty or "trade_date" not in hist.columns:
        return True
    latest = pd.to_datetime(hist["trade_date"], errors="coerce").max()
    if pd.isna(latest):
        return True
    end_dt = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(end_dt):
        return False
    lag_days = (end_dt.normalize() - latest.normalize()).days
    return lag_days > max_lag_days


def _fetch_stock_hist(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        out = _fetch_tushare_stock_hist(ts_code, start_date, end_date)
        if not out.empty:
            return out
    except Exception:
        pass
    try:
        out = _fetch_ifind_stock_hist(ts_code, start_date, end_date)
        if not out.empty:
            return out
    except Exception:
        pass

    return pd.DataFrame(columns=["ts_code", "trade_date", "close", "volume"])


def _is_st_stock_name(name: object) -> bool:
    normalized = str(name or "").strip().upper().replace(" ", "")
    if not normalized:
        return False
    normalized = normalized.replace("Ｓ", "S").replace("Ｔ", "T").replace("*", "")
    return normalized.startswith(("ST", "SST"))


def _load_stock_data(config: Config, target_codes: list[str]) -> pd.DataFrame:
    cache_dir = _resolve_cache_dir(config.hist_cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    if not target_codes:
        raise RuntimeError("股票池为空，无法拉取历史数据")

    max_workers = max(1, int(config.io_workers))
    batch_size = max(1, int(config.io_batch_size))
    frames: list[pd.DataFrame] = []
    cache_hit = 0
    refreshed = 0
    refresh_failed = 0
    t0 = time.time()

    def _load_one(ts_code: str) -> pd.DataFrame:
        hist = _load_hist_cache(ts_code, config.start_date, config.end_date, cache_dir)
        missing_volume = ("volume" not in hist.columns) or hist["volume"].isna().all()
        should_refresh = _is_hist_cache_stale(
            hist, config.end_date, max_lag_days=max(0, int(config.stock_data_max_staleness_days))
        )
        should_refresh = should_refresh or missing_volume
        from_cache_only = (not hist.empty) and (not should_refresh)
        if should_refresh:
            fresh = _fetch_stock_hist(ts_code, config.start_date, config.end_date)
            if not fresh.empty:
                _save_hist_cache(fresh, cache_dir)
                hist = fresh
                refresh_status = "refreshed"
            elif hist.empty:
                hist = fresh
                refresh_status = "refresh_failed_empty"
            else:
                print(f"warning: {ts_code} 缓存缺失成交量或较旧，且 Tushare/iFind 刷新失败，继续使用旧缓存。")
                refresh_status = "refresh_failed_fallback_cache"
        else:
            refresh_status = "cache_hit" if from_cache_only else "unknown"
        return hist, refresh_status

    total = len(target_codes)
    done = 0
    total_batches = int(math.ceil(total / batch_size))
    for bi in range(total_batches):
        batch_codes = target_codes[bi * batch_size : (bi + 1) * batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut_map = {ex.submit(_load_one, ts_code): ts_code for ts_code in batch_codes}
            for fut in concurrent.futures.as_completed(fut_map):
                ts_code = fut_map[fut]
                done += 1
                try:
                    hist, status = fut.result()
                except Exception as err:
                    print(f"warning: {ts_code} 拉取失败: {err}")
                    continue
                if status == "cache_hit":
                    cache_hit += 1
                elif status == "refreshed":
                    refreshed += 1
                elif status.startswith("refresh_failed"):
                    refresh_failed += 1
                if not hist.empty:
                    frames.append(hist)
        print(
            f"fetching stock hist... batch={bi + 1}/{total_batches} done={done}/{total} "
            f"valid={len(frames)} cache_hit={cache_hit} refreshed={refreshed} refresh_failed={refresh_failed}"
        )

    if not frames:
        raise RuntimeError("未拉取到个股历史数据")
    print(f"stock hist stage done in {time.time() - t0:.1f}s")
    return pd.concat(frames, ignore_index=True)


def _compute_stock_factors(stock_df: pd.DataFrame, stock_rps20_min: float) -> pd.DataFrame:
    out = stock_df.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out = out.dropna(subset=["trade_date", "close"]).copy()
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")

    g = out.groupby("ts_code")
    out["ret_20"] = g["close"].transform(lambda x: x / x.shift(20) - 1)
    out["ret_60"] = g["close"].transform(lambda x: x / x.shift(60) - 1)
    out["ret_120"] = g["close"].transform(lambda x: x / x.shift(120) - 1)
    out["RPS20"] = out.groupby("trade_date")["ret_20"].rank(pct=True, ascending=True) * 100
    out["RPS60"] = out.groupby("trade_date")["ret_60"].rank(pct=True, ascending=True) * 100
    out["RPS120"] = out.groupby("trade_date")["ret_120"].rank(pct=True, ascending=True) * 100
    out["delta_RPS20"] = g["RPS20"].transform(lambda x: x - x.shift(10))
    out["EMA12"] = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    out["EMA50"] = g["close"].transform(lambda x: x.ewm(span=50, adjust=False).mean())
    out["ema_spread"] = out["EMA12"] / out["EMA50"] - 1
    out["recent_5d_max_vol"] = g["volume"].transform(lambda x: x.rolling(window=5, min_periods=5).max())
    out["base_vol"] = g["volume"].transform(lambda x: x.shift(5).rolling(window=20, min_periods=20).mean())
    out["vol_spike"] = out["recent_5d_max_vol"] / out["base_vol"] - 1
    out["signal_cond1"] = (
        (out["EMA12"] > out["EMA50"]) & (out["ema_spread"] <= 0.07) & (out["vol_spike"] >= 0.5)
    ).fillna(False)
    out["signal_cond2"] = (
        (out["EMA12"] > out["EMA50"])
        & (g["EMA12"].shift(1) <= g["EMA50"].shift(1))
        & (out["vol_spike"] >= 0.5)
    ).fillna(False)
    out["trend_ok"] = (out["signal_cond1"] | out["signal_cond2"]).fillna(False)
    out["TrendScore"] = out["vol_spike"].fillna(-1) * 100.0
    out["StockScore"] = (
        out["signal_cond2"].astype(int) * 1000.0
        + out["vol_spike"].fillna(-1) * 100.0
        - out["ema_spread"].fillna(99) * 10.0
    )
    out["StockScoreFinal"] = out["StockScore"]
    return out


def run_strategy(config: Config) -> tuple[pd.DataFrame, pd.DataFrame, str | None, pd.DataFrame]:
    stage_rows: list[dict[str, object]] = []

    def _record(stage: str, count: int, note: str = "", industry_code: str = "") -> None:
        stage_rows.append({"stage": stage, "count": int(count), "industry_code": industry_code, "note": note})

    empty_stock_cols = [
        "industry_code",
        "industry_name",
        "ts_code",
        "stock_name",
        "is_st_stock",
        "close",
        "RPS20",
        "RPS60",
        "RPS120",
        "delta_RPS20",
        "EMA12",
        "EMA50",
        "ema_spread",
        "vol_spike",
        "signal_cond1",
        "signal_cond2",
        "TrendScore",
        "StockScore",
        "StockScoreFinal",
        "stock_data_date",
        "data_staleness_days",
        "is_data_fresh",
    ]

    cache_dir = _resolve_cache_dir(config.hist_cache_dir)
    universe = _load_sw2_universe(config.end_date, cache_dir)
    _record("industry_universe", len(universe))
    print(f"industry universe: {len(universe)}")

    industry_codes = universe["industry_code"].tolist()
    ind_frames = _load_industry_data(config, industry_codes, cache_dir)
    if not ind_frames:
        raise RuntimeError("行业历史数据为空")

    ind_factors = _compute_industry_factors(pd.concat(ind_frames, ignore_index=True))
    selected_ind_raw = _select_top_industries(
        ind_factors,
        config.industry_top_n,
        config.industry_rps50_min,
        config.industry_rps20_min,
        config.industry_delta_rps20_min,
    )
    # 补位用：保留所有通过门槛的行业排序，优先取有成分映射的行业
    selected_pool = _select_top_industries(
        ind_factors,
        len(universe),
        config.industry_rps50_min,
        config.industry_rps20_min,
        config.industry_delta_rps20_min,
    )
    _record("industry_selected_raw_topn", len(selected_ind_raw))
    _record("industry_selected_pool", len(selected_pool))
    selected_ind = selected_pool.copy()
    selected_ind["industry_code"] = selected_ind["industry_code"].astype(str)
    universe["industry_code"] = universe["industry_code"].astype(str)
    selected_ind = selected_ind.merge(universe, on="industry_code", how="left")
    selected_ind = selected_ind[
        ["industry_code", "industry_name", "RPS20_ind", "RPS50_ind", "delta_RPS20_ind", "IndustryScore"]
    ].copy()
    target_industry_count = min(config.industry_top_n, len(selected_ind))

    if selected_ind.empty:
        return selected_ind, pd.DataFrame(columns=empty_stock_cols), None, pd.DataFrame(stage_rows)

    component_frames: list[pd.DataFrame] = []
    selected_with_components: list[dict[str, object]] = []
    component_source_rows: list[dict[str, object]] = []
    for _, ind_row in selected_ind.iterrows():
        industry_code = str(ind_row["industry_code"])
        comp, source = _fetch_components_with_cache(
            industry_code,
            str(ind_row["industry_name"]),
            config.end_date,
            cache_dir,
            cache_max_age_days=config.component_cache_max_age_days,
        )
        component_source_rows.append(
            {"industry_code": str(industry_code), "stage": "component_source", "count": len(comp), "note": source}
        )
        if not comp.empty:
            component_frames.append(comp)
            selected_with_components.append(ind_row.to_dict())
        if len(selected_with_components) >= config.industry_top_n:
            break
    if selected_with_components:
        selected_ind = pd.DataFrame(selected_with_components)[
            ["industry_code", "industry_name", "RPS20_ind", "RPS50_ind", "delta_RPS20_ind", "IndustryScore"]
        ].copy()
    else:
        selected_ind = pd.DataFrame(columns=selected_ind.columns)

    stage_rows.extend(component_source_rows)
    _record("industry_selected", len(selected_ind))
    _record("industry_component_mapped", len(component_frames))
    coverage_ratio = len(component_frames) / max(target_industry_count, 1)
    coverage_note = f"mapped={len(component_frames)}/{target_industry_count} ratio={coverage_ratio:.2f}"
    _record("industry_component_coverage", int(round(coverage_ratio * 100)), note=coverage_note)
    if coverage_ratio < config.component_min_coverage_ratio:
        _record(
            "data_quality_flag",
            1,
            note=f"LOW_CONFIDENCE component coverage below {config.component_min_coverage_ratio:.2f}",
        )
    if not component_frames:
        return selected_ind, pd.DataFrame(columns=empty_stock_cols), None, pd.DataFrame(stage_rows)

    industry_map = pd.concat(component_frames, ignore_index=True).drop_duplicates(subset=["ts_code"])
    industry_map["industry_code"] = industry_map["industry_code"].astype(str)
    industry_map = industry_map.merge(
        selected_ind[["industry_code", "industry_name"]].drop_duplicates(subset=["industry_code"]),
        on="industry_code",
        how="left",
    )
    target_codes = industry_map["ts_code"].dropna().unique().tolist()
    _record("stock_candidates", len(target_codes))

    _record("stock_candidates_after_mkt_cap_prefilter", len(target_codes), note="disabled")

    code_limit = int(os.getenv("CODE_LIMIT", "0"))
    if code_limit > 0:
        target_codes = target_codes[:code_limit]
        industry_map = industry_map[industry_map["ts_code"].isin(target_codes)].copy()
        _record("stock_candidates_limited", len(target_codes), note=f"code_limit={code_limit}")

    stock_hist = _load_stock_data(config, target_codes)
    _record("stock_hist_loaded", stock_hist["ts_code"].nunique())
    stock_factors = _compute_stock_factors(stock_hist, config.stock_rps20_min)
    if stock_factors.empty:
        return selected_ind.reset_index(drop=True), pd.DataFrame(columns=empty_stock_cols), None, pd.DataFrame(stage_rows)

    latest_date = stock_factors["trade_date"].max()
    latest_date_dt = pd.to_datetime(latest_date, errors="coerce")
    stock_close_date = latest_date_dt.strftime("%Y-%m-%d") if pd.notna(latest_date_dt) else None
    latest = stock_factors.sort_values(["ts_code", "trade_date"]).groupby("ts_code", as_index=False).tail(1).copy()
    _record("stock_last_snapshot_rows", len(latest), note=stock_close_date or "")
    latest["stock_data_date"] = pd.to_datetime(latest["trade_date"], errors="coerce")
    freshness_ref_dt = pd.to_datetime(config.end_date, errors="coerce")
    if pd.isna(freshness_ref_dt):
        freshness_ref_dt = latest_date_dt
    latest["data_staleness_days"] = (
        freshness_ref_dt.normalize() - latest["stock_data_date"].dt.normalize()
    ).dt.days.clip(lower=0)
    latest["stock_data_date"] = latest["stock_data_date"].dt.strftime("%Y-%m-%d")
    latest["is_data_fresh"] = latest["data_staleness_days"] <= config.stock_data_max_staleness_days
    latest = latest.merge(industry_map, on="ts_code", how="inner")
    latest["is_st_stock"] = latest["stock_name"].map(_is_st_stock_name)
    _record("stock_in_selected_industries", len(latest))
    for k, v in latest.groupby("industry_code").size().to_dict().items():
        _record("stock_in_selected_industries_by_ind", int(v), industry_code=str(k))
    latest = latest[~latest["is_st_stock"]].copy()
    _record("stock_after_st_filter", len(latest))
    for k, v in latest.groupby("industry_code").size().to_dict().items():
        _record("stock_after_st_filter_by_ind", int(v), industry_code=str(k))
    latest = latest[latest["trend_ok"]].copy()
    _record("stock_after_trend_filter", len(latest))
    for k, v in latest.groupby("industry_code").size().to_dict().items():
        _record("stock_after_trend_filter_by_ind", int(v), industry_code=str(k))
    if config.stock_data_max_staleness_days >= 0:
        latest = latest[latest["data_staleness_days"] <= config.stock_data_max_staleness_days].copy()
        _record(
            "stock_after_data_freshness",
            len(latest),
            note=f"max_staleness_days={config.stock_data_max_staleness_days}",
        )
        for k, v in latest.groupby("industry_code").size().to_dict().items():
            _record("stock_after_data_freshness_by_ind", int(v), industry_code=str(k))
    _record("stock_after_mkt_cap_filter", len(latest), note="disabled")
    for k, v in latest.groupby("industry_code").size().to_dict().items():
        _record("stock_after_mkt_cap_filter_by_ind", int(v), industry_code=str(k))

    latest = latest.sort_values(["industry_code", "ema_spread"], ascending=[True, True])
    picks = latest.groupby("industry_code", as_index=False).head(config.stock_per_industry)
    _record("stock_final_picks", len(picks), note=f"per_industry={config.stock_per_industry}")
    for k, v in picks.groupby("industry_code").size().to_dict().items():
        _record("stock_final_picks_by_ind", int(v), industry_code=str(k))

    out_picks = picks[
        [
            "industry_code",
            "industry_name",
            "ts_code",
            "stock_name",
            "is_st_stock",
            "close",
            "RPS20",
            "RPS60",
            "delta_RPS20",
            "EMA12",
            "EMA50",
            "ema_spread",
            "vol_spike",
            "signal_cond1",
            "signal_cond2",
            "stock_data_date",
            "data_staleness_days",
            "is_data_fresh",
        ]
    ].copy()
    out_picks = out_picks.sort_values(["industry_code", "ema_spread"], ascending=[True, True]).reset_index(
        drop=True
    )

    return selected_ind.reset_index(drop=True), out_picks, stock_close_date, pd.DataFrame(stage_rows)


def save_results(
    selected_industries: pd.DataFrame,
    stock_picks: pd.DataFrame,
    diagnostics: pd.DataFrame,
    output_dir: str = SCRIPT_DIR,
    stock_close_date: str | None = None,
) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ind_sorted = selected_industries.sort_values("IndustryScore", ascending=False).reset_index(drop=True).copy()
    stock_sorted = stock_picks.sort_values("ema_spread", ascending=True).reset_index(drop=True).copy()

    ind_out = ind_sorted.copy()
    ind_out["row_type"] = "industry"
    stock_out = stock_sorted.copy()
    stock_out["row_type"] = "stock"

    ind_out["ts_code"] = pd.NA
    ind_out["stock_name"] = pd.NA
    ind_out["is_st_stock"] = pd.NA
    ind_out["close"] = pd.NA
    ind_out["RPS20"] = pd.NA
    ind_out["RPS60"] = pd.NA
    ind_out["delta_RPS20"] = pd.NA
    ind_out["EMA12"] = pd.NA
    ind_out["EMA50"] = pd.NA
    ind_out["ema_spread"] = pd.NA
    ind_out["vol_spike"] = pd.NA
    ind_out["signal_cond1"] = pd.NA
    ind_out["signal_cond2"] = pd.NA
    ind_out["stock_data_date"] = pd.NA
    ind_out["data_staleness_days"] = pd.NA
    ind_out["is_data_fresh"] = pd.NA

    stock_out["RPS20_ind"] = pd.NA
    stock_out["RPS50_ind"] = pd.NA
    stock_out["delta_RPS20_ind"] = pd.NA
    stock_out["IndustryScore"] = pd.NA

    cols = [
        "row_type",
        "industry_code",
        "industry_name",
        "RPS20_ind",
        "RPS50_ind",
        "delta_RPS20_ind",
        "IndustryScore",
        "ts_code",
        "stock_name",
        "is_st_stock",
        "close",
        "RPS20",
        "RPS60",
        "delta_RPS20",
        "EMA12",
        "EMA50",
        "ema_spread",
        "vol_spike",
        "signal_cond1",
        "signal_cond2",
        "stock_data_date",
        "data_staleness_days",
        "is_data_fresh",
    ]

    industry_sheet = ind_out[cols].copy()
    stock_sheet = stock_out[cols].copy()
    run_date = datetime.now().strftime("%Y%m%d")
    close_date = "NA"
    if stock_close_date:
        date_tag = pd.to_datetime(stock_close_date, errors="coerce")
        if pd.notna(date_tag):
            close_date = date_tag.strftime("%Y%m%d")
    industry_sheet["stock_close_date"] = stock_close_date if stock_close_date else pd.NA
    stock_sheet["stock_close_date"] = stock_close_date if stock_close_date else pd.NA

    filename = f"trend_selector_results_run{run_date}_close{close_date}.xlsx"
    output_path = os.path.join(output_dir, filename)
    with pd.ExcelWriter(output_path) as writer:
        industry_sheet.to_excel(writer, sheet_name="industry", index=False)
        stock_sheet.to_excel(writer, sheet_name="stock", index=False)
        diagnostics.to_excel(writer, sheet_name="diagnostics", index=False)

    # 清理旧的拆分输出
    for old_name in (
        "selected_industries.csv",
        "industry_stock_picks.csv",
        "merged_industry_stock_results.csv",
    ):
        old_path = os.path.join(output_dir, old_name)
        if os.path.exists(old_path):
            os.remove(old_path)
    return output_path


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="A股行业先筛选、行业内趋势选股")
    parser.add_argument("--start-date", default=(datetime.today() - timedelta(days=1200)).strftime("%Y-%m-%d"))
    parser.add_argument("--end-date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", default=SCRIPT_DIR)
    parser.add_argument("--hist-cache-dir", default=".hist_cache")
    parser.add_argument("--industry-top-n", type=int, default=7)
    parser.add_argument("--industry-rps50-min", type=float, default=60.0)
    parser.add_argument("--industry-rps20-min", type=float, default=60.0)
    parser.add_argument("--industry-delta-rps20-min", type=float, default=15.0)
    parser.add_argument("--stock-rps20-min", type=float, default=60.0)
    parser.add_argument("--stock-per-industry", type=int, default=5)
    parser.add_argument("--stock-min-mkt-cap-yi", type=float, default=0.0, help="已停用，保留仅为兼容旧参数")
    parser.add_argument("--stock-data-max-staleness-days", type=int, default=0, help="个股数据允许最大滞后天数；0 表示必须为当天数据")
    parser.add_argument("--component-min-coverage-ratio", type=float, default=0.8, help="行业成分映射最低覆盖率")
    parser.add_argument("--component-cache-max-age-days", type=int, default=5, help="行业成分缓存允许最大滞后天数")
    parser.add_argument(
        "--io-workers",
        type=int,
        default=int(os.getenv("IFIND_IO_WORKERS", "8")),
        help="并发拉取数据线程数（行业/个股历史）",
    )
    parser.add_argument(
        "--io-batch-size",
        type=int,
        default=int(os.getenv("IFIND_IO_BATCH_SIZE", "80")),
        help="个股历史拉取分批大小（每批提交的股票数）",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    config = Config(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        hist_cache_dir=args.hist_cache_dir,
        industry_top_n=args.industry_top_n,
        industry_rps50_min=args.industry_rps50_min,
        industry_rps20_min=args.industry_rps20_min,
        industry_delta_rps20_min=args.industry_delta_rps20_min,
        stock_rps20_min=args.stock_rps20_min,
        stock_per_industry=args.stock_per_industry,
        stock_min_mkt_cap_yi=args.stock_min_mkt_cap_yi,
        stock_data_max_staleness_days=args.stock_data_max_staleness_days,
        component_min_coverage_ratio=args.component_min_coverage_ratio,
        component_cache_max_age_days=args.component_cache_max_age_days,
        io_workers=args.io_workers,
        io_batch_size=args.io_batch_size,
    )

    selected_industries, stock_picks, stock_close_date, diagnostics = run_strategy(config)
    save_results(
        selected_industries,
        stock_picks,
        diagnostics,
        output_dir=config.output_dir,
        stock_close_date=stock_close_date,
    )
    print("done. output written to:", os.path.join(config.output_dir, f"trend_selector_results_run{datetime.now().strftime('%Y%m%d')}_close*.xlsx"))


if __name__ == "__main__":
    main()
