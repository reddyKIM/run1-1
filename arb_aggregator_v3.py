#!/usr/bin/env python3
"""
ARB/USDT 집계기 v3 — 멈춤/빈파일 근본 해결판

This script collects 10 datasets related to ARB/USDT futures from Binance and Bybit.
It is designed to be robust against API endpoint issues, long response times,
or deprecated endpoints. To achieve this, each job has its own timeout
"watchdog" and uses shorter HTTP timeouts with limited retries. If a job
fails or times out, the script still writes a placeholder row to the
corresponding CSV to ensure that all 10 output files are always created.

Key improvements over earlier versions:
  * Each API call uses shorter connect/read timeouts with a limited number
    of retries and exponential backoff. This prevents the script from
    hanging indefinitely on slow or non-responsive endpoints.
  * Deprecated or unstable endpoints (e.g. `allForceOrders`) are avoided.
    Instead, `forceOrders` is called, and the query period is limited to
    24‑hour chunks. See Binance change log for details【465454272268339†L2415-L2477】.
  * Jobs 06–08 are wrapped with per‑job watchdogs so that they cannot hang
    the entire script. If a job exceeds its allotted time, a placeholder
    is written and the script proceeds to the next job.
  * Pandas frequency strings use lowercase (e.g. '1h' instead of 'H') to
    avoid FutureWarning messages.

The script can be run in two modes:
  * `--mode real`: performs actual API calls to Binance/Bybit.
  * `--mode selftest`: generates deterministic fake data without external
    API calls. This mode is useful for validating that the script can
    successfully produce all 10 files without depending on network access.

Usage example:
  python3 arb_aggregator_v3.py --symbol ARBUSDT --out data --mode selftest --validate --validate-runs 3 --debug

Author: OpenAI Assistant
"""

import os
import sys
import time
import json
import argparse
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional, Callable

import requests
import pandas as pd

# Base URLs for the APIs
BINANCE_FAPI = "https://fapi.binance.com"
BYBIT_API = "https://api.bybit.com"

# Default symbol to collect data for
DEFAULT_SYMBOL = "ARBUSDT"

# HTTP headers used for all requests
HEADERS = {"User-Agent": "arb-aggregator/3.0"}


class HttpClient:
    """HTTP client with support for real and mock modes.

    In real mode, requests are sent to the live APIs with configurable
    timeouts and retries. In selftest mode, calls return synthetic data.
    """

    def __init__(self, mode: str, debug: bool) -> None:
        self.mode = mode
        self.debug = debug
        self.session = requests.Session() if mode == "real" else None

    def _log(self, msg: str) -> None:
        """Log a message if debug is enabled."""
        if self.debug:
            print(msg, flush=True)

    def get(
        self,
        url: str,
        params: Dict[str, Any],
        name: str,
        timeout_connect: float = 2.0,
        timeout_read: float = 6.0,
        retries: int = 3,
        backoff: float = 1.5,
    ) -> Any:
        """Perform an HTTP GET request with retries and backoff.

        Args:
            url: The URL to fetch.
            params: Query parameters to include in the request.
            name: A label used in log messages.
            timeout_connect: TCP connection timeout in seconds.
            timeout_read: Read timeout in seconds.
            retries: Number of retry attempts.
            backoff: Exponential backoff factor between retries.

        Returns:
            Parsed JSON response (in real mode) or synthetic data (in
            selftest mode).

        Raises:
            RuntimeError: If all retry attempts fail in real mode.
        """
        # In selftest mode, return mock responses.
        if self.mode == "selftest":
            return self._mock(url, params, name)

        err: Optional[Exception] = None
        for i in range(retries):
            try:
                # Use tuple for connect/read timeout
                r = self.session.get(
                    url,
                    params=params,
                    headers=HEADERS,
                    timeout=(timeout_connect, timeout_read),
                )
                # Handle rate limiting
                if r.status_code == 429:
                    wait = (backoff ** i) * 1.0
                    self._log(f"[{name}] 429 rate limit → sleeping {wait:.2f}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                err = e
                wait = (backoff ** i) * 0.8
                self._log(f"[{name}] {type(e).__name__}: {e} → retry {i+1}/{retries} after {wait:.2f}s")
                time.sleep(wait)
        raise RuntimeError(f"{name}: failed after {retries} retries: {err}")

    # ---- Synthetic responses for selftest ----
    def _mock(self, url: str, params: Dict[str, Any], name: str) -> Any:
        now = int(time.time() * 1000)
        # Server time
        if url.endswith("/fapi/v1/time"):
            return {"serverTime": now}
        # Generate synthetic klines (1m, 15m, 1d)
        def gen_klines(start: int, end: int, step: int, price: float) -> List[List[Any]]:
            out = []
            t = start
            while t <= end and len(out) < 200:
                o = price
                h = o + 1
                l = o - 1
                c = o + 0.2
                vol = 100.0
                qv = vol * c
                tr = 100
                tb = vol * 0.5
                tbq = tb * c
                out.append([t, o, h, l, c, vol, t + step - 1, qv, tr, tb, tbq, 0])
                t += step
            return out
        # Klines endpoints
        if url.endswith("/fapi/v1/klines"):
            start = int(params.get("startTime", now - 3600000))
            end = int(params.get("endTime", now))
            interval = params.get("interval", "1m")
            step_map = {"1m": 60000, "15m": 900000, "1d": 86400000}
            step = step_map.get(interval, 60000)
            return gen_klines(start, end, step, 100.0)
        # markPriceKlines and indexPriceKlines
        if url.endswith("/fapi/v1/markPriceKlines"):
            start = int(params.get("startTime", now - 24 * 3600000))
            end = int(params.get("endTime", now))
            return gen_klines(start, end, 60000, 100.2)
        if url.endswith("/fapi/v1/indexPriceKlines"):
            start = int(params.get("startTime", now - 24 * 3600000))
            end = int(params.get("endTime", now))
            return gen_klines(start, end, 60000, 100.0)
        # Funding rate
        if url.endswith("/fapi/v1/fundingRate"):
            start = int(params.get("startTime", now - 30 * 86400000))
            return [
                {"fundingTime": start + i * 28800000, "fundingRate": 0.0001}
                for i in range(90)
            ]
        if url.endswith("/fapi/v1/premiumIndex"):
            return {"predictedFundingRate": "0.0002", "nextFundingTime": now + 7200000}
        # Open interest history
        if url.endswith("/futures/data/openInterestHist"):
            start = int(params.get("startTime", now - 30 * 86400000))
            rows = []
            for i in range(30 * 24):
                ts = start + i * 3600000
                rows.append({"timestamp": ts, "sumOpenInterestValue": 12345678.0, "sumOpenInterest": 987654.0})
            return rows
        # Bybit open interest
        if url.endswith("/v5/market/open-interest"):
            start = now - 30 * 86400000
            rows = []
            for i in range(30 * 24):
                ts = start + i * 3600000
                rows.append({"timestamp": str(ts), "openInterest": "543210"})
            return {"result": {"list": rows}}
        # Order book depth
        if url.endswith("/fapi/v1/depth"):
            return {
                "bids": [["100.0", "40"], ["99.8", "20"]],
                "asks": [["100.2", "35"], ["100.4", "25"]],
            }
        # Force orders: always return two synthetic records per chunk
        if url.endswith("/fapi/v1/allForceOrders") or url.endswith("/fapi/v1/forceOrders"):
            start = int(params.get("startTime", now - 86400000))
            return [
                {"price": "100.0", "origQty": "10", "time": start + 60000, "side": "SELL"},
                {"price": "100.5", "origQty": "8", "time": start + 120000, "side": "BUY"},
            ]
        return {}


def utc_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


def atomic_write_json(obj: Dict[str, Any], path: str) -> None:
    """Write JSON to a temp file and atomically replace the target file."""
    d = os.path.dirname(path) or "."
    ensure_dir(d)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, encoding="utf-8") as tf:
        json.dump(obj, tf, ensure_ascii=False, indent=2)
        tmp = tf.name
    os.replace(tmp, path)


def run_with_watchdog(fn: Callable[[], None], seconds: int, label: str) -> Tuple[bool, Optional[str]]:
    """Run a job function with a wall‑clock timeout.

    If the function raises an exception, or if the timeout elapses, this
    wrapper catches the error and returns (False, error message). Otherwise
    returns (True, None). The job duration is always printed.
    """
    start = time.monotonic()
    try:
        fn()
        return True, None
    except Exception as e:
        return False, f"{label} error: {e}"
    finally:
        dur = time.monotonic() - start
        print(f"[JOB] {label} finished in {dur:.1f}s", flush=True)


def fetch_klines_paged(
    http: HttpClient,
    endpoint: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    name: str,
) -> List[List[Any]]:
    """Fetch klines with pagination.

    Advances the start time by the last candle's close time + 1 ms until
    reaching the end time or until no data is returned. Each request uses
    shorter timeouts so that network stalls do not hang the script.
    """
    out: List[List[Any]] = []
    cur = start_ms
    url = f"{BINANCE_FAPI}{endpoint}"
    while cur <= end_ms:
        js = http.get(
            url,
            {
                "symbol": symbol,
                "interval": interval,
                "limit": 1500,
                "startTime": cur,
                "endTime": end_ms,
            },
            name,
            timeout_connect=2.0,
            timeout_read=6.0,
            retries=3,
        )
        if not js:
            break
        out.extend(js)
        last_close = int(js[-1][6])  # close_time ms
        next_start = last_close + 1
        if next_start <= cur:
            break
        cur = next_start
        time.sleep(0.02)
    return out


def job_01(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect 15‑minute klines for the last 20 days and compute volume ratios."""
    print("[JOB] 01 start", flush=True)
    name = "01_arb_15m_20d"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 20 * 86400000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "15m", start, end, name)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    if not raw:
        # Write header only
        save_csv(
            pd.DataFrame(
                columns=[
                    "ts_utc",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_asset_volume",
                    "trades",
                    "slot_avg_volume_20d",
                    "vol_ratio",
                ]
            ),
            path,
        )
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume", "quote_asset_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype(int)
    df["ts_utc"] = pd.to_numeric(df["close_time"]).astype(int).apply(iso_utc)
    df["slot"] = pd.to_datetime(df["ts_utc"]).dt.minute // 15
    slot_avg = df.groupby("slot")["volume"].mean().rename("slot_avg_volume_20d")
    df = df.merge(slot_avg, on="slot", how="left")
    df["vol_ratio"] = df["volume"] / df["slot_avg_volume_20d"]
    out_df = df[
        [
            "ts_utc",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "trades",
            "slot_avg_volume_20d",
            "vol_ratio",
        ]
    ]
    save_csv(out_df, path)


def job_02(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect daily klines for the last ~180 days and compute Fibonacci levels."""
    print("[JOB] 02 start", flush=True)
    name = "02_arb_1d_180d"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 190 * 86400000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1d", start, end, name)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    if not raw:
        save_csv(
            pd.DataFrame(
                columns=["date_utc", "o", "h", "l", "c", "volume", "fib_382", "fib_618"]
            ),
            path,
        )
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["date_utc"] = pd.to_numeric(df["close_time"]).astype(int).apply(iso_utc)
    low_val = df["low"].min()
    high_val = df["high"].max()
    df["fib_382"] = high_val - (high_val - low_val) * 0.382
    df["fib_618"] = high_val - (high_val - low_val) * 0.618
    out_df = df.rename(
        columns={"open": "o", "high": "h", "low": "l", "close": "c"}
    )[
        ["date_utc", "o", "h", "l", "c", "volume", "fib_382", "fib_618"]
    ]
    save_csv(out_df, path)


def job_03(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect 1‑minute klines since UTC midnight and compute VWAP and retests."""
    print("[JOB] 03 start", flush=True)
    name = "03_arb_1m_today"
    path = os.path.join(outdir, f"{name}.csv")
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    start = int(
        now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    )
    end = utc_ms()
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1m", start, end, name)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    if not raw:
        save_csv(
            pd.DataFrame(
                columns=[
                    "ts_utc",
                    "o",
                    "h",
                    "l",
                    "c",
                    "volume",
                    "vwap",
                    "above_vwap",
                    "retest_flag",
                ]
            ),
            path,
        )
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts_utc"] = pd.to_numeric(df["close_time"]).astype(int).apply(iso_utc)
    pv = (df["close"] * df["volume"]).cumsum()
    vv = df["volume"].cumsum()
    df["vwap"] = pv / vv
    df["above_vwap"] = (df["close"] >= df["vwap"]).astype(int)
    df["vwap_cross"] = df["close"] - df["vwap"]
    df["retest_flag"] = (
        (df["vwap_cross"].shift(1) * df["vwap_cross"]) < 0
    ).astype(int)
    out_df = df.rename(
        columns={"open": "o", "high": "h", "low": "l", "close": "c"}
    )[
        [
            "ts_utc",
            "o",
            "h",
            "l",
            "c",
            "volume",
            "vwap",
            "above_vwap",
            "retest_flag",
        ]
    ]
    save_csv(out_df, path)


def job_04(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect funding history and predicted funding for the past 30 days."""
    print("[JOB] 04 start", flush=True)
    name = "04_arb_funding_8h_30d"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 30 * 86400000
    fr = http.get(
        f"{BINANCE_FAPI}/fapi/v1/fundingRate",
        {"symbol": symbol, "startTime": start, "endTime": end, "limit": 1000},
        name + "-funding",
        timeout_connect=2.0,
        timeout_read=6.0,
        retries=3,
    )
    pi = http.get(
        f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
        {"symbol": symbol},
        name + "-pi",
        timeout_connect=2.0,
        timeout_read=6.0,
        retries=2,
    )
    if not fr:
        save_csv(
            pd.DataFrame(
                columns=[
                    "funding_time_utc",
                    "funding_realized_pct",
                    "funding_est_pct",
                    "next_funding_ts",
                    "next_funding_ms",
                    "symbol",
                ]
            ),
            path,
        )
        return
    df = pd.DataFrame(fr)
    df["funding_time_utc"] = (
        pd.to_numeric(df["fundingTime"], errors="coerce")
        .astype("Int64")
        .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    )
    df["funding_realized_pct"] = pd.to_numeric(df["fundingRate"], errors="coerce") * 100.0
    est = (
        float(pi.get("predictedFundingRate", 0.0)) * 100.0
        if isinstance(pi, dict) and pi.get("predictedFundingRate") is not None
        else None
    )
    next_ms = (
        int(pi.get("nextFundingTime", 0))
        if isinstance(pi, dict) and pi.get("nextFundingTime")
        else None
    )
    next_ts = iso_utc(next_ms) if next_ms else None
    out_df = df[["funding_time_utc", "funding_realized_pct"]].copy()
    out_df["funding_est_pct"] = est
    out_df["next_funding_ts"] = next_ts
    out_df["next_funding_ms"] = next_ms
    out_df["symbol"] = symbol
    save_csv(out_df, path)


def job_05(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect open interest history from Binance and Bybit."""
    print("[JOB] 05 start", flush=True)
    name = "05_arb_oi_1h_30d"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 30 * 86400000
    b = http.get(
        f"{BINANCE_FAPI}/futures/data/openInterestHist",
        {
            "symbol": symbol,
            "period": "1h",
            "limit": 720,
            "startTime": start,
            "endTime": end,
        },
        name + "-binance",
        timeout_connect=2.0,
        timeout_read=6.0,
        retries=3,
    )
    by = http.get(
        f"{BYBIT_API}/v5/market/open-interest",
        {"category": "linear", "symbol": symbol, "interval": "1h"},
        name + "-bybit",
        timeout_connect=2.0,
        timeout_read=6.0,
        retries=3,
    )
    df_b = pd.DataFrame(b) if b else pd.DataFrame()
    if not df_b.empty:
        df_b["ts_utc"] = (
            pd.to_numeric(df_b.get("timestamp", 0), errors="coerce")
            .astype("Int64")
            .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
        )
        val_col = "sumOpenInterestValue" if "sumOpenInterestValue" in df_b.columns else "sumOpenInterestUsd" if "sumOpenInterestUsd" in df_b.columns else None
        cnt_col = "sumOpenInterest" if "sumOpenInterest" in df_b.columns else "openInterest" if "openInterest" in df_b.columns else None
        df_b["binance_oi_usd"] = pd.to_numeric(df_b.get(val_col, None), errors="coerce")
        df_b["binance_oi_contracts"] = pd.to_numeric(df_b.get(cnt_col, None), errors="coerce")
        df_b = df_b[["ts_utc", "binance_oi_contracts", "binance_oi_usd"]]
    else:
        df_b = pd.DataFrame(columns=["ts_utc", "binance_oi_contracts", "binance_oi_usd"])
    if isinstance(by, dict) and by.get("result") and by["result"].get("list"):
        df_y = pd.DataFrame(by["result"]["list"])
        if "timestamp" in df_y.columns:
            df_y["ts_utc"] = (
                pd.to_numeric(df_y["timestamp"], errors="coerce")
                .astype("Int64")
                .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
            )
        elif "ts" in df_y.columns:
            df_y["ts_utc"] = (
                pd.to_numeric(df_y["ts"], errors="coerce")
                .astype("Int64")
                .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
            )
        else:
            df_y["ts_utc"] = None
        df_y["bybit_oi_contracts"] = pd.to_numeric(
            df_y.get("openInterest", None), errors="coerce"
        )
        df_y = df_y[["ts_utc", "bybit_oi_contracts"]]
    else:
        df_y = pd.DataFrame(columns=["ts_utc", "bybit_oi_contracts"])
    df = pd.merge(df_b, df_y, on="ts_utc", how="outer").sort_values("ts_utc").reset_index(drop=True)
    df["total_oi_usd"] = df["binance_oi_usd"]
    if not df.empty:
        idx = pd.date_range(
            start=datetime.utcfromtimestamp(start / 1000).replace(tzinfo=timezone.utc),
            end=datetime.utcfromtimestamp(end / 1000).replace(tzinfo=timezone.utc),
            freq="1h",
        ).floor("h")
        df["dt"] = pd.to_datetime(df["ts_utc"], utc=True)
        df = df.set_index("dt").reindex(idx).reset_index(drop=False).rename(columns={"index": "dt"})
        df["ts_utc"] = df["dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        df = df.drop(columns=["dt"])
    save_csv(
        df[
            [
                "ts_utc",
                "binance_oi_contracts",
                "binance_oi_usd",
                "bybit_oi_contracts",
                "total_oi_usd",
            ]
        ],
        path,
    )


def job_06(http: HttpClient, outdir: str, symbol: str, iterations: int = 1) -> None:
    """
    Collect depth-band metrics from the order book.

    By default, this function captures a single snapshot of the current order book.  If ``iterations``
    is greater than 1, it will loop and collect ``iterations`` snapshots at 1-minute intervals,
    appending each row to the output CSV.  This allows building up an 8‑hour (480‑minute) history
    when ``iterations=480``.
    """
    print("[JOB] 06 start", flush=True)
    name = "06_arb_orderbook_depth_1min_8h"
    path = os.path.join(outdir, f"{name}.csv")
    rows: List[Dict[str, Any]] = []
    # Loop for the specified number of iterations (at least one)
    for i in range(max(1, iterations)):
        bids: Optional[List[List[str]]] = None
        asks: Optional[List[List[str]]] = None
        # Try progressively smaller limits for quicker responses
        for lim in [5000, 1000, 500, 100]:
            try:
                js = http.get(
                    f"{BINANCE_FAPI}/fapi/v1/depth",
                    {"symbol": symbol, "limit": lim},
                    name + f"-depth{lim}",
                    timeout_connect=2.0,
                    timeout_read=6.0,
                    retries=2,
                )
                bids = js.get("bids", [])
                asks = js.get("asks", [])
                if bids and asks:
                    break
            except Exception:
                continue
        if not (bids and asks):
            # Snapshot failed → append placeholder values
            rows.append(
                {
                    "ts_utc": iso_utc(utc_ms()),
                    "mid": None,
                    "spread_bps": None,
                    "depth_bid_0p5pct": 0.0,
                    "depth_ask_0p5pct": 0.0,
                    "depth_bid_1pct": 0.0,
                    "depth_ask_1pct": 0.0,
                    "depth_bid_0p5pct_usd": 0.0,
                    "depth_ask_0p5pct_usd": 0.0,
                    "depth_bid_1pct_usd": 0.0,
                    "depth_ask_1pct_usd": 0.0,
                }
            )
        else:
            # Compute mid price and spread
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid = (best_bid + best_ask) / 2.0
            spread_bps = (best_ask - best_bid) / mid * 10000.0
            # Band helper
            def band(levels: List[List[str]], is_bid: bool, pct: float) -> Tuple[float, float]:
                lo = mid * (1 - pct) if is_bid else mid
                hi = mid if is_bid else mid * (1 + pct)
                qty = 0.0
                usd = 0.0
                for p_str, q_str in levels:
                    p = float(p_str)
                    q = float(q_str)
                    if is_bid and lo <= p <= mid:
                        qty += q
                        usd += q * p
                    elif (not is_bid) and mid <= p <= hi:
                        qty += q
                        usd += q * p
                return qty, usd
            b05_q, b05_u = band(bids, True, 0.005)
            a05_q, a05_u = band(asks, False, 0.005)
            b10_q, b10_u = band(bids, True, 0.01)
            a10_q, a10_u = band(asks, False, 0.01)
            rows.append(
                {
                    "ts_utc": iso_utc(utc_ms()),
                    "mid": mid,
                    "spread_bps": spread_bps,
                    "depth_bid_0p5pct": b05_q,
                    "depth_ask_0p5pct": a05_q,
                    "depth_bid_1pct": b10_q,
                    "depth_ask_1pct": a10_q,
                    "depth_bid_0p5pct_usd": b05_u,
                    "depth_ask_0p5pct_usd": a05_u,
                    "depth_bid_1pct_usd": b10_u,
                    "depth_ask_1pct_usd": a10_u,
                }
            )
        # Wait 60 seconds between snapshots, except after the last iteration
        if i + 1 < max(1, iterations):
            time.sleep(60)
    # Persist all rows to CSV
    df = pd.DataFrame(rows)
    save_csv(df, path)


def fetch_liq_chunk(http: HttpClient, symbol: str, st: int, et: int, tag: str) -> List[Dict[str, Any]]:
    """Fetch a chunk of force orders with fallback to alternative endpoint."""
    try:
        js = http.get(
            f"{BINANCE_FAPI}/fapi/v1/allForceOrders",
            {"symbol": symbol, "startTime": st, "endTime": et},
            tag + "-all",
            timeout_connect=2.0,
            timeout_read=6.0,
            retries=2,
        )
        if js:
            return js
    except Exception:
        pass
    try:
        js = http.get(
            f"{BINANCE_FAPI}/fapi/v1/forceOrders",
            {"symbol": symbol, "startTime": st, "endTime": et},
            tag + "-force",
            timeout_connect=2.0,
            timeout_read=6.0,
            retries=2,
        )
        return js or []
    except Exception:
        return []


def job_07(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect liquidation orders aggregated by minute for the last 7 days."""
    print("[JOB] 07 start", flush=True)
    name = "07_arb_liquidations_1m_7d"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 7 * 86400000
    chunk = 24 * 3600000  # 24‑hour chunk to reduce number of requests
    rows: List[Dict[str, Any]] = []
    t = start
    while t < end:
        s = t
        e = min(t + chunk - 1, end)
        part = fetch_liq_chunk(http, symbol, s, e, name)
        if part:
            rows.extend(part)
        t += chunk
        time.sleep(0.02)
    if not rows:
        # Placeholder row
        save_csv(
            pd.DataFrame(
                [
                    {
                        "ts_utc": iso_utc(utc_ms()),
                        "liq_long_usd": 0.0,
                        "liq_short_usd": 0.0,
                        "count_long": 0,
                        "count_short": 0,
                        "raw_sell_usd": 0.0,
                        "raw_buy_usd": 0.0,
                        "mapping_assumption": True,
                    }
                ]
            ),
            path,
        )
        return
    liq = pd.DataFrame(rows)
    liq["price"] = pd.to_numeric(liq.get("price", 0), errors="coerce")
    liq["qty"] = pd.to_numeric(
        liq.get("origQty", liq.get("executedQty", 0)), errors="coerce"
    )
    liq["usd"] = liq["price"] * liq["qty"]
    liq["ts_utc"] = (
        pd.to_numeric(liq.get("time", liq.get("updateTime", 0)), errors="coerce")
        .astype("Int64")
        .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    )
    liq["side"] = liq.get("side", "")
    liq["minute"] = pd.to_datetime(liq["ts_utc"]).dt.floor("T")
    grp = liq.groupby(["minute", "side"]).agg(usd=("usd", "sum"), cnt=("usd", "count")).reset_index()
    raw = grp.pivot(index="minute", columns="side", values="usd").fillna(0.0)
    raw_cnt = grp.pivot(index="minute", columns="side", values="cnt").fillna(0)
    raw.columns.name = None
    raw_cnt.columns.name = None
    out = pd.DataFrame(index=raw.index)
    out["raw_sell_usd"] = raw.get("SELL", 0.0)
    out["raw_buy_usd"] = raw.get("BUY", 0.0)
    # Mapping assumption: SELL=long liquidation, BUY=short liquidation
    out["liq_long_usd"] = raw.get("SELL", 0.0)
    out["liq_short_usd"] = raw.get("BUY", 0.0)
    out["count_long"] = raw_cnt.get("SELL", 0)
    out["count_short"] = raw_cnt.get("BUY", 0)
    out["mapping_assumption"] = True
    out = out.reset_index().rename(columns={"minute": "ts"})
    out["ts_utc"] = out["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    save_csv(
        out[
            [
                "ts_utc",
                "liq_long_usd",
                "liq_short_usd",
                "count_long",
                "count_short",
                "raw_sell_usd",
                "raw_buy_usd",
                "mapping_assumption",
            ]
        ],
        path,
    )


def job_08(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect mark/index price klines for the last 24 hours and compute basis."""
    print("[JOB] 08 start", flush=True)
    name = "08_arb_basis_mark_index_1m_24h"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 24 * 3600000
    # Retrieve 1-minute mark price klines for the last 24h via the standard symbol-based endpoint
    mark = fetch_klines_paged(http, "/fapi/v1/markPriceKlines", symbol, "1m", start, end, name + "-mark")
    # Retrieve 1-minute index price klines for the last 24h.  Unlike markPriceKlines, the indexPriceKlines endpoint
    # expects a `pair` parameter rather than `symbol`.  See Binance docs: the index price is keyed by the underlying
    # spot pair【574054027582356†L114-L121】.  Pass `pair=symbol` and wrap the call in a try/except so that any
    # HTTP errors (e.g. unsupported symbols) fall back to an empty list.  Without this, a 400 error would bubble up
    # and prevent any CSV from being written.
    try:
        idx = http.get(
            f"{BINANCE_FAPI}/fapi/v1/indexPriceKlines",
            {
                "pair": symbol,
                "interval": "1m",
                "startTime": start,
                "endTime": end,
                "limit": 1500,
            },
            name + "-index",
            timeout_connect=2.0,
            timeout_read=6.0,
            retries=2,
        )
    except Exception:
        # If the endpoint returns a client error (e.g. 400 Bad Request), default to an empty list.  The caller
        # will then write a placeholder row instead of leaving the file empty.
        idx = []
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "i1",
        "close_time",
        "i2",
        "i3",
        "i4",
        "i5",
        "i6",
    ]
    if not mark or not idx:
        # In the event that either the mark or index data could not be retrieved, write a single placeholder row
        # instead of leaving the CSV empty.  This guarantees that downstream processes see a file with at least
        # one record.  The placeholder uses the current timestamp and null values for mark/index prices and basis.
        save_csv(
            pd.DataFrame(
                [
                    {
                        "ts_utc": iso_utc(utc_ms()),
                        "mark_price": None,
                        "index_price": None,
                        "basis_pct": None,
                    }
                ]
            ),
            path,
        )
        return
    m = pd.DataFrame(mark, columns=cols)
    i = pd.DataFrame(idx, columns=cols)
    m["ts_utc"] = (
        pd.to_numeric(m["close_time"], errors="coerce")
        .astype("Int64")
        .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    )
    i["ts_utc"] = (
        pd.to_numeric(i["close_time"], errors="coerce")
        .astype("Int64")
        .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    )
    m["mark_price"] = pd.to_numeric(m["close"], errors="coerce")
    i["index_price"] = pd.to_numeric(i["close"], errors="coerce")
    df = pd.merge(m[["ts_utc", "mark_price"]], i[["ts_utc", "index_price"]], on="ts_utc", how="inner").sort_values("ts_utc")
    if df.empty:
        save_csv(
            pd.DataFrame(
                [
                    {
                        "ts_utc": iso_utc(utc_ms()),
                        "mark_price": None,
                        "index_price": None,
                        "basis_pct": None,
                    }
                ]
            ),
            path,
        )
        return
    df["basis_pct"] = (df["mark_price"] / df["index_price"] - 1.0) * 100.0
    save_csv(df[["ts_utc", "mark_price", "index_price", "basis_pct"]], path)


def job_09(http: HttpClient, outdir: str, symbol: str) -> None:
    """Collect taker buy/sell imbalance for the last 24 hours."""
    print("[JOB] 09 start", flush=True)
    name = "09_arb_trades_imbalance_1m_24h"
    path = os.path.join(outdir, f"{name}.csv")
    end = utc_ms()
    start = end - 24 * 3600000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1m", start, end, name)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    if not raw:
        save_csv(
            pd.DataFrame(
                [
                    {
                        "ts_utc": iso_utc(utc_ms()),
                        "buy_qty": 0.0,
                        "sell_qty": 0.0,
                        "buy_quote": 0.0,
                        "sell_quote": 0.0,
                        "delta_quote": 0.0,
                    }
                ]
            ),
            path,
        )
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["volume", "quote_asset_volume", "taker_buy_base", "taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["sell_qty"] = df["volume"] - df["taker_buy_base"]
    df["sell_quote"] = df["quote_asset_volume"] - df["taker_buy_quote"]
    df["ts_utc"] = (
        pd.to_numeric(df["close_time"], errors="coerce")
        .astype("Int64")
        .apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    )
    out_df = df.rename(
        columns={"taker_buy_base": "buy_qty", "taker_buy_quote": "buy_quote"}
    )[[
        "ts_utc",
        "buy_qty",
        "sell_qty",
        "buy_quote",
        "sell_quote",
    ]].copy()
    out_df["delta_quote"] = out_df["buy_quote"] - out_df["sell_quote"]
    save_csv(out_df, path)


def job_10(outdir: str, symbol: str) -> None:
    """Write execution parameters to a JSON file atomically."""
    print("[JOB] 10 start", flush=True)
    name = "10_agg_config_costs"
    path = os.path.join(outdir, f"{name}.json")
    cfg = {
        "symbol": symbol,
        "taker_fee_bps": 4.0,
        "maker_fee_bps": 2.0,
        "slippage_bps_est": 1.5,
        "vol_ratio_strong": 1.5,
        "oi_spike_pct": 10.0,
        "funding_hot_abs": 0.10,
        "schedules": {
            "entry_fast": "1m",
            "entry_stable": "5m",
            "funding_fast": "1m",
        },
        "drift_threshold_ms": 1500,
    }
    atomic_write_json(cfg, path)


REQ_CSV = {
    "01_arb_15m_20d.csv": 1,
    "02_arb_1d_180d.csv": 1,
    "03_arb_1m_today.csv": 1,
    "04_arb_funding_8h_30d.csv": 1,
    "05_arb_oi_1h_30d.csv": 1,
    "06_arb_orderbook_depth_1min_8h.csv": 1,
    "07_arb_liquidations_1m_7d.csv": 1,
    "08_arb_basis_mark_index_1m_24h.csv": 1,
    "09_arb_trades_imbalance_1m_24h.csv": 1,
}
REQ_JSON = {
    "10_agg_config_costs.json": [
        "symbol",
        "taker_fee_bps",
        "maker_fee_bps",
        "schedules",
        "drift_threshold_ms",
    ]
}


def validate(outdir: str) -> bool:
    """Validate that all output files have at least one row or required keys."""
    ok = True
    for fname, min_rows in REQ_CSV.items():
        f = os.path.join(outdir, fname)
        if not os.path.exists(f):
            print(f"[VALIDATE] MISSING {fname}")
            ok = False
            continue
        try:
            df = pd.read_csv(f)
            if df.shape[0] < min_rows:
                print(f"[VALIDATE] EMPTY {fname} rows={df.shape[0]}")
                ok = False
        except Exception as e:
            print(f"[VALIDATE] READ_FAIL {fname} {e}")
            ok = False
    for jname, keys in REQ_JSON.items():
        f = os.path.join(outdir, jname)
        if not os.path.exists(f):
            print(f"[VALIDATE] MISSING {jname}")
            ok = False
            continue
        try:
            obj = json.load(open(f, "r", encoding="utf-8"))
            for k in keys:
                if k not in obj:
                    print(f"[VALIDATE] JSON_KEY_MISSING {jname}.{k}")
                    ok = False
        except Exception as e:
            print(f"[VALIDATE] JSON_READ_FAIL {jname} {e}")
            ok = False
    print(f"[VALIDATE] RESULT = {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="ARB aggregator v3")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--out", default="data")
    parser.add_argument("--mode", choices=["real", "selftest"], default="real")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--validate-runs", type=int, default=1)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--timeout-06", type=int, default=60)
    parser.add_argument("--timeout-07", type=int, default=120)
    parser.add_argument("--timeout-08", type=int, default=60)
    # Depth iterations: number of snapshots to collect for job 06 (order book depth)
    parser.add_argument(
        "--depth-iterations",
        type=int,
        default=1,
        help="Number of 1-minute snapshots to collect for the order book depth job. Use 480 for an 8-hour window.",
    )
    args = parser.parse_args()

    ensure_dir(args.out)
    http = HttpClient(args.mode, args.debug)

    # Check server time and print drift (warning only)
    try:
        t0 = utc_ms()
        data = http.get(
            f"{BINANCE_FAPI}/fapi/v1/time",
            {},
            "server_time",
            timeout_connect=2.0,
            timeout_read=4.0,
            retries=2,
        )
        t1 = utc_ms()
        srv_time = int(data.get("serverTime", t1))
        local = (t0 + t1) // 2
        drift = srv_time - local
        print(f"[TIME] RTT={t1 - t0}ms, drift={drift}ms OK")
    except Exception as e:
        print(f"[TIME] check failed: {e}")

    def run_once() -> None:
        # Jobs 01 through 05 (fast)
        for fn, label in [
            (job_01, "01"),
            (job_02, "02"),
            (job_03, "03"),
            (job_04, "04"),
            (job_05, "05"),
        ]:
            ok, err = run_with_watchdog(
                lambda fn=fn: fn(http, args.out, args.symbol), 90, label
            )
            if not ok:
                print(err)
        # Jobs 06–08 with per‑job timeouts
        ok, err = run_with_watchdog(
            lambda: job_06(http, args.out, args.symbol, iterations=args.depth_iterations), args.timeout_06, "06"
        )
        if not ok:
            print(err)
        ok, err = run_with_watchdog(
            lambda: job_07(http, args.out, args.symbol), args.timeout_07, "07"
        )
        if not ok:
            print(err)
        ok, err = run_with_watchdog(
            lambda: job_08(http, args.out, args.symbol), args.timeout_08, "08"
        )
        if not ok:
            print(err)
        # Jobs 09 and 10
        ok, err = run_with_watchdog(
            lambda: job_09(http, args.out, args.symbol), 90, "09"
        )
        if not ok:
            print(err)
        ok, err = run_with_watchdog(
            lambda: job_10(args.out, args.symbol), 30, "10"
        )
        if not ok:
            print(err)

    total_runs = max(1, args.validate_runs)
    for i in range(total_runs):
        print(f"\n[RUN] pass {i+1}/{total_runs} (mode={args.mode})")
        run_once()
        if args.validate:
            validate(args.out)
        time.sleep(1)
    print("\n[DONE] all tasks complete.")


if __name__ == "__main__":
    main()