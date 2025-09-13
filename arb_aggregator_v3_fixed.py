#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARB/USDT aggregator v3 (fixed) — covers 10 outputs with robust public REST calls.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import argparse
import time
import json
from datetime import datetime, timezone

import pandas as pd
import requests

BINANCE_FAPI = "https://fapi.binance.com"
BYBIT_API = "https://api.bybit.com"
DEFAULT_SYMBOL = "ARBUSDT"
HEADERS = {"User-Agent": "arb-aggregator/3.0"}


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def utc_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: Optional[int]) -> Optional[str]:
    if ms is None or pd.isna(ms):
        return None
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def save_csv(df: pd.DataFrame, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


class HttpClient:
    def __init__(self, mode: str = "real", debug: bool = False):
        self.mode = mode
        self.debug = debug
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        name: str = "",
        timeout_connect: float = 3.0,
        timeout_read: float = 8.0,
        retries: int = 3,
        backoff: float = 1.6,
    ) -> Any:
        err: Optional[Exception] = None
        for i in range(retries):
            try:
                r = self.session.get(url, params=params, timeout=(timeout_connect, timeout_read))
                r.raise_for_status()
                return r.json()
            except Exception as e:
                err = e
                time.sleep(backoff ** (i + 1) * 0.3)
        raise RuntimeError(f"[HTTP] failed {name}: {err}")


def fetch_klines_paged(http: HttpClient, endpoint: str, symbol: str, interval: str, start_ms: int, end_ms: int, tag: str) -> List[List[Any]]:
    out: List[List[Any]] = []
    cur = start_ms
    while cur < end_ms:
        try:
            js = http.get(
                f"{BINANCE_FAPI}{endpoint}",
                {"symbol": symbol, "interval": interval, "startTime": cur, "endTime": end_ms, "limit": 1500},
                tag,
                timeout_connect=2.0,
                timeout_read=8.0,
                retries=3,
            )
        except Exception:
            break
        if not js:
            break
        out.extend(js)
        last_close = int(js[-1][6])  # close_time ms
        next_start = last_close + 1
        if next_start <= cur:
            break
        cur = next_start
        time.sleep(0.03)
    return out


def job_01(http: HttpClient, outdir: str, symbol: str) -> None:
    """01_arb_15m_20d.csv"""
    name = "01_arb_15m_20d"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 20 * 86400000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "15m", start, end, name)
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore",
    ]
    if not raw:
        save_csv(pd.DataFrame(columns=["ts_utc","open","high","low","close","volume","quote_volume","trades","slot_avg_volume_20d","vol_ratio"]), path)
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume","quote_asset_volume","trades"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts_utc"] = df["close_time"].astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    df["slot"] = (pd.to_numeric(df["close_time"], errors="coerce") // (15 * 60 * 1000)) % 96
    slot_avg = df.groupby("slot")["volume"].mean()
    df = df.merge(slot_avg.rename("slot_avg_volume_20d"), left_on="slot", right_index=True, how="left")
    df["vol_ratio"] = df["volume"] / df["slot_avg_volume_20d"]
    out = df.rename(columns={"quote_asset_volume":"quote_volume"})[
        ["ts_utc","open","high","low","close","volume","quote_volume","trades","slot_avg_volume_20d","vol_ratio"]
    ]
    save_csv(out, path)


def job_02(http: HttpClient, outdir: str, symbol: str) -> None:
    """02_arb_1d_180d.csv"""
    name = "02_arb_1d_180d"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 190 * 86400000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1d", start, end, name)
    cols = ["open_time","open","high","low","close","volume","close_time","qv","t","tb","tbq","ig"]
    if not raw:
        save_csv(pd.DataFrame(columns=["date_utc","o","h","l","c","volume","fib_382","fib_618"]), path)
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume","close_time"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["date_utc"] = df["close_time"].astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    low_val = df["low"].min()
    high_val = df["high"].max()
    df["fib_382"] = high_val - (high_val - low_val) * 0.382
    df["fib_618"] = high_val - (high_val - low_val) * 0.618
    out = df.rename(columns={"open":"o","high":"h","low":"l","close":"c"})[["date_utc","o","h","l","c","volume","fib_382","fib_618"]]
    save_csv(out, path)


def job_03(http: HttpClient, outdir: str, symbol: str) -> None:
    """03_arb_1m_today.csv"""
    name = "03_arb_1m_today"
    path = f"{outdir}/{name}.csv"
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    start = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end = utc_ms()
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1m", start, end, name)
    cols = ["open_time","open","high","low","close","volume","close_time","qv","t","tb","tbq","ig"]
    if not raw:
        save_csv(pd.DataFrame(columns=["ts_utc","o","h","l","c","volume","vwap","above_vwap","retest_flag"]), path)
        return
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume","close_time"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts_utc"] = df["close_time"].astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    df["tpv"] = df["close"] * df["volume"]
    df["cum_tpv"] = df["tpv"].cumsum()
    df["cum_vol"] = df["volume"].cumsum()
    df["vwap"] = df["cum_tpv"] / df["cum_vol"]
    df["above_vwap"] = df["close"] >= df["vwap"]
    df["retest_flag"] = df["above_vwap"].astype(int).diff().abs().fillna(0).rolling(5, min_periods=1).max() >= 1
    out = df.rename(columns={"open":"o","high":"h","low":"l","close":"c"})[
        ["ts_utc","o","h","l","c","volume","vwap","above_vwap","retest_flag"]
    ]
    save_csv(out, path)


def job_04(http: HttpClient, outdir: str, symbol: str) -> None:
    """04_arb_funding_8h_30d.csv"""
    name = "04_arb_funding_8h_30d"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 30 * 86400000
    fr = http.get(f"{BINANCE_FAPI}/fapi/v1/fundingRate",
                  {"symbol": symbol, "startTime": start, "endTime": end, "limit": 1000},
                  name, timeout_connect=2.0, timeout_read=8.0, retries=3)
    pi = http.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex", {"symbol": symbol}, name, timeout_connect=2.0, timeout_read=6.0, retries=3)
    df = pd.DataFrame(fr or [])
    if df.empty:
        save_csv(pd.DataFrame(columns=["funding_time_utc","funding_realized_pct","funding_est_pct","next_funding_ts","next_funding_ms","symbol"]), path)
        return
    df["funding_time_utc"] = pd.to_numeric(df.get("fundingTime", None), errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    df["funding_realized_pct"] = pd.to_numeric(df.get("fundingRate", None), errors="coerce") * 100.0
    est = None
    next_ms = None
    if isinstance(pi, dict):
        est = float(pi.get("lastFundingRate") if pi.get("lastFundingRate") is not None else pi.get("predictedFundingRate", 0.0)) * 100.0
        try:
            next_ms = int(pi.get("nextFundingTime")) if pi.get("nextFundingTime") else None
        except Exception:
            next_ms = None
    out = df[["funding_time_utc","funding_realized_pct"]].copy()
    out["funding_est_pct"] = est
    out["next_funding_ts"] = iso_utc(next_ms) if next_ms else None
    out["next_funding_ms"] = next_ms
    out["symbol"] = symbol
    save_csv(out, path)


def job_05(http: HttpClient, outdir: str, symbol: str) -> None:
    """05_arb_oi_1h_30d.csv"""
    name = "05_arb_oi_1h_30d"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 30 * 86400000
    b = http.get(f"{BINANCE_FAPI}/futures/data/openInterestHist",
                 {"symbol": symbol, "period": "1h", "limit": 720, "startTime": start, "endTime": end},
                 name+"-binance", timeout_connect=2.0, timeout_read=8.0, retries=3)
    try:
        by = http.get(f"{BYBIT_API}/v5/market/open-interest",
                      {"category": "linear", "symbol": symbol, "intervalTime": "60", "limit": 200},
                      name+"-bybit", timeout_connect=2.0, timeout_read=8.0, retries=2)
    except Exception:
        by = None
    df_b = pd.DataFrame(b or [])
    if not df_b.empty:
        df_b["ts_utc"] = pd.to_numeric(df_b.get("timestamp", None), errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
        val_col = "sumOpenInterestValue" if "sumOpenInterestValue" in df_b.columns else ("sumOpenInterestUsd" if "sumOpenInterestUsd" in df_b.columns else None)
        cnt_col = "sumOpenInterest" if "sumOpenInterest" in df_b.columns else ("openInterest" if "openInterest" in df_b.columns else None)
        df_b["binance_oi_usd"] = pd.to_numeric(df_b.get(val_col, None), errors="coerce")
        df_b["binance_oi_contracts"] = pd.to_numeric(df_b.get(cnt_col, None), errors="coerce")
        df_b = df_b[["ts_utc","binance_oi_contracts","binance_oi_usd"]]
    else:
        df_b = pd.DataFrame(columns=["ts_utc","binance_oi_contracts","binance_oi_usd"])
    if isinstance(by, dict) and by.get("result") and by["result"].get("list"):
        df_y = pd.DataFrame(by["result"]["list"])
        if "timestamp" in df_y.columns:
            df_y["ts_utc"] = pd.to_numeric(df_y["timestamp"], errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
        elif "ts" in df_y.columns:
            df_y["ts_utc"] = pd.to_numeric(df_y["ts"], errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
        else:
            df_y["ts_utc"] = None
        df_y["bybit_oi_contracts"] = pd.to_numeric(df_y.get("openInterest", None), errors="coerce")
        df_y = df_y[["ts_utc","bybit_oi_contracts"]]
    else:
        df_y = pd.DataFrame(columns=["ts_utc","bybit_oi_contracts"])
    out = pd.merge(df_b, df_y, on="ts_utc", how="outer").sort_values("ts_utc")
    out["total_oi_usd"] = out["binance_oi_usd"]
    save_csv(out, path)


def job_06(http: HttpClient, outdir: str, symbol: str, iterations: int = 1) -> None:
    """06_arb_orderbook_depth_1min_8h.csv"""
    name = "06_arb_orderbook_depth_1min_8h"
    path = f"{outdir}/{name}.csv"
    rows: List[Dict[str, Any]] = []
    for i in range(max(1, iterations)):
        try:
            ob = http.get(f"{BINANCE_FAPI}/fapi/v1/depth", {"symbol": symbol, "limit": 5000}, name, timeout_connect=2.0, timeout_read=6.0, retries=2)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
        except Exception:
            bids, asks = [], []
        if not (bids and asks):
            rows.append({
                "ts_utc": iso_utc(utc_ms()),
                "mid": None, "spread_bps": None,
                "depth_bid_0p5pct": 0.0, "depth_ask_0p5pct": 0.0,
                "depth_bid_1pct": 0.0, "depth_ask_1pct": 0.0,
                "depth_bid_0p5pct_usd": 0.0, "depth_ask_0p5pct_usd": 0.0,
                "depth_bid_1pct_usd": 0.0, "depth_ask_1pct_usd": 0.0,
            })
        else:
            best_bid = float(bids[0][0]); best_ask = float(asks[0][0])
            mid = (best_bid + best_ask) / 2.0
            spread_bps = (best_ask - best_bid) / mid * 10000.0
            def band(levels: List[List[str]], is_bid: bool, pct: float) -> Tuple[float,float]:
                lo = mid * (1 - pct) if is_bid else mid
                hi = mid if is_bid else mid * (1 + pct)
                qty = 0.0; usd = 0.0
                for p_str, q_str in levels:
                    p = float(p_str); q = float(q_str)
                    if is_bid and lo <= p <= mid:
                        qty += q; usd += q * p
                    elif (not is_bid) and mid <= p <= hi:
                        qty += q; usd += q * p
                return qty, usd
            b05_q, b05_u = band(bids, True, 0.005); a05_q, a05_u = band(asks, False, 0.005)
            b10_q, b10_u = band(bids, True, 0.01);  a10_q, a10_u = band(asks, False, 0.01)
            rows.append({
                "ts_utc": iso_utc(utc_ms()),
                "mid": mid, "spread_bps": spread_bps,
                "depth_bid_0p5pct": b05_q, "depth_ask_0p5pct": a05_q,
                "depth_bid_1pct": b10_q, "depth_ask_1pct": a10_q,
                "depth_bid_0p5pct_usd": b05_u, "depth_ask_0p5pct_usd": a05_u,
                "depth_bid_1pct_usd": b10_u, "depth_ask_1pct_usd": a10_u,
            })
        if i < iterations - 1:
            time.sleep(60)
    save_csv(pd.DataFrame(rows), path)


def fetch_liq_chunk(http: HttpClient, symbol: str, st: int, et: int, tag: str) -> List[Dict[str, Any]]:
    try:
        js = http.get(f"{BINANCE_FAPI}/fapi/v1/allForceOrders", {"symbol": symbol, "startTime": st, "endTime": et}, tag+"-all", timeout_connect=2.0, timeout_read=6.0, retries=2)
        if js:
            return js
    except Exception:
        pass
    try:
        js = http.get(f"{BINANCE_FAPI}/fapi/v1/forceOrders", {"symbol": symbol, "startTime": st, "endTime": et}, tag+"-force", timeout_connect=2.0, timeout_read=6.0, retries=2)
        return js or []
    except Exception:
        return []


def job_07(http: HttpClient, outdir: str, symbol: str) -> None:
    """07_arb_liquidations_1m_7d.csv"""
    name = "07_arb_liquidations_1m_7d"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 7 * 86400000
    step = 12 * 60 * 60 * 1000  # 12h chunks
    rows: List[Dict[str, Any]] = []
    t = start
    while t < end:
        chunk_end = min(t + step - 1, end)
        part = fetch_liq_chunk(http, symbol, t, chunk_end, name)
        if part:
            rows.extend(part)
        t = chunk_end + 1
        time.sleep(0.05)
    if not rows:
        save_csv(pd.DataFrame([{
            "ts_utc": iso_utc(utc_ms()), "liq_long_usd": 0.0, "liq_short_usd": 0.0,
            "count_long": 0, "count_short": 0, "raw_sell_usd": 0.0, "raw_buy_usd": 0.0, "mapping_assumption": True
        }]), path)
        return
    liq = pd.DataFrame(rows)
    liq["price"] = pd.to_numeric(liq.get("price", 0), errors="coerce")
    liq["qty"] = pd.to_numeric(liq.get("origQty", liq.get("executedQty", 0)), errors="coerce")
    liq["usd"] = liq["price"] * liq["qty"]
    liq["ts_utc"] = pd.to_numeric(liq.get("time", liq.get("updateTime", 0)), errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    liq["minute"] = liq["ts_utc"].astype(str).str.slice(0,16)  # YYYY-MM-DDTHH:MM
    liq["side"] = liq.get("side", "SELL")
    grp = liq.groupby(["minute","side"]).agg(usd=("usd","sum"), cnt=("usd","count")).reset_index()
    raw = grp.pivot(index="minute", columns="side", values="usd").fillna(0.0)
    raw_cnt = grp.pivot(index="minute", columns="side", values="cnt").fillna(0)
    raw.columns.name = None; raw_cnt.columns.name = None
    out = pd.DataFrame(index=raw.index)
    out["raw_sell_usd"] = raw.get("SELL", 0.0); out["raw_buy_usd"] = raw.get("BUY", 0.0)
    out["liq_long_usd"] = out["raw_sell_usd"]; out["liq_short_usd"] = out["raw_buy_usd"]
    out["count_long"] = raw_cnt.get("SELL", 0); out["count_short"] = raw_cnt.get("BUY", 0)
    out["mapping_assumption"] = True
    out = out.reset_index().rename(columns={"minute":"ts_utc"})
    save_csv(out, path)


def job_08(http: HttpClient, outdir: str, symbol: str) -> None:
    """08_arb_basis_mark_index_1m_24h.csv"""
    name = "08_arb_basis_mark_index_1m_24h"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 24 * 60 * 60 * 1000
    mark = fetch_klines_paged(http, "/fapi/v1/markPriceKlines", symbol, "1m", start, end, name+"-mark")
    idx = fetch_klines_paged(http, "/fapi/v1/indexPriceKlines", symbol, "1m", start, end, name+"-index")
    cols = ["open_time","open","high","low","close","volume","close_time","qv","t","tb","tbq","ig"]
    dfm = pd.DataFrame(mark or [], columns=cols)
    dfi = pd.DataFrame(idx or [], columns=cols)
    if dfm.empty or dfi.empty:
        save_csv(pd.DataFrame(columns=["ts_utc","mark_price","index_price","basis_pct"]), path)
        return
    for d in (dfm, dfi):
        d["close"] = pd.to_numeric(d["close"], errors="coerce")
        d["ts_utc"] = pd.to_numeric(d["close_time"], errors="coerce").astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    m = dfm[["ts_utc","close"]].rename(columns={"close":"mark_price"})
    i = dfi[["ts_utc","close"]].rename(columns={"close":"index_price"})
    out = pd.merge(m, i, on="ts_utc", how="inner").dropna()
    out["basis_pct"] = (out["mark_price"] / out["index_price"] - 1.0) * 100.0
    save_csv(out[["ts_utc","mark_price","index_price","basis_pct"]], path)


def job_09(http: HttpClient, outdir: str, symbol: str) -> None:
    """09_arb_trades_imbalance_1m_24h.csv"""
    name = "09_arb_trades_imbalance_1m_24h"
    path = f"{outdir}/{name}.csv"
    end = utc_ms()
    start = end - 24 * 60 * 60 * 1000
    raw = fetch_klines_paged(http, "/fapi/v1/klines", symbol, "1m", start, end, name)
    cols = ["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ig"]
    df = pd.DataFrame(raw or [], columns=cols)
    if df.empty:
        save_csv(pd.DataFrame(columns=["ts_utc","buy_qty","sell_qty","buy_quote","sell_quote","delta_quote"]), path)
        return
    for c in ["volume","quote_volume","taker_buy_base","taker_buy_quote","close_time"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts_utc"] = df["close_time"].astype("Int64").apply(lambda x: iso_utc(int(x)) if pd.notna(x) else None)
    df["buy_qty"] = df["taker_buy_base"]
    df["sell_qty"] = df["volume"] - df["buy_qty"]
    df["buy_quote"] = df["taker_buy_quote"]
    df["sell_quote"] = df["quote_volume"] - df["buy_quote"]
    df["delta_quote"] = df["buy_quote"] - df["sell_quote"]
    out = df[["ts_utc","buy_qty","sell_qty","buy_quote","sell_quote","delta_quote"]]
    save_csv(out, path)


def job_10(outdir: str, symbol: str) -> None:
    """10_agg_config_costs.json"""
    name = "10_agg_config_costs"
    path = f"{outdir}/{name}.json"
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
            "funding_fast": "20m",
            "stable": "15m"
        },
        "drift_threshold_ms": 1500
    }
    Path(path).write_text(json.dumps(cfg, ensure_ascii=False, indent=2))


def validate_out(outdir: str) -> bool:
    expected = {
        "01_arb_15m_20d.csv": ["ts_utc","open","high","low","close","volume","quote_volume","trades"],
        "02_arb_1d_180d.csv": ["date_utc","o","h","l","c","volume","fib_382","fib_618"],
        "03_arb_1m_today.csv": ["ts_utc","o","h","l","c","volume","vwap","above_vwap","retest_flag"],
        "04_arb_funding_8h_30d.csv": ["funding_time_utc","funding_realized_pct","funding_est_pct","next_funding_ts","next_funding_ms","symbol"],
        "05_arb_oi_1h_30d.csv": ["ts_utc","binance_oi_contracts","binance_oi_usd","bybit_oi_contracts","total_oi_usd"],
        "06_arb_orderbook_depth_1min_8h.csv": ["ts_utc","mid","spread_bps","depth_bid_0p5pct","depth_ask_0p5pct","depth_bid_1pct","depth_ask_1pct","depth_bid_0p5pct_usd","depth_ask_0p5pct_usd","depth_bid_1pct_usd","depth_ask_1pct_usd"],
        "07_arb_liquidations_1m_7d.csv": ["ts_utc","liq_long_usd","liq_short_usd","count_long","count_short","raw_sell_usd","raw_buy_usd","mapping_assumption"],
        "08_arb_basis_mark_index_1m_24h.csv": ["ts_utc","mark_price","index_price","basis_pct"],
        "09_arb_trades_imbalance_1m_24h.csv": ["ts_utc","buy_qty","sell_qty","buy_quote","sell_quote","delta_quote"],
        "10_agg_config_costs.json": None,
    }
    ok = True
    for fname, cols in expected.items():
        p = Path(outdir) / fname
        if not p.exists():
            print(f"[VALIDATE] missing {fname}")
            ok = False
            continue
        if p.suffix == ".csv":
            try:
                df = pd.read_csv(p, nrows=2)
                if cols:
                    for c in cols:
                        if c not in df.columns:
                            print(f"[VALIDATE] {fname} missing column {c}")
                            ok = False
            except Exception as e:
                print(f"[VALIDATE] {fname} read error: {e}")
                ok = False
        else:
            try:
                json.loads(p.read_text())
            except Exception as e:
                print(f"[VALIDATE] {fname} JSON error: {e}")
                ok = False
    print(f"[VALIDATE] RESULT = {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    ap = argparse.ArgumentParser(description="ARB/USDT aggregator v3 (fixed)")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ap.add_argument("--out", default="data")
    ap.add_argument("--mode", choices=["real"], default="real")
    ap.add_argument("--depth-iterations", type=int, default=1, help="job06 snapshots (use 480 for 8h)")
    ap.add_argument("--validate", action="store_true")
    args, _ = ap.parse_known_args()  # tolerate notebook args

    ensure_dir(args.out)
    http = HttpClient(mode=args.mode, debug=False)

    # server time drift (warning only)
    try:
        srv = http.get(f"{BINANCE_FAPI}/fapi/v1/time", {}, "time", timeout_connect=2.0, timeout_read=5.0, retries=2)
        drift = int(srv.get("serverTime", utc_ms())) - utc_ms()
        print(f"[TIME] server_drift_ms={drift}")
    except Exception as e:
        print(f"[TIME] drift check failed: {e}")

    print("[RUN] start")
    job_01(http, args.out, args.symbol)
    job_02(http, args.out, args.symbol)
    job_03(http, args.out, args.symbol)
    job_04(http, args.out, args.symbol)
    job_05(http, args.out, args.symbol)
    job_06(http, args.out, args.symbol, iterations=args.depth_iterations)
    job_07(http, args.out, args.symbol)
    job_08(http, args.out, args.symbol)
    job_09(http, args.out, args.symbol)
    job_10(args.out, args.symbol)

    if args.validate:
        validate_out(args.out)


if __name__ == "__main__":
    main()
