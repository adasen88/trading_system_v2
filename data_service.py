#!/usr/bin/env python3
"""v2 Data Service - BTC + Polymarket → state.json"""
import json, os, time, math, requests
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "..", "state.json")

BTC_INTERVAL = 5
PM_INTERVAL = 2
HIST_INTERVAL = 60

_last_window_slug = None
_pm_client = None

def _get_pm_client():
    global _pm_client
    if _pm_client is None:
        from polymarket_pandas import PolymarketPandas
        _pm_client = PolymarketPandas()
    return _pm_client

def _write_state(data):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f: json.dump(data, f)
    os.replace(tmp, STATE_FILE)

def _read_state():
    if not os.path.exists(STATE_FILE): return {}
    try:
        with open(STATE_FILE) as f: return json.load(f)
    except: return {}

def _merge(updates):
    s = _read_state(); s.update(updates); _write_state(s)

def poll_btc():
    price = 0.0; source = None
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=3)
        if r.status_code == 200:
            p = float(r.json().get("price") or 0)
            if p > 0: price = p; source = "Binance"
    except: pass
    if price <= 0:
        try:
            r = requests.get("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT", timeout=3)
            if r.status_code == 200:
                p = float(r.json().get("data", [{}])[0].get("last") or 0)
                if p > 0: price = p; source = "OKX"
        except: pass
    return {"price": price, "source": source}

def fetch_candles(interval, limit=100):
    try:
        r = requests.get("https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": interval, "limit": limit}, timeout=5)
        if r.status_code == 200:
            return [{"ts": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                     "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in r.json()]
    except Exception as e:
        print("[DATA][WARN] klines:", e, flush=True)
    return []

def _fetch_pm_price():
    global _last_window_slug
    now_ts = int(time.time())
    window_end = math.ceil(now_ts / 300) * 300

    # BTC 5-min 窗口 slug（与 Polymarket series slug 对应）
    window_slug = "btc-updown-5m-" + str(window_end)

    if window_slug != _last_window_slug:
        _last_window_slug = window_slug
        print(f"[DATA] PM window: {window_slug} (ends in {window_end - now_ts}s)", flush=True)

    client = _get_pm_client()

    # 1. 找到当前活跃的 BTC 5-min series
    try:
        series = client.get_series(
            slug="btc-up-or-down-5m",
            expand_events=True,
            closed=False,
        )
        active = series[series["eventsEndDate"] >= pd.Timestamp.now(tz="UTC")]
        if active.empty:
            print("[DATA][WARN] No active BTC 5-min events", flush=True)
            return 0.0, 0.0, window_slug
        event_slug = active["eventsSlug"].iloc[0]
    except Exception as e:
        print(f"[DATA][WARN] get_series:", e, flush=True)
        return 0.0, 0.0, window_slug

    # 2. 获取当前窗口的 CLOB 可用 token_id（expand_clob_token_ids=True）
    try:
        markets = client.get_markets(
            slug=[window_slug],
            expand_clob_token_ids=True,
            expand_events=False,
            expand_series=False,
        )
        if markets.empty:
            print(f"[DATA][WARN] No market data for {window_slug}", flush=True)
            return 0.0, 0.0, window_slug
        row = markets.sort_values("endDate").iloc[0]
    except Exception as e:
        print(f"[DATA][WARN] get_markets:", e, flush=True)
        return 0.0, 0.0, window_slug

    # 3. 提取 YES/NO CLOB token_id
    clob_ids = row.get("clobTokenIds", []) or []
    outcomes = row.get("outcomes", []) or []

    yes_tid = None
    no_tid = None
    for i, outcome in enumerate(outcomes):
        if str(outcome).lower() == "yes" and i < len(clob_ids):
            yes_tid = clob_ids[i]
        elif str(outcome).lower() == "no" and i < len(clob_ids):
            no_tid = clob_ids[i]

    if not yes_tid or not no_tid:
        print(f"[DATA][WARN] Could not find YES/NO token_ids in {clob_ids} / {outcomes}", flush=True)
        return 0.0, 0.0, window_slug

    # 4. 从 CLOB 获取实时价格
    try:
        mid_yes = client.get_midpoint_price(yes_tid)
        mid_no = client.get_midpoint_price(no_tid)
        spread_yes = client.get_spread(yes_tid)
        spread_no = client.get_spread(no_tid)

        if mid_yes is not None and mid_no is not None:
            yes = float(mid_yes)
            no = float(mid_no)
            if 0 < yes < 1 and 0 < no < 1:
                print(f"[DATA]   CLOB: YES={yes:.4f} NO={no:.4f} spread_yes={spread_yes:.4f} spread_no={spread_no:.4f}", flush=True)
                return yes, no, window_slug
    except Exception as e:
        print(f"[DATA][WARN] CLOB price fetch:", e, flush=True)

    # 5. Fallback: Gamma outcomePrices
    try:
        GAMMA_URL = "https://gamma-api.polymarket.com/markets"
        r = requests.get(GAMMA_URL, params={"slug": window_slug}, timeout=5)
        if r.status_code == 200:
            markets_g = r.json()
            if markets_g:
                prices_raw = markets_g[0].get("outcomePrices")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
                if isinstance(prices, list) and len(prices) >= 2:
                    yes = float(prices[0])
                    no = float(prices[1])
                    if yes > 0 and no > 0:
                        print(f"[DATA]   Gamma fallback: YES={yes:.4f} NO={no:.4f}", flush=True)
                        return yes, no, window_slug
    except Exception:
        pass

    return 0.0, 0.0, window_slug

def main():
    global _last_window_slug
    print("=" * 50, flush=True)
    print("  v2 Data Service", flush=True)
    print("=" * 50, flush=True)
    _write_state({"btc": 0.0, "btc_source": None, "pm_yes": 0.0, "pm_no": 0.0,
                   "pm_spread": 0.0, "candles_1m": [], "candles_5m": [], "candles_15m": [], "data_ts": None})
    print("[DATA] 拉取历史 K线...", flush=True)
    for iv, bi in [("1m","1m"), ("5m","5m"), ("15m","15m")]:
        c = fetch_candles(bi, 100)
        if c: _merge({f"candles_{iv}": c}); print(f"[DATA]   {iv}: {len(c)} 根", flush=True)
    last_b = last_pm = last_c = 0
    while True:
        now = time.time()
        if now - last_b >= BTC_INTERVAL:
            b = poll_btc()
            if b["price"] > 0: _merge({"btc": b["price"], "btc_source": b["source"], "data_ts": now})
            print(f"[DATA] BTC=${b['price']:,.0f} ({b['source']})", flush=True)
            last_b = now
        if now - last_pm >= PM_INTERVAL:
            yes, no, slug = _fetch_pm_price()
            if yes > 0:
                _merge({"pm_yes": yes, "pm_no": no, "pm_spread": abs(yes - no), "data_ts": now})
                print(f"[DATA] PM YES={yes:.4f} NO={no:.4f}", flush=True)
            last_pm = now
        if now - last_c >= HIST_INTERVAL:
            for iv, bi in [("1m","1m"), ("5m","5m"), ("15m","15m")]:
                c = fetch_candles(bi, 100)
                if c: _merge({f"candles_{iv}": c})
            last_c = now
        time.sleep(1)

if __name__ == "__main__": main()
