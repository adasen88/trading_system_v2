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
    window_slug = "btc-updown-5m-" + str(window_end)

    if window_slug != _last_window_slug:
        _last_window_slug = window_slug
        print(f"[DATA] PM window: {window_slug} (ends in {window_end - now_ts}s)", flush=True)

    client = _get_pm_client()

    try:
        markets = client.get_markets(
            slug=[window_slug],
            expand_clob_token_ids=True,
            expand_events=False,
            expand_series=False,
        )
        if markets.empty:
            print(f"[DATA][WARN] No market for {window_slug}", flush=True)
            return 0.0, 0.0, window_slug
    except Exception as e:
        print(f"[DATA][WARN] get_markets:", e, flush=True)
        return 0.0, 0.0, window_slug

    # Gamma 返回一个市场，clobTokenIds 是包含两个 token 的列表
    # outcomes=['Up','Down'], outcomePrices=[p_up, p_down]
    rows = markets.to_dict("records")
    if len(rows) == 0:
        print(f"[DATA][WARN] No market rows", flush=True)
        return 0.0, 0.0, window_slug

    # 取第一行（应该只有一行）
    market = rows[0]
    
    # 解析 clobTokenIds （可能是 JSON 字符串或列表）
    clob_ids_raw = market.get("clobTokenIds")
    clob_ids = []
    if clob_ids_raw:
        if isinstance(clob_ids_raw, str):
            try:
                parsed = json.loads(clob_ids_raw)
                # json.loads 可能返回整数 0 或其他非列表
                if isinstance(parsed, list):
                    clob_ids = parsed
                else:
                    print(f"[DATA][WARN] clobTokenIds parsed to non-list: {type(parsed)}={parsed}", flush=True)
                    clob_ids = []
            except Exception as e:
                print(f"[DATA][WARN] Failed to parse clobTokenIds JSON: {e}", flush=True)
                clob_ids = []
        elif isinstance(clob_ids_raw, list):
            clob_ids = clob_ids_raw
        else:
            print(f"[DATA][WARN] Unknown clobTokenIds type: {type(clob_ids_raw)}={clob_ids_raw}", flush=True)
            clob_ids = []
    
    if len(clob_ids) < 2:
        print(f"[DATA][WARN] Need at least 2 token IDs, got {len(clob_ids)} (raw: {clob_ids_raw})", flush=True)
        return 0.0, 0.0, window_slug
    
    # 第一个 token 是 UP，第二个是 DOWN
    up_tid = clob_ids[0]
    down_tid = clob_ids[1]
    
    # 解析 outcomes 和 outcomePrices
    outcomes = market.get("outcomes") or []
    prices_raw = market.get("outcomePrices") or []
    
    # 解析 prices（可能是 JSON 字符串或列表）
    def parse_prices(raw):
        if isinstance(raw, list):
            return raw
        try:
            return json.loads(raw) if isinstance(raw, str) else []
        except:
            return []
    
    prices = parse_prices(prices_raw)
    
    # 找 UP 和 DOWN 在 outcomes 中的位置
    up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), None)
    down_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "down"), None)
    
    if up_idx is None or down_idx is None:
        print(f"[DATA][WARN] Could not find UP/DOWN in outcomes: {outcomes}", flush=True)
        return 0.0, 0.0, window_slug
    
    p_up = float(prices[up_idx]) if up_idx < len(prices) else None
    p_down = float(prices[down_idx]) if down_idx < len(prices) else None
    
    print(f"[DATA]   UP_token={up_tid} p={p_up}", flush=True)
    print(f"[DATA]   DOWN_token={down_tid} p={p_down}", flush=True)

    if not up_tid or not down_tid:
        print(f"[DATA][WARN] Missing token IDs", flush=True)
        return 0.0, 0.0, window_slug

    # 从 CLOB 获取实时中价
    try:
        mid_up = client.get_midpoint_price(up_tid)
        mid_down = client.get_midpoint_price(down_tid)
        print(f"[DATA]   CLOB mid: UP={mid_up} DOWN={mid_down}", flush=True)
        if mid_up is not None and mid_down is not None:
            yes = float(mid_up)
            no = float(mid_down)
            if 0 < yes < 1 and 0 < no < 1:
                return yes, no, window_slug
    except Exception as e:
        print(f"[DATA][WARN] CLOB:", e, flush=True)

    # Fallback: outcomePrices
    if p_up and p_down:
        print(f"[DATA]   Gamma: UP={p_up:.4f} DOWN={p_down:.4f}", flush=True)
        return p_up, p_down, window_slug

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
