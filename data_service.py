#!/usr/bin/env python3
"""v2 Data Service - BTC + Polymarket → state.json"""
import json, os, time, math, requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "..", "state.json")
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_URL = "https://clob.polymarket.com"

BTC_INTERVAL = 5
PM_INTERVAL = 2
HIST_INTERVAL = 60

_last_window_end = 0
_cached_yes_token = None
_cached_no_token = None
_clob_client = None

def _get_clob_client():
    """延迟初始化 CLOB 客户端（读操作无需认证）"""
    global _clob_client
    if _clob_client is None:
        from py_clob_client.client import ClobClient
        _clob_client = ClobClient(CLOB_URL)
    return _clob_client

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
    global _cached_yes_token, _cached_no_token, _last_window_end
    now_ts = int(time.time())
    window_end = math.ceil(now_ts / 300) * 300
    slug = "btc-updown-5m-" + str(window_end)

    if window_end != _last_window_end:
        _last_window_end = window_end
        _cached_yes_token = None
        _cached_no_token = None
        remaining = window_end - now_ts
        print(f"[DATA] PM window: {slug} (ends in {remaining}s)", flush=True)

    # 每个新窗口从 Gamma 获取 token_id
    if _cached_yes_token is None:
        try:
            r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
            if r.status_code == 200:
                markets = r.json()
                if markets:
                    clob_ids = markets[0].get("clobTokenIds") or []
                    if len(clob_ids) >= 2:
                        _cached_yes_token = clob_ids[0]
                        _cached_no_token = clob_ids[1]
                        print(f"[DATA]   token_ids: YES={_cached_yes_token[:16]}... NO={_cached_no_token[:16]}...", flush=True)
        except Exception as e:
            print(f"[DATA][WARN] PM gamma:", e, flush=True)
            return 0.0, 0.0, slug

    if _cached_yes_token is None:
        return 0.0, 0.0, slug

    # 方法1：py_clob_client.get_last_trade_price（实际成交价）
    try:
        client = _get_clob_client()
        last_yes = client.get_last_trade_price(_cached_yes_token)
        last_no = client.get_last_trade_price(_cached_no_token)
        if last_yes is not None and last_no is not None:
            yes = float(last_yes)
            no = float(last_no)
            if 0 < yes < 1 and 0 < no < 1:
                print(f"[DATA]   CLOB last trade: YES={yes:.4f} NO={no:.4f}", flush=True)
                return yes, no, slug
    except Exception as e:
        print(f"[DATA][WARN] CLOB last_trade:", e, flush=True)

    # 方法2：py_clob_client.get_midpoint（中价）
    try:
        client = _get_clob_client()
        mid_yes = client.get_midpoint(_cached_yes_token)
        mid_no = client.get_midpoint(_cached_no_token)
        if mid_yes is not None and mid_no is not None:
            yes = float(mid_yes)
            no = float(mid_no)
            if 0 < yes < 1 and 0 < no < 1:
                print(f"[DATA]   CLOB midpoint: YES={yes:.4f} NO={no:.4f}", flush=True)
                return yes, no, slug
    except Exception as e:
        print(f"[DATA][WARN] CLOB midpoint:", e, flush=True)

    # 方法3：Gamma outcomePrices（上次成交价，作兜底）
    try:
        r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
        if r.status_code == 200:
            markets = r.json()
            if markets:
                prices_raw = markets[0].get("outcomePrices")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
                if isinstance(prices, list) and len(prices) >= 2:
                    yes = float(prices[0])
                    no = float(prices[1])
                    if yes > 0 and no > 0:
                        return yes, no, slug
    except Exception:
        pass

    return 0.0, 0.0, slug

def main():
    global _last_window_end
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
