#!/usr/bin/env python3
"""v2 Data Service - BTC + Polymarket → state.json"""
import json, os, time, math, requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "..", "state.json")
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_URL = "https://clob.polymarket.com/book"

BTC_INTERVAL = 5
PM_INTERVAL = 0.1
HIST_INTERVAL = 60

_last_window_end = 0
_cached_condition_id = None
_cached_yes_token = None
_cached_no_token = None

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

def _fetch_clob_price(token_id: str) -> tuple[float, float] | None:
    """从 CLOB 获取单个 token 的 bid/ask，返 (bid, ask)"""
    try:
        r = requests.get(CLOB_URL, params={"token_id": token_id}, timeout=3,
            headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                return best_bid, best_ask
    except Exception:
        pass
    return None

def _fetch_pm_price():
    global _cached_condition_id, _cached_yes_token, _cached_no_token, _last_window_end
    slug = "btc-updown-5m-" + str(math.ceil(time.time() / 300) * 300)
    now_ts = int(time.time())
    window_end = math.ceil(time.time() / 300) * 300

    # 窗口切换时重新获取 token_id
    if window_end != _last_window_end:
        _last_window_end = window_end
        _cached_condition_id = None
        _cached_yes_token = None
        _cached_no_token = None
        remaining = window_end - now_ts
        print(f"[DATA]   → window ends in {remaining}s", flush=True)

    # 如果还没缓存 token_id，从 Gamma 获取
    if _cached_yes_token is None:
        try:
            r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
            if r.status_code == 200:
                markets = r.json()
                if markets:
                    m = markets[0]
                    _cached_condition_id = m.get("conditionId")
                    clob_ids = m.get("clobTokenIds") or []
                    if len(clob_ids) >= 2:
                        _cached_yes_token = clob_ids[0]
                        _cached_no_token = clob_ids[1]
        except Exception as e:
            print(f"[DATA][WARN] PM gamma:", e, flush=True)
            return 0.0, 0.0, slug

    # 优先：Gamma outcomePrices（上次成交价，更稳定）
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
                    return yes, no, slug
    except Exception:
        pass

    # Fallback：CLOB bid/ask 中价
    if _cached_yes_token:
        result = _fetch_clob_price(_cached_yes_token)
        if result:
            bid, ask = result
            yes = (bid + ask) / 2
            no = 1.0 - yes
            return yes, no, slug

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
