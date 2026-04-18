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

def _get_token_ids_for_window(slug: str):
    """从 py_clob_client.get_markets() 获取当前窗口的 YES/NO token_id"""
    try:
        client = _get_clob_client()
        resp = client.get_markets()
        markets = resp.get("data", []) if isinstance(resp, dict) else (resp or [])
        for m in markets:
            if m.get("slug") == slug:
                tokens = m.get("tokens", []) or []
                for t in tokens:
                    outcome = (t.get("outcome") or "").lower()
                    if outcome == "yes":
                        print(f"[DATA]   [DEBUG] YES token from get_markets: {t.get('token_id')}", flush=True)
                    elif outcome == "no":
                        print(f"[DATA]   [DEBUG] NO token from get_markets: {t.get('token_id')}", flush=True)
                yes_t = next((t["token_id"] for t in tokens if (t.get("outcome") or "").lower() == "yes"), None)
                no_t = next((t["token_id"] for t in tokens if (t.get("outcome") or "").lower() == "no"), None)
                if yes_t and no_t:
                    return yes_t, no_t
    except Exception as e:
        print(f"[DATA][WARN] get_markets:", e, flush=True)
    return None, None

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

    # 每个新窗口用 get_markets 获取 token_id
    if _cached_yes_token is None:
        yes_t, no_t = _get_token_ids_for_window(slug)
        if yes_t and no_t:
            _cached_yes_token = yes_t
            _cached_no_token = no_t
            print(f"[DATA]   tokens: YES={yes_t[:20]}... NO={no_t[:20]}...", flush=True)
        else:
            # Fallback: 从 Gamma 直接获取 clobTokenIds
            try:
                r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
                if r.status_code == 200:
                    markets = r.json()
                    if markets:
                        clob_ids = markets[0].get("clobTokenIds") or []
                        print(f"[DATA]   [DEBUG] Gamma clobTokenIds: {clob_ids}", flush=True)
                        if len(clob_ids) >= 2:
                            _cached_yes_token = clob_ids[0]
                            _cached_no_token = clob_ids[1]
                            print(f"[DATA]   Gamma fallback tokens: YES={_cached_yes_token[:20]}... NO={_cached_no_token[:20]}...", flush=True)
            except Exception as e:
                print(f"[DATA][WARN] PM gamma:", e, flush=True)

    if _cached_yes_token is None:
        return 0.0, 0.0, slug

    # 方法1：CLOB last trade price
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
        else:
            print(f"[DATA][DEBUG] last_trade: yes={last_yes} no={last_no}", flush=True)
    except Exception as e:
        print(f"[DATA][WARN] CLOB last_trade:", e, flush=True)

    # 方法2：CLOB midpoint
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
        else:
            print(f"[DATA][DEBUG] midpoint: yes={mid_yes} no={mid_no}", flush=True)
    except Exception as e:
        print(f"[DATA][WARN] CLOB midpoint:", e, flush=True)

    # 方法3：Gamma outcomePrices
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
