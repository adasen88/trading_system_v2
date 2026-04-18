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
    
    client = _get_pm_client()
    
    try:
        # 生成最近 10 个窗口的 slug
        window_end = math.ceil(now_ts / 300) * 300
        recent_slugs = []
        for i in range(10):  # 当前窗口 + 前9个
            w_end = window_end - i * 300
            if w_end <= 0:
                break
            recent_slugs.append(f"btc-updown-5m-{w_end}")
        
        # 获取这些窗口的市场
        markets = client.get_markets(
            slug=recent_slugs,
            expand_clob_token_ids=True,
            expand_events=False,
            expand_series=False,
        )
        if markets.empty:
            print(f"[DATA][WARN] No BTC 5min markets found among {len(recent_slugs)} slugs", flush=True)
            return 0.0, 0.0, ""
    except Exception as e:
        print(f"[DATA][WARN] get_markets:", e, flush=True)
        return 0.0, 0.0, ""
    
    rows = markets.to_dict("records")
    if len(rows) == 0:
        print(f"[DATA][WARN] No market rows", flush=True)
        return 0.0, 0.0, ""
    
    # 按结束时间倒序排序（最新的在前）
    def get_end_time(market):
        # 优先从 slug 中提取时间戳（格式: btc-updown-5m-{timestamp}）
        slug = market.get("slug", "")
        if slug.startswith("btc-updown-5m-"):
            try:
                return int(slug.split("-")[-1])
            except:
                pass
        
        # 回退到解析 end_date_iso
        end_str = market.get("end_date_iso", "")
        if end_str:
            try:
                # 尝试解析 ISO 8601 格式
                from datetime import datetime
                # 移除时区信息以便解析
                if end_str.endswith("Z"):
                    end_str = end_str[:-1] + "+00:00"
                dt = datetime.fromisoformat(end_str)
                return int(dt.timestamp())
            except Exception as e:
                print(f"[DATA][WARN] Failed to parse end_date_iso '{end_str}': {e}", flush=True)
        
        return 0
    
    rows.sort(key=get_end_time, reverse=True)
    
    selected_market = None
    selected_slug = ""
    
    # 从最新到最旧扫描，找到第一个可交易市场
    for market in rows:
        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-5m-"):
            continue
            
        # 检查是否为可交易市场（仅依赖 CLOB token IDs）
        clob_ids_raw = market.get("clobTokenIds")
        clob_ids_possible = False
        clob_ids = []
        
        if clob_ids_raw:
            if isinstance(clob_ids_raw, str):
                try:
                    parsed = json.loads(clob_ids_raw)
                    if isinstance(parsed, list) and len(parsed) >= 2:
                        clob_ids_possible = True
                        clob_ids = parsed
                except Exception as e:
                    print(f"[DATA][DEBUG] Failed to parse clobTokenIds JSON for {slug}: {e}", flush=True)
                    pass
            elif isinstance(clob_ids_raw, list) and len(clob_ids_raw) >= 2:
                clob_ids_possible = True
                clob_ids = clob_ids_raw
        
        # 可交易条件：必须有至少2个CLOB token IDs
        if clob_ids_possible:
            selected_market = market
            selected_slug = slug
            # 预先存储解析的clob_ids
            market["_parsed_clob_ids"] = clob_ids
            break
        else:
            # 记录原因
            if clob_ids_raw:
                if isinstance(clob_ids_raw, str):
                    print(f"[DATA][SKIP] Non-tradable: {slug} clobTokenIds parse failed or <2 tokens", flush=True)
                else:
                    print(f"[DATA][SKIP] Non-tradable: {slug} clobTokenIds={clob_ids_raw}", flush=True)
            else:
                print(f"[DATA][SKIP] Non-tradable: {slug} clobTokenIds missing", flush=True)
    
    if not selected_market:
        print(f"[DATA][WARN] No tradable market found among {len(rows)} markets", flush=True)
        return 0.0, 0.0, ""
    
    market = selected_market
    window_slug = selected_slug
    
    if window_slug != _last_window_slug:
        _last_window_slug = window_slug
        # 计算剩余时间
        end_time = get_end_time(market)
        if end_time > 0:
            remaining = end_time - now_ts
            print(f"[DATA] Selected tradable market: {window_slug} (ends in {remaining}s)", flush=True)
        else:
            print(f"[DATA] Selected tradable market: {window_slug}", flush=True)
    
    # 使用预先解析的 CLOB token IDs（如果存在）
    clob_ids = market.get("_parsed_clob_ids")
    
    if not clob_ids:
        # 回退到原始解析逻辑
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
    
    # 解析 outcomes（仅用于验证市场结构）
    outcomes = market.get("outcomes") or []
    
    # 找 UP 和 DOWN 在 outcomes 中的位置（仅验证）
    up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), None)
    down_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "down"), None)
    
    if up_idx is None or down_idx is None:
        print(f"[DATA][WARN] Could not find UP/DOWN in outcomes: {outcomes}", flush=True)
        return 0.0, 0.0, window_slug
    
    # 如果有两个 token，尝试 CLOB；否则价格不可用
    if len(clob_ids) >= 2:
        # 第一个 token 是 UP，第二个是 DOWN
        up_tid = clob_ids[0]
        down_tid = clob_ids[1]
        
        print(f"[DATA]   UP_token={up_tid}", flush=True)
        print(f"[DATA]   DOWN_token={down_tid}", flush=True)

        if not up_tid or not down_tid:
            print(f"[DATA][WARN] Missing token IDs", flush=True)
            return 0.0, 0.0, window_slug
        else:
            # 从 CLOB 获取实时中价（唯一允许的价格源）
            try:
                mid_up = client.get_midpoint_price(up_tid)
                mid_down = client.get_midpoint_price(down_tid)
                print(f"[DATA]   CLOB mid: UP={mid_up} DOWN={mid_down}", flush=True)
                
                if mid_up is None or mid_down is None:
                    print(f"[DATA][WARN] CLOB returned None prices", flush=True)
                    return 0.0, 0.0, window_slug
                
                yes = float(mid_up)
                no = float(mid_down)
                
                # 价格验证：必须在合理范围内
                if 0 < yes < 1 and 0 < no < 1:
                    # 检查价格和是否接近1.0（YES+NO≈1）
                    price_sum = yes + no
                    if 0.9 < price_sum < 1.1:  # 允许10%偏差
                        print(f"[DATA]   Valid prices: YES={yes:.4f} NO={no:.4f} sum={price_sum:.4f}", flush=True)
                        return yes, no, window_slug
                    else:
                        print(f"[DATA][WARN] Invalid price sum: {price_sum:.4f}", flush=True)
                        return 0.0, 0.0, window_slug
                else:
                    print(f"[DATA][WARN] Prices out of range: YES={yes:.4f} NO={no:.4f}", flush=True)
                    return 0.0, 0.0, window_slug
                    
            except Exception as e:
                print(f"[DATA][WARN] CLOB error:", e, flush=True)
                return 0.0, 0.0, window_slug
    else:
        print(f"[DATA][WARN] Only {len(clob_ids)} token(s), cannot trade", flush=True)
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
