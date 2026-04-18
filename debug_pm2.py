#!/usr/bin/env python3
"""Debug Polymarket BTC-5min CLOB token structure"""
import requests, json, math, time

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_URL = "https://clob.polymarket.com"

now_ts = int(time.time())
window_end = math.ceil(now_ts / 300) * 300
slug = f"btc-updown-5m-{window_end}"
print(f"Window: {slug} (ends in {window_end - now_ts}s)\n")

# ── 1. Gamma 原始数据 ───────────────────────────────────────
r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
markets = r.json()
print(f"[1] Gamma 返回 {len(markets)} 个市场:")
for i, m in enumerate(markets):
    print(f"  市场{i}: {m.get('question')}")
    print(f"    clobTokenIds: {m.get('clobTokenIds')}")
    print(f"    outcomes:     {m.get('outcomes')}")
    print(f"    outcomePrices: {m.get('outcomePrices')}")
    tokens = m.get("tokens") or []
    print(f"    tokens 数量: {len(tokens)}")
    for j, t in enumerate(tokens):
        print(f"      [{j}] outcome={t.get('outcome')} token_id={t.get('token_id')}")

# ── 2. 直接调 CLOB /price 和 /midpoint ────────────────────────────────────────────────────
clob_ids_raw = markets[0].get("clobTokenIds") if markets else None
print(f"\n[2] 直接调 CLOB API:")
print(f"    clobTokenIds raw: {clob_ids_raw}")
print(f"    type: {type(clob_ids_raw)}")

# 解析 clobTokenIds （可能是 JSON 字符串）
clob_ids = []
if clob_ids_raw:
    if isinstance(clob_ids_raw, str):
        try:
            clob_ids = json.loads(clob_ids_raw)
        except Exception as e:
            print(f"    JSON parse error: {e}")
            clob_ids = []
    elif isinstance(clob_ids_raw, list):
        clob_ids = clob_ids_raw
    else:
        print(f"    Unknown type: {type(clob_ids_raw)}")

print(f"    parsed clobTokenIds: {clob_ids}")
print(f"    length: {len(clob_ids)}")

for tid in clob_ids:
    print(f"\n  Token: {tid}")
    # /price?token_id=...&side=BUY
    r_buy = requests.get(f"{CLOB_URL}/price", params={"token_id": tid, "side": "BUY"}, timeout=3, headers={"User-Agent": "Mozilla/5.0"})
    # /price?token_id=...&side=SELL
    r_sell = requests.get(f"{CLOB_URL}/price", params={"token_id": tid, "side": "SELL"}, timeout=3, headers={"User-Agent": "Mozilla/5.0"})
    # /midpoint?token_id=...
    r_mid = requests.get(f"{CLOB_URL}/midpoint", params={"token_id": tid}, timeout=3, headers={"User-Agent": "Mozilla/5.0"})
    print(f"    BUY:  {r_buy.status_code} {r_buy.json()}")
    print(f"    SELL: {r_sell.status_code} {r_sell.json()}")
    print(f"    MID:  {r_mid.status_code} {r_mid.json()}")

# ── 3. py_clob_client.get_midpoint ──────────────────────────────
print(f"\n[3] py_clob_client.get_midpoint:")
try:
    from py_clob_client.client import ClobClient
    client = ClobClient(CLOB_URL)
    for tid in clob_ids:
        mid = client.get_midpoint_price(tid)
        print(f"  {tid[:40]}... → midpoint={mid}")
except Exception as e:
    print(f"  ERROR: {e}")
