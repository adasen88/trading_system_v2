#!/usr/bin/env python3
"""
v2 Execution Engine - 只负责：读行情 → 算信号 → 写决策
不依赖任何内部模块（无 WebSocket / aiohttp / uvicorn）
"""
import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "..", "state.json")

EXEC_INTERVAL = 10  # 每 10 秒算一次信号

# ── 硬编码配置（脱敏，不依赖 config_manager）───────────────
class Cfg:
    edge_threshold = 0.05
    maker_offset = 0.002
    confidence_taker_threshold = 0.65
    confidence_skip_threshold = 0.30
    position_size = 1.0       # U
    atr_threshold = 0.001     # ATR 最小波动率


# ── 读 state.json ────────────────────────────────────────
def _read_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _merge_write(updates: dict):
    """只更新自己的字段（signal / decision）"""
    state = _read_state()
    state.update(updates)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


# ── EMA 计算 ─────────────────────────────────────────────
def compute_ema(closes: list, period: int) -> float:
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def ema_cross_signal(closes: list) -> tuple:
    """返回 (bias: str, confidence: float)"""
    if len(closes) < 25:
        return "NONE", 0.0
    ema5 = compute_ema(closes, 5)
    ema20 = compute_ema(closes, 20)
    spread = ema5 - ema20
    avg_price = sum(closes[-20:]) / 20
    norm_spread = abs(spread) / avg_price if avg_price else 0.0
    if ema5 > ema20:
        return "UP", min(norm_spread * 50, 1.0)
    elif ema5 < ema20:
        return "DOWN", min(norm_spread * 50, 1.0)
    return "NONE", 0.0


def atr_filter(candles: list) -> float:
    """ATR 过滤，返回 True 表示通过"""
    if len(candles) < 14:
        return 1.0
    trs = []
    for i, c in enumerate(candles[-14:]):
        if i == 0:
            tr = c["h"] - c["l"]
        else:
            prev = candles[-14 + i - 1]
            tr = max(
                c["h"] - c["l"],
                abs(c["h"] - prev["c"]),
                abs(c["l"] - prev["c"]),
            )
        trs.append(tr)
    atr = sum(trs) / 14
    last_close = candles[-1]["c"]
    ratio = atr / last_close if last_close else 0
    return ratio


def trend_persistence(closes: list, direction: str) -> float:
    """最近 5 根 K线有多少顺着 direction"""
    if len(closes) < 6:
        return 0.5
    count = 0
    for i in range(1, 6):
        delta = (closes[-i] - closes[-i - 1]) / closes[-i - 1]
        if (direction == "UP" and delta > 0) or (direction == "DOWN" and delta < 0):
            count += 1
    return count / 5


# ── 信号计算 ─────────────────────────────────────────────
def compute_signal(state: dict) -> dict:
    """多周期 EMA 融合信号"""
    weights = {"1m": 0.2, "5m": 0.4, "15m": 0.4}
    bias_votes = {"UP": 0.0, "DOWN": 0.0, "NONE": 0.0}
    ema_str_total = 0.0
    trend_total = 0.0
    btc_price = state.get("btc", 0.0)

    for iv, w in weights.items():
        candles = state.get(f"candles_{iv}", [])
        if not candles:
            continue
        closes = [c["c"] for c in candles]
        bias, ema_str = ema_cross_signal(closes)
        persist = trend_persistence(closes, bias) if bias != "NONE" else 0.5
        bias_votes[bias] += w
        ema_str_total += ema_str * w
        trend_total += persist * w

    final_bias = max(bias_votes, key=bias_votes.get)
    multi_score = max(bias_votes[final_bias], 0.0) if final_bias != "NONE" else 0.0

    confidence = (
        multi_score * 0.40
        + ema_str_total * 0.30
        + trend_total * 0.30
    )
    confidence = min(max(confidence, 0.0), 1.0)

    # ATR 过滤
    if final_bias != "NONE":
        candles_5m = state.get("candles_5m", [])
        if candles_5m:
            atr_ratio = atr_filter(candles_5m)
            if atr_ratio < Cfg.atr_threshold:
                confidence *= 0.5  # 低波动降低置信度

    note = ""
    if final_bias == "NONE":
        note = "No signal - EMA flat"
    elif confidence < Cfg.confidence_skip_threshold:
        note = f"Weak signal (conf={confidence:.2f})"

    return {
        "bias": final_bias,
        "confidence": round(confidence, 4),
        "multi_cycle_score": round(multi_score, 4),
        "ema_strength": round(ema_str_total, 4),
        "trend_persistence": round(trend_total, 4),
        "btc_price": btc_price,
        "note": note,
        "ts": time.time(),
    }


# ── 决策计算 ─────────────────────────────────────────────
def compute_decision(signal: dict, state: dict) -> dict:
    """根据信号 + 市场数据输出交易决策"""
    bias = signal["bias"]
    confidence = signal["confidence"]
    yes_price = state.get("pm_yes", 0.0)
    no_price = state.get("pm_no", 0.0)
    btc_price = state.get("btc", 0.0)

    if yes_price == 0 or no_price == 0:
        return {
            "action": "NO_TRADE",
            "edge": 0.0,
            "win_rate": 0.5,
            "price": 0.0,
            "size": Cfg.position_size,
            "reason": "Polymarket price unavailable",
        }

    # 概率映射
    if bias == "UP":
        win_rate = 0.5 + confidence * 0.4
    elif bias == "DOWN":
        win_rate = 0.5 - confidence * 0.4
    else:
        win_rate = 0.5
    win_rate = min(max(win_rate, 0.01), 0.99)

    # Edge 计算
    yes_edge = win_rate - yes_price
    no_edge = (1 - win_rate) - no_price

    action = "NO_TRADE"
    edge = 0.0
    price = 0.0
    reason = ""

    if yes_edge > Cfg.edge_threshold and bias == "UP":
        action = "BUY_YES"
        edge = yes_edge
        price = round(yes_price - Cfg.maker_offset, 4)
        price = max(price, 0.0001)
        reason = f"YES edge={yes_edge:.4f} > {Cfg.edge_threshold} | wr={win_rate:.3f}"
    elif no_edge > Cfg.edge_threshold and bias == "DOWN":
        action = "BUY_NO"
        edge = no_edge
        price = round(no_price - Cfg.maker_offset, 4)
        price = max(price, 0.0001)
        reason = f"NO edge={no_edge:.4f} > {Cfg.edge_threshold} | wr={win_rate:.3f}"
    else:
        if bias == "UP":
            reason = f"No trade: YES edge={yes_edge:.4f} <= {Cfg.edge_threshold}"
        elif bias == "DOWN":
            reason = f"No trade: NO edge={no_edge:.4f} <= {Cfg.edge_threshold}"
        else:
            reason = "No signal - flat market"

    return {
        "action": action,
        "edge": round(edge, 4),
        "win_rate": round(win_rate, 4),
        "price": price,
        "size": Cfg.position_size,
        "reason": reason,
        "ts": time.time(),
    }


# ── 主循环 ───────────────────────────────────────────────
def main():
    print("=" * 50, flush=True)
    print("  v2 Execution Engine", flush=True)
    print(f"  STATE: {STATE_FILE}", flush=True)
    print("=" * 50, flush=True)

    last_exec_ts = 0
    tick = 0

    while True:
        now = time.time()

        if now - last_exec_ts >= EXEC_INTERVAL:
            state = _read_state()

            # 等数据
            btc = state.get("btc", 0)
            if btc <= 0:
                print("[EXEC] 等待 BTC 数据...", flush=True)
                time.sleep(2)
                last_exec_ts = now
                continue

            tick += 1
            sig = compute_signal(state)
            dec = compute_decision(sig, state)

            _merge_write({
                "signal": sig,
                "decision": dec,
            })

            print(
                f"[EXEC][Tick {tick}] "
                f"BTC=${sig['btc_price']:,.0f} | "
                f"Signal={sig['bias']}({sig['confidence']:.2f}) | "
                f"Decision={dec['action']} edge={dec['edge']:.4f} | "
                f"{dec['reason'][:60]}",
                flush=True,
            )

            last_exec_ts = now

        time.sleep(1)


if __name__ == "__main__":
    main()
