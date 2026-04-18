#!/usr/bin/env python3
"""
v2 API Server - 只负责：读 state.json，返回 JSON
绝不计算、绝不写文件、绝不使用 aiohttp/异步数据拉取
"""
import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "..", "state.json")

# 端口（与旧系统 8765 区分）
PORT = 9000


# ── 读取 state.json ──────────────────────────────────────
def _read_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


# ── FastAPI 路由（只读，不算）────────────────────────────
def create_app():
    try:
        from fastapi import FastAPI
        from fastapi.middleware.cors import CORSMiddleware
        import uvicorn
    except ImportError:
        print("[API][ERROR] FastAPI/uvicorn not installed")
        print("  Run: pip install fastapi uvicorn")
        sys.exit(1)

    app = FastAPI(title="TradingOS v2 API", version="2.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── /health ─────────────────────────────────────────
    @app.get("/health")
    async def health():
        state = _read_state()
        btc = state.get("btc", 0)
        return {
            "status": "ok",
            "btc": btc,
            "ts": state.get("data_ts"),
        }

    # ── /status ─────────────────────────────────────────
    @app.get("/status")
    async def status():
        state = _read_state()
        sig = state.get("signal", {})
        dec = state.get("decision", {})
        return {
            "state": "RUNNING",
            "signal": sig.get("bias", "NONE"),
            "confidence": sig.get("confidence", 0),
            "decision": dec.get("action", "NO_TRADE"),
            "equity": 100.0,
        }

    # ── /market ──────────────────────────────────────────
    @app.get("/market")
    async def market():
        state = _read_state()
        return {
            "btc_price": round(state.get("btc", 0), 2),
            "pm_yes": round(state.get("pm_yes", 0), 4),
            "pm_no": round(state.get("pm_no", 0), 4),
            "pm_spread": round(state.get("pm_spread", 0), 4),
            "timestamp": state.get("data_ts"),
        }

    # ── /signal ─────────────────────────────────────────
    @app.get("/signal")
    async def signal():
        state = _read_state()
        sig = state.get("signal", {})
        return {
            "bias": sig.get("bias", "NONE"),
            "confidence": sig.get("confidence", 0),
            "multi_cycle_score": sig.get("multi_cycle_score", 0),
            "ema_strength": sig.get("ema_strength", 0),
            "trend_persistence": sig.get("trend_persistence", 0),
            "btc_price": sig.get("btc_price", 0),
            "note": sig.get("note", ""),
        }

    # ── /decision ───────────────────────────────────────
    @app.get("/decision")
    async def decision():
        state = _read_state()
        dec = state.get("decision", {})
        return {
            "action": dec.get("action", "NO_TRADE"),
            "edge": dec.get("edge", 0),
            "win_rate": dec.get("win_rate", 0),
            "price": dec.get("price", 0),
            "size": dec.get("size", 0),
            "reason": dec.get("reason", ""),
        }

    # ── /dashboard (HTML) ───────────────────────────────
    @app.get("/")
    async def dashboard():
        return {"message": "TradingOS v2 API", "endpoints": ["/health", "/market", "/signal", "/decision"]}

    return app, uvicorn


app, _uvicorn = create_app()

if __name__ == "__main__":
    print("=" * 50, flush=True)
    print("  v2 API Server", flush=True)
    print(f"  STATE: {STATE_FILE}", flush=True)
    print(f"  PORT:  {PORT}", flush=True)
    print("=" * 50, flush=True)
    _uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
