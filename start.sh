#!/bin/bash
# v2 一键启动（三层独立）
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_DIR="$SCRIPT_DIR"
STATE_FILE="$STATE_DIR/../state.json"

echo "=========================================="
echo "  v2 TradingOS 启动"
echo "=========================================="

# 杀掉旧端口
lsof -ti :9000 | xargs kill -9 2>/dev/null || true
lsof -ti :8080 | xargs kill -9 2>/dev/null || true
sleep 1

# ── 终端1：Data Service ────────────────────────────────
echo "[1/3] 启动 Data Service..."
python3 "$SCRIPT_DIR/data_service.py" &
sleep 3

# 验证 data 层
if curl -sf --max-time 3 "http://localhost:9000/health" > /dev/null 2>&1; then
    echo "  ✅ API 就绪"
else
    echo "  ⚠️  先启动 API 才能验证"
fi

# ── 终端2：Execution Engine ────────────────────────────
echo "[2/3] 启动 Execution Engine..."
python3 "$SCRIPT_DIR/execution_engine.py" &
sleep 2

# ── 终端3：API Server ─────────────────────────────────
echo "[3/3] 启动 API Server (port 9000)..."
cd "$SCRIPT_DIR"
uvicorn api_server:app --port 9000 --host 0.0.0.0 &
sleep 2

echo ""
echo "=========================================="
echo "  ✅ v2 全部启动"
echo "  API:      http://localhost:9000"
echo "  Health:   http://localhost:9000/health"
echo "  Market:   http://localhost:9000/market"
echo "  Signal:   http://localhost:9000/signal"
echo "  Decision: http://localhost:9000/decision"
echo "=========================================="
echo ""
echo "验证："
curl -s http://localhost:9000/health
echo ""
