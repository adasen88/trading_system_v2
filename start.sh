#!/bin/bash
# v2 一键启动（三层独立）
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_DIR="$SCRIPT_DIR"
STATE_FILE="$STATE_DIR/../state.json"

echo "=========================================="
echo "  v2 TradingOS 启动"
echo "=========================================="

# 杀掉旧进程
lsof -ti :9000 | xargs kill -9 2>/dev/null || true
sleep 1

# ── 1. Data Service ────────────────────────────────────
echo "[1/3] 启动 Data Service..."
python3 "$SCRIPT_DIR/data_service.py" &
DATA_PID=$!
echo "  PID: $DATA_PID"

# ── 2. Execution Engine ────────────────────────────────
echo "[2/3] 启动 Execution Engine..."
python3 "$SCRIPT_DIR/execution_engine.py" &
EXEC_PID=$!
echo "  PID: $EXEC_PID"

# ── 3. API Server ─────────────────────────────────────
echo "[3/3] 启动 API Server (port 9000)..."
cd "$SCRIPT_DIR"
python3 -m uvicorn api_server:app --port 9000 --host 0.0.0.0 &
API_PID=$!
echo "  PID: $API_PID"

sleep 3

echo ""
echo "=========================================="
echo "  ✅ v2 全部启动"
echo "  Data Service:  $DATA_PID"
echo "  Exec Engine:   $EXEC_PID"
echo "  API Server:   $API_PID"
echo "=========================================="
echo ""
echo "验证接口："
echo "  curl http://localhost:9000/health"
echo "  curl http://localhost:9000/signal"
echo "  curl http://localhost:9000/market"
echo ""
curl -s http://localhost:9000/health
echo ""
