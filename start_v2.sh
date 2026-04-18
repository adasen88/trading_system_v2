#!/bin/bash
# start_v2.sh - 启动新版数据服务（端口9001）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "${LOG_DIR}"

echo "=========================================="
echo " v2 TradingOS - 新版数据服务启动"
echo "=========================================="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 未安装"
    exit 1
fi

# 检查依赖
echo "[1/3] 检查依赖..."
if ! python3 -c "import websockets, requests" 2>/dev/null; then
    echo "⚠️  缺少依赖，正在安装..."
    pip3 install --quiet websockets requests
fi

# 启动新版数据服务
echo "[2/3] 启动 Data Service V2..."
python3 "${SCRIPT_DIR}/data_service_v2.py" > "${LOG_DIR}/data_service_v2.log" 2>&1 &
DATA_PID=$!
echo " PID: $DATA_PID"

# 检查进程是否启动成功
sleep 2
if ! kill -0 $DATA_PID 2>/dev/null; then
    echo "❌ Data Service V2 启动失败"
    cat "${LOG_DIR}/data_service_v2.log"
    exit 1
fi

# 启动API Server（端口9001）
echo "[3/3] 启动 API Server V2 (port 9001)..."
# 复制并修改api_server.py以使用端口9001
if [ ! -f "${SCRIPT_DIR}/api_server_v2.py" ]; then
    cp "${SCRIPT_DIR}/api_server.py" "${SCRIPT_DIR}/api_server_v2.py"
    sed -i '' 's/port=9000/port=9001/g' "${SCRIPT_DIR}/api_server_v2.py" 2>/dev/null || \
    sed -i 's/port=9000/port=9001/g' "${SCRIPT_DIR}/api_server_v2.py"
fi

# 使用绝对路径启动uvicorn（macOS兼容）
UVICORN_PATH="/Library/Frameworks/Python.framework/Versions/3.11/bin/uvicorn"
if [ ! -f "$UVICORN_PATH" ]; then
    UVICORN_PATH="python3 -m uvicorn"
fi

$UVICORN_PATH api_server_v2:app --port 9001 --host 0.0.0.0 > "${LOG_DIR}/api_server_v2.log" 2>&1 &
API_PID=$!
echo " PID: $API_PID"

# 检查API Server是否启动成功
sleep 3
if ! curl -s http://localhost:9001/health >/dev/null 2>&1; then
    echo "⚠️  API Server V2 可能启动失败，检查日志"
    cat "${LOG_DIR}/api_server_v2.log" | tail -20
fi

echo "=========================================="
echo " ✅ v2 新版服务全部启动"
echo " Data Service V2: $DATA_PID"
echo " API Server V2: $API_PID (port 9001)"
echo "=========================================="

echo ""
echo "验证接口："
echo " curl http://localhost:9001/health"
echo " curl http://localhost:9001/signal"
echo " curl http://localhost:9001/market"
echo ""
echo "监控日志："
echo " tail -f ${LOG_DIR}/data_service_v2.log"
echo " tail -f ${LOG_DIR}/api_server_v2.log"
echo ""

# 保存PID文件
echo "$DATA_PID" > "${SCRIPT_DIR}/.data_service_v2.pid"
echo "$API_PID" > "${SCRIPT_DIR}/.api_server_v2.pid"

# 等待信号
trap 'echo "收到停止信号"; kill $DATA_PID $API_PID 2>/dev/null; exit 0' SIGINT SIGTERM
wait