# v2 三层架构

## 结构

```
v2/
├── data_service.py      # 数据层：拉 BTC + Polymarket → state.json
├── execution_engine.py  # 策略层：读行情 → 算信号/决策 → state.json
├── api_server.py        # 接口层：读 state.json → JSON API
├── start.sh             # 一键启动
└── requirements_v2.txt  # 依赖
```

## state.json 字段约定

| 字段 | 谁写 | 说明 |
|------|------|------|
| `btc` | data_service | BTC 价格 |
| `btc_source` | data_service | 数据源 |
| `pm_yes` | data_service | Polymarket YES 价格 |
| `pm_no` | data_service | Polymarket NO 价格 |
| `pm_spread` | data_service | 买卖价差 |
| `candles_1m` | data_service | K线（1分钟） |
| `candles_5m` | data_service | K线（5分钟） |
| `candles_15m` | data_service | K线（15分钟） |
| `data_ts` | data_service | 数据时间戳 |
| `signal` | execution_engine | {bias, confidence, ...} |
| `decision` | execution_engine | {action, edge, reason, ...} |

## 启动步骤

```bash
# 1. 安装依赖
pip install -r requirements_v2.txt

# 2. 启动三层（三个终端，或后台）
python data_service.py        # 终端1
python execution_engine.py    # 终端2
uvicorn api_server:app --port 9000  # 终端3

# 3. 验证
curl http://localhost:9000/health
curl http://localhost:9000/signal
curl http://localhost:9000/decision
```

## 并行上线流程

1. 现有 simple_main.py 继续跑（不动）
2. v2 用 port 9000 启动
3. Dashboard 暂时不改，继续用 8765
4. v2 验证 30 分钟不崩后，把 Dashboard API 地址改为 `http://localhost:9000`
5. 停掉 simple_main.py

## 验证标准

- data_service: BTC 价格持续刷新（每 5s）
- execution_engine: 每 10s 打印信号和决策
- api_server: `curl http://localhost:9000/health` 秒返回
