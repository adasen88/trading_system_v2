"""新版数据服务 - 三层架构整合"""
import asyncio
import json
import os
import time
import math
from typing import Dict, Optional, Tuple
import requests
import logging

from market_discovery import get_market_discovery, TradableMarket
from price_stream import get_price_stream, PriceData, StreamConfig
from polymarket_client.errors import MarketNotTradableError, PriceStreamError


logger = logging.getLogger(__name__)


class DataServiceV2:
    """新版数据服务（三层架构）"""
    
    def __init__(self, state_file: str, port: int = 9001):
        self.state_file = state_file
        self.port = port
        
        # 服务组件
        self.market_discovery = get_market_discovery()
        self.price_stream = None
        
        # 状态
        self.current_market: Optional[TradableMarket] = None
        self.price_cache: Dict[str, PriceData] = {}
        self.last_btc_price = 0.0
        self.last_btc_source = ""
        
        # 配置
        self.btc_interval = 5  # 秒
        self.pm_interval = 2   # 秒
        self.hist_interval = 60  # 秒
        
        # 控制
        self._running = False
        self._tasks = []
    
    async def initialize(self):
        """初始化服务"""
        logger.info("Initializing DataServiceV2...")
        
        # 初始化价格流
        config = StreamConfig(
            use_websocket=True,
            websocket_timeout=10.0,
            rest_fallback_interval=3.0,
            reconnect_interval=5.0,
            cache_ttl=60.0
        )
        self.price_stream = await get_price_stream(config)
        
        logger.info("DataServiceV2 initialized")
    
    async def shutdown(self):
        """关闭服务"""
        logger.info("Shutting down DataServiceV2...")
        self._running = False
        
        # 停止价格流
        if self.price_stream:
            await self.price_stream.shutdown()
        
        # 取消所有任务
        for task in self._tasks:
            task.cancel()
        
        logger.info("DataServiceV2 shutdown complete")
    
    async def run(self):
        """主运行循环"""
        self._running = True
        
        # 初始化状态
        self._write_state({
            "btc": 0.0,
            "btc_source": None,
            "pm_yes": 0.0,
            "pm_no": 0.0,
            "pm_spread": 0.0,
            "candles_1m": [],
            "candles_5m": [],
            "candles_15m": [],
            "data_ts": None
        })
        
        logger.info("Fetching historical candles...")
        await self._fetch_historical_candles()
        
        # 启动异步任务
        self._tasks = [
            asyncio.create_task(self._btc_price_loop()),
            asyncio.create_task(self._polymarket_price_loop()),
            asyncio.create_task(self._historical_data_loop())
        ]
        
        logger.info(f"DataServiceV2 running on port {self.port}")
        
        try:
            # 等待所有任务完成
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()
    
    async def _btc_price_loop(self):
        """BTC价格轮询循环"""
        while self._running:
            try:
                price_data = await self._fetch_btc_price()
                if price_data["price"] > 0:
                    self.last_btc_price = price_data["price"]
                    self.last_btc_source = price_data["source"]
                    
                    self._merge_state({
                        "btc": self.last_btc_price,
                        "btc_source": self.last_btc_source,
                        "data_ts": time.time()
                    })
                    
                    logger.info(f"BTC=${self.last_btc_price:,.0f} ({self.last_btc_source})")
            except Exception as e:
                logger.error(f"BTC price fetch error: {e}")
            
            await asyncio.sleep(self.btc_interval)
    
    async def _polymarket_price_loop(self):
        """Polymarket价格循环"""
        while self._running:
            try:
                # 发现可交易市场
                market = await self._discover_and_select_market()
                
                if market:
                    # 获取价格
                    price_pair = await self._fetch_polymarket_prices(market)
                    
                    if price_pair:
                        yes_data, no_data = price_pair
                        
                        self._merge_state({
                            "pm_yes": yes_data.mid,
                            "pm_no": no_data.mid,
                            "pm_spread": abs(yes_data.mid - no_data.mid),
                            "data_ts": time.time()
                        })
                        
                        logger.info(f"PM YES={yes_data.mid:.4f} NO={no_data.mid:.4f}")
                    else:
                        # 价格获取失败，清除状态
                        self._merge_state({
                            "pm_yes": 0.0,
                            "pm_no": 0.0,
                            "pm_spread": 0.0,
                            "data_ts": time.time()
                        })
                        
                        logger.warning("Polymarket price unavailable")
                else:
                    # 没有可交易市场
                    self._merge_state({
                        "pm_yes": 0.0,
                        "pm_no": 0.0,
                        "pm_spread": 0.0,
                        "data_ts": time.time()
                    })
                    
                    logger.warning("No tradable market found")
            except Exception as e:
                logger.error(f"Polymarket price error: {e}")
                await asyncio.sleep(1)
            
            await asyncio.sleep(self.pm_interval)
    
    async def _historical_data_loop(self):
        """历史数据更新循环"""
        while self._running:
            try:
                await self._fetch_historical_candles()
            except Exception as e:
                logger.error(f"Historical data error: {e}")
            
            await asyncio.sleep(self.hist_interval)
    
    async def _discover_and_select_market(self) -> Optional[TradableMarket]:
        """发现并选择可交易市场"""
        try:
            # 使用市场发现服务
            market = self.market_discovery.get_best_tradable_market(lookback_windows=10)
            
            if market:
                if self.current_market is None or market.slug != self.current_market.slug:
                    logger.info(f"Selected tradable market: {market.slug} (ends in {market.expires_at - time.time():.0f}s)")
                    self.current_market = market
                
                return market
            else:
                if self.current_market:
                    logger.info("Lost current market, clearing selection")
                    self.current_market = None
                
                return None
                
        except Exception as e:
            logger.error(f"Market discovery error: {e}")
            return None
    
    async def _fetch_polymarket_prices(self, market: TradableMarket) -> Optional[Tuple[PriceData, PriceData]]:
        """获取Polymarket价格"""
        if not self.price_stream:
            return None
        
        try:
            # 使用价格流服务获取YES/NO价格对
            price_pair = await self.price_stream.get_prices_for_pair(
                market.yes_token_id,
                market.no_token_id
            )
            
            if price_pair:
                yes_data, no_data = price_pair
                
                logger.info(f"CLOB prices: YES={yes_data.mid:.4f} (bid={yes_data.bid:.4f}, ask={yes_data.ask:.4f}) "
                          f"NO={no_data.mid:.4f} (bid={no_data.bid:.4f}, ask={no_data.ask:.4f})")
                
                return price_pair
            else:
                logger.warning(f"No CLOB prices available for market {market.slug}")
                return None
                
        except Exception as e:
            logger.error(f"Price fetch error: {e}")
            return None
    
    async def _fetch_btc_price(self) -> Dict:
        """获取BTC价格"""
        price = 0.0
        source = ""
        
        try:
            # 尝试OKX
            r = requests.get("https://www.okx.com/api/v5/market/ticker", 
                           params={"instId": "BTC-USDT-SWAP"}, timeout=3)
            if r.status_code == 200:
                data = r.json()
                if data.get("code") == "0":
                    ticker = data.get("data", [{}])[0]
                    p = float(ticker.get("last", 0))
                    if p > 0:
                        price = p
                        source = "OKX"
        except:
            pass
        
        if price == 0:
            try:
                # 尝试Binance
                r = requests.get("https://api.binance.com/api/v3/ticker/price",
                               params={"symbol": "BTCUSDT"}, timeout=3)
                if r.status_code == 200:
                    data = r.json()
                    p = float(data.get("price", 0))
                    if p > 0:
                        price = p
                        source = "Binance"
            except:
                pass
        
        return {"price": price, "source": source}
    
    async def _fetch_historical_candles(self):
        """获取历史K线数据"""
        try:
            # 1分钟K线
            candles_1m = self._fetch_candles_from_binance("1m", 100)
            if candles_1m:
                self._merge_state({"candles_1m": candles_1m})
            
            # 5分钟K线
            candles_5m = self._fetch_candles_from_binance("5m", 100)
            if candles_5m:
                self._merge_state({"candles_5m": candles_5m})
            
            # 15分钟K线
            candles_15m = self._fetch_candles_from_binance("15m", 100)
            if candles_15m:
                self._merge_state({"candles_15m": candles_15m})
                
        except Exception as e:
            logger.error(f"Historical candles error: {e}")
    
    def _fetch_candles_from_binance(self, interval: str, limit: int = 100):
        """从Binance获取K线数据"""
        try:
            r = requests.get("https://api.binance.com/api/v3/klines",
                           params={"symbol": "BTCUSDT", "interval": interval, "limit": limit},
                           timeout=5)
            if r.status_code == 200:
                return [{
                    "ts": int(k[0]),
                    "o": float(k[1]),
                    "h": float(k[2]),
                    "l": float(k[3]),
                    "c": float(k[4]),
                    "v": float(k[5])
                } for k in r.json()]
        except Exception as e:
            logger.error(f"Binance klines error: {e}")
        
        return []
    
    def _write_state(self, data: Dict):
        """写入状态文件"""
        tmp_file = self.state_file + ".tmp"
        try:
            with open(tmp_file, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_file, self.state_file)
        except Exception as e:
            logger.error(f"State write error: {e}")
    
    def _merge_state(self, updates: Dict):
        """合并更新到状态文件"""
        try:
            # 读取现有状态
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    state = json.load(f)
            else:
                state = {}
            
            # 合并更新
            state.update(updates)
            
            # 写入新状态
            self._write_state(state)
            
        except Exception as e:
            logger.error(f"State merge error: {e}")
    
    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "running": self._running,
            "current_market": self.current_market.slug if self.current_market else None,
            "btc_price": self.last_btc_price,
            "btc_source": self.last_btc_source,
            "price_stream_status": self.price_stream.get_status() if self.price_stream else None,
            "port": self.port
        }


async def main():
    """主函数"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 确定状态文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    state_file = os.path.join(script_dir, "..", "state.json")
    
    # 创建并运行服务
    service = DataServiceV2(state_file, port=9001)
    
    try:
        await service.initialize()
        await service.run()
    except KeyboardInterrupt:
        logger.info("Shutting down by user request")
    finally:
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())