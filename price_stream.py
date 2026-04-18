"""统一价格流服务（WebSocket优先，REST fallback）"""
import asyncio
import time
from typing import Optional, Dict, List, Tuple, AsyncIterator
from dataclasses import dataclass
import logging

from polymarket_client.errors import PriceStreamError, WebSocketError
from polymarket_client.clob_client import ClobRestClient, PriceData, DefaultClobClient
from polymarket_client.clob_ws import ClobWebSocketClient


logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """价格流配置"""
    use_websocket: bool = True  # 是否启用WebSocket
    websocket_timeout: float = 10.0  # WebSocket连接超时
    rest_fallback_interval: float = 3.0  # REST轮询间隔
    reconnect_interval: float = 5.0  # 重连间隔
    max_retries: int = 3  # 最大重试次数
    cache_ttl: float = 60.0  # 缓存TTL（秒）


class PriceStream:
    """统一价格流服务"""
    
    def __init__(self, config: Optional[StreamConfig] = None):
        self.config = config or StreamConfig()
        self.ws_client: Optional[ClobWebSocketClient] = None
        self.rest_client = DefaultClobClient()
        self.price_cache: Dict[str, Tuple[PriceData, float]] = {}
        self._streaming_tasks: Dict[str, asyncio.Task] = {}
        self._stop_event = asyncio.Event()
        
        # 状态
        self.websocket_available = False
        self.last_websocket_error: Optional[Exception] = None
        self.last_rest_error: Optional[Exception] = None
    
    async def initialize(self):
        """初始化价格流服务"""
        if self.config.use_websocket:
            try:
                self.ws_client = ClobWebSocketClient(
                    reconnect_interval=self.config.reconnect_interval
                )
                await self.ws_client.connect()
                self.websocket_available = True
                logger.info("WebSocket initialized successfully")
            except Exception as e:
                self.websocket_available = False
                self.last_websocket_error = e
                logger.warning(f"WebSocket initialization failed, will use REST fallback: {e}")
    
    async def shutdown(self):
        """关闭价格流服务"""
        self._stop_event.set()
        
        # 停止所有流任务
        for task in self._streaming_tasks.values():
            task.cancel()
        
        # 关闭WebSocket
        if self.ws_client:
            await self.ws_client.disconnect()
        
        await asyncio.sleep(0.1)  # 等待任务清理
    
    def get_cached_price(self, token_id: str) -> Optional[PriceData]:
        """获取缓存的价格数据"""
        if token_id not in self.price_cache:
            return None
        
        price_data, timestamp = self.price_cache[token_id]
        
        # 检查缓存是否过期
        if time.time() - timestamp > self.config.cache_ttl:
            del self.price_cache[token_id]
            return None
        
        return price_data
    
    async def get_price(self, token_id: str) -> Optional[PriceData]:
        """
        获取单个token的价格（首选WebSocket，失败时使用REST）
        
        Args:
            token_id: token ID
            
        Returns:
            PriceData对象，失败返回None
        """
        # 先检查缓存
        cached = self.get_cached_price(token_id)
        if cached:
            return cached
        
        # 优先使用WebSocket
        if self.websocket_available and self.ws_client:
            try:
                price_data = self.ws_client.get_cached_price(token_id)
                if price_data:
                    self._update_cache(token_id, price_data)
                    return price_data
            except Exception as e:
                logger.warning(f"WebSocket price fetch failed: {e}")
                self.websocket_available = False
        
        # 回退到REST
        try:
            price_data = self.rest_client.get_full_price_data(token_id)
            if price_data:
                self._update_cache(token_id, price_data)
                return price_data
        except Exception as e:
            logger.error(f"REST price fetch failed: {e}")
            self.last_rest_error = e
        
        return None
    
    async def get_prices_for_pair(self, yes_token_id: str, no_token_id: str) -> Optional[Tuple[PriceData, PriceData]]:
        """
        获取YES/NO token对的价格
        
        Args:
            yes_token_id: YES token ID
            no_token_id: NO token ID
            
        Returns:
            (yes_price_data, no_price_data) 元组，失败返回None
        """
        # 尝试批量获取
        token_ids = [yes_token_id, no_token_id]
        
        # 检查缓存
        cached_prices = []
        for token_id in token_ids:
            cached = self.get_cached_price(token_id)
            if cached:
                cached_prices.append(cached)
            else:
                cached_prices.append(None)
        
        # 如果缓存中都有，直接返回
        if cached_prices[0] and cached_prices[1]:
            return cached_prices[0], cached_prices[1]
        
        # 优先使用WebSocket
        if self.websocket_available and self.ws_client:
            try:
                # 订阅这两个token
                await self.ws_client.subscribe(token_ids)
                
                # 等待WebSocket更新（超时机制）
                timeout = self.config.websocket_timeout
                start_time = time.time()
                
                while time.time() - start_time < timeout:
                    yes_data = self.ws_client.get_cached_price(yes_token_id)
                    no_data = self.ws_client.get_cached_price(no_token_id)
                    
                    if yes_data and no_data:
                        # 验证价格对
                        try:
                            self.rest_client._validate_price_pair(yes_data, no_data)
                            self._update_cache(yes_token_id, yes_data)
                            self._update_cache(no_token_id, no_data)
                            return yes_data, no_data
                        except Exception as e:
                            logger.warning(f"Price pair validation failed: {e}")
                            break
                    
                    await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"WebSocket pair fetch failed: {e}")
                self.websocket_available = False
        
        # 回退到REST批量获取
        try:
            price_pair = self.rest_client.get_prices_for_pair(yes_token_id, no_token_id)
            if price_pair:
                yes_data, no_data = price_pair
                self._update_cache(yes_token_id, yes_data)
                self._update_cache(no_token_id, no_data)
                return price_pair
        except Exception as e:
            logger.error(f"REST pair fetch failed: {e}")
            self.last_rest_error = e
        
        # 尝试分别获取
        try:
            yes_data = await self.get_price(yes_token_id)
            no_data = await self.get_price(no_token_id)
            
            if yes_data and no_data:
                # 验证价格对
                try:
                    self.rest_client._validate_price_pair(yes_data, no_data)
                    return yes_data, no_data
                except Exception as e:
                    logger.warning(f"Price pair validation failed: {e}")
        except Exception as e:
            logger.error(f"Individual price fetch failed: {e}")
        
        return None
    
    async def stream_prices(self, token_ids: List[str]) -> AsyncIterator[PriceData]:
        """
        流式获取价格更新
        
        Args:
            token_ids: 要订阅的token ID列表
            
        Yields:
            PriceData对象
        """
        if not token_ids:
            return
        
        # 优先使用WebSocket流
        if self.config.use_websocket:
            for attempt in range(self.config.max_retries):
                try:
                    if not self.ws_client or not self.websocket_available:
                        await self.initialize()
                    
                    if self.ws_client:
                        async for price_data in self.ws_client.get_price_stream(token_ids):
                            self._update_cache(price_data.token_id, price_data)
                            yield price_data
                        return
                except Exception as e:
                    logger.warning(f"WebSocket stream attempt {attempt+1} failed: {e}")
                    self.websocket_available = False
                    
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(self.config.reconnect_interval)
                    else:
                        logger.warning("All WebSocket attempts failed, falling back to REST polling")
                        break
        
        # 回退到REST轮询
        logger.info(f"Starting REST polling for {len(token_ids)} token(s)")
        
        while not self._stop_event.is_set():
            try:
                for token_id in token_ids:
                    price_data = await self.get_price(token_id)
                    if price_data:
                        yield price_data
                
                await asyncio.sleep(self.config.rest_fallback_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"REST polling error: {e}")
                await asyncio.sleep(self.config.reconnect_interval)
    
    async def start_streaming_task(self, token_ids: List[str], callback):
        """
        启动后台价格流任务
        
        Args:
            token_ids: 要订阅的token ID列表
            callback: 价格更新回调函数
        """
        if not token_ids:
            return
        
        task_id = ",".join(sorted(token_ids))
        
        if task_id in self._streaming_tasks:
            # 任务已存在
            return
        
        async def _streaming_task():
            try:
                async for price_data in self.stream_prices(token_ids):
                    try:
                        callback(price_data)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Streaming task error: {e}")
            finally:
                self._streaming_tasks.pop(task_id, None)
        
        task = asyncio.create_task(_streaming_task())
        self._streaming_tasks[task_id] = task
        
        return task_id
    
    def stop_streaming_task(self, task_id: str):
        """停止指定流任务"""
        if task_id in self._streaming_tasks:
            task = self._streaming_tasks[task_id]
            task.cancel()
            del self._streaming_tasks[task_id]
    
    def _update_cache(self, token_id: str, price_data: PriceData):
        """更新价格缓存"""
        self.price_cache[token_id] = (price_data, time.time())
    
    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "websocket_available": self.websocket_available,
            "cache_size": len(self.price_cache),
            "active_streams": len(self._streaming_tasks),
            "last_websocket_error": str(self.last_websocket_error) if self.last_websocket_error else None,
            "last_rest_error": str(self.last_rest_error) if self.last_rest_error else None,
            "config": {
                "use_websocket": self.config.use_websocket,
                "rest_fallback_interval": self.config.rest_fallback_interval,
                "cache_ttl": self.config.cache_ttl
            }
        }


# 单例实例
_default_price_stream = None

async def get_price_stream(config: Optional[StreamConfig] = None) -> PriceStream:
    """获取价格流服务单例实例"""
    global _default_price_stream
    if _default_price_stream is None:
        _default_price_stream = PriceStream(config)
        await _default_price_stream.initialize()
    return _default_price_stream