"""CLOB WebSocket客户端（实时价格流）"""
import asyncio
import json
import time
from typing import Optional, Dict, List, Set, Callable, AsyncIterator
import websockets
import logging

from polymarket_client.errors import WebSocketError
from polymarket_client.clob_client import PriceData


logger = logging.getLogger(__name__)


class ClobWebSocketClient:
    """CLOB WebSocket客户端，支持实时价格订阅"""
    
    WS_URL = "wss://clob.polymarket.com/ws"
    
    def __init__(self, reconnect_interval: float = 5.0):
        self.ws = None
        self.connected = False
        self.reconnect_interval = reconnect_interval
        self.subscriptions: Set[str] = set()
        self.message_handlers: List[Callable] = []
        self.price_cache: Dict[str, PriceData] = {}
        self._stop_event = asyncio.Event()
        self._reconnect_task = None
    
    async def connect(self):
        """连接到WebSocket服务器"""
        if self.connected and self.ws:
            return
        
        try:
            logger.info(f"Connecting to {self.WS_URL}")
            self.ws = await websockets.connect(self.WS_URL, ping_interval=30, ping_timeout=10)
            self.connected = True
            logger.info("WebSocket connected")
            
            # 重新订阅之前订阅的token
            if self.subscriptions:
                await self._resubscribe()
                
        except Exception as e:
            self.connected = False
            raise WebSocketError(f"Failed to connect to WebSocket: {e}")
    
    async def disconnect(self):
        """断开连接"""
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
            finally:
                self.ws = None
                self.connected = False
        
        self._stop_event.set()
        
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
    
    async def subscribe(self, token_ids: List[str]):
        """
        订阅token的价格更新
        
        Args:
            token_ids: 要订阅的token ID列表
        """
        if not token_ids:
            return
        
        await self.ensure_connected()
        
        # 构建订阅消息
        subscribe_msg = {
            "type": "subscribe",
            "channel": "orderbook",
            "token_ids": token_ids
        }
        
        try:
            await self.ws.send(json.dumps(subscribe_msg))
            
            # 添加到订阅集合
            for token_id in token_ids:
                self.subscriptions.add(token_id)
            
            logger.info(f"Subscribed to {len(token_ids)} token(s)")
            
        except Exception as e:
            raise WebSocketError(f"Failed to subscribe: {e}")
    
    async def unsubscribe(self, token_ids: List[str]):
        """取消订阅"""
        if not token_ids or not self.connected:
            return
        
        unsubscribe_msg = {
            "type": "unsubscribe",
            "channel": "orderbook",
            "token_ids": token_ids
        }
        
        try:
            await self.ws.send(json.dumps(unsubscribe_msg))
            
            # 从订阅集合中移除
            for token_id in token_ids:
                self.subscriptions.discard(token_id)
            
            logger.info(f"Unsubscribed from {len(token_ids)} token(s)")
            
        except Exception as e:
            raise WebSocketError(f"Failed to unsubscribe: {e}")
    
    async def ensure_connected(self):
        """确保连接正常，如断开则自动重连"""
        if not self.connected or self.ws is None:
            await self.connect()
    
    async def _resubscribe(self):
        """重新订阅之前订阅的token"""
        if not self.subscriptions:
            return
        
        token_ids = list(self.subscriptions)
        await self.subscribe(token_ids)
    
    async def listen(self) -> AsyncIterator[PriceData]:
        """
        监听价格更新
        
        Yields:
            PriceData对象
        """
        await self.ensure_connected()
        
        while not self._stop_event.is_set():
            try:
                # 接收消息
                message = await self.ws.recv()
                
                # 解析消息
                data = json.loads(message)
                
                # 处理不同类型消息
                if data.get("type") == "orderbook_update":
                    price_data = self._parse_orderbook_update(data)
                    if price_data:
                        # 更新缓存
                        self.price_cache[price_data.token_id] = price_data
                        yield price_data
                
                elif data.get("type") == "heartbeat":
                    # 心跳消息，保持连接活跃
                    pass
                
                elif data.get("type") == "error":
                    logger.error(f"WebSocket error: {data.get('message')}")
                
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                self.connected = False
                
                # 触发重连
                await self._handle_reconnect()
                break
                
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}")
                await asyncio.sleep(1)  # 短暂延迟避免CPU爆满
    
    def _parse_orderbook_update(self, data: Dict) -> Optional[PriceData]:
        """解析订单簿更新消息"""
        try:
            token_id = data.get("token_id")
            if not token_id:
                return None
            
            # 提取最佳买卖价
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            
            # 计算中点价格
            mid = None
            if best_bid and best_ask:
                mid = (best_bid + best_ask) / 2
            
            # 提取买卖量
            bid_size = float(bids[0][1]) if bids else None
            ask_size = float(asks[0][1]) if asks else None
            
            return PriceData(
                token_id=token_id,
                bid=best_bid,
                ask=best_ask,
                mid=mid,
                bid_size=bid_size,
                ask_size=ask_size,
                timestamp=time.time(),
                source="websocket"
            )
            
        except Exception as e:
            logger.error(f"Failed to parse orderbook update: {e}")
            return None
    
    async def _handle_reconnect(self):
        """处理重连逻辑"""
        logger.info("Starting reconnect...")
        
        while not self._stop_event.is_set():
            try:
                await self.connect()
                logger.info("Reconnected successfully")
                break
            except Exception as e:
                logger.warning(f"Reconnect failed: {e}, retrying in {self.reconnect_interval}s")
                await asyncio.sleep(self.reconnect_interval)
    
    def get_cached_price(self, token_id: str) -> Optional[PriceData]:
        """获取缓存的价格数据"""
        return self.price_cache.get(token_id)
    
    async def get_price_stream(self, token_ids: List[str]) -> AsyncIterator[PriceData]:
        """
        获取指定token的价格流
        
        Args:
            token_ids: 要订阅的token ID列表
            
        Yields:
            PriceData对象
        """
        # 订阅token
        await self.subscribe(token_ids)
        
        # 开始监听
        async for price_data in self.listen():
            if price_data.token_id in token_ids:
                yield price_data
    
    async def start_background_listener(self):
        """启动后台监听任务"""
        if self._reconnect_task:
            return
        
        self._stop_event.clear()
        
        async def _background_listener():
            while not self._stop_event.is_set():
                try:
                    async for price_data in self.listen():
                        # 调用注册的处理器
                        for handler in self.message_handlers:
                            try:
                                handler(price_data)
                            except Exception as e:
                                logger.error(f"Error in message handler: {e}")
                except Exception as e:
                    logger.error(f"Background listener error: {e}")
                    await asyncio.sleep(self.reconnect_interval)
        
        self._reconnect_task = asyncio.create_task(_background_listener())
    
    def register_handler(self, handler: Callable):
        """注册消息处理器"""
        self.message_handlers.append(handler)
    
    def unregister_handler(self, handler: Callable):
        """注销消息处理器"""
        if handler in self.message_handlers:
            self.message_handlers.remove(handler)


# 单例实例（可选）
_default_ws_client = None

async def get_ws_client() -> ClobWebSocketClient:
    """获取WebSocket客户端单例实例"""
    global _default_ws_client
    if _default_ws_client is None:
        _default_ws_client = ClobWebSocketClient()
        await _default_ws_client.connect()
    return _default_ws_client