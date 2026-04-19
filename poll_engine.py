"""
独立线程轮询引擎 - 每100ms轮询CLOB订单簿
不要与 asyncio 混用，纯线程实现
"""
import threading
import time
import requests
import json
from typing import Dict, Optional
import logging

# 全局缓存（所有模块共享）
price_cache = {}  # token_id -> {"bid": float, "ask": float, "ts": float}
_price_cache_lock = threading.Lock()  # 只用于写（读不需要）

# 日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PollEngine")

class PollEngine:
    def __init__(self, poll_interval: float = 0.1):
        """
        :param poll_interval: 轮询间隔（秒），默认100ms
        """
        self.poll_interval = poll_interval
        self._active_tokens = set()  # 当前活跃token_ids
        self._stop = False
        self._thread = None
        self._session = None  # requests Session（线程安全）
    
    def update_tokens(self, token_ids):
        """动态更新要轮询的token列表（线程安全）"""
        with _price_cache_lock:
            self._active_tokens = set(token_ids)
        logger.info(f"PollEngine updated tokens: {len(token_ids)} tokens")
    
    def start(self):
        """启动轮询线程（只调用一次）"""
        if self._thread is not None and self._thread.is_alive():
            return
        
        self._stop = False
        self._session = requests.Session()
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="PollEngine",
            daemon=True  # 主线程退出时自动结束
        )
        self._thread.start()
        logger.info(f"PollEngine started (interval={self.poll_interval*1000:.0f}ms)")
    
    def stop(self):
        """停止轮询线程"""
        self._stop = True
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._session:
            self._session.close()
            self._session = None
        logger.info("PollEngine stopped")
    
    def _poll_loop(self):
        """轮询主循环（在独立线程中运行）"""
        logger.info("PollEngine loop started")
        
        while not self._stop:
            loop_start = time.time()
            
            # 复制当前token列表（避免迭代中修改）
            with _price_cache_lock:
                tokens_to_poll = list(self._active_tokens)
            
            if not tokens_to_poll:
                # 没有活跃token，等待并继续
                time.sleep(self.poll_interval)
                continue
            
            # 轮询每个token
            for token_id in tokens_to_poll:
                try:
                    resp = self._session.get(
                        f"https://clob.polymarket.com/book?token_id={token_id}",
                        timeout=0.2,  # 200ms超时
                        headers={"User-Agent": "PollEngine/1.0"}
                    )
                    
                    if resp.status_code == 200:
                        book = resp.json()
                        bid = float(book["bids"][0]["price"]) if book.get("bids") else 0.0
                        ask = float(book["asks"][0]["price"]) if book.get("asks") else 0.0
                        ts = time.time()
                        
                        # 更新全局缓存（加锁写）
                        with _price_cache_lock:
                            price_cache[token_id] = {
                                "bid": bid,
                                "ask": ask,
                                "ts": ts,
                                "source": "poll"
                            }
                        
                        # 调试日志（控制频率）
                        if loop_start % 10 < 0.1:  # 每10秒打一次
                            logger.debug(f"Polled {token_id[:8]}... bid={bid:.4f} ask={ask:.4f}")
                            
                except requests.exceptions.Timeout:
                    # 单次超时不处理
                    continue
                except Exception as e:
                    # 其他错误记录但不中断
                    if loop_start % 30 < 0.1:  # 每30秒打一次错误
                        logger.warning(f"Poll error for {token_id[:8]}: {e}")
                    continue
            
            # 精确控制轮询间隔
            elapsed = time.time() - loop_start
            sleep_time = self.poll_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            # 如果超时，立即开始下一轮
        
        logger.info("PollEngine loop stopped")
    
    @staticmethod
    def get_price(token_id: str):
        """获取最新价格（任何线程/进程可调用，读不需要锁）"""
        # 注意：Python dict 读是原子的（GIL保护）
        entry = price_cache.get(token_id)
        if entry:
            return entry["bid"], entry["ask"], entry["ts"]
        return 0.0, 0.0, 0.0
    
    @staticmethod
    def get_all_prices():
        """获取所有token价格（调试用）"""
        return price_cache.copy()

def start_poll_engine(tokens=None, interval=0.1):
    """快速启动函数"""
    engine = PollEngine(poll_interval=interval)
    if tokens:
        engine.update_tokens(tokens)
    engine.start()
    return engine

if __name__ == "__main__":
    # 测试 - 所有print语句确保在一行内
    test_tokens = [
        "2600000000000000000000000000000000000000000000000000000000000000",
        "4400000000000000000000000000000000000000000000000000000000000000"
    ]
    engine = start_poll_engine(test_tokens)
    
    try:
        for i in range(10):
            print(f"
--- Sample {i+1} ---")
            for token in test_tokens:
                bid, ask, ts = PollEngine.get_price(token)
                print(f"{token[:8]}: bid={bid:.4f}, ask={ask:.4f}, age={time.time()-ts:.3f}s")
            time.sleep(0.5)
    finally:
        engine.stop()
