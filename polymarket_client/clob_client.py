"""CLOB REST客户端封装"""
import time
import requests
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

from polymarket_client.errors import PolymarketAPIError, RateLimitError, PriceValidationError


@dataclass
class OrderBookLevel:
    """订单簿层级"""
    price: float
    size: float


@dataclass
class PriceData:
    """价格数据"""
    token_id: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    timestamp: float = 0.0
    source: str = "rest"  # "rest" or "websocket"


class ClobRestClient:
    """CLOB REST API客户端"""
    
    BASE_URL = "https://clob.polymarket.com"
    
    def __init__(self, rate_limit_delay: float = 0.1):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "TradingOS/2.0 (CLOB Client)",
            "Accept": "application/json"
        })
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
    
    def _rate_limit(self):
        """简单的速率限制"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def get_midpoint_price(self, token_id: str) -> Optional[float]:
        """
        获取token的中点价格
        
        Args:
            token_id: CLOB token ID
            
        Returns:
            中点价格（0-1之间），失败返回None
        """
        self._rate_limit()
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/midpoint",
                params={"token_id": token_id},
                timeout=5
            )
            
            if response.status_code == 429:
                raise RateLimitError("CLOB API rate limit exceeded")
            
            response.raise_for_status()
            
            data = response.json()
            
            # 检查返回格式
            if isinstance(data, dict) and "midpoint" in data:
                midpoint = float(data["midpoint"])
                return midpoint
            elif isinstance(data, (int, float)):
                return float(data)
            else:
                print(f"[CLOB] Unexpected response format for midpoint: {data}")
                return None
                
        except requests.RequestException as e:
            # 404表示没有订单簿，这是正常情况
            if isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                print(f"[CLOB] No orderbook for token {token_id[:20]}...")
                return None
            else:
                print(f"[CLOB] Failed to get midpoint for {token_id[:20]}...: {e}")
                return None
    
    def get_price(self, token_id: str, side: str = "BUY") -> Optional[Dict]:
        """
        获取指定方向的最佳价格
        
        Args:
            token_id: CLOB token ID
            side: "BUY" 或 "SELL"
            
        Returns:
            {"price": float, "size": float} 或 None
        """
        self._rate_limit()
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/price",
                params={"token_id": token_id, "side": side},
                timeout=5
            )
            
            if response.status_code == 429:
                raise RateLimitError("CLOB API rate limit exceeded")
            
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, dict) and "price" in data:
                return {
                    "price": float(data["price"]),
                    "size": float(data.get("size", 0))
                }
            else:
                print(f"[CLOB] Unexpected response format for price: {data}")
                return None
                
        except requests.RequestException as e:
            if isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                return None
            else:
                print(f"[CLOB] Failed to get price for {token_id[:20]}...: {e}")
                return None
    
    def get_bid_ask(self, token_id: str) -> Optional[Tuple[float, float]]:
        """
        获取买卖价
        
        Args:
            token_id: CLOB token ID
            
        Returns:
            (bid, ask) 元组，失败返回None
        """
        bid_data = self.get_price(token_id, "BUY")
        ask_data = self.get_price(token_id, "SELL")
        
        if bid_data and ask_data:
            return bid_data["price"], ask_data["price"]
        return None
    
    def get_full_price_data(self, token_id: str) -> Optional[PriceData]:
        """
        获取完整的价格数据
        
        Args:
            token_id: CLOB token ID
            
        Returns:
            PriceData对象，失败返回None
        """
        self._rate_limit()
        
        try:
            # 获取bid
            bid_data = self.get_price(token_id, "BUY")
            # 获取ask
            ask_data = self.get_price(token_id, "SELL")
            # 获取midpoint
            midpoint = self.get_midpoint_price(token_id)
            
            if not (bid_data or ask_data or midpoint):
                return None
            
            return PriceData(
                token_id=token_id,
                bid=bid_data["price"] if bid_data else None,
                ask=ask_data["price"] if ask_data else None,
                mid=midpoint,
                bid_size=bid_data["size"] if bid_data else None,
                ask_size=ask_data["size"] if ask_data else None,
                timestamp=time.time(),
                source="rest"
            )
                
        except Exception as e:
            print(f"[CLOB] Failed to get full price data for {token_id[:20]}...: {e}")
            return None
    
    def get_prices_for_pair(self, yes_token_id: str, no_token_id: str) -> Optional[Tuple[PriceData, PriceData]]:
        """
        获取YES/NO token对的价格数据
        
        Args:
            yes_token_id: YES token ID
            no_token_id: NO token ID
            
        Returns:
            (yes_price_data, no_price_data) 元组，失败返回None
        """
        yes_data = self.get_full_price_data(yes_token_id)
        no_data = self.get_full_price_data(no_token_id)
        
        if not yes_data or not no_data:
            return None
        
        # 验证价格合理性
        self._validate_price_pair(yes_data, no_data)
        
        return yes_data, no_data
    
    def _validate_price_pair(self, yes_data: PriceData, no_data: PriceData):
        """验证YES/NO价格对的合理性"""
        if yes_data.mid and no_data.mid:
            price_sum = yes_data.mid + no_data.mid
            
            # 检查价格和是否接近1.0
            if not (0.9 < price_sum < 1.1):
                raise PriceValidationError(
                    yes_data.token_id,
                    yes_data.mid,
                    f"Invalid price sum: {price_sum:.4f} (YES={yes_data.mid:.4f}, NO={no_data.mid:.4f})"
                )
            
            # 检查价格范围
            if not (0 < yes_data.mid < 1) or not (0 < no_data.mid < 1):
                raise PriceValidationError(
                    yes_data.token_id,
                    yes_data.mid,
                    f"Price out of range: YES={yes_data.mid:.4f}, NO={no_data.mid:.4f}"
                )
    
    def batch_get_midpoints(self, token_ids: List[str]) -> Dict[str, Optional[float]]:
        """
        批量获取中点价格
        
        Args:
            token_ids: token ID列表
            
        Returns:
            字典 {token_id: midpoint或None}
        """
        results = {}
        
        for token_id in token_ids:
            try:
                midpoint = self.get_midpoint_price(token_id)
                results[token_id] = midpoint
                # 小延迟避免速率限制
                time.sleep(0.05)
            except Exception as e:
                print(f"[CLOB] Failed to get midpoint for {token_id[:20]}... in batch: {e}")
                results[token_id] = None
        
        return results
    
    def health_check(self) -> bool:
        """检查CLOB API健康状态"""
        try:
            response = self.session.get(f"{self.BASE_URL}/health", timeout=5)
            return response.status_code == 200
        except:
            return False


# 工厂函数
def create_clob_client() -> ClobRestClient:
    """创建CLOB客户端"""
    return ClobRestClient()


# 单例实例（可选）
_default_clob_client = None

def get_clob_client() -> ClobRestClient:
    """获取CLOB客户端单例实例"""
    global _default_clob_client
    if _default_clob_client is None:
        _default_clob_client = ClobRestClient()
    return _default_clob_client


# 兼容层：包装polymarket-pandas（如果可用）
try:
    from polymarket_pandas import PolymarketPandas
    
    class PolymarketPandasClobAdapter(ClobRestClient):
        """polymarket-pandas CLOB适配器"""
        
        def __init__(self):
            super().__init__()
            self.pandas_client = PolymarketPandas()
        
        def get_midpoint_price(self, token_id: str) -> Optional[float]:
            """使用polymarket-pandas获取中点价格"""
            try:
                # polymarket-pandas可能直接提供此方法
                if hasattr(self.pandas_client, 'get_midpoint_price'):
                    return self.pandas_client.get_midpoint_price(token_id)
                else:
                    # 回退到原生实现
                    return super().get_midpoint_price(token_id)
            except Exception as e:
                print(f"[CLOB][pandas] Failed to get midpoint: {e}")
                return super().get_midpoint_price(token_id)
    
    # 默认使用适配器
    DefaultClobClient = PolymarketPandasClobAdapter
    
except ImportError:
    # 如果没有polymarket-pandas，使用原生实现
    DefaultClobClient = ClobRestClient