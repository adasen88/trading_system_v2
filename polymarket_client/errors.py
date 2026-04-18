"""Polymarket API错误定义"""


class PolymarketError(Exception):
    """Polymarket相关错误的基类"""
    pass


class PolymarketAPIError(PolymarketError):
    """API调用失败"""
    pass


class RateLimitError(PolymarketAPIError):
    """API速率限制"""
    pass


class MarketNotTradableError(PolymarketError):
    """市场不可交易"""
    
    def __init__(self, slug: str, reason: str):
        self.slug = slug
        self.reason = reason
        super().__init__(f"Market {slug} is not tradable: {reason}")


class TokenResolutionError(PolymarketError):
    """Token解析失败"""
    
    def __init__(self, market_slug: str, issue: str):
        self.market_slug = market_slug
        self.issue = issue
        super().__init__(f"Failed to resolve tokens for {market_slug}: {issue}")


class PriceStreamError(PolymarketError):
    """价格流错误"""
    pass


class WebSocketError(PriceStreamError):
    """WebSocket连接错误"""
    pass


class OrderBookError(PriceStreamError):
    """订单簿数据错误"""
    pass


class PriceValidationError(PriceStreamError):
    """价格验证失败"""
    
    def __init__(self, token_id: str, price: float, issue: str):
        self.token_id = token_id
        self.price = price
        self.issue = issue
        super().__init__(f"Price validation failed for {token_id[:20]}...: {issue} (price={price})")


# 错误处理工具
def should_retry(error: Exception) -> bool:
    """判断错误是否应该重试"""
    if isinstance(error, RateLimitError):
        # 速率限制需要等待后重试
        return True
    elif isinstance(error, (PolymarketAPIError, WebSocketError)):
        # 网络错误通常可以重试
        return True
    elif isinstance(error, (MarketNotTradableError, TokenResolutionError, PriceValidationError)):
        # 业务逻辑错误不应重试
        return False
    else:
        # 其他未知错误
        return False


def get_retry_delay(error: Exception, attempt: int) -> float:
    """获取重试延迟时间（指数退避）"""
    base_delay = 1.0
    
    if isinstance(error, RateLimitError):
        # 速率限制需要较长延迟
        base_delay = 5.0
    
    # 指数退避：1s, 2s, 4s, 8s, 16s...
    return min(base_delay * (2 ** (attempt - 1)), 60.0)


def format_error_for_log(error: Exception) -> dict:
    """格式化错误信息用于日志记录"""
    error_info = {
        "type": type(error).__name__,
        "message": str(error),
        "timestamp": time.time() if 'time' in globals() else None,
    }
    
    # 添加特定错误类型的额外信息
    if isinstance(error, MarketNotTradableError):
        error_info["slug"] = error.slug
        error_info["reason"] = error.reason
    elif isinstance(error, TokenResolutionError):
        error_info["market_slug"] = error.market_slug
        error_info["issue"] = error.issue
    elif isinstance(error, PriceValidationError):
        error_info["token_id"] = error.token_id
        error_info["price"] = error.price
        error_info["issue"] = error.issue
    
    return error_info


# 尝试导入time模块用于timestamp
try:
    import time
except ImportError:
    pass