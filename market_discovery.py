"""市场发现和过滤模块"""
import time
import math
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

from polymarket_client.gamma_client import DefaultGammaClient, Market
from polymarket_client.errors import MarketNotTradableError


@dataclass
class TradableMarket:
    """可交易市场标识"""
    slug: str
    yes_token_id: str
    no_token_id: str
    expires_at: int  # Unix时间戳
    outcomes: List[str]  # ["Up", "Down"] 或 ["Yes", "No"]
    question: str
    volume: float
    liquidity: float


class MarketDiscovery:
    """市场发现服务"""
    
    def __init__(self, gamma_client=None):
        self.gamma_client = gamma_client or DefaultGammaClient()
        self.cache: Dict[str, Tuple[Market, float]] = {}
        self.cache_ttl = 300  # 5分钟缓存
    
    def discover_btc_5min_markets(self, lookback_windows: int = 10) -> List[TradableMarket]:
        """
        发现BTC 5分钟可交易市场
        
        通过搜索Gamma API获取btc-updown-5m市场，然后按结束时间排序
        """
        from datetime import datetime, timezone
        now_ts = int(time.time())
        
        print(f"[MarketDiscovery] Searching for BTC 5min markets...", flush=True)
        print(f"[MarketDiscovery] Current time: {datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime('%H:%M:%S UTC')}", flush=True)
        
        try:
            # 方法1: 使用search_markets搜索相关市场
            search_results = []
            try:
                # 尝试搜索btc-updown-5m
                search_results = self.gamma_client.search_markets("btc-updown-5m", limit=50)
                print(f"[MarketDiscovery] Search 'btc-updown-5m' returned {len(search_results)} markets", flush=True)
                
                # 如果搜索返回空，尝试其他搜索词
                if not search_results:
                    print(f"[MarketDiscovery] Search returned empty, trying alternative keywords", flush=True)
                    # 尝试btc-updown
                    search_results = self.gamma_client.search_markets("btc-updown", limit=50)
                    print(f"[MarketDiscovery] Search 'btc-updown' returned {len(search_results)} markets", flush=True)
                    
                    if not search_results:
                        # 尝试btc
                        search_results = self.gamma_client.search_markets("btc", limit=50)
                        print(f"[MarketDiscovery] Search 'btc' returned {len(search_results)} markets", flush=True)
            except Exception as e:
                print(f"[MarketDiscovery] Search failed: {e}", flush=True)
                
            # 如果搜索结果为空，尝试获取所有市场然后过滤
            if not search_results:
                print(f"[MarketDiscovery] Search returned empty, trying alternative approach", flush=True)
                # 可能需要使用不同的查询参数或获取所有市场
                # 暂时回退到原来的slug方法（但用更灵活的查询）
                # 使用当前时间附近的窗口
                window_end = math.ceil(now_ts / 300) * 300
                recent_slugs = []
                
                for i in range(lookback_windows):
                    w_end = window_end - i * 300
                    if w_end <= 0:
                        break
                    recent_slugs.append(f"btc-updown-5m-{w_end}")
                
                if recent_slugs:
                    print(f"[MarketDiscovery] Falling back to slug query: {recent_slugs[:3]}...", flush=True)
                    search_results = self.gamma_client.get_markets_by_slugs(recent_slugs)
                    print(f"[MarketDiscovery] Slug query returned {len(search_results)} markets", flush=True)
            
            if not search_results:
                print(f"[MarketDiscovery] No markets found", flush=True)
                return []
            
            # 打印找到的市场信息
            for i, market in enumerate(search_results[:5]):
                print(f"[MarketDiscovery] Market {i}: slug={market.slug}, clob_token_ids={len(market.clob_token_ids)}, accepting_orders={market.accepting_orders}", flush=True)
                
            # 过滤可交易市场
            tradable_markets = []
            for market in search_results:
                try:
                    tradable = self._validate_and_create_tradable(market)
                    tradable_markets.append(tradable)
                except MarketNotTradableError as e:
                    # 记录但跳过不可交易市场
                    print(f"[MarketDiscovery] {e}", flush=True)
                    continue
            
            # 按结束时间倒序排序（最新的在前）
            tradable_markets.sort(key=lambda m: m.expires_at, reverse=True)
            
            print(f"[MarketDiscovery] Found {len(tradable_markets)} tradable markets", flush=True)
            return tradable_markets
            
        except Exception as e:
            print(f"[MarketDiscovery] Error discovering markets: {e}", flush=True)
            import traceback
            print(f"[MarketDiscovery] Traceback: {traceback.format_exc()}", flush=True)
            return []
    
    def _get_markets_by_slugs(self, slugs: List[str]) -> List[Market]:
        """获取市场数据，支持缓存"""
        # 检查缓存
        cached_results = []
        uncached_slugs = []
        
        for slug in slugs:
            if slug in self.cache:
                market, cached_time = self.cache[slug]
                if time.time() - cached_time < self.cache_ttl:
                    cached_results.append(market)
                    continue
            uncached_slugs.append(slug)
        
        # 获取未缓存的市场
        if uncached_slugs:
            try:
                fresh_markets = self.gamma_client.get_markets_by_slugs(uncached_slugs)
                # 更新缓存
                for market in fresh_markets:
                    self.cache[market.slug] = (market, time.time())
                cached_results.extend(fresh_markets)
            except Exception as e:
                print(f"[MarketDiscovery] Failed to fetch markets: {e}")
                # 如果获取失败，使用缓存中的旧数据（如果有）
                pass
        
        return cached_results
    
    def _validate_and_create_tradable(self, market: Market) -> TradableMarket:
        """验证市场可交易性并创建TradableMarket对象"""
        
        # 1. 只检查clobTokenIds字段（唯一可交易性标准）
        if len(market.clob_token_ids) < 2:
            raise MarketNotTradableError(
                market.slug,
                f"Insufficient CLOB token IDs: {len(market.clob_token_ids)}"
            )
        
        # 2. 记录市场状态（调试用）
        print(f"[MarketDiscovery] Market {market.slug}: clob_token_ids={len(market.clob_token_ids)}, accepting_orders={market.accepting_orders}, tokens={len(market.tokens)}", flush=True)
        
        # 4. 解析YES/NO token
        yes_token, no_token = self._resolve_yes_no_tokens(market)
        
        if not yes_token or not no_token:
            raise MarketNotTradableError(
                market.slug,
                "Failed to resolve YES/NO tokens"
            )
        
        # 5. 解析结束时间
        expires_at = self._parse_expiry_time(market)
        
        if expires_at <= time.time():
            raise MarketNotTradableError(
                market.slug,
                f"Market expired at {datetime.fromtimestamp(expires_at).isoformat()}"
            )
        
        return TradableMarket(
            slug=market.slug,
            yes_token_id=yes_token,
            no_token_id=no_token,
            expires_at=expires_at,
            outcomes=market.outcomes,
            question=market.question,
            volume=market.volume,
            liquidity=market.liquidity
        )
    
    def _resolve_yes_no_tokens(self, market: Market) -> Tuple[str, str]:
        """从市场数据解析YES和NO token ID"""
        
        # 方法1: 优先使用tokens字段
        if market.tokens and len(market.tokens) >= 2:
            # tokens格式: [{"outcome": "Yes", "token_id": "..."}, ...]
            token_map = {}
            for token_info in market.tokens:
                outcome = token_info.get("outcome", "").lower()
                token_id = token_info.get("token_id", "")
                if outcome and token_id:
                    token_map[outcome] = token_id
            
            # 查找YES/NO或UP/DOWN映射
            yes_token = (
                token_map.get("yes") or 
                token_map.get("up") or 
                (market.outcomes and market.outcomes[0] in token_map and token_map[market.outcomes[0].lower()])
            )
            
            no_token = (
                token_map.get("no") or 
                token_map.get("down") or 
                (len(market.outcomes) > 1 and market.outcomes[1] in token_map and token_map[market.outcomes[1].lower()])
            )
            
            if yes_token and no_token and yes_token != no_token:
                return yes_token, no_token
        
        # 方法2: 使用clobTokenIds（假设第一个是YES，第二个是NO）
        if market.clob_token_ids and len(market.clob_token_ids) >= 2:
            return market.clob_token_ids[0], market.clob_token_ids[1]
        
        # 方法3: 根据outcomes顺序匹配
        if market.outcomes and len(market.outcomes) >= 2:
            outcome_map = {"up": "yes", "down": "no", "yes": "yes", "no": "no"}
            
            first_outcome = market.outcomes[0].lower()
            second_outcome = market.outcomes[1].lower() if len(market.outcomes) > 1 else ""
            
            # 简单映射：第一个outcome对应YES token，第二个对应NO token
            if market.clob_token_ids and len(market.clob_token_ids) >= 2:
                if outcome_map.get(first_outcome) == "yes" and outcome_map.get(second_outcome) == "no":
                    return market.clob_token_ids[0], market.clob_token_ids[1]
        
        return "", ""
    
    def _parse_expiry_time(self, market: Market) -> int:
        """从市场数据解析过期时间"""
        
        # 方法1: 从slug中提取
        if market.slug.startswith("btc-updown-5m-"):
            try:
                return int(market.slug.split("-")[-1])
            except:
                pass
        
        # 方法2: 解析end_date_iso
        if market.end_date_iso:
            try:
                # 尝试解析ISO 8601格式
                dt = datetime.fromisoformat(market.end_date_iso.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except Exception as e:
                print(f"[MarketDiscovery] Failed to parse end_date_iso '{market.end_date_iso}': {e}")
        
        # 方法3: 默认过期时间（当前时间+5分钟）
        return int(time.time()) + 300
    
    def get_best_tradable_market(self, lookback_windows: int = 10) -> Optional[TradableMarket]:
        """
        获取最佳可交易市场（最近的可交易市场）
        
        Returns:
            最佳可交易市场，如果没有则返回None
        """
        markets = self.discover_btc_5min_markets(lookback_windows)
        if markets:
            return markets[0]  # 已经按时间倒序排序
        return None
    
    def is_market_still_valid(self, market_slug: str, token_pair: Tuple[str, str]) -> bool:
        """检查市场是否仍然有效可交易"""
        try:
            market = self._get_markets_by_slugs([market_slug])
            if not market:
                return False
            
            market_obj = market[0]
            tradable = self._validate_and_create_tradable(market_obj)
            
            # 检查token是否匹配
            return (tradable.yes_token_id == token_pair[0] and 
                    tradable.no_token_id == token_pair[1])
        except MarketNotTradableError:
            return False
        except Exception as e:
            print(f"[MarketDiscovery] Error checking market validity: {e}")
            return False


# 单例实例（可选）
_default_discovery = None

def get_market_discovery() -> MarketDiscovery:
    """获取市场发现单例实例"""
    global _default_discovery
    if _default_discovery is None:
        _default_discovery = MarketDiscovery()
    return _default_discovery