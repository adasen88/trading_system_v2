"""Gamma API客户端 - 仅用于市场发现"""
import time
import requests
from typing import List, Dict, Optional
from dataclasses import dataclass
import pandas as pd

from .errors import PolymarketAPIError, RateLimitError


@dataclass
class Market:
    """市场数据结构"""
    slug: str
    question: str
    outcomes: List[str]
    outcome_prices: List[float]
    tokens: List[Dict]  # [{outcome: "Yes", token_id: "..."}, ...]
    clob_token_ids: List[str]
    accepting_orders: bool
    end_date_iso: str
    volume: float
    liquidity: float


class GammaClient:
    """Gamma API客户端封装"""
    
    BASE_URL = "https://gamma-api.polymarket.com"
    
    def __init__(self, rate_limit_delay: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "TradingOS/2.0 (Data Service)",
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
    
    def get_markets_by_slugs(self, slugs: List[str]) -> List[Market]:
        """
        通过slug列表获取市场数据
        
        Args:
            slugs: 市场slug列表，如 ["btc-updown-5m-1776508500", ...]
            
        Returns:
            市场对象列表
            
        Raises:
            PolymarketAPIError: API调用失败
            RateLimitError: 触发速率限制
        """
        if not slugs:
            return []
        
        self._rate_limit()
        
        try:
            # Gamma API支持批量查询
            params = {"slug": ",".join(slugs)}
            print(f"[Gamma] GammaClient.get_markets_by_slugs calling API with params: {params}", flush=True)
            
            response = self.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                timeout=10
            )
            
            print(f"[Gamma] API response status: {response.status_code}", flush=True)
            
            if response.status_code == 429:
                raise RateLimitError("Gamma API rate limit exceeded")
            
            response.raise_for_status()
            
            markets_data = response.json()
            print(f"[Gamma] API returned {len(markets_data)} market(s)", flush=True)
            
            markets = []
            
            for idx, market_data in enumerate(markets_data):
                try:
                    print(f"[Gamma] Parsing market {idx}: slug={market_data.get('slug')}", flush=True)
                    market = self._parse_market(market_data)
                    markets.append(market)
                    print(f"[Gamma] Market {idx} parsed: clob_token_ids={len(market.clob_token_ids)}", flush=True)
                except Exception as e:
                    # 记录但跳过解析失败的市场
                    print(f"[Gamma] Failed to parse market {market_data.get('slug')}: {e}", flush=True)
                    continue
            
            print(f"[Gamma] Returning {len(markets)} markets", flush=True)
            return markets
            
        except requests.RequestException as e:
            raise PolymarketAPIError(f"Gamma API request failed: {e}")
    
    def _parse_market(self, data: Dict) -> Market:
        """解析API返回的市场数据"""
        # 解析tokens字段
        tokens = data.get("tokens", [])
        if isinstance(tokens, str):
            try:
                import json
                tokens = json.loads(tokens)
            except:
                tokens = []
        
        # 解析clobTokenIds字段
        clob_token_ids_raw = data.get("clobTokenIds", [])
        print(f"[Gamma] _parse_market clobTokenIds raw: type={type(clob_token_ids_raw)}, value={repr(clob_token_ids_raw)}", flush=True)
        
        clob_token_ids = clob_token_ids_raw
        if isinstance(clob_token_ids, str):
            try:
                import json
                clob_token_ids = json.loads(clob_token_ids)
                print(f"[Gamma] _parse_market clobTokenIds parsed: {clob_token_ids}, type={type(clob_token_ids)}", flush=True)
                if not isinstance(clob_token_ids, list):
                    print(f"[Gamma] _parse_market clobTokenIds not a list after parsing", flush=True)
                    clob_token_ids = []
            except Exception as e:
                print(f"[Gamma] _parse_market clobTokenIds JSON parse error: {e}", flush=True)
                clob_token_ids = []
        
        print(f"[Gamma] _parse_market final clob_token_ids: {clob_token_ids}", flush=True)
        
        # 解析outcomePrices字段
        outcome_prices = data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            try:
                import json
                outcome_prices = json.loads(outcome_prices)
                if not isinstance(outcome_prices, list):
                    outcome_prices = []
            except:
                outcome_prices = []
        
        # 转换为浮点数
        try:
            outcome_prices = [float(p) for p in outcome_prices]
        except:
            outcome_prices = []
        
        return Market(
            slug=data.get("slug", ""),
            question=data.get("question", ""),
            outcomes=data.get("outcomes", []),
            outcome_prices=outcome_prices,
            tokens=tokens,
            clob_token_ids=clob_token_ids,
            accepting_orders=data.get("accepting_orders", False),
            end_date_iso=data.get("end_date_iso", ""),
            volume=float(data.get("volume", 0)),
            liquidity=float(data.get("liquidity", 0))
        )
    
    def search_markets(self, query: str, limit: int = 20) -> List[Market]:
        """
        搜索市场（谨慎使用，可能触发速率限制）
        
        Args:
            query: 搜索关键词，如 "btc-updown-5m"
            limit: 返回数量限制
            
        Returns:
            市场对象列表
        """
        self._rate_limit()
        
        try:
            params = {"query": query, "limit": limit}
            response = self.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                timeout=10
            )
            
            if response.status_code == 429:
                raise RateLimitError("Gamma API rate limit exceeded")
            
            response.raise_for_status()
            
            markets_data = response.json()
            markets = []
            
            for market_data in markets_data[:limit]:
                try:
                    market = self._parse_market(market_data)
                    markets.append(market)
                except Exception as e:
                    print(f"[Gamma] Failed to parse market in search: {e}")
                    continue
            
            return markets
            
        except requests.RequestException as e:
            raise PolymarketAPIError(f"Gamma search request failed: {e}")

    def get_all_active_markets(self, limit: int = 1000) -> List[Market]:
        """
        获取所有活跃市场（最稳定的方法）
        
        Args:
            limit: 返回数量限制，默认1000
            
        Returns:
            市场对象列表
        """
        self._rate_limit()
        
        try:
            params = {"active": "true", "limit": limit}
            response = self.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                timeout=10
            )
            
            if response.status_code == 429:
                raise RateLimitError("Gamma API rate limit exceeded")
            
            response.raise_for_status()
            
            markets_data = response.json()
            markets = []
            
            for market_data in markets_data[:limit]:
                try:
                    market = self._parse_market(market_data)
                    markets.append(market)
                except Exception as e:
                    print(f"[Gamma] Failed to parse market in get_all_active_markets: {e}")
                    continue
            
            return markets
            
        except requests.RequestException as e:
            raise PolymarketAPIError(f"Gamma get_all_active_markets request failed: {e}")


# 兼容层：包装polymarket-pandas（如果可用）
try:
    from polymarket_pandas import PolymarketPandas
    
    class PolymarketPandasAdapter(GammaClient):
        """polymarket-pandas适配器，提供统一接口"""
        
        def __init__(self):
            super().__init__()
            self.pandas_client = PolymarketPandas()
        
        def get_markets_by_slugs(self, slugs: List[str]) -> List[Market]:
            """使用polymarket-pandas获取市场数据"""
            try:
                print(f"[Gamma] PolymarketPandasAdapter.get_markets_by_slugs called with slugs: {slugs}", flush=True)
                
                df = self.pandas_client.get_markets(
                    slug=slugs,
                    expand_clob_token_ids=True,
                    expand_events=False,
                    expand_series=False
                )
                
                print(f"[Gamma] pandas get_markets returned DataFrame shape: {df.shape}", flush=True)
                if not df.empty:
                    print(f"[Gamma] DataFrame columns: {df.columns.tolist()}", flush=True)
                    # 打印前几行的clobTokenIds值
                    for idx, row in df.head(3).iterrows():
                        clob_raw = row.get("clobTokenIds")
                        print(f"[Gamma] Row {idx} slug={row.get('slug')}, clobTokenIds type={type(clob_raw)}, value={repr(clob_raw)}", flush=True)
                
                if df.empty:
                    print(f"[Gamma] DataFrame is empty, falling back to native Gamma API", flush=True)
                    # 主动触发回退到原生实现
                    raise Exception("polymarket-pandas returned empty DataFrame")
                
                # 按slug分组，合并同一slug的YES/NO token IDs
                markets_by_slug = {}
                for idx, row in df.iterrows():
                    try:
                        slug = row.get("slug", "")
                        if not slug:
                            continue
                        
                        clob_raw = row.get("clobTokenIds")
                        clob_parsed = self._parse_clob_ids(clob_raw)
                        
                        # 检查是否有outcome信息
                        outcomes = row.get("outcomes", [])
                        outcome_prices = self._parse_prices(row.get("outcomePrices"))
                        
                        # 如果已经存在该slug的市场，合并token IDs
                        if slug in markets_by_slug:
                            existing = markets_by_slug[slug]
                            # 合并token IDs（去重）
                            combined_tokens = list(set(existing["clob_token_ids"] + clob_parsed))
                            existing["clob_token_ids"] = combined_tokens
                            
                            # 如果这个行有更完整的outcomes信息，更新
                            if outcomes and not existing["outcomes"]:
                                existing["outcomes"] = outcomes
                            if outcome_prices and not existing["outcome_prices"]:
                                existing["outcome_prices"] = outcome_prices
                            
                            print(f"[Gamma] Merged token IDs for slug {slug}: now has {len(combined_tokens)} token IDs", flush=True)
                        else:
                            # 创建新市场记录
                            markets_by_slug[slug] = {
                                "slug": slug,
                                "question": row.get("question", ""),
                                "outcomes": outcomes,
                                "outcome_prices": outcome_prices,
                                "tokens": row.get("tokens", []),
                                "clob_token_ids": clob_parsed,
                                "accepting_orders": row.get("accepting_orders", False),
                                "end_date_iso": row.get("end_date_iso", ""),
                                "volume": float(row.get("volume", 0)),
                                "liquidity": float(row.get("liquidity", 0))
                            }
                            print(f"[Gamma] Created market record for {slug}: {len(clob_parsed)} token IDs", flush=True)
                    except Exception as e:
                        print(f"[Gamma] Failed to process pandas row {idx}: {e}", flush=True)
                        continue
                
                # 从分组数据创建Market对象
                markets = []
                for slug, data in markets_by_slug.items():
                    try:
                        market = Market(
                            slug=data["slug"],
                            question=data["question"],
                            outcomes=data["outcomes"],
                            outcome_prices=data["outcome_prices"],
                            tokens=data["tokens"],
                            clob_token_ids=data["clob_token_ids"],
                            accepting_orders=data["accepting_orders"],
                            end_date_iso=data["end_date_iso"],
                            volume=data["volume"],
                            liquidity=data["liquidity"]
                        )
                        markets.append(market)
                        print(f"[Gamma] Final market: {market.slug}, clob_token_ids={len(market.clob_token_ids)}", flush=True)
                    except Exception as e:
                        print(f"[Gamma] Failed to create Market object for {slug}: {e}", flush=True)
                        continue
                
                print(f"[Gamma] Returning {len(markets)} markets (after merging)", flush=True)
                return markets
                
            except Exception as e:
                # 失败时回退到原生实现
                print(f"[Gamma] pandas adapter failed, falling back: {e}")
                return super().get_markets_by_slugs(slugs)
        
        def _parse_prices(self, prices_raw) -> List[float]:
            """解析价格字段"""
            if isinstance(prices_raw, list):
                return [float(p) for p in prices_raw]
            elif isinstance(prices_raw, str):
                try:
                    import json
                    parsed = json.loads(prices_raw)
                    if isinstance(parsed, list):
                        return [float(p) for p in parsed]
                except:
                    pass
            return []
        
        def _parse_clob_ids(self, clob_ids_raw) -> List[str]:
            """解析clobTokenIds字段"""
            print(f"[Gamma] _parse_clob_ids raw type: {type(clob_ids_raw)}, value: {repr(clob_ids_raw)}", flush=True)
            
            if isinstance(clob_ids_raw, list):
                result = [str(id_) for id_ in clob_ids_raw]
                print(f"[Gamma] _parse_clob_ids list result: {result}", flush=True)
                return result
            elif isinstance(clob_ids_raw, str):
                try:
                    import json
                    parsed = json.loads(clob_ids_raw)
                    print(f"[Gamma] _parse_clob_ids parsed: {parsed}, type: {type(parsed)}", flush=True)
                    
                    # 处理不同类型的解析结果
                    if isinstance(parsed, list):
                        result = [str(id_) for id_ in parsed]
                        print(f"[Gamma] _parse_clob_ids str->list result: {result}", flush=True)
                        return result
                    elif isinstance(parsed, (int, str)):
                        # 单个token ID的情况（单个数字字符串）
                        result = [str(parsed)]
                        print(f"[Gamma] _parse_clob_ids str->single result: {result}", flush=True)
                        return result
                except Exception as e:
                    print(f"[Gamma] _parse_clob_ids JSON parse error: {e}", flush=True)
                    pass
            
            # 尝试直接转换其他类型
            if clob_ids_raw is not None:
                try:
                    result = [str(clob_ids_raw)]
                    print(f"[Gamma] _parse_clob_ids direct conversion: {result}", flush=True)
                    return result
                except:
                    pass
            
            print(f"[Gamma] _parse_clob_ids returning empty list", flush=True)
            return []
    
    # 默认使用适配器
    DefaultGammaClient = PolymarketPandasAdapter
    
except ImportError:
    # 如果没有polymarket-pandas，使用原生实现
    DefaultGammaClient = GammaClient


# 工厂函数
def create_gamma_client(use_pandas: bool = True) -> GammaClient:
    """创建Gamma客户端"""
    if use_pandas:
        try:
            return PolymarketPandasAdapter()
        except:
            pass
    return GammaClient()