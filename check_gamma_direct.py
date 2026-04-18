#!/usr/bin/env python3
"""直接调用Gamma API检查clobTokenIds数据源"""
import requests
import json
import time
import math

def check_gamma_api_directly():
    """直接调用Gamma API，绕过polymarket-pandas"""
    now_ts = int(time.time())
    window_end = math.ceil(now_ts / 300) * 300
    slug = f"btc-updown-5m-{window_end}"
    
    print(f"检查窗口: {slug} (结束于 {window_end})")
    print("=" * 60)
    
    try:
        # 直接调用Gamma API
        url = "https://gamma-api.polymarket.com/markets"
        params = {"slug": slug}
        
        print(f"请求: {url}?slug={slug}")
        response = requests.get(url, params=params, timeout=10)
        
        print(f"状态码: {response.status_code}")
        response.raise_for_status()
        
        markets = response.json()
        print(f"返回市场数量: {len(markets)}")
        
        if not markets:
            print("⚠️  无市场数据返回")
            return False
        
        for i, market in enumerate(markets):
            print(f"\n市场 #{i+1}:")
            print(f"  slug: {market.get('slug')}")
            print(f"  question: {market.get('question', '')[:50]}...")
            
            # clobTokenIds
            clob_raw = market.get("clobTokenIds")
            print(f"  clobTokenIds (raw): {clob_raw}")
            print(f"  clobTokenIds type: {type(clob_raw)}")
            
            # 尝试解析
            if clob_raw:
                if isinstance(clob_raw, str):
                    try:
                        parsed = json.loads(clob_raw)
                        print(f"  clobTokenIds (parsed): {parsed}")
                        print(f"  parsed type: {type(parsed)}")
                        if isinstance(parsed, list):
                            print(f"  token数量: {len(parsed)}")
                            if len(parsed) >= 2:
                                print("  ✅ 有效CLOB token IDs")
                            else:
                                print(f"  ⚠️  token数量不足: {len(parsed)}")
                        else:
                            print(f"  ❌ 解析后不是列表: {parsed}")
                    except json.JSONDecodeError as e:
                        print(f"  ❌ JSON解析失败: {e}")
                elif isinstance(clob_raw, list):
                    print(f"  clobTokenIds (list): {clob_raw}")
                    print(f"  token数量: {len(clob_raw)}")
                else:
                    print(f"  ⚠️ 未知类型: {type(clob_raw)}")
            else:
                print("  ❌ clobTokenIds为空")
            
            # tokens字段
            tokens = market.get("tokens", [])
            print(f"  tokens字段: {tokens}")
            print(f"  tokens类型: {type(tokens)}")
            if isinstance(tokens, list):
                print(f"  tokens数量: {len(tokens)}")
            
            # accepting_orders
            accepting = market.get("accepting_orders", False)
            print(f"  accepting_orders: {accepting}")
            
            print("-" * 40)
        
        return True
        
    except Exception as e:
        print(f"❌ API调用失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_multiple_windows():
    """检查多个窗口"""
    now_ts = int(time.time())
    window_end = math.ceil(now_ts / 300) * 300
    
    slugs = []
    for i in range(5):
        w_end = window_end - i * 300
        if w_end <= 0:
            break
        slugs.append(f"btc-updown-5m-{w_end}")
    
    print(f"\n检查最近{len(slugs)}个窗口: {slugs}")
    print("=" * 60)
    
    for slug in slugs:
        try:
            url = "https://gamma-api.polymarket.com/markets"
            response = requests.get(url, params={"slug": slug}, timeout=5)
            
            if response.status_code == 200:
                markets = response.json()
                if markets:
                    market = markets[0]
                    clob_raw = market.get("clobTokenIds")
                    tokens = market.get("tokens", [])
                    
                    print(f"{slug}:")
                    print(f"  clobTokenIds: {clob_raw}")
                    print(f"  tokens: {len(tokens) if isinstance(tokens, list) else 'N/A'}")
                    print(f"  accepting_orders: {market.get('accepting_orders', False)}")
                else:
                    print(f"{slug}: ⚠️ 无数据")
            else:
                print(f"{slug}: ❌ 状态码 {response.status_code}")
                
        except Exception as e:
            print(f"{slug}: ❌ 错误 {e}")

if __name__ == "__main__":
    print("Gamma API直接检查工具")
    print("=" * 60)
    
    # 检查当前窗口
    check_gamma_api_directly()
    
    # 检查多个窗口
    check_multiple_windows()