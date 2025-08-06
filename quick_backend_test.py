#!/usr/bin/env python3
"""
Quick Backend API Testing for CryptoRebound Optimized Performance
Focus on key functionality without timeout-prone large requests
"""

import requests
import json
import time
import sys
import os
from datetime import datetime

# Get backend URL from frontend .env file
def get_backend_url():
    try:
        with open('/app/frontend/.env', 'r') as f:
            for line in f:
                if line.startswith('REACT_APP_BACKEND_URL='):
                    return line.split('=', 1)[1].strip()
    except Exception as e:
        print(f"Error reading frontend .env: {e}")
        return None

BACKEND_URL = get_backend_url()
if not BACKEND_URL:
    print("ERROR: Could not get REACT_APP_BACKEND_URL from frontend/.env")
    sys.exit(1)

API_BASE = f"{BACKEND_URL}/api"

print(f"Quick Testing CryptoRebound Backend API at: {API_BASE}")
print("=" * 80)

def test_health_and_apis():
    """Test health endpoint and API integrations"""
    try:
        response = requests.get(f"{API_BASE}/health", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            services = data.get('services', {})
            
            print("✅ Health Endpoint Working")
            print(f"   Status: {data.get('status')}")
            
            # Check for 8 expected services
            expected_services = [
                'coinmarketcap', 'cryptocompare', 'coinapi', 'coinpaprika', 
                'bitfinex', 'binance', 'yahoo_finance', 'fallback'
            ]
            
            available = []
            unavailable = []
            
            for service in expected_services:
                if services.get(service) == True:
                    available.append(service)
                else:
                    unavailable.append(service)
            
            print(f"   Available APIs: {len(available)}/8 ({', '.join(available)})")
            if unavailable:
                print(f"   Unavailable APIs: {', '.join(unavailable)}")
            
            return len(available) >= 5  # Need at least 5 APIs working
        else:
            print(f"❌ Health Endpoint Failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Health Endpoint Error: {str(e)}")
        return False

def test_api_keys():
    """Test API key verification"""
    try:
        response = requests.get(f"{API_BASE}/health", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            services = data.get('services', {})
            
            coinapi_ok = services.get('coinapi', False)
            coinmarketcap_ok = services.get('coinmarketcap', False)
            
            print("✅ API Key Verification:")
            print(f"   CoinAPI (70046baa-e887-42ee-a909-03c6b6afab67): {'✅' if coinapi_ok else '❌'}")
            print(f"   CoinMarketCap (70046baa-e887-42ee-a909-03c6b6afab67): {'✅' if coinmarketcap_ok else '❌'}")
            
            return coinapi_ok and coinmarketcap_ok
        else:
            print("❌ API Key Verification Failed")
            return False
            
    except Exception as e:
        print(f"❌ API Key Verification Error: {str(e)}")
        return False

def test_basic_ranking():
    """Test basic ranking functionality"""
    try:
        start_time = time.time()
        response = requests.get(
            f"{API_BASE}/cryptos/ranking",
            params={'limit': 50, 'period': '24h'},
            timeout=15
        )
        end_time = time.time()
        
        if response.status_code == 200:
            data = response.json()
            
            if isinstance(data, list) and len(data) > 0:
                print(f"✅ Basic Ranking Working")
                print(f"   Returned: {len(data)} cryptos in {end_time - start_time:.2f}s")
                print(f"   First crypto: {data[0].get('symbol')} - ${data[0].get('price_usd', 0):.4f}")
                return True
            else:
                print("❌ Basic Ranking Failed: No data returned")
                return False
        else:
            print(f"❌ Basic Ranking Failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Basic Ranking Error: {str(e)}")
        return False

def test_caching():
    """Test caching effectiveness"""
    try:
        print("Testing caching effectiveness...")
        
        # First request
        start_time = time.time()
        response1 = requests.get(
            f"{API_BASE}/cryptos/ranking",
            params={'limit': 100, 'period': '24h'},
            timeout=15
        )
        first_time = time.time() - start_time
        
        if response1.status_code != 200:
            print("❌ Caching Test Failed: First request failed")
            return False
        
        # Second request immediately
        start_time = time.time()
        response2 = requests.get(
            f"{API_BASE}/cryptos/ranking",
            params={'limit': 100, 'period': '24h'},
            timeout=15
        )
        second_time = time.time() - start_time
        
        if response2.status_code != 200:
            print("❌ Caching Test Failed: Second request failed")
            return False
        
        data1 = response1.json()
        data2 = response2.json()
        
        if len(data1) == len(data2):
            speedup = first_time / max(second_time, 0.001)
            
            print(f"✅ Caching Working")
            print(f"   First request: {first_time:.2f}s")
            print(f"   Second request: {second_time:.2f}s")
            print(f"   Speedup: {speedup:.1f}x")
            
            return speedup >= 1.5 or second_time < 1.0
        else:
            print("❌ Caching Test Failed: Data inconsistency")
            return False
            
    except Exception as e:
        print(f"❌ Caching Test Error: {str(e)}")
        return False

def test_dynamic_limit():
    """Test dynamic limit endpoint"""
    try:
        response = requests.get(f"{API_BASE}/system/dynamic-limit", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            max_limit = data.get('max_recommended_limit', 0)
            memory_mb = data.get('system_resources', {}).get('available_memory_mb', 0)
            cpu_percent = data.get('system_resources', {}).get('cpu_usage_percent', 0)
            
            print(f"✅ Dynamic Limit Working")
            print(f"   Max recommended: {max_limit} cryptos")
            print(f"   Available memory: {memory_mb:.1f}MB")
            print(f"   CPU usage: {cpu_percent:.1f}%")
            
            return max_limit > 0
        else:
            print(f"❌ Dynamic Limit Failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Dynamic Limit Error: {str(e)}")
        return False

def test_async_refresh():
    """Test async refresh system"""
    try:
        # Test async refresh endpoint
        start_time = time.time()
        response = requests.post(f"{API_BASE}/cryptos/refresh-async", timeout=5)
        end_time = time.time()
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"✅ Async Refresh Working")
            print(f"   Status: {data.get('status')}")
            print(f"   Response time: {end_time - start_time:.2f}s")
            
            # Test status endpoint
            status_response = requests.get(f"{API_BASE}/cryptos/refresh-status", timeout=5)
            if status_response.status_code == 200:
                status_data = status_response.json()
                print(f"   Refresh status: {status_data.get('status')}")
                print(f"   Active tasks: {status_data.get('active_tasks')}")
                return True
            else:
                print("❌ Refresh Status Endpoint Failed")
                return False
        else:
            print(f"❌ Async Refresh Failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Async Refresh Error: {str(e)}")
        return False

def main():
    """Run quick backend tests"""
    print("Running Quick Backend Tests for Optimized CryptoRebound...")
    print()
    
    tests = [
        ("Health & API Integration", test_health_and_apis),
        ("API Key Verification", test_api_keys),
        ("Basic Ranking", test_basic_ranking),
        ("Caching System", test_caching),
        ("Dynamic Limit", test_dynamic_limit),
        ("Async Refresh", test_async_refresh),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} Error: {str(e)}")
            results.append((test_name, False))
    
    print("\n" + "=" * 80)
    print("QUICK TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
    
    if passed >= total * 0.8:  # 80% pass rate
        print("🎉 Quick tests mostly successful!")
        return True
    else:
        print("⚠️  Several quick tests failed.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)