#!/usr/bin/env python3
"""
Simple CryptoRebound Refresh Performance Test
Focused test for diagnosing refresh slowness issues
"""

import requests
import json
import time
import sys

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
API_BASE = f"{BACKEND_URL}/api"

print(f"Testing CryptoRebound Refresh Performance at: {API_BASE}")
print("=" * 80)

def test_health():
    """Test health endpoint"""
    print("1. Testing Health Endpoint...")
    try:
        start_time = time.time()
        response = requests.get(f"{API_BASE}/health", timeout=10)
        duration = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            services = data.get('services', {})
            print(f"   ✅ Health check passed ({duration:.2f}s)")
            print(f"   Services: {services}")
            return True
        else:
            print(f"   ❌ Health check failed: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Health check error: {e}")
        return False

def test_basic_refresh():
    """Test basic refresh endpoint"""
    print("\n2. Testing Basic Refresh (force=false)...")
    try:
        start_time = time.time()
        response = requests.post(
            f"{API_BASE}/cryptos/refresh",
            json={"force": False},
            timeout=30
        )
        duration = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                updated = data.get('updated_rankings', {})
                print(f"   ✅ Basic refresh passed ({duration:.2f}s)")
                print(f"   Updated periods: {updated}")
                return duration
            else:
                print(f"   ❌ Basic refresh failed: {data.get('status')}")
                return None
        else:
            print(f"   ❌ Basic refresh failed: HTTP {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        print(f"   ❌ Basic refresh timed out after 30s")
        return None
    except Exception as e:
        print(f"   ❌ Basic refresh error: {e}")
        return None

def test_force_refresh():
    """Test force refresh endpoint"""
    print("\n3. Testing Force Refresh (force=true)...")
    try:
        start_time = time.time()
        response = requests.post(
            f"{API_BASE}/cryptos/refresh",
            json={"force": True},
            timeout=60
        )
        duration = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                updated = data.get('updated_rankings', {})
                total_cryptos = sum(updated.values()) if updated else 0
                print(f"   ✅ Force refresh passed ({duration:.2f}s)")
                print(f"   Updated {total_cryptos} cryptos across {len(updated)} periods")
                
                # Performance assessment
                if duration < 10:
                    print(f"   🟢 Excellent performance")
                elif duration < 30:
                    print(f"   🟡 Good performance")
                elif duration < 60:
                    print(f"   🟠 Acceptable performance")
                else:
                    print(f"   🔴 Slow performance")
                
                return duration
            else:
                print(f"   ❌ Force refresh failed: {data.get('status')}")
                return None
        else:
            print(f"   ❌ Force refresh failed: HTTP {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        print(f"   ❌ Force refresh timed out after 60s")
        return None
    except Exception as e:
        print(f"   ❌ Force refresh error: {e}")
        return None

def test_ranking_performance():
    """Test ranking endpoint performance with different parameters"""
    print("\n4. Testing Ranking Endpoint Performance...")
    
    test_cases = [
        {"limit": 50, "force_refresh": False, "name": "Small dataset (no force)"},
        {"limit": 50, "force_refresh": True, "name": "Small dataset (force)"},
        {"limit": 500, "force_refresh": False, "name": "Medium dataset (no force)"},
        {"limit": 500, "force_refresh": True, "name": "Medium dataset (force)"},
    ]
    
    results = []
    
    for case in test_cases:
        try:
            print(f"   Testing: {case['name']}")
            start_time = time.time()
            
            response = requests.get(
                f"{API_BASE}/cryptos/ranking",
                params={
                    'limit': case['limit'],
                    'period': '24h',
                    'force_refresh': case['force_refresh']
                },
                timeout=45
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                count = len(data) if isinstance(data, list) else 0
                print(f"     ✅ Success: {count} cryptos in {duration:.2f}s")
                results.append({
                    'name': case['name'],
                    'duration': duration,
                    'count': count,
                    'success': True
                })
            else:
                print(f"     ❌ Failed: HTTP {response.status_code}")
                results.append({
                    'name': case['name'],
                    'success': False
                })
                
        except requests.exceptions.Timeout:
            print(f"     ❌ Timed out after 45s")
            results.append({
                'name': case['name'],
                'success': False,
                'timeout': True
            })
        except Exception as e:
            print(f"     ❌ Error: {e}")
            results.append({
                'name': case['name'],
                'success': False,
                'error': str(e)
            })
    
    return results

def main():
    print("Starting Focused Refresh Performance Test...")
    
    # Test 1: Health
    health_ok = test_health()
    
    # Test 2: Basic refresh
    basic_duration = test_basic_refresh()
    
    # Test 3: Force refresh
    force_duration = test_force_refresh()
    
    # Test 4: Ranking performance
    ranking_results = test_ranking_performance()
    
    # Summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    
    print(f"Health Check: {'✅ PASS' if health_ok else '❌ FAIL'}")
    
    if basic_duration:
        print(f"Basic Refresh: ✅ PASS ({basic_duration:.2f}s)")
    else:
        print(f"Basic Refresh: ❌ FAIL")
    
    if force_duration:
        print(f"Force Refresh: ✅ PASS ({force_duration:.2f}s)")
    else:
        print(f"Force Refresh: ❌ FAIL")
    
    successful_ranking = len([r for r in ranking_results if r.get('success')])
    total_ranking = len(ranking_results)
    print(f"Ranking Tests: {successful_ranking}/{total_ranking} passed")
    
    # Performance analysis
    print("\nPERFORMANCE ANALYSIS:")
    
    if not health_ok:
        print("🔴 External services have issues - this may cause slow refresh")
    
    if force_duration and force_duration > 30:
        print(f"🟠 Force refresh is slow ({force_duration:.2f}s) - consider optimization")
    elif force_duration and force_duration < 10:
        print(f"🟢 Force refresh performance is excellent ({force_duration:.2f}s)")
    
    # Check for force refresh impact
    force_ranking = next((r for r in ranking_results if 'force' in r.get('name', '').lower() and r.get('success')), None)
    no_force_ranking = next((r for r in ranking_results if 'force' not in r.get('name', '').lower() and r.get('success')), None)
    
    if force_ranking and no_force_ranking and 'duration' in force_ranking and 'duration' in no_force_ranking:
        impact = force_ranking['duration'] - no_force_ranking['duration']
        if impact > 10:
            print(f"🔴 Force refresh adds significant delay (+{impact:.2f}s)")
        elif impact > 5:
            print(f"🟠 Force refresh adds moderate delay (+{impact:.2f}s)")
        else:
            print(f"🟢 Force refresh impact is minimal (+{impact:.2f}s)")
    
    timeout_count = len([r for r in ranking_results if r.get('timeout')])
    if timeout_count > 0:
        print(f"🔴 {timeout_count} tests timed out - indicates serious performance issues")
    
    print("\nRECOMMENDATIONS:")
    if not health_ok:
        print("1. Check external API connectivity (Binance API appears to be down)")
    if force_duration and force_duration > 30:
        print("2. Consider implementing background refresh jobs")
        print("3. Optimize data aggregation service")
    if timeout_count > 0:
        print("4. Implement request timeouts and better error handling")
        print("5. Consider caching strategies for large datasets")
    
    # Overall assessment
    critical_issues = [
        not health_ok,
        force_duration is None,  # Force refresh failed
        timeout_count > 1
    ]
    
    if any(critical_issues):
        print("\n🔴 CRITICAL ISSUES DETECTED")
        return False
    elif force_duration and force_duration > 30:
        print("\n🟠 PERFORMANCE ISSUES DETECTED")
        return False
    else:
        print("\n🟢 REFRESH FUNCTIONALITY IS WORKING ACCEPTABLY")
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)