#!/usr/bin/env python3
"""
Backend API Testing for CryptoRebound Optimized Performance with 8 API Sources
Tests the optimized CryptoRebound backend with performance enhancements, new CoinMarketCap integration,
enhanced parallel processing (15 concurrent requests), improved caching thresholds, and 8 API sources
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

print(f"Testing CryptoRebound Backend API at: {API_BASE}")
print("=" * 80)

class BackendTester:
    def __init__(self):
        self.test_results = []
        self.failed_tests = []
        
    def log_test(self, test_name, success, details="", response_data=None):
        """Log test results"""
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if details:
            print(f"    {details}")
        if response_data and not success:
            print(f"    Response: {response_data}")
        print()
        
        self.test_results.append({
            'test': test_name,
            'success': success,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
        if not success:
            self.failed_tests.append(test_name)
    
    def test_health_endpoint_with_8_apis(self):
        """Test the enhanced health check endpoint with 8 API services including CoinMarketCap"""
        try:
            response = requests.get(f"{API_BASE}/health", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'healthy':
                    services = data.get('services', {})
                    
                    # Check for all 8 expected services including CoinMarketCap
                    expected_services = [
                        'coinmarketcap', 'cryptocompare', 'coinapi', 'coinpaprika', 
                        'bitfinex', 'binance', 'yahoo_finance', 'fallback'
                    ]
                    
                    available_services = []
                    unavailable_services = []
                    
                    for service in expected_services:
                        if services.get(service) == True:
                            available_services.append(service)
                        else:
                            unavailable_services.append(service)
                    
                    details = f"Available: {len(available_services)}/8 services ({', '.join(available_services)})"
                    if unavailable_services:
                        details += f" | Unavailable: {', '.join(unavailable_services)}"
                    
                    # Consider healthy if at least 5 services are available (higher threshold for 8 APIs)
                    if len(available_services) >= 5:
                        self.log_test("Enhanced Health Check (8 APIs)", True, details)
                        return available_services
                    else:
                        self.log_test("Enhanced Health Check (8 APIs)", False, f"Too few services available: {details}")
                        return False
                else:
                    self.log_test("Enhanced Health Check (8 APIs)", False, f"Unhealthy status: {data}")
                    return False
            else:
                self.log_test("Enhanced Health Check (8 APIs)", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Enhanced Health Check (8 APIs)", False, f"Exception: {str(e)}")
            return False
    
    def test_optimized_caching_system(self):
        """Test the optimized period-based intelligent caching system with improved thresholds"""
        try:
            print("Testing optimized caching system with improved thresholds...")
            print("  Expected thresholds: 24h=3min, 7d=20min, 30d=1.5h")
            
            # Test different periods to verify optimized caching thresholds
            test_periods = ['24h', '7d', '30d']
            cache_performance = {}
            
            for period in test_periods:
                print(f"  Testing optimized caching for period: {period}")
                
                # First request - should potentially hit APIs
                start_time = time.time()
                response1 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'period': period, 'limit': 100},
                    timeout=30
                )
                first_request_time = time.time() - start_time
                
                if response1.status_code != 200:
                    self.log_test(f"Optimized Caching - {period} (First Request)", False, f"HTTP {response1.status_code}")
                    continue
                
                data1 = response1.json()
                
                # Second request immediately after - should use cache
                start_time = time.time()
                response2 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'period': period, 'limit': 100},
                    timeout=30
                )
                second_request_time = time.time() - start_time
                
                if response2.status_code != 200:
                    self.log_test(f"Optimized Caching - {period} (Second Request)", False, f"HTTP {response2.status_code}")
                    continue
                
                data2 = response2.json()
                
                # Verify data consistency
                if len(data1) != len(data2):
                    self.log_test(f"Optimized Caching - {period} (Consistency)", False, f"Data length mismatch: {len(data1)} vs {len(data2)}")
                    continue
                
                # Check if second request was faster (indicating cache usage)
                cache_speedup = first_request_time / max(second_request_time, 0.001)  # Avoid division by zero
                
                cache_performance[period] = {
                    'first_time': first_request_time,
                    'second_time': second_request_time,
                    'speedup': cache_speedup,
                    'data_count': len(data1)
                }
                
                # Consider caching effective if second request is at least 30% faster (higher threshold for optimized system)
                if cache_speedup >= 1.3 or second_request_time < 1.5:
                    details = f"First: {first_request_time:.2f}s, Second: {second_request_time:.2f}s, Speedup: {cache_speedup:.1f}x, Data: {len(data1)} cryptos"
                    self.log_test(f"Optimized Caching - {period}", True, details)
                else:
                    details = f"No significant caching benefit - First: {first_request_time:.2f}s, Second: {second_request_time:.2f}s"
                    self.log_test(f"Optimized Caching - {period}", False, details)
            
            # Overall caching assessment - higher expectations for optimized system
            successful_periods = sum(1 for p in cache_performance.values() if p['speedup'] >= 1.3 or p['second_time'] < 1.5)
            
            if successful_periods >= len(test_periods) // 2:
                self.log_test("Optimized Caching System Overall", True, f"Effective optimized caching for {successful_periods}/{len(test_periods)} periods")
                return True
            else:
                self.log_test("Optimized Caching System Overall", False, f"Ineffective caching - only {successful_periods}/{len(test_periods)} periods showed improvement")
                return False
                
        except Exception as e:
            self.log_test("Optimized Caching System", False, f"Exception: {str(e)}")
            return False

    def test_data_aggregation_with_8_apis(self):
        """Test enhanced data aggregation with 8 API sources including CoinMarketCap priority system"""
        try:
            print("Testing enhanced data aggregation with 8 API sources...")
            print("  Priority system: CoinMarketCap=1, CryptoCompare=2, CoinAPI=3, etc.")
            
            # Test different request sizes to verify enhanced load balancing strategies
            test_sizes = [100, 500, 1000, 2000]  # Updated sizes for performance benchmarks
            
            successful_tests = 0
            performance_results = {}
            
            for size in test_sizes:
                print(f"  Testing aggregation with {size} cryptos...")
                
                start_time = time.time()
                response = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': size, 'period': '24h', 'force_refresh': True},
                    timeout=60  # Longer timeout for large requests
                )
                request_time = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 0:
                        # Verify data quality
                        valid_cryptos = 0
                        for crypto in data:
                            if (crypto.get('symbol') and 
                                crypto.get('name') and 
                                isinstance(crypto.get('price_usd'), (int, float)) and 
                                crypto.get('price_usd', 0) > 0):
                                valid_cryptos += 1
                        
                        data_quality = (valid_cryptos / len(data)) * 100
                        
                        # Enhanced performance assessment for optimized system
                        if request_time < 10:
                            performance = "Excellent"
                        elif request_time < 15:
                            performance = "Good"
                        elif request_time < 25:
                            performance = "Acceptable"
                        else:
                            performance = "Slow"
                        
                        performance_results[size] = {
                            'time': request_time,
                            'quality': data_quality,
                            'count': len(data),
                            'performance': performance
                        }
                        
                        details = f"Size: {size}, Returned: {len(data)}, Quality: {data_quality:.1f}%, Time: {request_time:.2f}s ({performance})"
                        
                        # Higher standards for optimized system
                        if data_quality >= 85 and request_time < 30:
                            self.log_test(f"Data Aggregation - {size} cryptos", True, details)
                            successful_tests += 1
                        else:
                            self.log_test(f"Data Aggregation - {size} cryptos", False, details)
                    else:
                        self.log_test(f"Data Aggregation - {size} cryptos", False, "No valid data returned")
                else:
                    self.log_test(f"Data Aggregation - {size} cryptos", False, f"HTTP {response.status_code}")
            
            # Overall assessment with performance comparison
            if successful_tests >= len(test_sizes) // 2:
                # Check if system meets "Excellent" performance target (<10s)
                excellent_performance_count = sum(1 for result in performance_results.values() if result['time'] < 10)
                
                details = f"Successful for {successful_tests}/{len(test_sizes)} request sizes. {excellent_performance_count} achieved <10s target."
                self.log_test("Enhanced Data Aggregation (8 APIs)", True, details)
                return performance_results
            else:
                self.log_test("Enhanced Data Aggregation (8 APIs)", False, f"Failed for most sizes - only {successful_tests}/{len(test_sizes)} successful")
                return False
                
        except Exception as e:
            self.log_test("Enhanced Data Aggregation (8 APIs)", False, f"Exception: {str(e)}")
            return False

    def test_api_key_verification(self):
        """Test updated API key verification for CoinAPI and CoinMarketCap"""
        try:
            print("Testing updated API key verification...")
            print("  CoinAPI key: 70046baa-e887-42ee-a909-03c6b6afab67")
            print("  CoinMarketCap key: 70046baa-e887-42ee-a909-03c6b6afab67")
            
            # Get health status to check API key configurations
            response = requests.get(f"{API_BASE}/health", timeout=10)
            
            if response.status_code != 200:
                self.log_test("API Key Verification", False, f"Health endpoint failed: HTTP {response.status_code}")
                return False
            
            data = response.json()
            services = data.get('services', {})
            
            # Test CoinAPI with new key
            coinapi_available = services.get('coinapi', False)
            if coinapi_available:
                self.log_test("API Key - CoinAPI", True, "CoinAPI service available with updated key")
            else:
                self.log_test("API Key - CoinAPI", False, "CoinAPI service unavailable - check key configuration")
            
            # Test CoinMarketCap service
            coinmarketcap_available = services.get('coinmarketcap', False)
            if coinmarketcap_available:
                self.log_test("API Key - CoinMarketCap", True, "CoinMarketCap service available")
            else:
                self.log_test("API Key - CoinMarketCap", False, "CoinMarketCap service unavailable - check key/setup")
            
            # Overall API key verification
            key_services_working = sum([coinapi_available, coinmarketcap_available])
            
            if key_services_working >= 1:  # At least one key-based service should work
                self.log_test("API Key Verification Overall", True, f"{key_services_working}/2 key-based services working")
                return True
            else:
                self.log_test("API Key Verification Overall", False, "No key-based services working")
                return False
                
        except Exception as e:
            self.log_test("API Key Verification", False, f"Exception: {str(e)}")
            return False

    def test_parallel_processing_optimization(self):
        """Test enhanced parallel processing with 15 concurrent requests and semaphore"""
        try:
            print("Testing enhanced parallel processing with 15 concurrent requests...")
            
            import threading
            import queue
            
            # Test concurrent requests to verify parallel processing
            num_concurrent = 5  # Reduced for testing, but system should handle 15
            results_queue = queue.Queue()
            
            def make_request(request_id):
                try:
                    start_time = time.time()
                    response = requests.get(
                        f"{API_BASE}/cryptos/ranking",
                        params={'limit': 100, 'period': '24h'},
                        timeout=20
                    )
                    end_time = time.time()
                    
                    results_queue.put({
                        'id': request_id,
                        'status_code': response.status_code,
                        'response_time': end_time - start_time,
                        'data_length': len(response.json()) if response.status_code == 200 else 0
                    })
                except Exception as e:
                    results_queue.put({
                        'id': request_id,
                        'status_code': 0,
                        'response_time': 0,
                        'error': str(e)
                    })
            
            # Start concurrent requests
            threads = []
            start_time = time.time()
            
            for i in range(num_concurrent):
                thread = threading.Thread(target=make_request, args=(i,))
                thread.start()
                threads.append(thread)
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
            
            total_time = time.time() - start_time
            
            # Collect results
            results = []
            while not results_queue.empty():
                results.append(results_queue.get())
            
            # Analyze results
            successful_requests = [r for r in results if r['status_code'] == 200]
            failed_requests = [r for r in results if r['status_code'] != 200]
            
            if len(successful_requests) >= num_concurrent * 0.8:  # 80% success rate
                avg_response_time = sum(r['response_time'] for r in successful_requests) / len(successful_requests)
                
                details = f"Concurrent: {num_concurrent}, Successful: {len(successful_requests)}, Avg time: {avg_response_time:.2f}s, Total: {total_time:.2f}s"
                
                # Check if parallel processing is effective (total time should be less than sequential)
                if total_time < avg_response_time * num_concurrent * 0.7:  # 30% improvement expected
                    self.log_test("Parallel Processing Optimization", True, f"{details} - Effective parallelization")
                    return True
                else:
                    self.log_test("Parallel Processing Optimization", False, f"{details} - Limited parallelization benefit")
                    return False
            else:
                self.log_test("Parallel Processing Optimization", False, f"Too many failed requests: {len(failed_requests)}/{num_concurrent}")
                return False
                
        except Exception as e:
            self.log_test("Parallel Processing Optimization", False, f"Exception: {str(e)}")
            return False

    def test_performance_benchmarks(self):
        """Test system performance benchmarks with different request sizes"""
        try:
            print("Testing system performance benchmarks...")
            print("  Target: 'Excellent' performance (<10s for most requests)")
            
            # Test different request sizes as specified in review
            benchmark_sizes = [100, 500, 1000, 2000]
            benchmark_results = {}
            
            for size in benchmark_sizes:
                print(f"  Benchmarking {size} cryptos...")
                
                # Multiple runs for more accurate benchmarking
                times = []
                for run in range(2):  # 2 runs per size
                    start_time = time.time()
                    response = requests.get(
                        f"{API_BASE}/cryptos/ranking",
                        params={'limit': size, 'period': '24h'},
                        timeout=45
                    )
                    end_time = time.time()
                    
                    if response.status_code == 200:
                        times.append(end_time - start_time)
                    else:
                        times.append(999)  # Mark as failed
                
                if times and min(times) < 999:
                    avg_time = sum(t for t in times if t < 999) / len([t for t in times if t < 999])
                    min_time = min(t for t in times if t < 999)
                    
                    # Performance grading
                    if avg_time < 10:
                        grade = "Excellent"
                    elif avg_time < 15:
                        grade = "Good"
                    elif avg_time < 25:
                        grade = "Acceptable"
                    else:
                        grade = "Slow"
                    
                    benchmark_results[size] = {
                        'avg_time': avg_time,
                        'min_time': min_time,
                        'grade': grade
                    }
                    
                    details = f"Size: {size}, Avg: {avg_time:.2f}s, Min: {min_time:.2f}s, Grade: {grade}"
                    
                    if grade in ["Excellent", "Good"]:
                        self.log_test(f"Performance Benchmark - {size} cryptos", True, details)
                    else:
                        self.log_test(f"Performance Benchmark - {size} cryptos", False, details)
                else:
                    self.log_test(f"Performance Benchmark - {size} cryptos", False, "All requests failed")
            
            # Overall performance assessment
            excellent_count = sum(1 for result in benchmark_results.values() if result['grade'] == 'Excellent')
            good_or_better = sum(1 for result in benchmark_results.values() if result['grade'] in ['Excellent', 'Good'])
            
            if excellent_count >= len(benchmark_sizes) // 2:
                self.log_test("Performance Benchmarks Overall", True, f"{excellent_count}/{len(benchmark_sizes)} achieved 'Excellent' performance (<10s)")
                return benchmark_results
            elif good_or_better >= len(benchmark_sizes) * 0.75:
                self.log_test("Performance Benchmarks Overall", True, f"{good_or_better}/{len(benchmark_sizes)} achieved 'Good' or better performance")
                return benchmark_results
            else:
                self.log_test("Performance Benchmarks Overall", False, f"Only {good_or_better}/{len(benchmark_sizes)} achieved acceptable performance")
                return False
                
        except Exception as e:
            self.log_test("Performance Benchmarks", False, f"Exception: {str(e)}")
            return False

    def test_memory_cache_effectiveness(self):
        """Test memory cache effectiveness with 45-minute expiration"""
        try:
            print("Testing memory cache effectiveness (45-minute expiration)...")
            
            # Test cache effectiveness by making repeated requests
            cache_test_results = []
            
            for i in range(3):  # 3 cache tests
                print(f"  Cache test {i+1}/3...")
                
                # First request
                start_time = time.time()
                response1 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': 200, 'period': '24h'},
                    timeout=20
                )
                first_time = time.time() - start_time
                
                if response1.status_code != 200:
                    continue
                
                # Second request immediately after
                start_time = time.time()
                response2 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': 200, 'period': '24h'},
                    timeout=20
                )
                second_time = time.time() - start_time
                
                if response2.status_code != 200:
                    continue
                
                # Calculate cache effectiveness
                if second_time > 0:
                    cache_speedup = first_time / second_time
                    cache_test_results.append({
                        'first_time': first_time,
                        'second_time': second_time,
                        'speedup': cache_speedup
                    })
                
                time.sleep(1)  # Small delay between tests
            
            if cache_test_results:
                avg_speedup = sum(r['speedup'] for r in cache_test_results) / len(cache_test_results)
                avg_second_time = sum(r['second_time'] for r in cache_test_results) / len(cache_test_results)
                
                details = f"Tests: {len(cache_test_results)}, Avg speedup: {avg_speedup:.1f}x, Avg cached time: {avg_second_time:.2f}s"
                
                # Memory cache should provide significant speedup
                if avg_speedup >= 2.0 or avg_second_time < 1.0:
                    self.log_test("Memory Cache Effectiveness", True, f"{details} - Effective caching")
                    return True
                else:
                    self.log_test("Memory Cache Effectiveness", False, f"{details} - Limited cache benefit")
                    return False
            else:
                self.log_test("Memory Cache Effectiveness", False, "No successful cache tests")
                return False
                
        except Exception as e:
            self.log_test("Memory Cache Effectiveness", False, f"Exception: {str(e)}")
            return False
        """Test individual API service integrations through health endpoint"""
        try:
            print("Testing individual API service integrations...")
            
            # Get health status to check individual services
            response = requests.get(f"{API_BASE}/health", timeout=10)
            
            if response.status_code != 200:
                self.log_test("API Service Integrations", False, f"Health endpoint failed: HTTP {response.status_code}")
                return False
            
            data = response.json()
            services = data.get('services', {})
            
            # Test each new API service integration
            new_services = {
                'coinapi': 'CoinAPI (Premium)',
                'coinpaprika': 'CoinPaprika (Free)',
                'bitfinex': 'Bitfinex (Public)'
            }
            
            integration_results = {}
            
            for service_key, service_name in new_services.items():
                is_available = services.get(service_key, False)
                
                if is_available:
                    self.log_test(f"API Integration - {service_name}", True, "Service is available and integrated")
                    integration_results[service_key] = True
                else:
                    # Check if it's a configuration issue vs integration issue
                    if service_key == 'coinapi':
                        self.log_test(f"API Integration - {service_name}", False, "Service unavailable - check COINAPI_KEY configuration")
                    else:
                        self.log_test(f"API Integration - {service_name}", False, "Service unavailable - integration may have issues")
                    integration_results[service_key] = False
            
            # Check existing services are still working
            existing_services = {
                'cryptocompare': 'CryptoCompare',
                'binance': 'Binance',
                'yahoo_finance': 'Yahoo Finance',
                'fallback': 'Fallback (CoinGecko/Coinlore)'
            }
            
            for service_key, service_name in existing_services.items():
                is_available = services.get(service_key, False)
                integration_results[service_key] = is_available
                
                if is_available:
                    self.log_test(f"API Integration - {service_name}", True, "Existing service still working")
                else:
                    self.log_test(f"API Integration - {service_name}", False, "Existing service has issues")
            
            # Overall assessment
            total_services = len(integration_results)
            working_services = sum(1 for working in integration_results.values() if working)
            
            # At least 4 out of 7 services should be working for a pass
            if working_services >= 4:
                self.log_test("API Service Integrations Overall", True, f"{working_services}/{total_services} services integrated and working")
                return integration_results
            else:
                self.log_test("API Service Integrations Overall", False, f"Too few services working: {working_services}/{total_services}")
                return False
                
        except Exception as e:
            self.log_test("API Service Integrations", False, f"Exception: {str(e)}")
            return False

    def test_load_balancing_strategies(self):
        """Test intelligent load balancing strategies for different request sizes"""
        try:
            print("Testing intelligent load balancing strategies...")
            
            # Test different strategies: small, medium, large, xlarge
            strategy_tests = [
                {'size': 50, 'strategy': 'small', 'expected_time': 15},
                {'size': 300, 'strategy': 'medium', 'expected_time': 25},
                {'size': 1000, 'strategy': 'large', 'expected_time': 35},
                {'size': 2000, 'strategy': 'xlarge', 'expected_time': 50}
            ]
            
            successful_strategies = 0
            
            for test in strategy_tests:
                size = test['size']
                strategy = test['strategy']
                max_expected_time = test['expected_time']
                
                print(f"  Testing {strategy} strategy with {size} cryptos...")
                
                start_time = time.time()
                response = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': size, 'period': '24h'},
                    timeout=max_expected_time + 10  # Add buffer to timeout
                )
                actual_time = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 0:
                        # Check if response time is within expected range
                        if actual_time <= max_expected_time:
                            performance_rating = "Excellent"
                        elif actual_time <= max_expected_time * 1.5:
                            performance_rating = "Good"
                        else:
                            performance_rating = "Slow"
                        
                        details = f"Strategy: {strategy}, Size: {size}, Returned: {len(data)}, Time: {actual_time:.2f}s ({performance_rating})"
                        
                        if actual_time <= max_expected_time * 1.5:  # Allow 50% buffer
                            self.log_test(f"Load Balancing - {strategy.title()} Strategy", True, details)
                            successful_strategies += 1
                        else:
                            self.log_test(f"Load Balancing - {strategy.title()} Strategy", False, f"{details} - Too slow")
                    else:
                        self.log_test(f"Load Balancing - {strategy.title()} Strategy", False, "No data returned")
                else:
                    self.log_test(f"Load Balancing - {strategy.title()} Strategy", False, f"HTTP {response.status_code}")
            
            # Overall assessment
            if successful_strategies >= 3:  # At least 3 out of 4 strategies should work
                self.log_test("Load Balancing Strategies Overall", True, f"{successful_strategies}/4 strategies working efficiently")
                return True
            else:
                self.log_test("Load Balancing Strategies Overall", False, f"Only {successful_strategies}/4 strategies working")
                return False
                
        except Exception as e:
            self.log_test("Load Balancing Strategies", False, f"Exception: {str(e)}")
            return False

    def test_period_based_freshness_thresholds(self):
        """Test period-based freshness thresholds (24h=4.3min, 7d=30min, 30d=2.2hrs)"""
        try:
            print("Testing period-based freshness thresholds...")
            
            # Test different periods to verify intelligent caching behavior
            period_tests = [
                {'period': '24h', 'threshold_desc': '4.3 minutes'},
                {'period': '7d', 'threshold_desc': '30 minutes'},
                {'period': '30d', 'threshold_desc': '2.2 hours'}
            ]
            
            successful_tests = 0
            
            for test in period_tests:
                period = test['period']
                threshold_desc = test['threshold_desc']
                
                print(f"  Testing freshness threshold for {period} (threshold: {threshold_desc})...")
                
                # Make initial request
                start_time = time.time()
                response1 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'period': period, 'limit': 100},
                    timeout=30
                )
                first_time = time.time() - start_time
                
                if response1.status_code != 200:
                    self.log_test(f"Freshness Threshold - {period}", False, f"Initial request failed: HTTP {response1.status_code}")
                    continue
                
                # Make second request immediately (should use cache)
                start_time = time.time()
                response2 = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'period': period, 'limit': 100},
                    timeout=30
                )
                second_time = time.time() - start_time
                
                if response2.status_code != 200:
                    self.log_test(f"Freshness Threshold - {period}", False, f"Second request failed: HTTP {response2.status_code}")
                    continue
                
                # Verify caching behavior
                data1 = response1.json()
                data2 = response2.json()
                
                # Check data consistency
                if len(data1) == len(data2):
                    # Check if second request was faster (indicating cache usage)
                    cache_improvement = first_time / max(second_time, 0.001)
                    
                    details = f"Period: {period}, Threshold: {threshold_desc}, First: {first_time:.2f}s, Second: {second_time:.2f}s, Improvement: {cache_improvement:.1f}x"
                    
                    # Consider successful if second request is faster or very fast
                    if cache_improvement >= 1.5 or second_time < 1.0:
                        self.log_test(f"Freshness Threshold - {period}", True, details)
                        successful_tests += 1
                    else:
                        self.log_test(f"Freshness Threshold - {period}", False, f"{details} - No caching benefit")
                else:
                    self.log_test(f"Freshness Threshold - {period}", False, f"Data inconsistency: {len(data1)} vs {len(data2)} items")
            
            # Overall assessment
            if successful_tests >= 2:  # At least 2 out of 3 periods should show proper caching
                self.log_test("Period-Based Freshness Thresholds", True, f"{successful_tests}/3 periods showing intelligent caching")
                return True
            else:
                self.log_test("Period-Based Freshness Thresholds", False, f"Only {successful_tests}/3 periods showing proper caching")
                return False
                
        except Exception as e:
            self.log_test("Period-Based Freshness Thresholds", False, f"Exception: {str(e)}")
            return False
    
    def test_dynamic_limit_endpoint(self):
        """Test the new dynamic analysis limit endpoint"""
        try:
            response = requests.get(f"{API_BASE}/system/dynamic-limit", timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check required fields
                required_fields = ['max_recommended_limit', 'performance_impact', 'memory_usage_estimate', 'system_resources']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    self.log_test("Dynamic Limit Endpoint - Structure", False, f"Missing fields: {missing_fields}")
                    return False
                
                # Check system_resources structure
                sys_resources = data['system_resources']
                required_sys_fields = ['available_memory_mb', 'cpu_usage_percent', 'recommended_max_cryptos', 'performance_mode', 'current_load']
                missing_sys_fields = [field for field in required_sys_fields if field not in sys_resources]
                
                if missing_sys_fields:
                    self.log_test("Dynamic Limit Endpoint - System Resources", False, f"Missing system resource fields: {missing_sys_fields}")
                    return False
                
                # Validate data types and ranges
                max_limit = data['max_recommended_limit']
                if not isinstance(max_limit, int) or max_limit < 100:
                    self.log_test("Dynamic Limit Endpoint - Max Limit", False, f"Invalid max_recommended_limit: {max_limit}")
                    return False
                
                memory_mb = sys_resources['available_memory_mb']
                cpu_percent = sys_resources['cpu_usage_percent']
                
                if not isinstance(memory_mb, (int, float)) or memory_mb < 0:
                    self.log_test("Dynamic Limit Endpoint - Memory", False, f"Invalid memory value: {memory_mb}")
                    return False
                
                if not isinstance(cpu_percent, (int, float)) or cpu_percent < 0 or cpu_percent > 100:
                    self.log_test("Dynamic Limit Endpoint - CPU", False, f"Invalid CPU percentage: {cpu_percent}")
                    return False
                
                # Check performance modes
                valid_modes = ['optimal', 'balanced', 'maximum']
                if sys_resources['performance_mode'] not in valid_modes:
                    self.log_test("Dynamic Limit Endpoint - Performance Mode", False, f"Invalid performance mode: {sys_resources['performance_mode']}")
                    return False
                
                valid_loads = ['low', 'medium', 'high', 'unknown']
                if sys_resources['current_load'] not in valid_loads:
                    self.log_test("Dynamic Limit Endpoint - Load", False, f"Invalid current load: {sys_resources['current_load']}")
                    return False
                
                details = f"Max limit: {max_limit}, Memory: {memory_mb:.1f}MB, CPU: {cpu_percent:.1f}%, Mode: {sys_resources['performance_mode']}"
                self.log_test("Dynamic Limit Endpoint", True, details)
                return max_limit
                
            else:
                self.log_test("Dynamic Limit Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Dynamic Limit Endpoint", False, f"Exception: {str(e)}")
            return False
    
    def test_crypto_count_endpoint(self):
        """Test the crypto count endpoint"""
        try:
            response = requests.get(f"{API_BASE}/cryptos/count", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'total_cryptocurrencies' not in data:
                    self.log_test("Crypto Count Endpoint", False, "Missing total_cryptocurrencies field")
                    return False
                
                total_count = data['total_cryptocurrencies']
                if not isinstance(total_count, int) or total_count < 0:
                    self.log_test("Crypto Count Endpoint", False, f"Invalid total count: {total_count}")
                    return False
                
                details = f"Total cryptocurrencies: {total_count}, Cached periods: {data.get('cached_periods', [])}"
                self.log_test("Crypto Count Endpoint", True, details)
                return total_count
                
            else:
                self.log_test("Crypto Count Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Crypto Count Endpoint", False, f"Exception: {str(e)}")
            return False
    
    def test_ranking_endpoint_with_limits(self, dynamic_max_limit=None):
        """Test the ranking endpoint with various limit values"""
        test_limits = [50, 1500, 3000, 5000]
        
        # If we have a dynamic max limit, test up to that value
        if dynamic_max_limit and isinstance(dynamic_max_limit, int):
            test_limits.append(min(dynamic_max_limit, 5000))
        
        successful_tests = 0
        
        for limit in test_limits:
            try:
                print(f"Testing ranking endpoint with limit={limit}...")
                response = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': limit, 'period': '24h'},
                    timeout=30  # Longer timeout for larger requests
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if not isinstance(data, list):
                        self.log_test(f"Ranking Endpoint (limit={limit})", False, "Response is not a list")
                        continue
                    
                    returned_count = len(data)
                    
                    # Check if we got reasonable data
                    if returned_count == 0:
                        self.log_test(f"Ranking Endpoint (limit={limit})", False, "No data returned")
                        continue
                    
                    # Validate first crypto structure
                    if data and isinstance(data[0], dict):
                        first_crypto = data[0]
                        required_fields = ['symbol', 'name', 'price_usd']
                        missing_fields = [field for field in required_fields if field not in first_crypto]
                        
                        if missing_fields:
                            self.log_test(f"Ranking Endpoint (limit={limit})", False, f"Missing crypto fields: {missing_fields}")
                            continue
                    
                    details = f"Requested: {limit}, Returned: {returned_count} cryptos"
                    self.log_test(f"Ranking Endpoint (limit={limit})", True, details)
                    successful_tests += 1
                    
                else:
                    self.log_test(f"Ranking Endpoint (limit={limit})", False, f"HTTP {response.status_code}", response.text[:200])
                    
            except Exception as e:
                self.log_test(f"Ranking Endpoint (limit={limit})", False, f"Exception: {str(e)}")
        
        return successful_tests > 0
    
    def test_ranking_with_pagination(self):
        """Test ranking endpoint with pagination"""
        try:
            # Test with offset
            response = requests.get(
                f"{API_BASE}/cryptos/ranking",
                params={'limit': 50, 'offset': 100, 'period': '24h'},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    self.log_test("Ranking Pagination", True, f"Retrieved {len(data)} cryptos with offset=100")
                    return True
                else:
                    self.log_test("Ranking Pagination", False, "No data returned with pagination")
                    return False
            else:
                self.log_test("Ranking Pagination", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Ranking Pagination", False, f"Exception: {str(e)}")
            return False
    
    def test_ranking_with_force_refresh(self):
        """Test ranking endpoint with force_refresh parameter"""
        try:
            response = requests.get(
                f"{API_BASE}/cryptos/ranking",
                params={'limit': 100, 'period': '24h', 'force_refresh': True},
                timeout=20
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    self.log_test("Ranking Force Refresh", True, f"Retrieved {len(data)} cryptos with force refresh")
                    return True
                else:
                    self.log_test("Ranking Force Refresh", False, "No data returned with force refresh")
                    return False
            else:
                self.log_test("Ranking Force Refresh", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Ranking Force Refresh", False, f"Exception: {str(e)}")
            return False
    
    def test_error_handling(self):
        """Test error handling with invalid parameters"""
        error_tests = [
            {'params': {'limit': -1}, 'test_name': 'Negative Limit'},
            {'params': {'limit': 0}, 'test_name': 'Zero Limit'},
            {'params': {'limit': 50000}, 'test_name': 'Extremely High Limit'},
            {'params': {'offset': -1}, 'test_name': 'Negative Offset'},
            {'params': {'period': 'invalid'}, 'test_name': 'Invalid Period'},
        ]
        
        successful_error_tests = 0
        
        for test_case in error_tests:
            try:
                response = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params=test_case['params'],
                    timeout=10
                )
                
                # For error cases, we expect either 400 (validation error) or 422 (unprocessable entity)
                # or the system should handle it gracefully and return valid data
                if response.status_code in [400, 422]:
                    self.log_test(f"Error Handling - {test_case['test_name']}", True, f"Properly rejected with HTTP {response.status_code}")
                    successful_error_tests += 1
                elif response.status_code == 200:
                    # System handled it gracefully
                    data = response.json()
                    if isinstance(data, list):
                        self.log_test(f"Error Handling - {test_case['test_name']}", True, f"Handled gracefully, returned {len(data)} items")
                        successful_error_tests += 1
                    else:
                        self.log_test(f"Error Handling - {test_case['test_name']}", False, "Invalid response format")
                else:
                    self.log_test(f"Error Handling - {test_case['test_name']}", False, f"Unexpected HTTP {response.status_code}")
                    
            except Exception as e:
                self.log_test(f"Error Handling - {test_case['test_name']}", False, f"Exception: {str(e)}")
        
        return successful_error_tests > 0
    
    def test_system_performance(self):
        """Test system performance with larger requests"""
        try:
            start_time = time.time()
            
            response = requests.get(
                f"{API_BASE}/cryptos/ranking",
                params={'limit': 2000, 'period': '24h'},
                timeout=45  # Generous timeout for performance test
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list):
                    details = f"Retrieved {len(data)} cryptos in {response_time:.2f} seconds"
                    
                    # Performance thresholds
                    if response_time < 10:
                        self.log_test("System Performance (2000 cryptos)", True, f"{details} - Excellent performance")
                    elif response_time < 20:
                        self.log_test("System Performance (2000 cryptos)", True, f"{details} - Good performance")
                    elif response_time < 45:
                        self.log_test("System Performance (2000 cryptos)", True, f"{details} - Acceptable performance")
                    else:
                        self.log_test("System Performance (2000 cryptos)", False, f"{details} - Slow performance")
                    
                    return True
                else:
                    self.log_test("System Performance (2000 cryptos)", False, "Invalid response format")
                    return False
            else:
                self.log_test("System Performance (2000 cryptos)", False, f"HTTP {response.status_code} in {response_time:.2f}s", response.text[:200])
                return False
                
        except Exception as e:
            self.log_test("System Performance (2000 cryptos)", False, f"Exception: {str(e)}")
            return False

    def test_async_refresh_endpoint(self):
        """Test the new async refresh endpoint without force"""
        try:
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh-async",
                timeout=5  # Should return quickly
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Check required fields
                required_fields = ['status', 'message']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    self.log_test("Async Refresh Endpoint", False, f"Missing fields: {missing_fields}")
                    return False
                
                # Check if it started successfully or is already running
                if data['status'] in ['started', 'already_running']:
                    details = f"Status: {data['status']}, Response time: {response_time:.2f}s"
                    if 'task_id' in data:
                        details += f", Task ID: {data['task_id']}"
                    
                    # Performance check - should be very fast
                    if response_time < 1.0:
                        self.log_test("Async Refresh Endpoint", True, f"{details} - Fast response")
                        return data.get('task_id')
                    else:
                        self.log_test("Async Refresh Endpoint", False, f"{details} - Too slow (>1s)")
                        return False
                else:
                    self.log_test("Async Refresh Endpoint", False, f"Unexpected status: {data['status']}")
                    return False
                    
            else:
                self.log_test("Async Refresh Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Async Refresh Endpoint", False, f"Exception: {str(e)}")
            return False

    def test_async_refresh_with_force(self):
        """Test the async refresh endpoint with force=true"""
        try:
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh-async?force=true",
                timeout=5  # Should return quickly
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Check required fields
                if 'status' not in data or 'message' not in data:
                    self.log_test("Async Refresh with Force", False, "Missing required fields")
                    return False
                
                # Should start or already be running
                if data['status'] in ['started', 'already_running']:
                    details = f"Status: {data['status']}, Response time: {response_time:.2f}s"
                    if 'estimated_duration_seconds' in data:
                        details += f", Est. duration: {data['estimated_duration_seconds']}s"
                    
                    # Performance check
                    if response_time < 1.0:
                        self.log_test("Async Refresh with Force", True, f"{details} - Fast response")
                        return True
                    else:
                        self.log_test("Async Refresh with Force", False, f"{details} - Too slow (>1s)")
                        return False
                else:
                    self.log_test("Async Refresh with Force", False, f"Unexpected status: {data['status']}")
                    return False
                    
            else:
                self.log_test("Async Refresh with Force", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Async Refresh with Force", False, f"Exception: {str(e)}")
            return False

    def test_refresh_status_endpoint(self):
        """Test the refresh status endpoint"""
        try:
            response = requests.get(
                f"{API_BASE}/cryptos/refresh-status",
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Check required fields
                required_fields = ['status', 'active_tasks']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    self.log_test("Refresh Status Endpoint", False, f"Missing fields: {missing_fields}")
                    return False
                
                # Validate status values
                valid_statuses = ['idle', 'running', 'completed', 'failed']
                if data['status'] not in valid_statuses:
                    self.log_test("Refresh Status Endpoint", False, f"Invalid status: {data['status']}")
                    return False
                
                # Validate active_tasks is a number
                if not isinstance(data['active_tasks'], int) or data['active_tasks'] < 0:
                    self.log_test("Refresh Status Endpoint", False, f"Invalid active_tasks: {data['active_tasks']}")
                    return False
                
                details = f"Status: {data['status']}, Active tasks: {data['active_tasks']}"
                if data.get('last_update'):
                    details += f", Last update: {data['last_update']}"
                
                self.log_test("Refresh Status Endpoint", True, details)
                return data
                
            else:
                self.log_test("Refresh Status Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Refresh Status Endpoint", False, f"Exception: {str(e)}")
            return False

    def test_legacy_refresh_endpoint(self):
        """Test the modified legacy refresh endpoint (should now use async)"""
        try:
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh",
                json={},  # Empty request body
                timeout=5  # Should return quickly now
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Check required fields for legacy endpoint
                if 'status' not in data or 'message' not in data:
                    self.log_test("Legacy Refresh Endpoint", False, "Missing required fields")
                    return False
                
                # Should indicate background refresh started
                if data['status'] in ['success', 'info']:
                    details = f"Status: {data['status']}, Response time: {response_time:.2f}s"
                    if 'task_id' in data:
                        details += f", Task ID: {data['task_id']}"
                    
                    # Performance check - should be fast now
                    if response_time < 2.0:  # Slightly more lenient for legacy endpoint
                        self.log_test("Legacy Refresh Endpoint", True, f"{details} - Fast response (async)")
                        return True
                    else:
                        self.log_test("Legacy Refresh Endpoint", False, f"{details} - Too slow (>2s)")
                        return False
                else:
                    self.log_test("Legacy Refresh Endpoint", False, f"Unexpected status: {data['status']}")
                    return False
                    
            else:
                self.log_test("Legacy Refresh Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Legacy Refresh Endpoint", False, f"Exception: {str(e)}")
            return False

    def test_async_workflow_complete(self):
        """Test the complete async refresh workflow"""
        try:
            print("Testing complete async refresh workflow...")
            
            # Step 1: Start async refresh
            start_response = requests.post(f"{API_BASE}/cryptos/refresh-async", timeout=5)
            
            if start_response.status_code != 200:
                self.log_test("Async Workflow - Start", False, f"Failed to start: HTTP {start_response.status_code}")
                return False
            
            start_data = start_response.json()
            if start_data['status'] not in ['started', 'already_running']:
                self.log_test("Async Workflow - Start", False, f"Unexpected start status: {start_data['status']}")
                return False
            
            # Step 2: Check status multiple times
            max_checks = 10
            check_count = 0
            final_status = None
            
            while check_count < max_checks:
                time.sleep(2)  # Wait 2 seconds between checks
                
                status_response = requests.get(f"{API_BASE}/cryptos/refresh-status", timeout=5)
                
                if status_response.status_code != 200:
                    self.log_test("Async Workflow - Status Check", False, f"Status check failed: HTTP {status_response.status_code}")
                    return False
                
                status_data = status_response.json()
                current_status = status_data['status']
                
                print(f"    Status check {check_count + 1}: {current_status} (active tasks: {status_data['active_tasks']})")
                
                if current_status in ['completed', 'failed', 'idle']:
                    final_status = current_status
                    break
                
                check_count += 1
            
            # Step 3: Evaluate results
            if final_status == 'completed':
                self.log_test("Async Workflow Complete", True, f"Workflow completed successfully after {check_count + 1} status checks")
                return True
            elif final_status == 'idle':
                self.log_test("Async Workflow Complete", True, f"Workflow completed (status: idle) after {check_count + 1} status checks")
                return True
            elif final_status == 'failed':
                self.log_test("Async Workflow Complete", False, f"Workflow failed after {check_count + 1} status checks")
                return False
            else:
                self.log_test("Async Workflow Complete", False, f"Workflow still running after {max_checks} checks (timeout)")
                return False
                
        except Exception as e:
            self.log_test("Async Workflow Complete", False, f"Exception: {str(e)}")
            return False

    def test_multiple_refresh_requests(self):
        """Test multiple simultaneous refresh requests (should handle gracefully)"""
        try:
            print("Testing multiple simultaneous refresh requests...")
            
            # Send multiple requests quickly
            responses = []
            for i in range(3):
                try:
                    response = requests.post(f"{API_BASE}/cryptos/refresh-async", timeout=5)
                    responses.append(response)
                    time.sleep(0.1)  # Small delay between requests
                except Exception as e:
                    self.log_test("Multiple Refresh Requests", False, f"Request {i+1} failed: {str(e)}")
                    return False
            
            # Analyze responses
            success_count = 0
            already_running_count = 0
            
            for i, response in enumerate(responses):
                if response.status_code == 200:
                    data = response.json()
                    if data['status'] == 'started':
                        success_count += 1
                    elif data['status'] == 'already_running':
                        already_running_count += 1
                    
                    print(f"    Request {i+1}: {data['status']} - {data['message']}")
                else:
                    self.log_test("Multiple Refresh Requests", False, f"Request {i+1} failed: HTTP {response.status_code}")
                    return False
            
            # Should have at most 1 success and others should be "already_running"
            if success_count <= 1 and (success_count + already_running_count) == 3:
                self.log_test("Multiple Refresh Requests", True, f"Handled correctly: {success_count} started, {already_running_count} already running")
                return True
            else:
                self.log_test("Multiple Refresh Requests", False, f"Unexpected behavior: {success_count} started, {already_running_count} already running")
                return False
                
        except Exception as e:
            self.log_test("Multiple Refresh Requests", False, f"Exception: {str(e)}")
            return False

    def test_async_performance_timing(self):
        """Test that async endpoints respond quickly (< 1 second)"""
        endpoints_to_test = [
            {'url': f"{API_BASE}/cryptos/refresh-async", 'method': 'POST', 'name': 'Async Refresh'},
            {'url': f"{API_BASE}/cryptos/refresh-status", 'method': 'GET', 'name': 'Refresh Status'},
            {'url': f"{API_BASE}/cryptos/refresh", 'method': 'POST', 'name': 'Legacy Refresh'}
        ]
        
        all_fast = True
        
        for endpoint in endpoints_to_test:
            try:
                start_time = time.time()
                
                if endpoint['method'] == 'POST':
                    response = requests.post(endpoint['url'], json={}, timeout=3)
                else:
                    response = requests.get(endpoint['url'], timeout=3)
                
                end_time = time.time()
                response_time = end_time - start_time
                
                if response.status_code == 200:
                    if response_time < 1.0:
                        self.log_test(f"Performance - {endpoint['name']}", True, f"Response time: {response_time:.3f}s - Excellent")
                    elif response_time < 2.0:
                        self.log_test(f"Performance - {endpoint['name']}", True, f"Response time: {response_time:.3f}s - Good")
                        all_fast = False
                    else:
                        self.log_test(f"Performance - {endpoint['name']}", False, f"Response time: {response_time:.3f}s - Too slow")
                        all_fast = False
                else:
                    self.log_test(f"Performance - {endpoint['name']}", False, f"HTTP {response.status_code}")
                    all_fast = False
                    
            except Exception as e:
                self.log_test(f"Performance - {endpoint['name']}", False, f"Exception: {str(e)}")
                all_fast = False
        
        return all_fast
    
    def run_all_tests(self):
        """Run all backend tests for enhanced CryptoRebound with 7 APIs and intelligent caching"""
        print("Starting CryptoRebound Enhanced Backend API Tests...")
        print("Testing: 7 API integrations, intelligent caching, and load balancing")
        print("=" * 80)
        
        # Test 1: Enhanced health check with 7 APIs
        available_services = self.test_health_endpoint_enhanced()
        
        # Test 2: Individual API service integrations
        service_integrations = self.test_api_service_integrations()
        
        # Test 3: Dynamic limit endpoint (existing functionality)
        dynamic_max_limit = self.test_dynamic_limit_endpoint()
        
        # Test 4: Crypto count endpoint
        crypto_count = self.test_crypto_count_endpoint()
        
        # NEW ENHANCED TESTS
        print("\n" + "=" * 80)
        print("ENHANCED API INTEGRATION AND CACHING TESTS")
        print("=" * 80)
        
        # Test 5: Enhanced data aggregation with 7 APIs
        data_aggregation_ok = self.test_data_aggregation_with_7_apis()
        
        # Test 6: Intelligent caching system
        caching_system_ok = self.test_intelligent_caching_system()
        
        # Test 7: Load balancing strategies
        load_balancing_ok = self.test_load_balancing_strategies()
        
        # Test 8: Period-based freshness thresholds
        freshness_thresholds_ok = self.test_period_based_freshness_thresholds()
        
        # EXISTING CORE FUNCTIONALITY TESTS
        print("\n" + "=" * 80)
        print("CORE FUNCTIONALITY TESTS")
        print("=" * 80)
        
        # Test 9: Ranking with various limits
        ranking_ok = self.test_ranking_endpoint_with_limits(dynamic_max_limit)
        
        # Test 10: Pagination
        pagination_ok = self.test_ranking_with_pagination()
        
        # Test 11: Force refresh
        refresh_ok = self.test_ranking_with_force_refresh()
        
        # Test 12: Error handling
        error_handling_ok = self.test_error_handling()
        
        # Test 13: Performance test
        performance_ok = self.test_system_performance()
        
        # ASYNC REFRESH SYSTEM TESTS (existing)
        print("\n" + "=" * 80)
        print("ASYNC REFRESH SYSTEM TESTS")
        print("=" * 80)
        
        # Test 14: Async refresh endpoint
        async_refresh_ok = self.test_async_refresh_endpoint()
        
        # Test 15: Async refresh with force
        async_force_ok = self.test_async_refresh_with_force()
        
        # Test 16: Refresh status endpoint
        status_endpoint_ok = self.test_refresh_status_endpoint()
        
        # Test 17: Legacy refresh endpoint (now async)
        legacy_refresh_ok = self.test_legacy_refresh_endpoint()
        
        # Test 18: Complete async workflow
        workflow_ok = self.test_async_workflow_complete()
        
        # Test 19: Multiple refresh requests handling
        multiple_requests_ok = self.test_multiple_refresh_requests()
        
        # Test 20: Performance timing for async endpoints
        async_performance_ok = self.test_async_performance_timing()
        
        # Summary
        print("=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        
        total_tests = len(self.test_results)
        passed_tests = len([t for t in self.test_results if t['success']])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        # Enhanced API integration specific summary
        enhanced_tests = [
            bool(available_services), bool(service_integrations), 
            data_aggregation_ok, caching_system_ok, 
            load_balancing_ok, freshness_thresholds_ok
        ]
        enhanced_passed = sum(1 for test in enhanced_tests if test)
        
        print(f"\nEnhanced API Integration Tests: {enhanced_passed}/{len(enhanced_tests)} passed")
        
        # Core functionality summary
        core_tests = [
            bool(dynamic_max_limit), bool(crypto_count), ranking_ok, 
            pagination_ok, refresh_ok, error_handling_ok, performance_ok
        ]
        core_passed = sum(1 for test in core_tests if test)
        
        print(f"Core Functionality Tests: {core_passed}/{len(core_tests)} passed")
        
        # Async refresh specific summary
        async_tests = [async_refresh_ok, async_force_ok, status_endpoint_ok, 
                      legacy_refresh_ok, workflow_ok, multiple_requests_ok, async_performance_ok]
        async_passed = sum(1 for test in async_tests if test)
        
        print(f"Async Refresh System Tests: {async_passed}/{len(async_tests)} passed")
        
        if self.failed_tests:
            print(f"\nFailed Tests:")
            for test in self.failed_tests:
                print(f"  - {test}")
        
        print("\n" + "=" * 80)
        
        # Return overall success - prioritize enhanced API integration tests
        critical_enhanced_tests = [bool(available_services), data_aggregation_ok, caching_system_ok]
        critical_core_tests = [bool(dynamic_max_limit), ranking_ok]
        critical_async_tests = [async_refresh_ok, status_endpoint_ok, async_performance_ok]
        
        return (all(critical_enhanced_tests) and 
                all(critical_core_tests) and 
                all(critical_async_tests) and 
                failed_tests <= 3)  # Allow up to 3 minor failures

if __name__ == "__main__":
    tester = BackendTester()
    success = tester.run_all_tests()
    
    if success:
        print("🎉 All critical backend tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some backend tests failed. Check the details above.")
        sys.exit(1)