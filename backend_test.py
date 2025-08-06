#!/usr/bin/env python3
"""
Backend API Testing for CryptoRebound Dynamic Analysis Limit Improvements
Tests the new dynamic analysis limit functionality and higher crypto count support
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
    
    def test_health_endpoint(self):
        """Test the health check endpoint"""
        try:
            response = requests.get(f"{API_BASE}/health", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'healthy':
                    self.log_test("Health Check", True, f"Services: {data.get('services', {})}")
                    return True
                else:
                    self.log_test("Health Check", False, f"Unhealthy status: {data}")
                    return False
            else:
                self.log_test("Health Check", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Health Check", False, f"Exception: {str(e)}")
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
    
    def run_all_tests(self):
        """Run all backend tests"""
        print("Starting CryptoRebound Backend API Tests...")
        print("=" * 80)
        
        # Test 1: Health check
        health_ok = self.test_health_endpoint()
        
        # Test 2: Dynamic limit endpoint
        dynamic_max_limit = self.test_dynamic_limit_endpoint()
        
        # Test 3: Crypto count
        crypto_count = self.test_crypto_count_endpoint()
        
        # Test 4: Ranking with various limits
        ranking_ok = self.test_ranking_endpoint_with_limits(dynamic_max_limit)
        
        # Test 5: Pagination
        pagination_ok = self.test_ranking_with_pagination()
        
        # Test 6: Force refresh
        refresh_ok = self.test_ranking_with_force_refresh()
        
        # Test 7: Error handling
        error_handling_ok = self.test_error_handling()
        
        # Test 8: Performance test
        performance_ok = self.test_system_performance()
        
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
        
        if self.failed_tests:
            print(f"\nFailed Tests:")
            for test in self.failed_tests:
                print(f"  - {test}")
        
        print("\n" + "=" * 80)
        
        # Return overall success
        critical_tests = [health_ok, dynamic_max_limit, ranking_ok]
        return all(critical_tests) and failed_tests == 0

if __name__ == "__main__":
    tester = BackendTester()
    success = tester.run_all_tests()
    
    if success:
        print("🎉 All critical backend tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some backend tests failed. Check the details above.")
        sys.exit(1)