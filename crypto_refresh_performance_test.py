#!/usr/bin/env python3
"""
CryptoRebound Refresh Performance Testing
Specifically tests the crypto data refresh functionality to diagnose slowness issues
"""

import requests
import json
import time
import sys
import os
from datetime import datetime
import threading
import concurrent.futures

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

print(f"Testing CryptoRebound Refresh Performance at: {API_BASE}")
print("=" * 80)

class RefreshPerformanceTester:
    def __init__(self):
        self.test_results = []
        self.failed_tests = []
        
    def log_test(self, test_name, success, details="", response_data=None, duration=None):
        """Log test results with timing information"""
        status = "✅ PASS" if success else "❌ FAIL"
        duration_str = f" ({duration:.2f}s)" if duration else ""
        print(f"{status} {test_name}{duration_str}")
        if details:
            print(f"    {details}")
        if response_data and not success:
            print(f"    Response: {response_data}")
        print()
        
        self.test_results.append({
            'test': test_name,
            'success': success,
            'details': details,
            'duration': duration,
            'timestamp': datetime.now().isoformat()
        })
        
        if not success:
            self.failed_tests.append(test_name)
    
    def test_health_and_external_apis(self):
        """Test health endpoint to check external API status"""
        try:
            start_time = time.time()
            response = requests.get(f"{API_BASE}/health", timeout=30)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                services = data.get('services', {})
                
                # Analyze service health
                healthy_services = []
                unhealthy_services = []
                
                for service, status in services.items():
                    if isinstance(status, dict):
                        if status.get('status') == 'healthy' or status.get('healthy', False):
                            healthy_services.append(service)
                        else:
                            unhealthy_services.append(f"{service}: {status}")
                    elif status == 'healthy' or status is True:
                        healthy_services.append(service)
                    else:
                        unhealthy_services.append(f"{service}: {status}")
                
                details = f"Healthy: {healthy_services}, Issues: {unhealthy_services}" if unhealthy_services else f"All services healthy: {healthy_services}"
                self.log_test("Health Check & External APIs", True, details, duration=duration)
                
                return {
                    'healthy_services': healthy_services,
                    'unhealthy_services': unhealthy_services,
                    'all_healthy': len(unhealthy_services) == 0
                }
            else:
                self.log_test("Health Check & External APIs", False, f"HTTP {response.status_code}", response.text, duration)
                return {'all_healthy': False, 'error': f"HTTP {response.status_code}"}
                
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            self.log_test("Health Check & External APIs", False, f"Exception: {str(e)}", duration=duration)
            return {'all_healthy': False, 'error': str(e)}
    
    def test_refresh_endpoint_basic(self):
        """Test POST /api/cryptos/refresh with basic parameters"""
        try:
            print("Testing basic refresh endpoint...")
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh",
                json={"force": False},
                timeout=60  # 60 second timeout
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    updated_rankings = data.get('updated_rankings', {})
                    total_updated = sum(updated_rankings.values()) if updated_rankings else 0
                    
                    details = f"Updated {len(updated_rankings)} periods, {total_updated} total cryptos"
                    self.log_test("Basic Refresh Endpoint", True, details, duration=duration)
                    return {'success': True, 'duration': duration, 'updated_rankings': updated_rankings}
                else:
                    self.log_test("Basic Refresh Endpoint", False, f"Status: {data.get('status')}", data, duration)
                    return {'success': False, 'duration': duration}
            else:
                self.log_test("Basic Refresh Endpoint", False, f"HTTP {response.status_code}", response.text[:500], duration)
                return {'success': False, 'duration': duration}
                
        except requests.exceptions.Timeout:
            duration = 60  # Timeout duration
            self.log_test("Basic Refresh Endpoint", False, "Request timed out after 60 seconds", duration=duration)
            return {'success': False, 'duration': duration, 'timeout': True}
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            self.log_test("Basic Refresh Endpoint", False, f"Exception: {str(e)}", duration=duration)
            return {'success': False, 'duration': duration}
    
    def test_refresh_endpoint_force(self):
        """Test POST /api/cryptos/refresh with force=true"""
        try:
            print("Testing forced refresh endpoint...")
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh",
                json={"force": True},
                timeout=120  # 2 minute timeout for forced refresh
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    updated_rankings = data.get('updated_rankings', {})
                    total_updated = sum(updated_rankings.values()) if updated_rankings else 0
                    
                    details = f"Force refresh: Updated {len(updated_rankings)} periods, {total_updated} total cryptos"
                    
                    # Performance assessment
                    if duration < 30:
                        perf_note = "Excellent performance"
                    elif duration < 60:
                        perf_note = "Good performance"
                    elif duration < 120:
                        perf_note = "Acceptable performance"
                    else:
                        perf_note = "Slow performance"
                    
                    self.log_test("Force Refresh Endpoint", True, f"{details} - {perf_note}", duration=duration)
                    return {'success': True, 'duration': duration, 'updated_rankings': updated_rankings}
                else:
                    self.log_test("Force Refresh Endpoint", False, f"Status: {data.get('status')}", data, duration)
                    return {'success': False, 'duration': duration}
            else:
                self.log_test("Force Refresh Endpoint", False, f"HTTP {response.status_code}", response.text[:500], duration)
                return {'success': False, 'duration': duration}
                
        except requests.exceptions.Timeout:
            duration = 120  # Timeout duration
            self.log_test("Force Refresh Endpoint", False, "Request timed out after 120 seconds", duration=duration)
            return {'success': False, 'duration': duration, 'timeout': True}
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            self.log_test("Force Refresh Endpoint", False, f"Exception: {str(e)}", duration=duration)
            return {'success': False, 'duration': duration}
    
    def test_refresh_with_specific_period(self):
        """Test refresh with specific period parameter"""
        try:
            print("Testing refresh with specific period...")
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh",
                json={"force": True, "period": "24h"},
                timeout=90
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    updated_rankings = data.get('updated_rankings', {})
                    details = f"Period-specific refresh: {updated_rankings}"
                    self.log_test("Refresh with Specific Period", True, details, duration=duration)
                    return {'success': True, 'duration': duration}
                else:
                    self.log_test("Refresh with Specific Period", False, f"Status: {data.get('status')}", data, duration)
                    return {'success': False, 'duration': duration}
            else:
                self.log_test("Refresh with Specific Period", False, f"HTTP {response.status_code}", response.text[:500], duration)
                return {'success': False, 'duration': duration}
                
        except requests.exceptions.Timeout:
            duration = 90
            self.log_test("Refresh with Specific Period", False, "Request timed out after 90 seconds", duration=duration)
            return {'success': False, 'duration': duration, 'timeout': True}
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            self.log_test("Refresh with Specific Period", False, f"Exception: {str(e)}", duration=duration)
            return {'success': False, 'duration': duration}
    
    def test_ranking_force_refresh_performance(self):
        """Test /api/cryptos/ranking with force_refresh=true and measure performance"""
        test_cases = [
            {'limit': 50, 'timeout': 30},
            {'limit': 100, 'timeout': 45},
            {'limit': 500, 'timeout': 90},
            {'limit': 1000, 'timeout': 120}
        ]
        
        results = []
        
        for case in test_cases:
            try:
                print(f"Testing ranking force refresh with limit={case['limit']}...")
                
                # Test without force refresh first
                start_time = time.time()
                response_normal = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': case['limit'], 'period': '24h', 'force_refresh': False},
                    timeout=case['timeout']
                )
                normal_duration = time.time() - start_time
                
                # Test with force refresh
                start_time = time.time()
                response_force = requests.get(
                    f"{API_BASE}/cryptos/ranking",
                    params={'limit': case['limit'], 'period': '24h', 'force_refresh': True},
                    timeout=case['timeout']
                )
                force_duration = time.time() - start_time
                
                if response_normal.status_code == 200 and response_force.status_code == 200:
                    normal_data = response_normal.json()
                    force_data = response_force.json()
                    
                    normal_count = len(normal_data) if isinstance(normal_data, list) else 0
                    force_count = len(force_data) if isinstance(force_data, list) else 0
                    
                    performance_diff = force_duration - normal_duration
                    performance_ratio = force_duration / normal_duration if normal_duration > 0 else float('inf')
                    
                    details = f"Normal: {normal_count} cryptos in {normal_duration:.2f}s, Force: {force_count} cryptos in {force_duration:.2f}s (diff: +{performance_diff:.2f}s, ratio: {performance_ratio:.1f}x)"
                    
                    # Assess if performance is acceptable
                    success = force_duration < case['timeout'] * 0.8  # Should complete within 80% of timeout
                    
                    self.log_test(f"Ranking Force Refresh Performance (limit={case['limit']})", success, details, duration=force_duration)
                    
                    results.append({
                        'limit': case['limit'],
                        'normal_duration': normal_duration,
                        'force_duration': force_duration,
                        'performance_diff': performance_diff,
                        'success': success
                    })
                else:
                    error_msg = f"Normal: HTTP {response_normal.status_code}, Force: HTTP {response_force.status_code}"
                    self.log_test(f"Ranking Force Refresh Performance (limit={case['limit']})", False, error_msg, duration=force_duration)
                    results.append({'limit': case['limit'], 'success': False})
                    
            except requests.exceptions.Timeout:
                self.log_test(f"Ranking Force Refresh Performance (limit={case['limit']})", False, f"Timed out after {case['timeout']} seconds", duration=case['timeout'])
                results.append({'limit': case['limit'], 'success': False, 'timeout': True})
            except Exception as e:
                self.log_test(f"Ranking Force Refresh Performance (limit={case['limit']})", False, f"Exception: {str(e)}")
                results.append({'limit': case['limit'], 'success': False, 'error': str(e)})
        
        return results
    
    def test_concurrent_refresh_requests(self):
        """Test multiple concurrent refresh requests to see how system handles load"""
        try:
            print("Testing concurrent refresh requests...")
            
            def make_refresh_request(request_id):
                try:
                    start_time = time.time()
                    response = requests.post(
                        f"{API_BASE}/cryptos/refresh",
                        json={"force": False},
                        timeout=60
                    )
                    duration = time.time() - start_time
                    
                    return {
                        'request_id': request_id,
                        'success': response.status_code == 200,
                        'duration': duration,
                        'status_code': response.status_code
                    }
                except Exception as e:
                    return {
                        'request_id': request_id,
                        'success': False,
                        'duration': 0,
                        'error': str(e)
                    }
            
            # Make 3 concurrent requests
            start_time = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(make_refresh_request, i) for i in range(3)]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
            
            total_duration = time.time() - start_time
            
            successful_requests = [r for r in results if r['success']]
            failed_requests = [r for r in results if not r['success']]
            
            avg_duration = sum(r['duration'] for r in successful_requests) / len(successful_requests) if successful_requests else 0
            
            details = f"Concurrent requests: {len(successful_requests)}/{len(results)} successful, avg duration: {avg_duration:.2f}s, total time: {total_duration:.2f}s"
            
            success = len(successful_requests) >= 2  # At least 2 out of 3 should succeed
            self.log_test("Concurrent Refresh Requests", success, details, duration=total_duration)
            
            return {'success': success, 'results': results}
            
        except Exception as e:
            self.log_test("Concurrent Refresh Requests", False, f"Exception: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def test_extended_timeout_refresh(self):
        """Test refresh with very long timeout to see if it eventually completes"""
        try:
            print("Testing refresh with extended timeout (3 minutes)...")
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE}/cryptos/refresh",
                json={"force": True},
                timeout=180  # 3 minute timeout
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    updated_rankings = data.get('updated_rankings', {})
                    total_updated = sum(updated_rankings.values()) if updated_rankings else 0
                    
                    details = f"Extended timeout test: {total_updated} cryptos updated in {len(updated_rankings)} periods"
                    
                    # This test is mainly to see if the request completes at all
                    self.log_test("Extended Timeout Refresh", True, details, duration=duration)
                    return {'success': True, 'duration': duration, 'completed': True}
                else:
                    self.log_test("Extended Timeout Refresh", False, f"Status: {data.get('status')}", data, duration)
                    return {'success': False, 'duration': duration}
            else:
                self.log_test("Extended Timeout Refresh", False, f"HTTP {response.status_code}", response.text[:500], duration)
                return {'success': False, 'duration': duration}
                
        except requests.exceptions.Timeout:
            duration = 180
            self.log_test("Extended Timeout Refresh", False, "Still timed out after 3 minutes - indicates serious performance issue", duration=duration)
            return {'success': False, 'duration': duration, 'timeout': True, 'serious_issue': True}
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            self.log_test("Extended Timeout Refresh", False, f"Exception: {str(e)}", duration=duration)
            return {'success': False, 'duration': duration}
    
    def run_performance_diagnostics(self):
        """Run comprehensive performance diagnostics for crypto refresh"""
        print("Starting CryptoRebound Refresh Performance Diagnostics...")
        print("=" * 80)
        
        # Test 1: Health check and external API status
        health_result = self.test_health_and_external_apis()
        
        # Test 2: Basic refresh
        basic_refresh_result = self.test_refresh_endpoint_basic()
        
        # Test 3: Force refresh
        force_refresh_result = self.test_refresh_endpoint_force()
        
        # Test 4: Refresh with specific period
        period_refresh_result = self.test_refresh_with_specific_period()
        
        # Test 5: Ranking force refresh performance comparison
        ranking_performance_results = self.test_ranking_force_refresh_performance()
        
        # Test 6: Concurrent requests
        concurrent_result = self.test_concurrent_refresh_requests()
        
        # Test 7: Extended timeout test
        extended_timeout_result = self.test_extended_timeout_refresh()
        
        # Performance Analysis
        print("=" * 80)
        print("PERFORMANCE ANALYSIS")
        print("=" * 80)
        
        # Analyze refresh performance
        refresh_times = []
        if basic_refresh_result.get('success') and 'duration' in basic_refresh_result:
            refresh_times.append(('Basic Refresh', basic_refresh_result['duration']))
        if force_refresh_result.get('success') and 'duration' in force_refresh_result:
            refresh_times.append(('Force Refresh', force_refresh_result['duration']))
        if period_refresh_result.get('success') and 'duration' in period_refresh_result:
            refresh_times.append(('Period Refresh', period_refresh_result['duration']))
        
        if refresh_times:
            print("Refresh Performance Summary:")
            for name, duration in refresh_times:
                if duration < 10:
                    status = "🟢 FAST"
                elif duration < 30:
                    status = "🟡 MODERATE"
                elif duration < 60:
                    status = "🟠 SLOW"
                else:
                    status = "🔴 VERY SLOW"
                print(f"  {name}: {duration:.2f}s {status}")
        
        # Analyze ranking performance
        successful_ranking_tests = [r for r in ranking_performance_results if r.get('success')]
        if successful_ranking_tests:
            print("\nRanking Force Refresh Performance:")
            for result in successful_ranking_tests:
                if 'performance_diff' in result:
                    diff = result['performance_diff']
                    if diff < 5:
                        impact = "🟢 LOW IMPACT"
                    elif diff < 15:
                        impact = "🟡 MODERATE IMPACT"
                    elif diff < 30:
                        impact = "🟠 HIGH IMPACT"
                    else:
                        impact = "🔴 SEVERE IMPACT"
                    print(f"  Limit {result['limit']}: +{diff:.2f}s {impact}")
        
        # Identify issues
        print("\nISSUE IDENTIFICATION:")
        issues_found = []
        
        if not health_result.get('all_healthy'):
            issues_found.append("🔴 External API services are not healthy - this may cause slow refresh")
        
        if force_refresh_result.get('timeout'):
            issues_found.append("🔴 CRITICAL: Force refresh times out - indicates serious performance problem")
        elif force_refresh_result.get('duration', 0) > 60:
            issues_found.append("🟠 Force refresh is very slow (>60s) - performance optimization needed")
        
        if extended_timeout_result.get('timeout'):
            issues_found.append("🔴 CRITICAL: Even with 3-minute timeout, refresh doesn't complete")
        
        timeout_count = sum(1 for r in ranking_performance_results if r.get('timeout'))
        if timeout_count > 0:
            issues_found.append(f"🟠 {timeout_count} ranking tests timed out - indicates scalability issues")
        
        if not concurrent_result.get('success'):
            issues_found.append("🟠 System struggles with concurrent refresh requests")
        
        if issues_found:
            for issue in issues_found:
                print(f"  {issue}")
        else:
            print("  🟢 No critical performance issues detected")
        
        # Recommendations
        print("\nRECOMMENDATIONS:")
        recommendations = []
        
        if not health_result.get('all_healthy'):
            recommendations.append("Check external API connectivity and rate limits")
        
        if force_refresh_result.get('duration', 0) > 30:
            recommendations.append("Consider implementing background refresh jobs instead of synchronous refresh")
            recommendations.append("Optimize data aggregation service for better performance")
        
        if any(r.get('timeout') for r in ranking_performance_results):
            recommendations.append("Implement pagination and streaming for large dataset requests")
            recommendations.append("Add caching layers to reduce computation time")
        
        if extended_timeout_result.get('timeout'):
            recommendations.append("URGENT: Investigate database query performance and external API response times")
            recommendations.append("Consider implementing async processing with job queues")
        
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")
        else:
            print("  🟢 System performance is acceptable")
        
        # Summary
        print("\n" + "=" * 80)
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
        
        # Overall assessment
        critical_failures = [
            extended_timeout_result.get('timeout'),
            force_refresh_result.get('timeout'),
            not health_result.get('all_healthy')
        ]
        
        if any(critical_failures):
            print("\n🔴 CRITICAL PERFORMANCE ISSUES DETECTED")
            print("The refresh functionality has serious performance problems that need immediate attention.")
        elif failed_tests > total_tests * 0.3:  # More than 30% failure rate
            print("\n🟠 MODERATE PERFORMANCE ISSUES")
            print("The refresh functionality works but has performance problems.")
        else:
            print("\n🟢 REFRESH FUNCTIONALITY IS WORKING")
            print("Performance is within acceptable ranges.")
        
        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'critical_issues': any(critical_failures),
            'recommendations': recommendations
        }

if __name__ == "__main__":
    tester = RefreshPerformanceTester()
    results = tester.run_performance_diagnostics()
    
    if results['critical_issues']:
        print("\n⚠️  CRITICAL PERFORMANCE ISSUES FOUND - Immediate attention required!")
        sys.exit(2)
    elif results['failed_tests'] > 0:
        print("\n⚠️  Some performance issues detected - Review recommended.")
        sys.exit(1)
    else:
        print("\n🎉 Refresh performance is acceptable!")
        sys.exit(0)