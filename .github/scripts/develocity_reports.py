import os
import requests
import json
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime, timedelta
import pytz  # Add this import for timezone handling

@dataclass
class TestOutcome:
    passed: int
    failed: int 
    skipped: int
    flaky: int
    not_selected: int = field(metadata={'name': 'notSelected'})
    total: int

@dataclass
class TestResult:
    name: str
    outcome_distribution: TestOutcome
    first_seen: datetime

class TestAnalyzer:
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url
        self.headers = {
            'Authorization': f'Bearer {auth_token}',
            'Accept': 'application/json'
        }

    def get_test_results(self, project: str, days: int = 90, test_type: str = "quarantinedTest",
                        outcomes: List[str] = None) -> List[TestResult]:
        """
        Fetch test results from the API using absolute time ranges
        """
        if outcomes is None:
            outcomes = ["failed", "flaky"]

        # Calculate the time range
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(days=days)
        
        # Create chunks of 30 days each
        chunk_size = timedelta(days=30)
        all_results = {}
        
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + chunk_size, end_time)
            
            # Format timestamps in ISO-8601 format
            chunk_start_str = chunk_start.isoformat()
            chunk_end_str = chunk_end.isoformat()
            
            query_params = {
                'query': f'project:{project} buildStartTime:[{chunk_start_str} TO {chunk_end_str}] requested:"*{test_type}*"',
                'testOutcomes': outcomes,
                'container': '*'
            }

            response = requests.get(
                f'{self.base_url}/api/tests/containers',
                headers=self.headers,
                params=query_params
            )
            response.raise_for_status()
            
            for test in response.json()['content']:
                test_name = test['name']
                if test_name not in all_results:
                    outcome_data = test['outcomeDistribution']
                    if 'notSelected' in outcome_data:
                        outcome_data['not_selected'] = outcome_data.pop('notSelected')
                    outcome = TestOutcome(**outcome_data)
                    # Record when we first saw this test
                    all_results[test_name] = TestResult(test_name, outcome, chunk_start)
            
            chunk_start = chunk_end

        return list(all_results.values())

    def get_defective_tests(self, results: List[TestResult]) -> Dict[str, TestResult]:
        """
        Analyze test results to find defective tests (failed or flaky)
        """
        defective_tests = {}
        
        for result in results:
            if result.outcome_distribution.failed > 0 or result.outcome_distribution.flaky > 0:
                defective_tests[result.name] = result
                
        return defective_tests

    def get_long_quarantined_tests(self, results: List[TestResult], quarantine_threshold_days: int = 60) -> Dict[str, TestResult]:
        """
        Find tests that have been quarantined longer than the threshold.
        These are candidates for removal or rewriting.
        
        Args:
            results: List of test results
            quarantine_threshold_days: Number of days after which a quarantined test should be considered for removal/rewrite
        """
        long_quarantined = {}
        current_time = datetime.now(pytz.UTC)
        
        for result in results:
            days_quarantined = (current_time - result.first_seen).days
            if days_quarantined >= quarantine_threshold_days:
                long_quarantined[result.name] = (result, days_quarantined)
        
        return long_quarantined

    def get_problematic_quarantined_tests(self, results: List[TestResult], 
                                        quarantine_threshold_days: int = 60,
                                        min_failure_rate: float = 0.3) -> Dict[str, TestResult]:
        """
        Find tests that:
        1. Have been quarantined longer than the threshold
        2. Have a high failure or flaky rate
        
        Args:
            results: List of test results
            quarantine_threshold_days: Days after which a test is considered long-quarantined
            min_failure_rate: Minimum combined failure/flaky rate to be considered problematic (0.3 = 30%)
        """
        problematic_tests = {}
        current_time = datetime.now(pytz.UTC)
        
        for result in results:
            days_quarantined = (current_time - result.first_seen).days
            if days_quarantined >= quarantine_threshold_days:
                # Calculate failure rate
                total_runs = result.outcome_distribution.total
                if total_runs > 0:
                    problem_runs = result.outcome_distribution.failed + result.outcome_distribution.flaky
                    failure_rate = problem_runs / total_runs
                    
                    if failure_rate >= min_failure_rate:
                        problematic_tests[result.name] = (result, days_quarantined, failure_rate)
        
        return problematic_tests

def main():
    # Configuration
    BASE_URL = "https://ge.apache.org"
    AUTH_TOKEN = os.environ.get("DEVELOCITY_ACCESS_TOKEN")
    PROJECT = "kafka"
    QUARANTINE_THRESHOLD_DAYS = 7
    MIN_FAILURE_RATE = 0.1  # 10% failure rate threshold

    analyzer = TestAnalyzer(BASE_URL, AUTH_TOKEN)
    
    try:
        results = analyzer.get_test_results(PROJECT)
        problematic_tests = analyzer.get_problematic_quarantined_tests(
            results, 
            QUARANTINE_THRESHOLD_DAYS,
            MIN_FAILURE_RATE
        )
        
        print(f"\nHigh-Priority Quarantined Tests Report ({datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC)")
        print("=" * 80)
        print(f"Tests that have been quarantined for more than {QUARANTINE_THRESHOLD_DAYS} days")
        print(f"AND have a failure/flaky rate of at least {MIN_FAILURE_RATE*100}%")
        print("These tests are prime candidates for removal or rewriting")
        print("=" * 80)
        
        if not problematic_tests:
            print("\nNo tests found matching the criteria.")
        else:
            # Sort by failure rate (highest first), then by quarantine duration
            sorted_tests = sorted(
                problematic_tests.items(), 
                key=lambda x: (x[1][2], x[1][1]),
                reverse=True
            )
            
            print(f"\nFound {len(sorted_tests)} high-priority tests:")
            for test_name, (result, days_quarantined, failure_rate) in sorted_tests:
                print(f"\nTest: {test_name}")
                print(f"Failure Rate: {failure_rate*100:.1f}%")
                print(f"Quarantine Duration: {days_quarantined} days")
                print(f"First Quarantined: {result.first_seen.strftime('%Y-%m-%d')}")
                print("Recent Outcomes:")
                print(f"  Failed: {result.outcome_distribution.failed}")
                print(f"  Flaky: {result.outcome_distribution.flaky}")
                print(f"  Passed: {result.outcome_distribution.passed}")
                print(f"  Total Runs: {result.outcome_distribution.total}")
                
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main()
