from pprint import pprint
from connection import get_database_connection

db = get_database_connection()

def analyse_results():
    # Get all test results

    results = list(db.testResults.find())        #.sort("testCaseId", 1))
    
    # Compare across machines
    for test_id in range(1, 17):
        test_results = [r for r in results if r['testCaseId'] == test_id]
        print(f"Test {test_id} results:")
        for result in test_results:
            print(f"Test Case: {result['testName']}")
            print(f"Execution Time: {result['executionTime']:.2f} ms")
            print(f"CPU Usage: {result.get('cpuUsage', 'N/A')}%")
            print(f"Memory Usage: {result.get('memoryUsageMB', 'N/A')} MB")
            print("Machine Info:")
            pprint(result['machineInfo'])
            print(f"Execution Time: {result['executionTime']:.2f} ms")
            print(f"Avg CPU Usage: {result.get('avgCpuUsage', 'N/A')}%")
            print(f"Peak Memory Usage: {result.get('peakMemoryUsageMB', 'N/A')} MB")
            print("-" * 60)

if __name__ == "__main__":
    analyse_results()

