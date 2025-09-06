import time
import datetime
import psutil
import socket
import platform
import os
import threading

from connection import get_database_connection
from test_data import generate_test_data
from analysis import analyse_results
from test_cases_environment import *
from import_data import get_data
from load_data import ( load_yelp_data, load_ev_population_data, load_environmental_data )


db = get_database_connection()

def start_resource_monitoring(interval=0.1):
    cpu_data = []
    mem_data = []
    running = True

    def monitor():
        while running:
            cpu_data.append(psutil.cpu_percent(interval=None))
            mem_data.append(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024))  # MB
            time.sleep(interval)

    thread = threading.Thread(target=monitor)
    thread.daemon = True
    thread.start()

    def stop():
        nonlocal running
        running = False
        thread.join()
        return {
            'avgCpuUsage': round(sum(cpu_data) / len(cpu_data), 2) if cpu_data else 0,
            'peakMemoryUsageMB': round(max(mem_data), 2) if mem_data else 0
        }

    return stop

def run_test(test_id, test_name, test_function):

    print(f"Running test {test_id}: {test_name}")
    
    stop_monitoring = start_resource_monitoring()

    # run test and measure execution time
    start_time = time.time()
    result = test_function()
    end_time = time.time()
    
    resource_stats = stop_monitoring()

    execution_time = (end_time - start_time) * 1000  # milliseconds

    # Record results
    db.testResults.insert_one({
        'testCaseId': test_id,
        'testName': test_name,
        'executionTime': execution_time,
        'timestamp': datetime.now(),
        'result': result,
        'machineInfo': {
            'hostname' : socket.gethostname(),
            'platform' : platform.platform(),
            'cpu' : platform.processor(),
            'cores' : psutil.cpu_count(logical=False), 
            'logical_cores' : psutil.cpu_count(logical=True),
            'ram' : round(psutil.virtual_memory().total / (1024 ** 3), 2)
         },
    'cpuUsage': psutil.cpu_percent(interval=1),
    'memoryUsageMB': psutil.Process(os.getpid()).memory_info().rss // (1024 * 1024)     #memory use of THIS ython process ***IN MEGATBYTES*** 
    })
    
    print(f"Test {test_id} completed in {execution_time:.2f} ms\n")
    print(f"Avg CPU Usage: {resource_stats['avgCpuUsage']}%, Peak Memory: {resource_stats['peakMemoryUsageMB']} MB\n")
    return execution_time

def run_all_tests():


    #db.testCollection.delete_many({})
    # use test data 
    # print("Preparing test data...")
    # test_data = generate_test_data(10000)
    # db.testCollection.insert_many(test_data)
    # print(f"Inserted 10,000 documents for testing\n")
    
    #select_data()

    # Run all tests
    results = []
    
    results.append(run_test(1, "Bulk Insert of Data", test_bulk_insert))
    #results.append(run_test(2, "Retrieving Data using Primary Key", test_primary_key_retrieval))
    #results.append(run_test(3, "Retrieving Data using a Secondary Index", test_secondary_index_retrieval))
    #results.append(run_test(4, "Retrieving Data using no Index", test_find_using_no_index))
    #results.append(run_test(5, "Sorting Data using a Secondary Index", test_sort_with_secondary_index))
    #results.append(run_test(6, "Sorting Data using No Index", test_sort_without_index))
    #results.append(run_test(7, "Update using Primary Index", test_update_primary_key))
    #results.append(run_test(8, "Bulk Update using a Secondary Index", test_bulk_update_secondary_index))
    #results.append(run_test(9, "Delete Primary Key", test_delete_primary_key)) 
    #results.append(run_test(10, "Delete using Primary Index", test_delete_primary_key))
    #results.append(run_test(11, "Bulk Delete using a Secondary Index", test_bulk_delete_secondary_index))
    #results.append(run_test(12, "Bulk Delete using No Index", test_bulk_delete_no_index))
    #results.append(run_test(13, "Sort Descending using Primary Index", test_sort_primary_index_desc))
    #results.append(run_test(14, "Query Sparse Columns with Index", test_query_sparse_with_index))
    #results.append(run_test(15, "Query Sparse Columns without Index", test_query_sparse_no_index))
    results.append(run_test(16, "Retrieve Results from dual Dataset", test_dual_dataset_retrieval))

    
    # Print summary
    print("\nTest Summary:")
    for i, result in enumerate(results):
        print(f"Test {i+1}: {result:.2f} ms")
    
    print("\nTesting complete")

# Run the tests
if __name__ == "__main__":
    run_all_tests()
    #run_test()
    analyse_results()

