import time
import os
from pymongo import MongoClient
import time
import json
import os

from import_data import get_data
from connection import get_database_connection
from test_data import generate_test_data
from load_data import *

db = get_database_connection()
dataset_name, dataset_path, test_data = "", "",""
test_data = load_ev_population_data(dataset_path)

# 1
def test_bulk_insert():
    print("running test 1")
        # Convert DataFrame to list of dicts
    
    start_time = time.time()
    result = db.testCollection.insert_many(test_data)
    end_time = time.time()

    return {
        'docsInserted': len(result.inserted_ids),
        'executionTime': (end_time - start_time) * 1000
    }

# 2
def test_primary_key_retrieval():
    print("running test 2")
    result = db.testCollection.find_one({'_id': 5000})
    return {'found': result is not None}

# 3
def test_secondary_index_retrieval():
    # Use 'Make' as a secondary field
    result = list(db.testCollection.find({'Make': 'TESLA'}))
    return {'resultCount': len(result)}

# 4
def test_find_using_no_index():
    col = db['sparseField']
    result = col.find_one()

# 5
def test_sort_with_secondary_index():
    db.testCollection.create_index('Model Year')
    start_time = time.time()
    results = list(db.testCollection.find().sort('Model Year', 1))
    end_time = time.time()
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

# 6
def test_sort_without_index():
    try:
        db.testCollection.drop_index('Model Year_1')
    except Exception:
        pass
    start_time = time.time()
    results = list(db.testCollection.find().sort('Model Year', 1))
    end_time = time.time()
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

# 7
def test_update_primary_key():
    doc = db.testCollection.find_one()
    if not doc:
        return {'error': 'No documents to update'}
    start_time = time.time()
    result = db.testCollection.update_one({'_id': doc['_id']}, {'$set': {'updatedField': True}})
    end_time = time.time()
    return {
        'matchedCount': result.matched_count,
        'modifiedCount': result.modified_count,
        'executionTime': (end_time - start_time) * 1000
    }

# 8
def test_bulk_update_secondary_index():
    db.testCollection.create_index('Make')
    try:
        start_time = time.time()
        result = db.testCollection.update_many(
            {'Make': {'$regex': '^T'}},
            {'$set': {'bulkUpdated': True}},
            max_time_ms = 200000
        )
        end_time = time.time()
        return {
            'matchedCount': result.matched_count,
            'modifiedCount': result.modified_count,
            'executionTime': (end_time - start_time) * 1000
        }
    
    except Exception as e:
        print ("Error" + str(e))


# 9
def test_delete_primary_key():
    inserted = db.testCollection.insert_one({'tempField': 'delete-me'})
    start_time = time.time()
    result = db.testCollection.delete_one({'_id': inserted.inserted_id})
    end_time = time.time()
    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

# 10
def test_bulk_delete_secondary_index():
    print("running test 11")

    # Ensure index exists
    db.testCollection.create_index('Make')

    try:
        # For safer test: only delete documents with exact match
        filter_query = {'Make': {'$regex': '^C'}}

        count = db.testCollection.count_documents(filter_query)
        print(f"Matching documents: {count}")

        start_time = time.time()
        result = db.testCollection.delete_many(filter_query)
        end_time = time.time()

        return {
            'deletedCount': result.deleted_count,
            'executionTime': (end_time - start_time) * 1000
        }

    except Exception as e:
        return {'error': str(e)}


# 11
def test_bulk_delete_no_index():
    try:
        db.testCollection.drop_index('Make_1')
    except:
        pass
    start_time = time.time()
    result = db.testCollection.delete_many({'Make': {'$regex': '^C'}})
    end_time = time.time()
    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

# 12
def test_sort_primary_index_desc():
    print("running test 12")

    try:
        start_time = time.time()
        results = list(db.testCollection.find().sort('_id', -1).limit(100))  # Limit added
        end_time = time.time()

        return {
            'sortedCount': len(results),
            'executionTime': (end_time - start_time) * 1000
        }

    except Exception as e:
        return {'error': str(e)}

# 13
def test_query_sparse_with_index():
    print("running test 13")

    try:
        db.testCollection.create_index('Electric Range', sparse=True)
        start_time = time.time()
        results = list(db.testCollection.find({'Electric Range': {'$exists': True}}).limit(100))  # <-- Add limit
        end_time = time.time()

        return {
            'resultCount': len(results),
            'executionTime': (end_time - start_time) * 1000
        }

    except Exception as e:
        return {'error': str(e)}


# 14
def test_query_sparse_no_index():
    try:
        db.testCollection.drop_index('Electric Range_1')
    except:
        pass
    start_time = time.time()
    results = list(db.testCollection.find({'Electric Range': {'$exists': True}}))
    end_time = time.time()
    return {
        'resultCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

# 15
def test_dual_dataset_retrieval():
    print("Running dual dataset retrieval test...")

    try:
        # Pull a few from each collection
        primary_sample = list(db.testCollection.find().limit(2))
        secondary_sample = list(db.secondCollection.find().limit(2))

        print("\nPrimary Dataset Sample (testCollection):")
        for doc in primary_sample:
            print(doc)

        print("\nSecondary Dataset Sample (secondCollection):")
        for doc in secondary_sample:
            print(doc)

        return {
            'primaryRetrieved': len(primary_sample),
            'secondaryRetrieved': len(secondary_sample),
            'success': True
        }

    except Exception as e:
        return {
            'error': str(e),
            'success': False
        }


