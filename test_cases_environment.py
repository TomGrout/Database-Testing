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
test_data = load_environmental_data(dataset_path)

def test_bulk_insert():
    print("using environment trends dataset")
    print("running test 1")
    start_time = time.time()
    result = db.testCollection.insert_many(test_data)
    end_time = time.time()
    return {
        'docsInserted': len(result.inserted_ids),
        'executionTime': (end_time - start_time) * 1000
    }

def test_primary_key_retrieval():
    print("running test 2")
    result = db.testCollection.find_one({'_id': 5000})
    return {'found': result is not None}

def test_secondary_index_retrieval():
    result = list(db.testCollection.find({'Country': 'France'}))
    return {'resultCount': len(result)}

def test_find_using_no_index():
    col = db['testCollection']
    result = col.find_one({'Extreme_Weather_Events': {'$exists': True}})

def test_sort_with_secondary_index():
    db.testCollection.create_index('Forest_Area_pct')
    start_time = time.time()
    results = list(db.testCollection.find().sort('Forest_Area_pct', 1))
    end_time = time.time()
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

def test_sort_without_index():
    try:
        db.testCollection.drop_index('Forest_Area_pct_1')
    except Exception:
        pass
    start_time = time.time()
    results = list(db.testCollection.find().sort('Forest_Area_pct', 1))
    end_time = time.time()
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

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

def test_bulk_update_secondary_index():
    db.testCollection.create_index('Country')
    start_time = time.time()
    result = db.testCollection.update_many(
        {'Country': 'USA'},
        {'$set': {'bulkUpdated': True}}
    )
    end_time = time.time()
    return {
        'matchedCount': result.matched_count,
        'modifiedCount': result.modified_count,
        'executionTime': (end_time - start_time) * 1000
    }

def test_delete_primary_key():
    inserted = db.testCollection.insert_one({'tempField': 'delete-me'})
    start_time = time.time()
    result = db.testCollection.delete_one({'_id': inserted.inserted_id})
    end_time = time.time()
    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

def test_bulk_delete_secondary_index():
    db.testCollection.create_index('Country')
    start_time = time.time()
    result = db.testCollection.delete_many({'Country': 'India'})
    end_time = time.time()
    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

def test_bulk_delete_no_index():
    try:
        db.testCollection.drop_index('Country_1')
    except:
        pass
    start_time = time.time()
    result = db.testCollection.delete_many({'Country': 'Brazil'})
    end_time = time.time()
    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

def test_sort_primary_index_desc():
    start_time = time.time()
    results = list(db.testCollection.find().sort('_id', -1))
    end_time = time.time()
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

def test_query_sparse_with_index():
    db.testCollection.create_index('Extreme_Weather_Events', sparse=True)
    start_time = time.time()
    results = list(db.testCollection.find({'Extreme_Weather_Events': {'$exists': True}}))
    end_time = time.time()
    return {
        'resultCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

def test_query_sparse_no_index():
    try:
        db.testCollection.drop_index('Extreme_Weather_Events_1')
    except:
        pass
    start_time = time.time()
    results = list(db.testCollection.find({'Extreme_Weather_Events': {'$exists': True}}))
    end_time = time.time()
    return {
        'resultCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

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