import time
import json
import os

from import_data import get_data
from connection import get_database_connection
from test_data import generate_test_data
from load_data import *

db = get_database_connection()
dataset_name, dataset_path, test_data = "", "",""
test_data = load_yelp_data(test_data)


def select_data():
        # --- Choose Dataset ---
    print("Select dataset to test with:")
    print("1 - Dog Images")
    print("2 - Global Environmental Trends")
    print("3 - Yelp Dataset")
    print("4 - Electric Vehicle Population")

    choice = input("Enter the dataset number of your choice: ").strip() 

    dog_path, env_path, yelp_path, ev_path = get_data()

    dataset_map = {
        "1": ("dog_images", dog_path),
        "2": ("environmental_trends", env_path),
        "3": ("yelp", yelp_path),
        "4": ("ev_pop", ev_path)
    }

    if choice not in dataset_map:
        print("Invalid selection. Exiting.")
        return


    global dataset_name, dataset_path, test_data
    dataset_name, dataset_path = dataset_map[choice]
    test_data = dataset_path

    print(f"\nUsing dataset: {dataset_name}")

    if dataset_name == "environmental_trends":
        # Insert or process Environmental Trends dataset
        test_data = load_environmental_data(dataset_path)
    elif dataset_name == "yelp":
        # Insert or process Yelp dataset
        test_data = load_yelp_data(test_data)
    elif dataset_name == "ev_pop":
        # Insert or process Electric Vehicle Population dataset
        test_data = load_ev_population_data(dataset_path)

    #return dataset_name, dataset_path

#dataset_name, test_data = 

#1
def test_bulk_insert():
    # Clear existing collection
        
    # Time the bulk insert
    start_time = time.time()
    result = db.testCollection.insert_many(test_data)
    end_time = time.time()
    
    return {
        'docsInserted': len(result.inserted_ids),
        'executionTime': (end_time - start_time) * 1000
    }

#2
def test_primary_key_retrieval():
    print("running test 2")
    result = db.testCollection.find_one({'_id': 5000})
    return {'found': result is not None}

#3
def test_secondary_index_retrieval():
    result = list(db.testCollection.find({'secondaryField': 'value-5000'}))
    return {'resultCount': len(result)}

#4
def test_find_using_no_index():
    col = db['sparseField']
    result = col.find_one()

#5
def test_sort_with_secondary_index():
    # Ensure index exists
    db.testCollection.create_index('secondaryField')
    
    # Time sorting on indexed field
    start_time = time.time()
    results = list(db.testCollection.find().sort('secondaryField', 1))
    end_time = time.time()
    
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

#6
def test_sort_without_index():
    # Drop index if it exists to simulate no-index sort
    try:
        db.testCollection.drop_index('business_id_1')
    except Exception:
        pass

    # Time sorting on a non-indexed field
    start_time = time.time()
    results = list(db.testCollection.find().sort('business_id', 1))
    end_time = time.time()
    
    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }


#7
def test_update_primary_key():
    # Get one document's _id
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

#8
def test_bulk_update_secondary_index():
    # Ensure index exists
    db.testCollection.create_index('business_id')

    # Update all where business_id starts with '1' or any pattern you expect
    start_time = time.time()
    result = db.testCollection.update_many(
        {'business_id': {'$regex': '^1'}},
        {'$set': {'bulkUpdated': True}}
    )
    end_time = time.time()

    return {
        'matchedCount': result.matched_count,
        'modifiedCount': result.modified_count,
        'executionTime': (end_time - start_time) * 1000
    }


#9
def test_delete_primary_key():
    # Insert a test document to delete
    inserted = db.testCollection.insert_one({'tempField': 'delete-me'})
    
    start_time = time.time()
    result = db.testCollection.delete_one({'_id': inserted.inserted_id})
    end_time = time.time()

    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }


#10
def test_bulk_delete_secondary_index():
    # Ensure index exists
    db.testCollection.create_index('business_id')

    # Delete docs where business_id starts with '2' (adjust pattern as needed)
    start_time = time.time()
    result = db.testCollection.delete_many({'business_id': {'$regex': '^2'}})
    end_time = time.time()

    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }
    

#11
def test_bulk_delete_no_index():
    # Drop index to ensure no index is used
    try:
        db.testCollection.drop_index('secondaryField_1')
    except:
        pass

    start_time = time.time()
    result = db.testCollection.delete_many({'secondaryField': {'$regex': '^value-8'}})
    end_time = time.time()

    return {
        'deletedCount': result.deleted_count,
        'executionTime': (end_time - start_time) * 1000
    }

#12
def test_sort_primary_index_desc():
    start_time = time.time()
    results = list(db.testCollection.find().sort('_id', -1))
    end_time = time.time()

    return {
        'sortedCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

#13
def test_query_sparse_with_index():
    db.testCollection.create_index('sparseField', sparse=True)

    start_time = time.time()
    results = list(db.testCollection.find({'sparseField': {'$exists': True}}))
    end_time = time.time()

    return {
        'resultCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

#14
def test_query_sparse_no_index():
    try:
        db.testCollection.drop_index('sparseField_1')
    except:
        pass

    start_time = time.time()
    results = list(db.testCollection.find({'sparseField': {'$exists': True}}))
    end_time = time.time()

    return {
        'resultCount': len(results),
        'executionTime': (end_time - start_time) * 1000
    }

#15
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

