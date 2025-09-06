from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="secrets.env")

def get_database_connection():
    connection_string = os.getenv("COSMOS_KEY")
    client = MongoClient(connection_string)
    db = client['testID']
    collection = db["testCollection"]
    dblist = client.list_database_names()
    if "testCollection" in dblist:
        print("The database exists.")
    return db

