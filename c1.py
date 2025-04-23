from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["nosql_project"]
collection = db["grades"]

for doc in collection.find().limit(5):
    print(doc)
