from pymongo import MongoClient
import os

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGODB_URL)
db = client.cinematch_db

user = db.users.find_one({"quiz": {"$exists": True}})
if user:
    print(f"✅ Found user with quiz stats: {user['username']}")
    print(f"Quiz Data: {user['quiz']}")
else:
    print("❌ No user found with nested quiz stats")

client.close()
