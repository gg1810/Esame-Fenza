from pymongo import MongoClient
import os

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGODB_URL)
db = client.cinematch_db

# Controlla struttura in user_stats per un utente
user = db.user_stats.find_one({})
if user:
    print(f"User ID: {user.get('user_id')}")
    print(f"Monthly Counts Type: {type(user.get('monthly_counts'))}")
    print(f"Monthly Counts Keys: {list(user.get('monthly_counts', {}).keys())}")
    for year, months in user.get('monthly_counts', {}).items():
        print(f"  Year {year}: {months}")
else:
    print("Nessun utente trovato.")

client.close()
