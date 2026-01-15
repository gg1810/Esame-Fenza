import random
import os
import sys
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz

# Add parent directory to sys.path to allow importing from 'auth'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from auth import get_password_hash
except ImportError:
    # Fallback if manual run outside docker
    print("⚠️ Could not import auth, using dummy password hash")
    def get_password_hash(p): return p

# Config
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client.cinematch_db
users_collection = db.users
movies_collection = db.movies
movies_catalog = db.movies_catalog

# Specific counts requested by user
PROVINCE_COUNTS = {
    "benevento": 0,
    "napoli": 0,
    "caserta": 0,
    "avellino": 1,
    "salerno": 2
}

def generate_users():
    total_users = 0
    total_movies = 0
    italy_tz = pytz.timezone('Europe/Rome')
    
    # Fetch some catalog movies for random activity
    catalog_movies = list(movies_catalog.find({}, {"title": 1, "year": 1}).limit(100))
    if not catalog_movies:
        print("⚠️ No movies in catalog! Cannot generate activity.")
        catalog_movies = [{"title": "Test Movie", "year": 2024}]

    print("🚀 Starting specific test user generation...")

    for province, count in PROVINCE_COUNTS.items():
        if count <= 0:
            continue

        print(f"  📍 Processing {province} ({count} users)...")
        for i in range(1, count + 1):
            # Create a unique username to avoid conflicts
            timestamp = datetime.now().strftime("%H%M%S")
            username = f"user_{province}_{i}_{timestamp}"
            email = f"{username}@example.com"
            
            # Create User document
            new_user = {
                "username": username,
                "password": get_password_hash("password123"),
                "user_id": username,
                "email": email,
                "full_name": f"Test {province.capitalize()} {i}",
                "province": province,
                "created_at": datetime.now(italy_tz).isoformat(),
                "is_active": True,
                "has_data": True,
                "movies_count": 0
            }
            users_collection.insert_one(new_user)
            total_users += 1
            
            # Generate Random Activity
            num_movies = random.randint(5, 12)
            user_movies = []
            selected_movies = random.sample(catalog_movies, min(num_movies, len(catalog_movies)))
            
            for movie in selected_movies:
                days_ago = random.randint(0, 30)
                watch_date = (datetime.now(italy_tz) - timedelta(days=days_ago)).date().isoformat()
                
                user_movies.append({
                    "user_id": username,
                    "name": movie.get("title", "Unknown"),
                    "year": movie.get("year"),
                    "rating": random.randint(3, 5),
                    "date": watch_date,
                    "added_at": datetime.now(italy_tz).isoformat()
                })
            
            if user_movies:
                movies_collection.insert_many(user_movies)
                users_collection.update_one(
                    {"user_id": username},
                    {"$set": {"movies_count": len(user_movies)}}
                )
                total_movies += len(user_movies)

    print(f"\n✅ Done! Created {total_users} users and {total_movies} watch events.")

if __name__ == "__main__":
    generate_users()
