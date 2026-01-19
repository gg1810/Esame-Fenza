"""
PARTE 2: Rimuove 100 visioni per "Batman"
Eseguire con: docker exec cinematch_backend python test_delete_batman.py
"""
import os
import json
import time
from kafka import KafkaProducer
from datetime import datetime
from pymongo import MongoClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
TOPIC = "user-movie-events"

def check_mongo():
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    stats = db.global_stats.find_one({"type": "global_trends"})
    print("\n📊 Stato global_stats:")
    if stats:
        print(f"   Batman count: {stats.get('movie_counts', {}).get('Batman', 0)}")
        print(f"   Top 3:")
        for i, m in enumerate(stats.get('top_movies', [])[:3]):
            print(f"      {i+1}. {m['title']} - {m['count']}")
    client.close()

def main():
    print("=" * 50)
    print("PARTE 2: RIMOZIONE 100 VISIONI BATMAN")
    print("=" * 50)
    
    check_mongo()
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print("\n🚀 Invio 100 eventi DELETE per Batman...")
    for i in range(100):
        event = {
            "event_type": "DELETE",
            "user_id": f"test_user_{i % 10}",
            "movie": {
                "name": "Batman",
                "year": 2022,
                "rating": 8,
                "genres": ["Action", "Adventure"],
                "duration": 176
            },
            "timestamp": datetime.now().isoformat()
        }
        producer.send(TOPIC, value=event)
        if (i + 1) % 25 == 0:
            print(f"   Inviati {i + 1}/100...")
    
    producer.flush()
    producer.close()
    print("✅ 100 eventi DELETE inviati!")
    print("\n⏳ Attendi 40 secondi, poi controlla la dashboard...")

if __name__ == "__main__":
    main()
