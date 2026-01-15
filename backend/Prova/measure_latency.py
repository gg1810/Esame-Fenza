
"""
Script per misurare la latenza END-TO-END (Kafka -> Spark -> MongoDB).
1. Invia un evento di UPDATE a Kafka.
2. Polla MongoDB ogni 0.1s finché le stats dell'utente non cambiano.
3. Stampa il tempo trascorso.
"""
import os
import json
import time
import logging
from datetime import datetime
import pytz
from pymongo import MongoClient
from kafka import KafkaProducer

# Config
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "user-movie-events"
USER_ID = "latency_test_user"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("LatencyTest")

def main():
    client = MongoClient(MONGO_URL)
    db = client.cinematch_db
    
    # 1. Setup Data: Assicura che l'utente abbia almeno un film
    logger.info(f"🔧 Setup utente {USER_ID}...")
    db.movies.update_one(
        {"user_id": USER_ID, "name": "Latency Test Movie"},
        {"$set": {
            "rating": 3, 
            "genres": ["Test"], 
            "date": "2024-01-01", 
            "added_at": datetime.now().isoformat()
        }},
        upsert=True
    )
    
    # Recupera stato iniziale (updated_at)
    initial_stats = db.user_stats.find_one({"user_id": USER_ID})
    initial_ts = initial_stats.get("updated_at") if initial_stats else None
    
    logger.info(f"📊 Stato iniziale: {initial_ts}")

    # 2. Invia Evento Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    event = {
        "event_type": "UPDATE",
        "user_id": USER_ID,
        "movie": {"name": "Latency Test Movie", "rating": 5}, # Cambio rating
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info("⏱️  Invio evento a Kafka...")
    start_time = time.time()
    producer.send(TOPIC, key=USER_ID, value=event)
    producer.flush()
    logger.info(f"🚀 Evento inviato! Inizio polling...")
    
    # 3. Polling MongoDB
    timeout = 10 # secondi
    elapsed = 0
    poll_interval = 0.1
    
    while elapsed < timeout:
        current_stats = db.user_stats.find_one({"user_id": USER_ID})
        current_ts = current_stats.get("updated_at") if current_stats else None
        
        # Controlla se updated_at è cambiato
        if current_ts != initial_ts:
            end_time = time.time()
            total_latency = end_time - start_time
            logger.info(f"✅ CAMBIO RILEVATO!")
            logger.info(f"   Iniziale: {initial_ts}")
            logger.info(f"   Finale:   {current_ts}")
            logger.info(f"⏱️  LATENZA REALE: {total_latency:.3f} secondi")
            return
            
        time.sleep(poll_interval)
        elapsed += poll_interval
        
    logger.error("❌ TIMEOUT: Stats non aggiornate entro 10 secondi.")

if __name__ == "__main__":
    main()
