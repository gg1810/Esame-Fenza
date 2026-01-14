
"""
🎬 CineMatch - MASSIVE Load Test (1M Events / 50k Users)
========================================================

Simula un carico MASSIVO di 50.000 utenti e ~1.000.000 di eventi.

Architecture Flow:
1.  **Generazione**: 50k Utenti * ~20 Film ciascuno = 1 Milione di Eventi.
2.  **Scrittura Duale**: Scrittura su MongoDB e Kafka.
3.  **Elaborazione Spark**: Spark Streaming macina 1 milione di eventi.
4.  **Persistenza**: Risultati salvati in user_stats.

Fasi del Test:
*   **FASE 1 (Setup)**: Creazione 50.000 utenti.
*   **FASE 2 (Esecuzione)**: Generazione 1.000.000 eventi (Add/Update/Delete).
*   **FASE 3 (Verifica)**: Attesa elaborazione completa.
*   **FASE 4 (Cleanup)**: Cancellazione totale dati.
"""

import os
import sys
import json
import time
import random
import logging
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, InsertOne, UpdateOne, DeleteOne
from pymongo.errors import BulkWriteError
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("LoadTest1M")

# Configuration MASSIVE
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

NUM_USERS = 50_000
MOVIES_PER_USER_AVG = 20  # 50k * 20 = 1,000,000 Events
USER_PREFIX = "loadtest_1m_" 
BATCH_SIZE = 2000  # Increased batch size for speed

# Probabilities
PROB_UPDATE = 0.10 
PROB_DELETE = 0.05

def get_mongo_client():
    return MongoClient(MONGO_URL)

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks=1, 
            retries=3,
            batch_size=32768, # Bigger batch for massive load
            linger_ms=20,
            compression_type='gzip' # Compress to save network bandwidth
        )
        return producer
    except Exception as e:
        logger.error(f"❌ Kafka error: {e}")
        return None

def load_catalog_films(limit=1000): # Load more films for variety
    client = get_mongo_client()
    db = client.cinematch_db
    films = list(db.movies_catalog.find(
        {"poster_url": {"$exists": True, "$ne": ""}},
        {"title": 1, "year": 1, "genres": 1, "genre": 1, "director": 1, "actors": 1, "imdb_id": 1, "poster_url": 1, "_id": 0}
    ).limit(limit))
    client.close()
    return films

def generate_random_rating():
    return random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0]

def phase_1_setup_users(num_users):
    """Creates N users in MongoDB using bulk writes."""
    logger.info(f"🚀 FASE 1: Creazione {num_users:,} utenti...")
    client = get_mongo_client()
    db = client.cinematch_db
    italy_tz = pytz.timezone('Europe/Rome')
    now_iso = datetime.now(italy_tz).isoformat()
    
    requests = []
    created_count = 0
    
    for i in range(num_users):
        user_id = f"{USER_PREFIX}{i}"
        requests.append(InsertOne({
            "user_id": user_id,
            "username": user_id,
            "email": f"{user_id}@test.local",
            "full_name": "Massive Load User",
            "created_at": now_iso,
            "is_active": True,
            "is_test": True
        }))
        
        if len(requests) >= BATCH_SIZE:
            try:
                db.users.bulk_write(requests, ordered=False)
                created_count += len(requests)
            except BulkWriteError as bwe:
                created_count += bwe.details.get('nInserted', 0)
            
            if created_count % 10000 == 0:
                logger.info(f"   ... creati {created_count:,} utenti")
            requests = []
            
    if requests:
        try:
            db.users.bulk_write(requests, ordered=False)
            created_count += len(requests)
        except BulkWriteError as bwe:
             created_count += bwe.details.get('nInserted', 0)
        
    logger.info(f"✅ FASE 1 completata: {created_count:,} utenti nel DB.")
    client.close()
    return created_count

def phase_2_generate_and_execute(num_users, films):
    """Generates and executes operations (Mongo + Kafka)."""
    logger.info(f"🚀 FASE 2: Generazione 1 MILIONE di Eventi...")
    
    client = get_mongo_client()
    db = client.cinematch_db
    producer = get_kafka_producer()
    italy_tz = pytz.timezone('Europe/Rome')
    topic = "user-movie-events"
    
    total_events = 0
    mongo_requests = []
    kafka_events = [] 
    
    start_time = time.time()
    
    for i in range(num_users):
        user_id = f"{USER_PREFIX}{i}"
        
        # Determine how many movies this user watches (random around average)
        # Some watch 5, some watch 35. Avg 20.
        n_movies = random.randint(5, 35)
        
        user_movies = [] # Keep track for update/delete
        
        for _ in range(n_movies):
            # 1. ADD
            movie = random.choice(films)
            movie_data = {
                "name": movie.get("title"),
                "year": movie.get("year"),
                "rating": generate_random_rating(),
                "genres": movie.get("genres") or (movie.get("genre", "").split(",") if movie.get("genre") else []),
                "director": movie.get("director"),
                "actors": movie.get("actors"),
                "imdb_id": movie.get("imdb_id"),
                "poster_url": movie.get("poster_url"),
                "date": datetime.now(italy_tz).strftime("%Y-%m-%d"),
                "user_id": user_id,
                "added_at": datetime.now(italy_tz).isoformat()
            }
            
            mongo_requests.append(InsertOne(movie_data))
            
            event_add = {
                "event_type": "ADD",
                "user_id": user_id,
                "movie": movie_data,
                "timestamp": datetime.now(italy_tz).isoformat()
            }
            kafka_events.append((user_id, event_add))
            total_events += 1
            user_movies.append(movie_data)
        
        # Optional Random Interactions on their own list
        # UPDATE
        if user_movies and random.random() < PROB_UPDATE:
            target_movie = random.choice(user_movies)
            new_rating = generate_random_rating()
            
            mongo_requests.append(UpdateOne(
                {"user_id": user_id, "name": target_movie["name"]},
                {"$set": {"rating": new_rating, "updated_at": datetime.now(italy_tz).isoformat()}}
            ))
            
            target_movie_copy = target_movie.copy()
            target_movie_copy["rating"] = new_rating
            event_update = {
                "event_type": "UPDATE",
                "user_id": user_id,
                "movie": target_movie_copy,
                "timestamp": datetime.now(italy_tz).isoformat()
            }
            kafka_events.append((user_id, event_update))
            total_events += 1

        # DELETE
        if user_movies and random.random() < PROB_DELETE:
            target_movie = random.choice(user_movies)
            mongo_requests.append(DeleteOne({"user_id": user_id, "name": target_movie["name"]}))
            
            event_delete = {
                "event_type": "DELETE",
                "user_id": user_id,
                "movie": {"name": target_movie["name"]},
                "timestamp": datetime.now(italy_tz).isoformat()
            }
            kafka_events.append((user_id, event_delete))
            total_events += 1


        # Batch flush
        if len(mongo_requests) >= BATCH_SIZE:
            db.movies.bulk_write(mongo_requests, ordered=True)
            mongo_requests = []
            
            for key, val in kafka_events:
                 producer.send(topic, key=key, value=val)
            # Flush Kafka asynchronously reasonably often, but not every batch to save time? 
            # Actually flush every batch is safer for consistency in checking
            # But for 1M events, maybe flush every 5 batches?
            # Let's flush every batch to be safe, but use large batch size.
            # producer.flush() -> actually let's rely on background sending to speed up
            
            kafka_events = []
            
            if total_events % 50000 < BATCH_SIZE * 2: # Print every ~50k
                 elapsed = time.time() - start_time
                 rate = total_events / elapsed if elapsed > 0 else 0
                 logger.info(f"   ... generati {total_events:,} eventi | {rate:.0f} ev/s")

    # Final flush
    if mongo_requests:
        db.movies.bulk_write(mongo_requests, ordered=True)
    
    if kafka_events:
        for key, val in kafka_events:
            producer.send(topic, key=key, value=val)
    
    logger.info("   ⏳ Flushing Kafka Producer...")
    producer.flush()
    producer.close()
    client.close()
    
    logger.info(f"✅ FASE 2 completata: {total_events:,} eventi inviati in {time.time()-start_time:.1f}s.")
    return total_events

def phase_3_verify(num_users):
    """Waits for user_stats to populate."""
    logger.info(f"🚀 FASE 3: Verifica Elaborazione Spark (Attesa)...")
    client = get_mongo_client()
    db = client.cinematch_db
    
    # We expect stats for most users
    target_min = int(num_users * 0.90) 
    logger.info(f"   Target: almeno {target_min:,} utenti processati")
    
    count = 0
    start_wait = time.time()
    # Give it more time: 1M events might take 1-2 minutes or more
    timeout = 600 # 10 minutes
    
    while time.time() - start_wait < timeout:
        count = db.user_stats.count_documents({"user_id": {"$regex": f"^{USER_PREFIX}"}})
        
        # Progress Log every 10s
        elapsed = time.time() - start_wait
        if int(elapsed) % 10 == 0:
            logger.info(f"   Stats: {count:,} / {num_users:,} (Time: {elapsed:.0f}s)")
        
        if count >= target_min:
            logger.info(f"✅ OBIETTIVO RAGGIUNTO! {count:,} utenti elaborati.")
            break
        
        time.sleep(5)
    
    client.close()
    return count

def phase_4_cleanup():
    """Deletes all test data."""
    logger.info(f"🚀 FASE 4: Pulizia Totale (Massive)...")
    client = get_mongo_client()
    db = client.cinematch_db
    
    # 1. Users
    res_u = db.users.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_u.deleted_count:,} users")
    
    # 2. Movies (This will be huge)
    res_m = db.movies.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_m.deleted_count:,} movies")
    
    # 3. Stats
    res_s = db.user_stats.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_s.deleted_count:,} stats")
    
    client.close()
    logger.info("✅ Pulizia completata.")

def generate_report(stats):
    # Ensure it saves in the same directory as the script (Prova/)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(base_dir, "spark_kafka_1m_results.json")
    
    logger.info(f"📝 Generazione report in {filename}...")
    try:
        with open(filename, "w") as f:
            json.dump(stats, f, indent=4)
        logger.info("✅ Report salvato.")
    except Exception as e:
        logger.error(f"❌ Errore salvataggio report: {e}")

def main():
    logger.info(f"🎬 STARTING MASSIVE LOAD TEST (50k Users / ~1M Events)")
    test_start_time = time.time()
    stats = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "num_users": NUM_USERS,
            "target_events": "1_000_000",
            "movies_per_user_avg": MOVIES_PER_USER_AVG
        },
        "test_name": "Massive 1 Million Events Test",
        "steps": [
            "1. Create 50k Users",
            "2. Generate 1M Events (ADD/UPDATE/DELETE)",
            "3. Verify Processing",
            "4. Delete All Data"
        ],
        "glossary": {
            "duration": "Tempo in secondi impiegato per completare la fase.",
            "eps": "Events Per Second (Eventi al Secondo). Indica la velocità di throughput.",
            "events": "Numero totale di messaggi Kafka/Mongo generati.",
            "stats_found": "Numero di documenti 'user_stats' trovati in MongoDB (conferma elaborazione Spark).",
            "success": "True se almeno il 90% degli utenti ha ricevuto le statistiche aggiornate.",
            "total_time": "Tempo totale dall'inizio alla fine del test, inclusa pulizia."
        }
    }
    
    try:
        films = load_catalog_films()
        if not films: return

        # 0. Pre-Cleanup
        phase_4_cleanup()

        # Phase 1
        t0 = time.time()
        created = phase_1_setup_users(NUM_USERS)
        stats["phase_1_setup"] = {"duration": round(time.time()-t0, 2), "users": created}

        # Phase 2
        t0 = time.time()
        total_events = phase_2_generate_and_execute(NUM_USERS, films)
        duration_p2 = time.time() - t0
        stats["phase_2_execution"] = {
            "duration": round(duration_p2, 2),
            "events": total_events,
            "eps": round(total_events/duration_p2, 2)
        }
        
        # Phase 3
        t0 = time.time()
        final_count = phase_3_verify(NUM_USERS)
        stats["phase_3_verification"] = {
            "duration": round(time.time() - t0, 2),
            "stats_found": final_count,
            "success": final_count >= int(NUM_USERS * 0.9)
        }
        
        stats["total_time"] = round(time.time() - test_start_time, 2)
        
        generate_report(stats)
        
        phase_4_cleanup()
        
        logger.info("\n🏁 TEST MASSIVO COMPLETATO!")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrotto! Pulizia...")
        phase_4_cleanup()
    except Exception as e:
        logger.error(f"❌ ERROR: {e}")
        phase_4_cleanup()

if __name__ == "__main__":
    main()
