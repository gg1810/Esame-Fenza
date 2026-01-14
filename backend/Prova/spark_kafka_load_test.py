"""
🎬 CineMatch - Scalable Spark/Kafka Load Test (10k Users)
=========================================================

Simula un carico massiccio di 10.000 utenti concorrenti per testare la latenza e la robustezza della pipeline.

Architecture Flow:
1.  **Generazione**: Lo script genera 10.000 operazioni realistiche (ADD, UPDATE, DELETE).
2.  **Scrittura Duale**: Lo script scrive su MongoDB (collection 'movies') E invia evento a Kafka (topic 'user-movie-events').
3.  **Elaborazione Spark**: Spark Streaming legge eventi da Kafka come trigger.
4.  **Recupero Dati**: Spark legge i dati aggiornati da MongoDB 'movies' per l'utente.
5.  **Calcolo**: Spark calcola statistiche complesse (trend, generi, registi preferiti).
6.  **Persistenza**: Spark salva i risultati in MongoDB 'user_stats'.

Fasi del Test:
*   **FASE 1 (Setup)**: Creazione massiva di 10.000 profili utente nel DB (bulk insert). Non genera traffico Kafka, solo prepara il DB.
*   **FASE 2 (Esecuzione)**: Il cuore del test. Genera traffico reale simulando utenti che aggiungono, votano o rimuovono film. Misura il throughput di scrittura del sistema.
*   **FASE 3 (Verifica)**: Monitora la collection `user_stats` per vedere quanto tempo impiega Spark a processare tutto il backlog generato nella Fase 2.
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
logger = logging.getLogger("LoadTest")

# Configuration
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
NUM_USERS = 10_000
USER_PREFIX = "loadtest_u"  # Short prefix to save bytes
BATCH_SIZE = 1000  # MongoDB bulk write batch size

# Probabilities
PROB_UPDATE = 0.20  # 20% of users will update a movie
PROB_DELETE = 0.10  # 10% of users will delete a movie

def get_mongo_client():
    return MongoClient(MONGO_URL)

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks=1, # Faster ack for load test
            retries=1,
            batch_size=16384,
            linger_ms=10
        )
        return producer
    except Exception as e:
        logger.error(f"❌ Kafka error: {e}")
        return None

def load_catalog_films(limit=500):
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

# ============================================================================
# PHASES
# ============================================================================

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
            "full_name": "Load Test User",
            "created_at": now_iso,
            "is_active": True,
            "is_test": True
        }))
        
        if len(requests) >= BATCH_SIZE:
            try:
                db.users.bulk_write(requests, ordered=False)
                created_count += len(requests)
            except BulkWriteError as bwe:
                # Ignore duplicates, count only inserted (approx)
                logger.warning(f"   ⚠️ Bulk write warning (likely duplicates): {bwe.details.get('nInserted', 0)} inserted")
                created_count += bwe.details.get('nInserted', 0)
            logger.info(f"   ... creati {created_count:,} utenti")
            requests = []
            
    if requests:
        try:
            db.users.bulk_write(requests, ordered=False)
            created_count += len(requests)
        except BulkWriteError as bwe:
             logger.warning(f"   ⚠️ Bulk write warning (likely duplicates): {bwe.details.get('nInserted', 0)} inserted")
             created_count += bwe.details.get('nInserted', 0)
        
    logger.info(f"✅ FASE 1 completata: {created_count:,} utenti nel DB.")
    client.close()
    return created_count


def phase_2_generate_and_execute(num_users, films):
    """Generates and executes operations (Mongo + Kafka)."""
    logger.info(f"🚀 FASE 2: Generazione ed Esecuzione Eventi...")
    
    client = get_mongo_client()
    db = client.cinematch_db
    producer = get_kafka_producer()
    italy_tz = pytz.timezone('Europe/Rome')
    topic = "user-movie-events"
    
    total_events = 0
    mongo_requests = []
    kafka_events = [] # Buffer Kafka events
    
    start_time = time.time()
    
    # We iterate users and generate a sequence for each
    for i in range(num_users):
        user_id = f"{USER_PREFIX}{i}"
        
        # 1. ADD Operation (Base for everyone)
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
        
        # Queue Mongo Insert
        mongo_requests.append(InsertOne(movie_data))
        
        # Buffer Kafka Event (ADD)
        event_add = {
            "event_type": "ADD",
            "user_id": user_id,
            "movie": movie_data,
            "timestamp": datetime.now(italy_tz).isoformat()
        }
        kafka_events.append((user_id, event_add))
        total_events += 1
        
        # 2. UPDATE Operation (20% chance)
        if random.random() < PROB_UPDATE:
            # ... (Update logic)
            new_rating = generate_random_rating()
            while new_rating == movie_data["rating"]:
                new_rating = generate_random_rating()
            
            # Queue Mongo Update
            mongo_requests.append(UpdateOne(
                {"user_id": user_id, "name": movie_data["name"]},
                {"$set": {"rating": new_rating, "updated_at": datetime.now(italy_tz).isoformat()}}
            ))
            
            # Buffer Kafka Event (UPDATE)
            movie_updated = movie_data.copy()
            movie_updated["rating"] = new_rating
            event_update = {
                "event_type": "UPDATE",
                "user_id": user_id,
                "movie": movie_updated,
                "timestamp": datetime.now(italy_tz).isoformat()
            }
            kafka_events.append((user_id, event_update))
            total_events += 1
            
        # 3. DELETE Operation (10% chance)
        if random.random() < PROB_DELETE:
             # Queue Mongo Delete
            mongo_requests.append(DeleteOne({"user_id": user_id, "name": movie_data["name"]}))
            
            # Buffer Kafka Event (DELETE)
            event_delete = {
                "event_type": "DELETE",
                "user_id": user_id,
                "movie": {"name": movie_data["name"]},
                "timestamp": datetime.now(italy_tz).isoformat()
            }
            kafka_events.append((user_id, event_delete))
            total_events += 1

        # Execute Bulk Write every BATCH_SIZE users (or events)
        if len(mongo_requests) >= BATCH_SIZE:
            db.movies.bulk_write(mongo_requests, ordered=True) # Ordered to respect sequence
            mongo_requests = []
            
            # Send Kafka Events AFTER Mongo Write
            for key, val in kafka_events:
                 producer.send(topic, key=key, value=val)
            producer.flush() # Ensure sent
            kafka_events = []
            
            if i % 1000 == 0:
                elapsed = time.time() - start_time
                rate = total_events / elapsed if elapsed > 0 else 0
                logger.info(f"   ... elaborati {i+1} utenti | {total_events} eventi | {rate:.0f} ev/s")

    # Final flush
    if mongo_requests:
        db.movies.bulk_write(mongo_requests, ordered=True)
    
    if kafka_events:
        for key, val in kafka_events:
            producer.send(topic, key=key, value=val)
    
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
    
    target_min = int(num_users * 0.85) # Conservative target
    logger.info(f"   Target: almeno {target_min:,} documenti in user_stats")
    
    count = 0
    start_wait = time.time()
    while time.time() - start_wait < 300: # 5 min timeout
        count = db.user_stats.count_documents({"user_id": {"$regex": f"^{USER_PREFIX}"}})
        logger.info(f"   Stats attuali: {count:,} / {num_users:,}")
        
        if count >= target_min:
            logger.info("✅ Stats Sufficienti! Spark sta elaborando.")
            # Check detailed stats for last user
            stats = db.user_stats.find_one({"user_id": f"{USER_PREFIX}{num_users-1}"})
            if stats:
                logger.info(f"   Sample user ({USER_PREFIX}{num_users-1}) stats: {stats.get('total_watched')} watched")
            break
        
        time.sleep(5)
    
    client.close()
    return count

def phase_4_cleanup():
    """Deletes all test data."""
    logger.info(f"🚀 FASE 4: Pulizia Totale...")
    client = get_mongo_client()
    db = client.cinematch_db
    
    # 1. Users
    res_u = db.users.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_u.deleted_count:,} users")
    
    # 2. Movies
    res_m = db.movies.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_m.deleted_count:,} movies")
    
    # 3. Stats
    res_s = db.user_stats.delete_many({"user_id": {"$regex": f"^{USER_PREFIX}"}})
    logger.info(f"   🗑️  Deleted {res_s.deleted_count:,} stats")
    
    client.close()
    
    # 4. Local File
    if os.path.exists("load_test_summary.json"):
        os.remove("load_test_summary.json")
        logger.info("   🗑️  Deleted local summary report")
        
    logger.info("✅ Pulizia completata.")







def generate_report(stats):
    """Generates a JSON report file with the test results."""
    # Ensure it saves in the same directory as the script (Prova/)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(base_dir, "spark_kafka_load_results.json")
    
    logger.info(f"📝 Generazione report in {filename}...")
    try:
        with open(filename, "w") as f:
            json.dump(stats, f, indent=4)
        logger.info("✅ Report salvato.")
    except Exception as e:
        logger.error(f"❌ Errore salvataggio report: {e}")

def main():
    logger.info(f"🎬 STARTING LOAD TEST ({NUM_USERS:,} USERS)")
    test_start_time = time.time()
    stats = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "num_users": NUM_USERS,
            "prob_update": PROB_UPDATE,
            "prob_delete": PROB_DELETE
        },
        "architecture_flow": [
            "1. Script genera 10.000 operazioni realistiche (ADD, UPDATE, DELETE).",
            "2. Scrittura Duale: Script scrive su MongoDB (collection 'movies') E invia evento a Kafka (topic 'user-movie-events').",
            "3. Spark Streaming legge eventi da Kafka come trigger.",
            "4. Spark legge i dati aggiornati da MongoDB 'movies' per l'utente.",
            "5. Spark calcola statistiche complesse (trend, generi, registi preferiti).",
            "6. Spark salva i risultati in MongoDB 'user_stats'."
        ],
        "test_phases": [
            "FASE 1 (Setup): Creazione massiva di 10.000 profili utente nel DB (bulk insert). Non genera traffico Kafka, solo prepara il DB.",
            "FASE 2 (Esecuzione): Il cuore del test. Genera traffico reale simulando utenti che aggiungono, votano o rimuovono film. Misura il throughput di scrittura del sistema.",
            "FASE 3 (Verifica): Monitora la collection 'user_stats' per vedere quanto tempo impiega Spark a processare tutto il backlog generato nella Fase 2."
        ]
    }
    
    try:
        # Load catalogs once
        films = load_catalog_films()
        if not films:
            logger.error("No films found in catalog!")
            return

        # 0. Pre-Cleanup to ensure clean state
        phase_4_cleanup()

        # Execute Phase 1
        t0 = time.time()
        created = phase_1_setup_users(NUM_USERS)
        stats["phase_1_setup"] = {
            "duration_seconds": round(time.time() - t0, 2),
            "users_created": created
        }

        # Execute Phase 2
        t0 = time.time()
        total_events = phase_2_generate_and_execute(NUM_USERS, films)
        duration_p2 = time.time() - t0
        stats["phase_2_execution"] = {
            "duration_seconds": round(duration_p2, 2),
            "total_events": total_events,
            "events_per_second": round(total_events / duration_p2, 2) if duration_p2 > 0 else 0
        }
        
        # Execute Phase 3
        t0 = time.time()
        final_count = phase_3_verify(NUM_USERS)
        stats["phase_3_verification"] = {
            "duration_seconds": round(time.time() - t0, 2),
            "final_stats_count": final_count,
            "target_met": final_count >= int(NUM_USERS * 0.85)
        }
        
        stats["total_duration_seconds"] = round(time.time() - test_start_time, 2)
        
        # Generate Report BEFORE cleanup
        generate_report(stats)
        
        phase_4_cleanup()
        
        logger.info("\n🏁 TEST COMPLETATO CON SUCCESSO!")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrotto dall'utente! Avvio pulizia di emergenza...")
        phase_4_cleanup()
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        phase_4_cleanup()

if __name__ == "__main__":
    main()
