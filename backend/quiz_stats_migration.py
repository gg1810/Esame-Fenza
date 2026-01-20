import os
from pymongo import MongoClient
import pytz
from datetime import datetime

# Configurazione
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGODB_URL)
db = client.cinematch_db

users_collection = db.users
stats_collection = db.user_stats

def migrate_quiz_stats():
    """
    Migra le statistiche dei quiz da user_stats (flat) a users (nested 'quiz').
    """
    print("🚀 Avvio migrazione statistiche quiz...")
    
    # Trova tutti gli utenti che hanno statistiche quiz in user_stats
    stats_cursor = stats_collection.find({
        "$or": [
            {"quiz_total_attempts": {"$exists": True}},
            {"quiz_correct_count": {"$exists": True}},
            {"quiz_wrong_count": {"$exists": True}},
            {"last_quiz_date": {"$exists": True}}
        ]
    })
    
    count = 0
    updated = 0
    
    for stats in stats_cursor:
        user_id = stats.get("user_id")
        if not user_id:
            continue
            
        count += 1
        
        # Estrai dati quiz vecchi
        quiz_data = {
            "last_date": stats.get("last_quiz_date"),
            "correct_count": stats.get("quiz_correct_count", 0),
            "wrong_count": stats.get("quiz_wrong_count", 0),
            "total_attempts": stats.get("quiz_total_attempts", 0)
        }
        
        # Se non c'è nessuna info utile, salta
        if not any(quiz_data.values()):
            continue
            
        print(f"🔄 Migrazione utente {user_id}: {quiz_data}")
        
        # Aggiorna users collection con struttura nested
        result = users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"quiz": quiz_data}}
        )
        
        if result.modified_count > 0:
            updated += 1
            
    print(f"✅ Migrazione completata. Processati {count} record, Aggiornati {updated} utenti.")

if __name__ == "__main__":
    try:
        migrate_quiz_stats()
    except Exception as e:
        print(f"❌ Errore durante la migrazione: {e}")
    finally:
        client.close()
