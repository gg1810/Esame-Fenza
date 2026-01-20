"""
Script di migrazione: Estrae director_stats e actor_stats da user_stats
e popola la nuova collezione user_affinities (struttura piatta).

Esecuzione:
    python migrate_affinities.py

Questo script:
1. Legge tutti gli user_stats esistenti
2. Estrae director_stats, actor_stats, genre_counts (nidificati)
3. Crea documenti piatti in user_affinities
4. Crea gli indici necessari
"""
from pymongo import MongoClient, UpdateOne
import os
from datetime import datetime
import pytz

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")

def create_indexes(db):
    """Crea gli indici per user_affinities."""
    print("📊 Creazione indici su user_affinities...")
    
    # Indice principale: query per user_id + type
    db.user_affinities.create_index(
        [("user_id", 1), ("type", 1)],
        name="idx_user_type"
    )
    
    # Indice per sort per count (Top Directors/Actors)
    db.user_affinities.create_index(
        [("user_id", 1), ("type", 1), ("count", -1)],
        name="idx_user_type_count"
    )
    
    # Indice per calcolo avg_rating (sum_voti / count)
    db.user_affinities.create_index(
        [("user_id", 1), ("type", 1), ("sum_voti", -1)],
        name="idx_user_type_rating"
    )
    
    print("✅ Indici creati con successo!")
    
    # Mostra indici creati
    indexes = db.user_affinities.list_indexes()
    for idx in indexes:
        print(f"   📌 {idx['name']}: {idx['key']}")


def migrate_affinities():
    """Migra i dati da struttura nidificata a piatta."""
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    print("🚀 Inizio migrazione user_stats → user_affinities...")
    print(f"   Database: {MONGODB_URL}")
    
    # Leggi tutti gli user_stats
    all_stats = list(db.user_stats.find({}))
    print(f"📊 Trovati {len(all_stats)} utenti con statistiche")
    
    if not all_stats:
        print("⚠️ Nessun utente trovato, creazione indici e uscita...")
        create_indexes(db)
        client.close()
        return
    
    total_affinities = 0
    italy_tz = pytz.timezone('Europe/Rome')
    now = datetime.now(italy_tz).isoformat()
    
    for stats in all_stats:
        user_id = stats.get("user_id")
        if not user_id:
            continue
        
        bulk_ops = []
        
        # ═══════════════════════════════════════════════════════════
        # 1. Estrai director_stats
        # ═══════════════════════════════════════════════════════════
        director_stats = stats.get("director_stats", {})
        for dir_name, dir_data in director_stats.items():
            count = dir_data.get("count", 0) if isinstance(dir_data, dict) else 0
            sum_voti = dir_data.get("sum_voti", 0) if isinstance(dir_data, dict) else 0
            
            if count > 0:
                # Ripristina nome leggibile (underscore → spazi)
                display_name = dir_name.replace("_", " ")
                
                bulk_ops.append(UpdateOne(
                    {"_id": f"{user_id}_director_{dir_name}"},
                    {
                        "$set": {
                            "user_id": user_id,
                            "type": "director",
                            "name": display_name,
                            "name_key": dir_name,  # Chiave originale per lookup
                            "count": count,
                            "sum_voti": sum_voti,
                            "migrated_at": now
                        }
                    },
                    upsert=True
                ))
        
        # ═══════════════════════════════════════════════════════════
        # 2. Estrai actor_stats
        # ═══════════════════════════════════════════════════════════
        actor_stats = stats.get("actor_stats", {})
        for actor_name, actor_data in actor_stats.items():
            count = actor_data.get("count", 0) if isinstance(actor_data, dict) else 0
            sum_voti = actor_data.get("sum_voti", 0) if isinstance(actor_data, dict) else 0
            
            if count > 0:
                display_name = actor_name.replace("_", " ")
                
                bulk_ops.append(UpdateOne(
                    {"_id": f"{user_id}_actor_{actor_name}"},
                    {
                        "$set": {
                            "user_id": user_id,
                            "type": "actor",
                            "name": display_name,
                            "name_key": actor_name,
                            "count": count,
                            "sum_voti": sum_voti,
                            "migrated_at": now
                        }
                    },
                    upsert=True
                ))
        
        # ═══════════════════════════════════════════════════════════
        # 3. Estrai genre_counts (opzionale - per coerenza)
        # ═══════════════════════════════════════════════════════════
        genre_counts = stats.get("genre_counts", {})
        for genre_name, count in genre_counts.items():
            if count > 0:
                display_name = genre_name.replace("_", " ")
                
                bulk_ops.append(UpdateOne(
                    {"_id": f"{user_id}_genre_{genre_name}"},
                    {
                        "$set": {
                            "user_id": user_id,
                            "type": "genre",
                            "name": display_name,
                            "name_key": genre_name,
                            "count": count,
                            "sum_voti": 0,  # I generi non hanno rating
                            "migrated_at": now
                        }
                    },
                    upsert=True
                ))
        
        # Scrivi bulk
        if bulk_ops:
            result = db.user_affinities.bulk_write(bulk_ops, ordered=False)
            total_affinities += len(bulk_ops)
            print(f"  ✅ {user_id}: {len(bulk_ops)} affinities migrate "
                  f"(directors: {len(director_stats)}, actors: {len(actor_stats)}, genres: {len(genre_counts)})")
    
    print(f"\n🎉 Migrazione completata: {total_affinities} affinities create")
    
    # Crea indici
    create_indexes(db)
    
    # Statistiche finali
    total_docs = db.user_affinities.count_documents({})
    directors_count = db.user_affinities.count_documents({"type": "director"})
    actors_count = db.user_affinities.count_documents({"type": "actor"})
    genres_count = db.user_affinities.count_documents({"type": "genre"})
    
    print(f"\n📊 Statistiche collezione user_affinities:")
    print(f"   📁 Totale documenti: {total_docs}")
    print(f"   🎬 Directors: {directors_count}")
    print(f"   🎭 Actors: {actors_count}")
    print(f"   🏷️ Genres: {genres_count}")
    
    client.close()


def cleanup_old_fields():
    """
    Rimuove i campi obsoleti da user_stats (OPZIONALE - eseguire dopo test).
    ⚠️ Eseguire SOLO dopo aver verificato che tutto funziona!
    """
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    print("🧹 CLEANUP: Rimozione campi obsoleti da user_stats...")
    print("⚠️ ATTENZIONE: Questa operazione è IRREVERSIBILE!")
    
    confirm = input("Digitare 'CONFERMA' per procedere: ")
    if confirm != "CONFERMA":
        print("❌ Operazione annullata.")
        client.close()
        return
    
    result = db.user_stats.update_many(
        {},
        {
            "$unset": {
                "director_stats": "",
                "actor_stats": "",
                "best_rated_directors": "",
                "most_watched_directors": "",
                "best_rated_actors": "",
                "most_watched_actors": ""
            }
        }
    )
    
    print(f"✅ Cleanup completato: {result.modified_count} documenti aggiornati")
    client.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_old_fields()
    else:
        migrate_affinities()
        print("\n💡 Per rimuovere i campi obsoleti da user_stats, esegui:")
        print("   python migrate_affinities.py --cleanup")
