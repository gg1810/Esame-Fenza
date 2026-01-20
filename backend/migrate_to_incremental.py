"""
Script di migrazione per inizializzare i contatori incrementali O(1)
dai dati legacy esistenti in user_stats.

Esegui PRIMA di attivare process_partition_incremental in Spark.

Uso: python migrate_to_incremental.py
"""
import os
import re
from pymongo import MongoClient
from collections import Counter

# Configurazione
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")

def clean_key(name):
    """Pulisce i nomi per usarli come chiavi MongoDB (no punti, no $)."""
    if not name:
        return "Unknown"
    return name.strip().replace(".", "_").replace("$", "_").replace(" ", "_")


def migrate_user_stats():
    """
    Migra le user_stats dal formato legacy al formato incrementale.
    Inizializza i contatori dai dati calcolati esistenti.
    """
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    print("🚀 Inizio migrazione user_stats a schema incrementale O(1)...")
    
    # Trova tutti gli utenti con stats legacy
    users = list(db.user_stats.find({}))
    print(f"📊 Trovati {len(users)} utenti da migrare")
    
    migrated = 0
    errors = 0
    
    for user_stats in users:
        user_id = user_stats.get("user_id")
        if not user_id:
            continue
            
        try:
            # Leggi TUTTI i film dell'utente per costruire i contatori
            user_movies = list(db.movies.find({"user_id": user_id}))
            
            if not user_movies:
                print(f"  ⚠️ {user_id}: Nessun film, skip")
                continue
            
            # Inizializza contatori
            total_watched = len(user_movies)
            sum_ratings = 0
            watch_time_minutes = 0
            genre_counts = Counter()
            director_stats = {}
            actor_stats = {}
            rating_distribution = Counter()
            monthly_counts = Counter()
            
            # Batch lookup catalogo
            titles = [m.get("name") for m in user_movies if m.get("name")]
            catalog_map = {}
            
            if titles:
                catalog_docs = list(db.movies_catalog.find({
                    "$or": [
                        {"title": {"$in": titles}},
                        {"original_title": {"$in": titles}}
                    ]
                }))
                for d in catalog_docs:
                    if d.get('title'): catalog_map[d.get('title')] = d
                    if d.get('original_title'): catalog_map[d.get('original_title')] = d
            
            # Processa ogni film
            for movie in user_movies:
                title = movie.get("name")
                rating = movie.get("rating") or 0
                date = movie.get("date")
                
                # Sum ratings
                if rating:
                    sum_ratings += rating
                    if 1 <= rating <= 5:
                        rating_distribution[str(rating)] += 1
                
                # Catalog info
                cat = catalog_map.get(title, {})
                
                # Duration
                duration = cat.get("duration") or movie.get("duration") or 0
                try:
                    watch_time_minutes += int(duration)
                except:
                    pass
                
                # Genres
                genres = cat.get("genres") or movie.get("genres") or []
                if isinstance(genres, str):
                    genres = [g.strip() for g in genres.split(',')]
                for g in genres:
                    if g:
                        genre_counts[clean_key(g)] += 1
                
                # Director
                director = cat.get("director") or movie.get("director") or ""
                if director:
                    for d in re.split(r'[,|]', director):
                        d = d.strip()
                        if d:
                            d_key = clean_key(d)
                            if d_key not in director_stats:
                                director_stats[d_key] = {"count": 0, "sum_voti": 0}
                            director_stats[d_key]["count"] += 1
                            if rating:
                                director_stats[d_key]["sum_voti"] += rating
                
                # Actors
                actors = cat.get("actors") or movie.get("actors") or ""
                if actors:
                    for a in re.split(r'[,|]', actors)[:5]:  # Max 5
                        a = a.strip()
                        if a:
                            a_key = clean_key(a)
                            if a_key not in actor_stats:
                                actor_stats[a_key] = {"count": 0, "sum_voti": 0}
                            actor_stats[a_key]["count"] += 1
                            if rating:
                                actor_stats[a_key]["sum_voti"] += rating
                
                # Monthly counts (Nidificati per anno)
                if date:
                    try:
                        year_key = date[:4]  # "2026"
                        month_key = date[5:7] # "01"
                        if year_key not in monthly_counts:
                            monthly_counts[year_key] = Counter()
                        monthly_counts[year_key][month_key] += 1
                    except:
                        pass
            
            # Converti i Counter nidificati in dict
            monthly_counts_dict = {y: dict(m) for y, m in monthly_counts.items()}
            update_fields = {
                "total_watched": total_watched,
                "sum_ratings": sum_ratings,
                "watch_time_minutes": watch_time_minutes,
                "genre_counts": dict(genre_counts),
                "director_stats": director_stats,
                "actor_stats": actor_stats,
                "rating_distribution": dict(rating_distribution),
                "monthly_counts": monthly_counts_dict,
                "stats_version": "4.0_incremental",
                "migrated_at": __import__('datetime').datetime.now().isoformat()
            }
            
            db.user_stats.update_one(
                {"user_id": user_id},
                {"$set": update_fields}
            )
            
            migrated += 1
            print(f"  ✅ {user_id}: {total_watched} film, sum_ratings={sum_ratings}")
            
        except Exception as e:
            errors += 1
            print(f"  ❌ {user_id}: Errore - {e}")
    
    print(f"\n{'='*50}")
    print(f"✅ Migrazione completata!")
    print(f"   Migrati: {migrated}")
    print(f"   Errori: {errors}")
    print(f"\nOra puoi attivare process_partition_incremental in Spark.")
    
    client.close()


if __name__ == "__main__":
    migrate_user_stats()
