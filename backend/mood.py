"""
Mood-Based Movie Recommendation System
=======================================
Genera ogni giorno 10 film per ciascun mood e li salva nella collezione MongoDB `mood_movies`.
Eseguito automaticamente dallo scheduler alle 2:00 ogni notte.

Author: CineMatch Team
"""

import os
import pytz
from datetime import datetime
from pymongo import MongoClient
from typing import Dict, List

# ============================================
# CONFIGURATION
# ============================================

# Connessione MongoDB
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME = "cinematch_db"
COLLECTION_CATALOG = "movies_catalog"
COLLECTION_MOOD_MOVIES = "mood_movies"

# Timezone italiano (coerente con il resto del progetto)
ITALY_TZ = pytz.timezone('Europe/Rome')

# Numero di film da generare per ogni mood
FILMS_PER_MOOD = 10

# Criteri di selezione film
MIN_AVG_VOTE = 6.0
MIN_VOTES = 20000

# ============================================
# MAPPING MOOD → GENRES
# ============================================
# Centralizzato in una costante per facile manutenzione

MOOD_GENRE_MAPPING = {
    "felice": ["comedy"],
    "malinconico": ["drama"],
    "eccitato": ["action", "adventure", "fantasy"],
    "rilassato": ["animation", "history"],
    "romantico": ["romance"],
    "thriller": ["thriller", "horror"]
}


# ============================================
# MAIN FUNCTION
# ============================================

def generate_mood_recommendations() -> None:
    """
    Genera raccomandazioni per tutti i mood e le salva nel database.
    
    Il processo:
    1. Per ogni mood, esegue una query MongoDB con $match e $sample
    2. Seleziona casualmente fino a 10 film per mood
    3. Salva tutto in un singolo documento in `mood_movies` con replace/upsert
    
    Returns:
        None
    """
    print("🎭 [Mood Recommender] Inizio generazione raccomandazioni per mood...")
    
    # Connessione al database
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    catalog_collection = db[COLLECTION_CATALOG]
    mood_collection = db[COLLECTION_MOOD_MOVIES]
    
    # Timestamp per questa generazione (UTC, coerente con il progetto)
    generated_at = datetime.now(ITALY_TZ).isoformat()
    
    # Dizionario che conterrà i risultati per ogni mood
    mood_results = {}
    
    # Genera raccomandazioni per ogni mood
    for mood, genres in MOOD_GENRE_MAPPING.items():
        print(f"   🔍 Generazione film per mood: {mood.upper()}")
        
        # Pipeline MongoDB: $match + $sample per selezione casuale
        pipeline = [
            {
                "$match": {
                    "avg_vote": {"$gte": MIN_AVG_VOTE},
                    "votes": {"$gte": MIN_VOTES},
                    "genres": {"$elemMatch": {"$in": genres}}  # Almeno uno dei generi del mood
                }
            },
            {
                "$sample": {"size": FILMS_PER_MOOD}  # Selezione casuale
            },
            {
                "$project": {
                    "_id": 0,
                    "imdb_id": 1,
                    "title": {"$ifNull": ["$original_title", "$title"]},  # Preferisce original_title
                }
            }
        ]
        
        # Esegui la pipeline
        results = list(catalog_collection.aggregate(pipeline))
        
        # Prepara i film per questo mood
        mood_films = []
        for film in results:
            mood_films.append({
                "imdb_id": film.get("imdb_id"),
                "title": film.get("title")
            })
        
        # Salva nel dizionario
        mood_results[mood] = mood_films
        
        # Log del risultato
        films_count = len(mood_films)
        print(f"      ✅ Generati {films_count} film per '{mood}' (target: {FILMS_PER_MOOD})")
        
        # Se ci sono meno di 10 film, avvisa
        if films_count < FILMS_PER_MOOD:
            print(f"      ⚠️ Attenzione: solo {films_count} film disponibili (meno di {FILMS_PER_MOOD})")
    
    # Crea il documento finale da salvare
    mood_document = {
        "generated_at": generated_at,
        **mood_results  # Aggiunge tutti i mood come chiavi del documento
    }
    
    # Salva nel database con replace/upsert
    # Usa un placeholder _id fisso per sovrascrivere sempre lo stesso documento
    mood_collection.replace_one(
        {"_id": "daily_mood_recommendations"},  # Filtro: usa sempre lo stesso _id
        mood_document,                          # Documento da salvare
        upsert=True                             # Crea se non esiste, sostituisce se esiste
    )
    
    # Chiudi connessione
    client.close()
    
    # Summary finale
    total_films = sum(len(films) for films in mood_results.values())
    print(f"✅ [Mood Recommender] Completato! {total_films} film generati per {len(MOOD_GENRE_MAPPING)} mood.")
    print(f"   📅 Timestamp: {generated_at}")
    print(f"   💾 Salvato in: {DB_NAME}.{COLLECTION_MOOD_MOVIES}")


# ============================================
# ENTRY POINT (per testing manuale)
# ============================================

if __name__ == "__main__":
    """
    Permette di eseguire lo script manualmente per testing:
    python mood.py
    """
    try:
        generate_mood_recommendations()
    except Exception as e:
        print(f"❌ [Mood Recommender] Errore durante la generazione: {e}")
        import traceback
        traceback.print_exc()
