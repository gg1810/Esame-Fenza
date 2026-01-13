"""
Script per caricare il catalogo film (movies_final.csv) su MongoDB.
Include gestione dei poster con fallback a immagine stock.
"""
import os
import pandas as pd
import pytz
from datetime import datetime
from pymongo import MongoClient
from typing import Optional

# Configurazione
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
CSV_PATH = os.getenv("MOVIES_CSV_PATH", "/data/movies_final.csv")

# URL immagine stock di fallback
STOCK_POSTER_URL = "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster"

# Alternative per immagini stock più belle:
# STOCK_POSTER_URL = "https://placehold.co/500x750/1a1a2e/e50914?text=No+Poster&font=montserrat"
# STOCK_POSTER_URL = "/assets/no-poster.png"  # Immagine locale


def clean_value(value):
    """Pulisce i valori NaN/None."""
    if pd.isna(value):
        return None
    return value


def parse_genres(genre_string: str) -> list:
    """Converte la stringa dei generi in lista."""
    if not genre_string or pd.isna(genre_string):
        return []
    return [g.strip() for g in str(genre_string).split(",")]


def normalize_title(text: str) -> str:
    """Rimuove accenti e caratteri speciali per matching/ricerca."""
    if not text: return ""
    import unicodedata
    import re
    normalized = unicodedata.normalize('NFD', text)
    result = "".join([c for c in normalized if not unicodedata.combining(c)])
    
    special_chars = {
        'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u',
        'Ā': 'A', 'Ē': 'E', 'Ī': 'I', 'Ō': 'O', 'Ū': 'U',
        'ł': 'l', 'Ł': 'L', 'ø': 'o', 'Ø': 'O', 'æ': 'ae', 'Æ': 'AE',
        'œ': 'oe', 'Œ': 'OE', 'ß': 'ss', 'đ': 'd', 'Đ': 'D',
        'ñ': 'n', 'Ñ': 'N', 'ç': 'c', 'Ç': 'C'
    }
    for char, replacement in special_chars.items():
        result = result.replace(char, replacement)
    
    result = re.sub(r'[^a-zA-Z0-9\s]', ' ', result)
    result = " ".join(result.split()).lower()
    return result


def get_poster_url(row) -> str:
    """
    Restituisce l'URL del poster.
    Priorità:
    1. poster_url dal CSV (se presente e valido)
    2. Costruzione URL da IMDb ID (se disponibile)
    3. Immagine stock di fallback
    """
    # 1. Controlla se c'è un poster_url valido nel CSV
    poster_url = clean_value(row.get('poster_url'))
    if poster_url and str(poster_url).startswith('http'):
        return poster_url
    
    # 2. Se non c'è, usa immagine stock
    return STOCK_POSTER_URL


def load_movies_catalog():
    """Carica il catalogo completo dei film su MongoDB."""
    print("🎬 Caricamento catalogo film su MongoDB...")
    
    client = MongoClient(MONGO_URL)
    db = client.cinematch_db
    
    # ============================================
    # COLLEZIONE: movies_catalog (catalogo generale)
    # ============================================
    catalog = db.movies_catalog
    
    # Crea indici per ricerche veloci
    catalog.create_index("imdb_id", unique=True, sparse=True)
    catalog.create_index("title")
    catalog.create_index("year")
    catalog.create_index("genres")
    catalog.create_index("normalized_title")
    catalog.create_index("normalized_original_title")
    catalog.create_index([("title", "text"), ("original_title", "text"), ("director", "text")])
    
    print(f"📂 Lettura file: {CSV_PATH}")
    
    if not os.path.exists(CSV_PATH):
        print(f"❌ File non trovato: {CSV_PATH}")
        return False
    
    # Leggi CSV
    df = pd.read_csv(CSV_PATH, low_memory=False)
    print(f"📊 Trovati {len(df)} film nel CSV")
    
    # Prepara i documenti
    movies = []
    skipped = 0
    
    for idx, row in df.iterrows():
        try:
            # Salta film senza titolo
            if pd.isna(row.get('title')):
                skipped += 1
                continue
            
            title = str(row['title'])
            original_title = clean_value(row.get('original_title'))

            movie = {
                "imdb_id": clean_value(row.get('imdb_title_id')),
                "title": title,
                "original_title": original_title,
                "normalized_title": normalize_title(title),
                "normalized_original_title": normalize_title(original_title) if original_title else None,
                "year": int(row['year']) if pd.notna(row.get('year')) else None,
                "date_published": clean_value(row.get('date_published')),
                "genres": parse_genres(row.get('genre')),
                "duration": int(row['duration']) if pd.notna(row.get('duration')) else None,
                "country": clean_value(row.get('country')),
                "movie_language": clean_value(row.get('language')),  # Rinominato per evitare conflitto con MongoDB text index
                "director": clean_value(row.get('director')),
                "writer": clean_value(row.get('writer')),
                "production_company": clean_value(row.get('production_company')),
                "actors": clean_value(row.get('actors')),
                "description": clean_value(row.get('description')),
                "avg_vote": float(row['avg_vote']) if pd.notna(row.get('avg_vote')) else None,
                "votes": int(row['votes']) if pd.notna(row.get('votes')) else None,
                "budget": clean_value(row.get('budget')),
                "usa_gross_income": clean_value(row.get('usa_gross_income')),
                "worldwide_gross_income": clean_value(row.get('worlwide_gross_income')),  # typo nel CSV originale
                "metascore": float(row['metascore']) if pd.notna(row.get('metascore')) else None,
                "reviews_from_users": int(row['reviews_from_users']) if pd.notna(row.get('reviews_from_users')) else None,
                "reviews_from_critics": int(row['reviews_from_critics']) if pd.notna(row.get('reviews_from_critics')) else None,
                "link_imdb": clean_value(row.get('link_imdb')),
                "poster_url": get_poster_url(row),
                "has_real_poster": bool(clean_value(row.get('poster_url'))),
                "loaded_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat()
            }
            
            movies.append(movie)
            
            # Progress ogni 10000 film
            if (idx + 1) % 10000 == 0:
                print(f"  📦 Processati {idx + 1}/{len(df)} film...")
                
        except Exception as e:
            print(f"⚠️ Errore riga {idx}: {e}")
            skipped += 1
            continue
    
    print(f"✅ Preparati {len(movies)} film ({skipped} saltati)")
    
    # Inserimento in batch
    if movies:
        print("🔄 Inserimento nel database...")
        
        # Svuota la collezione esistente
        catalog.delete_many({})
        
        # Inserisci in batch da 5000
        batch_size = 5000
        for i in range(0, len(movies), batch_size):
            batch = movies[i:i + batch_size]
            catalog.insert_many(batch)
            print(f"  📦 Inseriti {min(i + batch_size, len(movies))}/{len(movies)}")
    
    # Statistiche finali
    total = catalog.count_documents({})
    with_poster = catalog.count_documents({"has_real_poster": True})
    
    print(f"\n📊 Statistiche Catalogo:")
    print(f"  📁 Totale film: {total}")
    print(f"  🖼️  Con poster reale: {with_poster}")
    print(f"  🎭 Con poster stock: {total - with_poster}")
    
    # Esempio generi
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_genres = list(catalog.aggregate(pipeline))
    print(f"\n🎭 Top 10 Generi:")
    for g in top_genres:
        print(f"  - {g['_id']}: {g['count']} film")
    
    print("\n✅ Caricamento completato!")
    return True


def search_movie_by_title(title: str, year: Optional[int] = None) -> dict:
    """Cerca un film nel catalogo per titolo (e opzionalmente anno)."""
    client = MongoClient(MONGO_URL)
    db = client.cinematch_db
    catalog = db.movies_catalog
    
    query = {"title": {"$regex": title, "$options": "i"}}
    if year:
        query["year"] = year
    
    return catalog.find_one(query)


def get_movie_poster(imdb_id: str) -> str:
    """Ottiene l'URL del poster per un film dato l'ID IMDb."""
    client = MongoClient(MONGO_URL)
    db = client.cinematch_db
    catalog = db.movies_catalog
    
    movie = catalog.find_one({"imdb_id": imdb_id})
    if movie:
        return movie.get("poster_url", STOCK_POSTER_URL)
    return STOCK_POSTER_URL


if __name__ == "__main__":
    load_movies_catalog()
