"""
CineMatch Backend API
Sistema di raccomandazione film con analisi sentiment.
"""
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, status, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from apscheduler.schedulers.background import BackgroundScheduler
from movie_updater import MovieUpdater
import asyncio
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
import io
import os
import random
import re
import unicodedata
from datetime import datetime, timedelta
from pymongo import MongoClient
from pydantic import BaseModel
from typing import Optional, List, Dict
from auth import get_password_hash, verify_password, create_access_token, get_current_user_id

# ============================================
# APP CONFIGURATION
# ============================================
app = FastAPI(
    title="CineMatch API",
    description="Sistema di raccomandazione film personalizzato",
    version="1.0.0"
)

# CORS - permette chiamate dal frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In produzione specificare i domini
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# DATABASE CONNECTION
TMDB_API_KEY = "272643841dd72057567786d8fa7f8c5f"
# ============================================
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client.cinematch_db

# Collections
users_collection = db.users
movies_collection = db.movies
movies_catalog = db.movies_catalog
stats_collection = db.user_stats
sentiment_collection = db.sentiment_history
activity_collection = db.activity_log

# URL immagine stock di fallback
STOCK_POSTER_URL = "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster"

# ============================================
# MODELS
# ============================================
class UserAuth(BaseModel):
    username: str
    password: str

class UserRegister(BaseModel):
    username: str
    password: str
    email: str
    full_name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    province: Optional[str] = None  # e.g., "napoli"
    region: Optional[str] = None    # e.g., "Campania"

# ============================================
# STARTUP EVENT
# ============================================
@app.on_event("startup")
async def startup_event():
    """Inizializza il database al primo avvio."""
    print("🚀 Avvio CineMatch Backend...")
    
    # Crea indici se non esistono
    try:
        users_collection.create_index("username", unique=True)
        users_collection.create_index("user_id", unique=True)
        movies_collection.create_index("user_id")
        stats_collection.create_index("user_id", unique=True)
        
        # Indici per il catalogo (titoli normalizzati per ricerca veloce)
        movies_catalog.create_index("normalized_title")
        movies_catalog.create_index("normalized_original_title")
        
        # Indici per showtimes
        db.showtimes.create_index("province_slug")
        db.showtimes.create_index("updated_at")
        
        print("✅ Indici MongoDB creati")
    except Exception as e:
        print(f"⚠️ Errore creazione indici: {e}")
    
    # Controlla se c'è l'utente di default
    default_user = users_collection.find_one({"username": "pasquale.langellotti"})
    if not default_user:
        # Crea utente di default
        users_collection.insert_one({
            "username": "pasquale.langellotti",
            "email": "langellotti19@live.it",
            "password": get_password_hash("Pasquale19!"),
            "user_id": "pasquale.langellotti",
            "full_name": "Pasquale Langellotti",
            "created_at": datetime.utcnow().isoformat(),
            "is_active": True,
            "has_data": False
        })
        print("✅ Utente di default creato: pasquale.langellotti")
    else:
        print("✅ Utente pasquale.langellotti già esistente")

    # --- SCHEDULER UPDATE FILM ---
    # Avvia scheduler per aggiornamento automatico film (Cinema/Digital 2026+)
    updater = MovieUpdater(MONGO_URL, TMDB_API_KEY)
    
    scheduler = BackgroundScheduler()
    # Esegue ogni 24 ore
    scheduler.add_job(updater.fetch_new_releases, 'interval', hours=24)
    
    # --- SCHEDULER CINEMA CAMPANIA ---
    from scrape_comingsoon import main as run_cinema_scraper
    from cinema_film_sync import sync_films_to_catalog
    
    # Scraper ComingSoon alle 00:00 (mezzanotte)
    scheduler.add_job(run_cinema_scraper, 'cron', hour=0, minute=0, id='cinema_scraper')
    # Sync film al catalogo alle 00:30
    scheduler.add_job(sync_films_to_catalog, 'cron', hour=0, minute=30, id='cinema_sync')
    
    scheduler.start()
    print("🕒 Scheduler avviato: Movie Updater ogni 24h + Cinema Scraper a mezzanotte.")
    
    # Eseguiamo anche un update SUBITO all'avvio in background
    # per riempire il buco del 2026 adesso
    import threading
    t = threading.Thread(target=updater.fetch_new_releases)
    t.start()
    
    # Carica dati CSV se esistono e non sono già stati processati
    csv_path = "/data/ratings.csv"
    user_id = "pasquale.langellotti"
    
    # Verifica se l'utente ha già dati
    user_data = users_collection.find_one({"user_id": user_id})
    has_data = user_data.get("has_data", False) if user_data else False
    existing_stats = stats_collection.find_one({"user_id": user_id})
    
    if os.path.exists(csv_path) and (not existing_stats or not has_data):
        print(f"📂 Caricamento dati da {csv_path}...")
        try:
            df = pd.read_csv(csv_path)
            df = df.dropna(subset=['Rating'])
            df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
            df = df.dropna(subset=['Rating'])
            
            # Prepara lista film
            movies = []
            for _, row in df.iterrows():
                movie = {
                    "user_id": user_id,
                    "name": row['Name'],
                    "year": int(row['Year']) if pd.notna(row.get('Year')) else None,
                    "rating": int(row['Rating']),
                    "date": str(row.get('Date', '')) if pd.notna(row.get('Date')) else None,
                    "letterboxd_uri": row.get('Letterboxd URI', None),
                    "added_at": datetime.utcnow().isoformat()
                }
                movies.append(movie)
            
            # Salva film
            if movies:
                movies_collection.delete_many({"user_id": user_id})
                movies_collection.insert_many(movies)
            
            # Calcola e salva statistiche
            stats = calculate_stats(df, movies)
            stats["user_id"] = user_id
            stats["source_file"] = "ratings.csv"
            stats_collection.update_one(
                {"user_id": user_id},
                {"$set": stats},
                upsert=True
            )
            
            # Aggiorna utente
            users_collection.update_one(
                {"user_id": user_id},
                {"$set": {"has_data": True, "movies_count": len(movies)}}
            )
            
            print(f"✅ Caricati {len(movies)} film per {user_id}")
        except Exception as e:
            print(f"❌ Errore caricamento CSV: {e}")
    elif existing_stats:
        print(f"✅ Dati già presenti per {user_id}: {existing_stats.get('total_watched', 0)} film")
    else:
        print(f"⚠️ File CSV non trovato: {csv_path}")

# ============================================
# HELPER FUNCTIONS
# ============================================
def calculate_stats(df: pd.DataFrame, movies: list) -> dict:
    """Calcola le statistiche dai dati."""
    rating_distribution = df['Rating'].value_counts().to_dict()
    # MongoDB richiede chiavi stringa
    rating_distribution = {str(int(k)): int(v) for k, v in rating_distribution.items()}
    
    # Distribuzione rating per grafico a barre
    rating_chart_data = [
        {"rating": "⭐1", "count": int(df[df['Rating'] == 1].shape[0]), "stars": 1},
        {"rating": "⭐2", "count": int(df[df['Rating'] == 2].shape[0]), "stars": 2},
        {"rating": "⭐3", "count": int(df[df['Rating'] == 3].shape[0]), "stars": 3},
        {"rating": "⭐4", "count": int(df[df['Rating'] == 4].shape[0]), "stars": 4},
        {"rating": "⭐5", "count": int(df[df['Rating'] == 5].shape[0]), "stars": 5},
    ]
    
    # Campi estesi per top_rated e recent (per mostrare descrizioni/poster nella dashboard)
    extended_cols = ['Name', 'Year', 'Rating', 'poster_url', 'description', 'director', 'actors', 'duration', 'imdb_id', 'genres']
    existing_cols = [c for c in extended_cols if c in df.columns]
    
    top_rated = df[df['Rating'] >= 4].nlargest(10, 'Rating')[existing_cols].to_dict('records')
    
    months = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    if 'Date' in df.columns:
        df_copy = df.copy()
        df_copy['DateParsed'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        df_copy['Month'] = df_copy['DateParsed'].dt.month
        monthly_counts = df_copy.groupby('Month').size().to_dict()
        monthly_data = [{"month": months[i], "films": int(monthly_counts.get(i+1, 0))} for i in range(12)]
        
        # Campi per recent
        recent_cols = ['Name', 'Year', 'Rating', 'Date'] + [c for c in ['poster_url', 'description', 'director', 'actors', 'duration', 'imdb_id', 'genres'] if c in df_copy.columns]
        recent = df_copy.nlargest(10, 'DateParsed')[recent_cols].to_dict('records')
    else:
        monthly_data = [{"month": m, "films": 0} for m in months]
        recent = df.head(10)[existing_cols].to_dict('records')
    
    # Calcola tutte le statistiche avanzate dal catalogo
    advanced_stats = calculate_advanced_stats(movies)
    
    return {
        "total_watched": len(movies),
        "avg_rating": round(float(df['Rating'].mean()), 2),
        "rating_distribution": rating_distribution,
        "rating_chart_data": rating_chart_data,
        "top_rated_movies": top_rated,
        "recent_movies": recent,
        "monthly_data": monthly_data,
        "genre_data": advanced_stats["genre_data"],
        "favorite_genre": advanced_stats["favorite_genre"],
        "total_5_stars": int(df[df['Rating'] == 5].shape[0]),
        "total_4_stars": int(df[df['Rating'] == 4].shape[0]),
        "total_3_stars": int(df[df['Rating'] == 3].shape[0]),
        "total_2_stars": int(df[df['Rating'] == 2].shape[0]),
        "total_1_stars": int(df[df['Rating'] == 1].shape[0]),
        # Nuove statistiche
        "watch_time_hours": advanced_stats["total_watch_time_hours"],
        "watch_time_minutes": advanced_stats["total_watch_time_minutes"],
        "avg_duration": advanced_stats["avg_duration"],
        "top_directors": advanced_stats["top_directors"],
        "top_actors": advanced_stats["top_actors"],
        "rating_vs_imdb": advanced_stats["rating_vs_imdb"],
        "total_unique_directors": advanced_stats.get("total_unique_directors", 0),
        "total_unique_actors": advanced_stats.get("total_unique_actors", 0),
        "updated_at": datetime.utcnow().isoformat(),
        "top_years": calculate_top_years(movies)
    }


def calculate_advanced_stats(movies: list) -> dict:
    """
    Calcola statistiche avanzate dai film dell'utente con JOIN al catalogo.
    Include: generi, durata, registi, attori, confronto IMDb.
    """
    from collections import Counter, defaultdict
    
    # Colori per i generi (Supporto sia Inglese che Italiano per TMDB)
    genre_colors = {
        # English keys
        "Drama": "#E50914", "Comedy": "#FF6B35", "Action": "#00529B",
        "Thriller": "#8B5CF6", "Horror": "#6B21A8", "Romance": "#EC4899",
        "Sci-Fi": "#06B6D4", "Adventure": "#10B981", "Crime": "#F59E0B",
        "Mystery": "#7C3AED", "Fantasy": "#8B5CF6", "Animation": "#F472B6",
        "Documentary": "#22C55E", "Family": "#FBBF24", "War": "#78716C",
        "History": "#A78BFA", "Music": "#FB7185", "Western": "#D97706",
        "Sport": "#34D399", "Biography": "#60A5FA", "Musical": "#DB2777",
        "TV Movie": "#94A3B8",
        # Italian keys (per chi scarica dati da TMDB in italiano)
        "Dramma": "#E50914", "Commedia": "#FF6B35", "Azione": "#00529B",
        "Fantascienza": "#06B6D4", "Avventura": "#10B981", "Crimine": "#F59E0B",
        "Mistero": "#7C3AED", "Fantastico": "#8B5CF6", "Fantasy": "#8B5CF6",
        "Animazione": "#F472B6", "Documentario": "#22C55E", "Famiglia": "#FBBF24",
        "Guerra": "#78716C", "Storia": "#A78BFA", "Musica": "#FB7185",
        "Biografico": "#60A5FA", "Biografia": "#60A5FA", "Film TV": "#94A3B8"
    }
    default_color = "#9CA3AF"
    
    # Raccoglie tutti i titoli per batch lookup
    titles = [m.get('name', '').lower() for m in movies if m.get('name')]
    
    if not titles:
        return {
            "genre_data": [], "favorite_genre": "Nessuno",
            "total_watch_time_hours": 0, "total_watch_time_minutes": 0,
            "avg_duration": 0, "top_directors": [], "top_actors": [],
            "rating_vs_imdb": []
        }
    
    # Batch lookup nel catalogo
    import re
    escaped_titles = [re.escape(t) for t in titles[:500]]
    regex_pattern = f"^({'|'.join(escaped_titles)})$"
    
    catalog_movies = list(movies_catalog.find(
        {
            "$or": [
                {"title": {"$regex": regex_pattern, "$options": "i"}},
                {"original_title": {"$regex": regex_pattern, "$options": "i"}}
            ]
        },
        {"title": 1, "original_title": 1, "genres": 1, "duration": 1, "director": 1, "actors": 1, "avg_vote": 1}
    ))
    
    # Crea mapping titolo -> dati catalogo
    title_data = {}
    for cm in catalog_movies:
        # Map primary title
        if cm.get('title'):
            title_data[cm['title'].lower()] = cm
        # Map original title too
        if cm.get('original_title'):
             title_data[cm['original_title'].lower()] = cm
    
    # Inizializza contatori
    genre_counter = Counter()
    director_counter = Counter()
    director_ratings = defaultdict(list)
    actor_counter = Counter()
    actor_ratings = defaultdict(list)
    total_duration = 0
    duration_count = 0
    rating_vs_imdb = []
    
    # Processa ogni film dell'utente
    for movie in movies:
        title = movie.get('name', '').lower()
        user_rating = movie.get('rating', 0)
        catalog_info = title_data.get(title, {})
        
        # Generi
        for genre in catalog_info.get('genres', []):
            if genre:
                genre_counter[genre] += 1
        
        # Durata
        duration = catalog_info.get('duration')
        if duration and isinstance(duration, (int, float)) and duration > 0:
            total_duration += duration
            duration_count += 1
        
        # Registi
        director = catalog_info.get('director')
        if director:
            # Alcuni film hanno più registi separati da virgola
            for d in director.split(','):
                d = d.strip()
                if d:
                    director_counter[d] += 1
                    director_ratings[d].append(user_rating)
        
        # Attori (prendi i primi 3 per film)
        actors = catalog_info.get('actors', '')
        if actors:
            for i, actor in enumerate(actors.split(',')):
                if i >= 3:  # Solo i primi 3 attori per film
                    break
                actor = actor.strip()
                if actor:
                    actor_counter[actor] += 1
                    actor_ratings[actor].append(user_rating)
        
        # Rating vs IMDb
        imdb_rating = catalog_info.get('avg_vote')
        if imdb_rating and user_rating:
            # Converti rating utente (1-5) in scala 1-10 per confronto
            user_rating_10 = user_rating * 2
            rating_vs_imdb.append({
                "title": movie.get('name', ''),
                "user_rating": user_rating,
                "user_rating_10": user_rating_10,
                "imdb_rating": round(imdb_rating, 1),
                "difference": round(user_rating_10 - imdb_rating, 1)
            })
    
    # Calcola statistiche generi
    total_genres = sum(genre_counter.values()) or 1
    top_genres = genre_counter.most_common(8)
    genre_data = []
    for genre, count in top_genres:
        percentage = round((count / total_genres) * 100, 1)
        color = genre_colors.get(genre, default_color)
        genre_data.append({"name": genre, "value": percentage, "color": color, "count": count})
    
    favorite_genre = top_genres[0][0] if top_genres else "Nessuno"
    
    # Calcola statistiche registi
    total_unique_directors = len(director_counter)
    top_directors = []
    for director, count in director_counter.most_common(10):  # Aumentiamo a 10 per sicurezza
        ratings = director_ratings[director]
        avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else 0
        top_directors.append({
            "name": director,
            "count": count,
            "avg_rating": avg_rating
        })
    
    # Calcola statistiche attori
    total_unique_actors = len(actor_counter)
    top_actors = []
    for actor, count in actor_counter.most_common(15):
        ratings = actor_ratings[actor]
        avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else 0
        top_actors.append({
            "name": actor,
            "count": count,
            "avg_rating": avg_rating
        })
    
    # Classifica per miglior rating (minimo 2 film se possibile, altrimenti 1)
    best_rated_directors = []
    # Prendiamo tutti i registi con almeno 1 film e calcoliamo avg
    all_directors_stats = []
    for d, count in director_counter.items():
        ratings = director_ratings[d]
        avg = sum(ratings) / len(ratings)
        all_directors_stats.append({"name": d, "count": count, "avg_rating": round(avg, 2)})
    
    # Ordina per rating desc, poi count desc
    best_rated_directors = sorted(
        [d for d in all_directors_stats if d['count'] >= 1], 
        key=lambda x: (x['avg_rating'], x['count']), 
        reverse=True
    )[:80] # Aumentato ulteriormente per permettere filtraggio esteso in frontend

    best_rated_actors = []
    all_actors_stats = []
    for a, count in actor_counter.items():
        ratings = actor_ratings[a]
        avg = sum(ratings) / len(ratings)
        all_actors_stats.append({"name": a, "count": count, "avg_rating": round(avg, 2)})
    
    best_rated_actors = sorted(
        [a for a in all_actors_stats if a['count'] >= 1], 
        key=lambda x: (x['avg_rating'], x['count']), 
        reverse=True
    )[:80] # Aumentato ulteriormente per permettere filtraggio esteso in frontend

    # Calcola durate
    avg_duration = round(total_duration / duration_count) if duration_count > 0 else 0
    total_hours = total_duration // 60
    total_minutes = total_duration % 60
    
    # Ordina rating_vs_imdb per differenza (più controversi)
    rating_vs_imdb.sort(key=lambda x: abs(x['difference']), reverse=True)
    
    return {
        "genre_data": genre_data,
        "favorite_genre": favorite_genre,
        "total_watch_time_hours": total_hours,
        "total_watch_time_minutes": total_minutes,
        "avg_duration": avg_duration,
        "top_directors": top_directors,
        "top_actors": top_actors,
        "best_rated_directors": best_rated_directors,
        "best_rated_actors": best_rated_actors,
        "rating_vs_imdb": rating_vs_imdb[:20],  # Top 20 più controversi
        "total_unique_directors": total_unique_directors,
        "total_unique_actors": total_unique_actors
    }


def calculate_top_years(movies: list) -> list:
    """Calcola i 5 anni con più film visti."""
    from collections import Counter
    years = [m['year'] for m in movies if m.get('year')]
    year_counts = Counter(years)
    top_5 = year_counts.most_common(5)
    return [{"year": year, "count": count} for year, count in top_5]

# ============================================
# AUTH ENDPOINTS
# ============================================
@app.get("/")
def read_root():
    return {
        "message": "CineMatch API is running",
        "version": "1.0.0",
        "status": "healthy"
    }

@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        client.admin.command('ping')
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/register")
async def register(user: UserRegister):
    """Registra un nuovo utente."""
    # Verifica username esistente
    if users_collection.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username già esistente")
    
    # Verifica email esistente (se fornita)
    if user.email and users_collection.find_one({"email": user.email}):
        raise HTTPException(status_code=400, detail="Email già registrata")
    
    # Crea utente
    new_user = {
        "username": user.username,
        "password": get_password_hash(user.password),
        "user_id": user.username,
        "email": user.email,
        "full_name": user.full_name,
        "address": user.address,
        "city": user.city,
        "province": user.province.lower() if user.province else None,
        "region": user.region,
        "created_at": datetime.utcnow().isoformat(),
        "is_active": True,
        "has_data": False,
        "movies_count": 0
    }
    
    users_collection.insert_one(new_user)
    
    return {"message": "Utente registrato con successo", "username": user.username}

@app.post("/login")
async def login(user: UserAuth):
    """Effettua il login."""
    db_user = users_collection.find_one({
        "$or": [
            {"username": user.username},
            {"email": user.username}
        ]
    })
    
    if not db_user:
        raise HTTPException(status_code=401, detail="Utente non trovato")
    
    if not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Password non corretta")
    
    # Aggiorna last login
    users_collection.update_one(
        {"user_id": db_user["user_id"]},
        {"$set": {"last_login": datetime.utcnow().isoformat()}}
    )
    
    access_token = create_access_token(data={"sub": db_user["user_id"]})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "username": db_user["username"],
        "has_data": db_user.get("has_data", False)
    }

@app.get("/me")
async def get_current_user(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene i dati dell'utente corrente (con conteggio film reale)."""
    user = users_collection.find_one({"user_id": current_user_id}, {"password": 0, "_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    
    # Calcola il conteggio reale dei film per assicurare sincronizzazione con la dashboard
    real_count = movies_collection.count_documents({"user_id": current_user_id})
    user["movies_count"] = real_count
    
    return user

# ============================================
# CINEMA ENDPOINTS
# ============================================
# Collection per showtimes cinema
# usiamo sempre db (cinematch_db) coerentemente
showtimes_collection = db["showtimes"]

def normalize_title(text: str) -> str:
    """Rimuove accenti e caratteri speciali per matching/ricerca."""
    if not text: return ""
    # Normalizza in NFD (decomposizione) e rimuove i caratteri non-spacing mark (accenti)
    normalized = unicodedata.normalize('NFD', text)
    result = "".join([c for c in normalized if not unicodedata.combining(c)])
    
    # Mappa caratteri speciali comuni che non vengono decomposti
    special_chars = {
        'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u',
        'Ā': 'A', 'Ē': 'E', 'Ī': 'I', 'Ō': 'O', 'Ū': 'U',
        'ł': 'l', 'Ł': 'L', 'ø': 'o', 'Ø': 'O', 'æ': 'ae', 'Æ': 'AE',
        'œ': 'oe', 'Œ': 'OE', 'ß': 'ss', 'đ': 'd', 'Đ': 'D',
        'ñ': 'n', 'Ñ': 'N', 'ç': 'c', 'Ç': 'C'
    }
    for char, replacement in special_chars.items():
        result = result.replace(char, replacement)
    
    # Rimuove tutto ciò che non è alfanumerico o spazio, e normalizza gli spazi
    result = re.sub(r'[^a-zA-Z0-9\s]', ' ', result)
    result = " ".join(result.split()).lower()
    return result

@app.get("/cinema/films")
async def get_cinema_films(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene i film in programmazione nella provincia dell'utente con matching robusto."""
    
    def find_in_catalog(title: str, original_title: str = None) -> dict:
        """Cerca un film nel catalogo con logica robusta usando i campi indicizzati."""
        # Campi da restituire per velocizzare (MOLTO importante)
        projection = {
            "poster_url": 1, "description": 1, "avg_vote": 1, 
            "genres": 1, "year": 1, "duration": 1, "actors": 1, 
            "director": 1, "_id": 0
        }
        
        # 1. Ricerca esatta per titolo/titolo originale
        or_conditions = []
        if title:
            escaped_title = re.escape(title)
            or_conditions.extend([
                {"title": {"$regex": f"^{escaped_title}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{escaped_title}$", "$options": "i"}},
                {"normalized_title": normalize_title(title)}
            ])
        
        if original_title:
            escaped_original = re.escape(original_title)
            or_conditions.extend([
                {"title": {"$regex": f"^{escaped_original}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{escaped_original}$", "$options": "i"}},
                {"normalized_title": normalize_title(original_title)}
            ])

        if or_conditions:
            result = movies_catalog.find_one({"$or": or_conditions}, projection)
            if result:
                return result
        
        # 2. Ricerca per titoli normalizzati (già indicizzati)
        norm_title = normalize_title(title) if title else ""
        norm_original = normalize_title(original_title) if original_title else ""
        
        for norm in [norm_title, norm_original]:
            if not norm or len(norm) < 3:
                continue
            
            # Match esatto su campo normalizzato
            result = movies_catalog.find_one({
                "$or": [
                    {"normalized_title": norm},
                    {"normalized_original_title": norm}
                ]
            }, projection)
            if result:
                return result
            
            # Match parziale su titoli lunghi
            if len(norm) > 8:
                result = movies_catalog.find_one({
                    "$or": [
                        {"normalized_title": {"$regex": f".*{re.escape(norm)}.*"}},
                        {"normalized_original_title": {"$regex": f".*{re.escape(norm)}.*"}}
                    ]
                }, projection)
                if result:
                    return result

        return None

    # Ottieni provincia utente (default: napoli / Pompei)
    user = users_collection.find_one({"user_id": current_user_id})
    user_province = user.get("province", "napoli") if user else "napoli"
    if not user_province:
        user_province = "napoli"
    user_province = user_province.lower()
    
    # Ottieni film visti dall'utente per filtraggio
    user_watched = list(movies_collection.find({"user_id": current_user_id}, {"name": 1}))
    watched_titles = {normalize_title(m['name']) for m in user_watched}
    
    # Query showtimes per la provincia (prendiamo fino a 50 per avere margine dopo il filtraggio)
    showtimes_cursor = showtimes_collection.find(
        {"province_slug": user_province},
        {"_id": 0}
    ).sort("updated_at", -1).limit(50)
    
    showtimes_list = list(showtimes_cursor)
    
    # 1. Filtra per film non visti e raccogli titoli per ricerca batch
    filtered_showtimes = []
    titles_to_query = set()
    
    for st in showtimes_list:
        ft = st.get("film_title", "")
        fot = st.get("film_original_title", "")
        nt = normalize_title(ft)
        notit = normalize_title(fot) if fot else None
        
        if nt in watched_titles or (notit and notit in watched_titles):
            continue
            
        filtered_showtimes.append(st)
        if nt: titles_to_query.add(nt)
        if notit: titles_to_query.add(notit)
        
        if len(filtered_showtimes) >= 20: # max_films
            break

    # 2. Ricerca batch nel catalogo (MOLTO più veloce di 20 query singole)
    projection = {
        "poster_url": 1, "description": 1, "avg_vote": 1, 
        "genres": 1, "year": 1, "duration": 1, "actors": 1, 
        "director": 1, "normalized_title": 1, "normalized_original_title": 1, "_id": 0
    }
    
    catalog_results = list(movies_catalog.find({
        "$or": [
            {"normalized_title": {"$in": list(titles_to_query)}},
            {"normalized_original_title": {"$in": list(titles_to_query)}}
        ]
    }, projection))
    
    # Crea mappa di lookup
    catalog_map = {}
    for item in catalog_results:
        nt = item.get("normalized_title")
        notit = item.get("normalized_original_title")
        if nt: catalog_map[nt] = item
        if notit: catalog_map[notit] = item
    
    films = []
    for showtime in filtered_showtimes:
        film_title = showtime.get("film_title", "")
        film_original_title = showtime.get("film_original_title", "")
        
        # Lookup in mappa
        nt = normalize_title(film_title)
        notit = normalize_title(film_original_title) if film_original_title else None
        catalog_info = catalog_map.get(nt) or (catalog_map.get(notit) if notit else None)
        
        # Fallback alla funzione robusta se non trovato per match esatto normalizzato
        if not catalog_info:
            catalog_info = find_in_catalog(film_title, film_original_title)

        director = showtime.get("director", "")
        # Usa il regista dal catalogo se disponibile
        if catalog_info and catalog_info.get("director"):
            director = catalog_info.get("director")

        # Limita a 5 cinema per film
        cinemas = showtime.get("cinemas", [])[:5]
        
        # Formatta orari per ogni cinema
        formatted_cinemas = []
        for cinema in cinemas:
            formatted_cinemas.append({
                "name": cinema.get("cinema_name", ""),
                "address": cinema.get("address", ""),
                "showtimes": [
                    {"time": s.get("time", ""), "price": s.get("price", ""), "sala": s.get("sala", "")}
                    for s in cinema.get("showtimes", [])[:6]  # Max 6 orari
                ]
            })
        
        # Costruisci oggetto film
        film = {
            "id": showtime.get("film_id", ""),
            "title": film_title,
            "original_title": film_original_title,
            "director": director,
            "poster": catalog_info.get("poster_url") if (catalog_info and catalog_info.get("poster_url")) else "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster",
            "description": catalog_info.get("description") if catalog_info else "Trama non disponibile per questo film in programmazione.",
            "rating": catalog_info.get("avg_vote") if catalog_info else None,
            "genres": catalog_info.get("genres", []) if catalog_info else ["In Sala"],
            "year": catalog_info.get("year") if catalog_info else datetime.now().year,
            "duration": catalog_info.get("duration") if catalog_info else None,
            "actors": catalog_info.get("actors") if catalog_info else None,
            "cinemas": formatted_cinemas,
            "province": showtime.get("province", "")
        }
        
        # Fallback poster se non trovato
        if not film["poster"]:
            film["poster"] = "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster"
        
        films.append(film)
    
    return {
        "province": user_province.capitalize(),
        "films": films,
        "total": len(films)
    }


# ============================================
# DATA ENDPOINTS
# ============================================
def process_missing_movies_background(titles_years: list, user_id: str):
    """
    Task in background per cercare i film mancanti su TMDB 
    e poi aggiornare le statistiche dell'utente.
    """
    print(f"🔄 [Background] Verifica catalogo per {len(titles_years)} titoli...")
    
    # 1. Cerca e aggiungi film mancanti
    added_count = 0
    # Import re qui se non è globale
    import re
    
    for title, year in titles_years:
        # Cerca nel catalogo locale (veloce) includendo l'anno per evitare omonimi errati
        query = {
            "$or": [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
            ]
        }
        
        if year:
            # Tolleranza ±1 anno anche qui per consistenza
            query["year"] = {"$in": [year, year-1, year+1]}
            
        exists = movies_catalog.find_one(query)
        
        # Se non esiste O se esiste ma ha solo la copertina stock (placeholder), cerchiamo su TMDB
        needs_tmdb = False
        if not exists:
            needs_tmdb = True
        elif not exists.get("poster_url") or STOCK_POSTER_URL in exists.get("poster_url", ""):
            needs_tmdb = True
            
        if needs_tmdb:
            # Se non esiste, cerca su TMDB e aggiungi/aggiorna
            result = fetch_metadata_from_tmdb(title, year)
            if result:
                added_count += 1
                # Se esisteva già nel catalogo locale ma senza poster, facciamo un merge (già gestito da upsert in fetch_metadata_from_tmdb)
                pass
                
    print(f"✅ [Background] Aggiunti {added_count} nuovi film al catalogo.")
    
    # 2. Ricalcola le statistiche se sono stati aggiunti film
    # O anche se non ne sono stati aggiunti, per sicurezza (se il primo calcolo era parziale)
    if added_count > 0:
        print("🔄 [Background] Ricalcolo statistiche utente...")
        movies = list(movies_collection.find({"user_id": user_id}))
        if movies:
            # Ricostruisci il dataframe (approssimativo dai dati salvati)
            # Nota: qui perdiamo alcune info originali del CSV se non salvate in movie, 
            # ma movie object ha 'rating', 'year', 'name' che bastano per le stats di base.
            # Per stats temporali servirebbe la 'date' originale se salvata.
            
            # Creiamo un DF minimale
            data_for_df = []
            for m in movies:
                data_for_df.append({
                    "Name": m["name"],
                    "Year": m["year"],
                    "Rating": m["rating"],
                    "Date": m.get("date")
                })
            
            df = pd.DataFrame(data_for_df)
            
            # Ricalcola
            stats = calculate_stats(df, movies)
            stats["user_id"] = user_id
            stats["last_background_update"] = datetime.utcnow().isoformat()
            
            stats_collection.update_one(
                {"user_id": user_id},
                {"$set": stats},
                upsert=True
            )
            print("✅ [Background] Statistiche aggiornate.")


@app.post("/upload-csv")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...), 
    current_user_id: str = Depends(get_current_user_id)
):
    """Carica e processa un file CSV di Letterboxd."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Il file deve essere un CSV")
    
    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))
    
    # Verifica colonne necessarie
    if 'Name' not in df.columns or 'Rating' not in df.columns:
        raise HTTPException(status_code=400, detail="Il CSV deve contenere le colonne 'Name' e 'Rating'")
    
    # Pulizia dati
    df = df.dropna(subset=['Rating'])
    df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
    df = df.dropna(subset=['Rating'])
    
    if len(df) == 0:
        raise HTTPException(status_code=400, detail="Nessun film valido trovato nel CSV")
    
    # Prepara lista film
    movies = []
    for _, row in df.iterrows():
        movie = {
            "user_id": current_user_id,
            "name": row['Name'],
            "year": int(row['Year']) if pd.notna(row.get('Year')) else None,
            "rating": int(row['Rating']),
            "date": str(row.get('Date', '')) if pd.notna(row.get('Date')) else None,
            "letterboxd_uri": row.get('Letterboxd URI', None),
            "added_at": datetime.utcnow().isoformat()
        }
        movies.append(movie)
    
    # Salva film
    movies_collection.delete_many({"user_id": current_user_id})
    movies_collection.insert_many(movies)
    
    # --- ARRICCHIMENTO CATALOGO DA CSV (SE PRESENTE) ---
    # Se il CSV contiene già metadati (come poster_url, director, ecc.), li salviamo nel catalogo
    catalog_updates = 0
    possible_cols = [
        'imdb_title_id', 'original_title', 'genre', 'duration', 'country', 
        'language', 'director', 'writer', 'production_company', 'actors', 
        'description', 'avg_vote', 'votes', 'budget', 'usa_gross_income', 
        'worlwide_gross_income', 'metascore', 'reviews_from_users', 
        'reviews_from_critics', 'link_imdb', 'poster_url'
    ]
    
    # Se ci sono colonne di metadati, aggiorna il catalogo
    if any(col in df.columns for col in possible_cols):
        for _, row in df.iterrows():
            if pd.isna(row.get('imdb_title_id')) and pd.isna(row.get('poster_url')):
                continue
                
            entry = {
                "title": row['Name'],
                "year": int(row['Year']) if pd.notna(row.get('Year')) else None,
                "normalized_title": normalize_title(row['Name']),
                "loaded_at": datetime.utcnow().isoformat(),
                "source": "csv_upload_enriched"
            }
            
            orig_title = row.get('original_title')
            if pd.notna(orig_title):
                entry["normalized_original_title"] = normalize_title(str(orig_title))
            
            # Mappa tutte le colonne presenti
            for col in possible_cols:
                if col in df.columns and pd.notna(row[col]):
                    val = row[col]
                    # Gestione speciale generi
                    if col == 'genre':
                        entry['genre'] = val
                        entry['genres'] = [g.strip() for g in str(val).split(',')]
                    elif col == 'imdb_title_id':
                        entry['imdb_id'] = val
                        entry['imdb_title_id'] = val
                    else:
                        entry[col] = val
            
            # Salva o aggiorna nel catalogo
            if entry.get('imdb_id'):
                movies_catalog.update_one(
                    {"imdb_id": entry['imdb_id']},
                    {"$set": entry},
                    upsert=True
                )
                catalog_updates += 1
            else:
                # Se non ha IMDB ID, usa titolo e anno come chiave
                movies_catalog.update_one(
                    {"title": entry['title'], "year": entry['year']},
                    {"$set": entry},
                    upsert=True
                )
                catalog_updates += 1
    
    if catalog_updates > 0:
        print(f"📊 Aggiornati {catalog_updates} film nel catalogo dai metadati del CSV.")

    # Calcola e salva statistiche iniziali (con quello che c'è)
    stats = calculate_stats(df, movies)
    stats["user_id"] = current_user_id
    stats["source_file"] = file.filename
    stats_collection.update_one(
        {"user_id": current_user_id},
        {"$set": stats},
        upsert=True
    )
    
    # Aggiorna utente
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {
            "has_data": True,
            "movies_count": len(movies),
            "data_updated_at": datetime.utcnow().isoformat()
        }}
    )
    
    # Avvia task in background per i film mancanti
    titles_years = list(set([(m["name"], m["year"]) for m in movies]))
    background_tasks.add_task(process_missing_movies_background, titles_years, current_user_id)
    
    return {
        "status": "success",
        "filename": file.filename,
        "count": len(movies),
        "stats": stats,
        "message": f"Caricati {len(movies)} film. Ricerca copertine avviata in background."
    }


@app.post("/recalculate-stats")
async def recalculate_stats(current_user_id: str = Depends(get_current_user_id)):
    """Ricalcola le statistiche dell'utente basandosi sul catalogo per i generi."""
    movies = list(movies_collection.find({"user_id": current_user_id}))
    
    if not movies:
        raise HTTPException(status_code=404, detail="Nessun film trovato")
    
    # Crea DataFrame per le stats
    df = pd.DataFrame(movies)
    df = df.rename(columns={"name": "Name", "year": "Year", "rating": "Rating", "date": "Date"})
    
    # Ricalcola le statistiche con i generi reali
    stats = calculate_stats(df, movies)
    stats["user_id"] = current_user_id
    
    # Aggiorna nel database
    stats_collection.update_one(
        {"user_id": current_user_id},
        {"$set": stats},
        upsert=True
    )
    
    return {"message": "Statistiche ricalcolate con successo!", "stats": stats}


@app.get("/user-stats")
async def get_user_stats(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene le statistiche dell'utente (sempre sincronizzate col DB)."""
    stats = stats_collection.find_one({"user_id": current_user_id}, {"_id": 0})
    
    if not stats:
        raise HTTPException(status_code=404, detail="Nessun dato trovato. Carica prima un file CSV.")
    
    # Calcola il conteggio REALE dei film ogni volta per evitare discrepanze con la navbar/catalogo
    real_count = movies_collection.count_documents({"user_id": current_user_id})
    stats["total_watched"] = real_count
    
    # Se mancano le nuove statistiche o sono incomplete (es. meno di 40 per i best_rated), ricalcola tutto
    needs_recalc = False
    if "best_rated_directors" not in stats or "best_rated_actors" not in stats:
        needs_recalc = True
    elif len(stats.get("best_rated_directors", [])) < 30: # Forza ricalcolo se abbiamo ancora le vecchie stats "corte"
        needs_recalc = True
    elif "genre_data" in stats and stats["genre_data"] and ("count" not in stats["genre_data"][0] or "color" not in stats["genre_data"][0]):
        needs_recalc = True
    
    if needs_recalc:
        movies = list(movies_collection.find({"user_id": current_user_id}))
        advanced_stats = calculate_advanced_stats(movies)
        
        # Aggiorna le stats con i nuovi dati
        stats.update({
            "genre_data": advanced_stats["genre_data"],
            "favorite_genre": advanced_stats["favorite_genre"],
            "watch_time_hours": advanced_stats["total_watch_time_hours"],
            "watch_time_minutes": advanced_stats["total_watch_time_minutes"],
            "avg_duration": advanced_stats["avg_duration"],
            "top_directors": advanced_stats["top_directors"],
            "top_actors": advanced_stats["top_actors"],
            "best_rated_directors": advanced_stats.get("best_rated_directors", []),
            "best_rated_actors": advanced_stats.get("best_rated_actors", []),
            "rating_vs_imdb": advanced_stats["rating_vs_imdb"],
            "total_unique_directors": advanced_stats.get("total_unique_directors", 0),
            "total_unique_actors": advanced_stats.get("total_unique_actors", 0)
        })
        
        # Salva nel database
        stats_collection.update_one(
            {"user_id": current_user_id},
            {"$set": stats}
        )
    
    if "top_years" not in stats:
        movies = list(movies_collection.find({"user_id": current_user_id}))
        stats["top_years"] = calculate_top_years(movies)
    
    return stats

@app.get("/movies")
async def get_movies(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene tutti i film dell'utente (per pagina Film Visti)."""
    movies = list(movies_collection.find(
        {"user_id": current_user_id},
        {"_id": 0, "user_id": 0}
    ))
    return movies

@app.get("/movies/person")
async def get_movies_by_person(name: str, type: str, current_user_id: str = Depends(get_current_user_id)):
    """Ottiene i film dell'utente filtrati per regista o attore."""
    user_movies = list(movies_collection.find({"user_id": current_user_id}))
    titles = [m.get('name') for m in user_movies]
    
    import re
    field = "director" if type == "director" else "actors"
    
    # Cerchiamo nel catalogo tutti i film di quella persona
    catalog_matches = list(movies_catalog.find({
        field: {"$regex": re.escape(name), "$options": "i"}
    }, {"title": 1, "original_title": 1, "genres": 1, "poster_url": 1, "poster_path": 1, "year": 1, "avg_vote": 1, "director": 1, "actors": 1, "description": 1, "duration": 1}))
    
    catalog_map = {}
    for cm in catalog_matches:
        if cm.get('title'): catalog_map[cm['title'].lower()] = cm
        if cm.get('original_title'): catalog_map[cm['original_title'].lower()] = cm
        
    results = []
    for m in user_movies:
        title = m.get('name', '').lower()
        if title in catalog_map:
            cat_info = catalog_map[title]
            
            # Priorità al poster_url (stessa logica del catalogo), poi poster_path TMDB
            poster = cat_info.get('poster_url')
            if not poster and cat_info.get('poster_path'):
                poster = f"https://image.tmdb.org/t/p/w500{cat_info['poster_path']}"
            
            results.append({
                "id": str(m.get('_id', random.randint(1, 100000))),
                "title": m.get('name'),
                "year": m.get('year') or cat_info.get('year'),
                "poster": poster or STOCK_POSTER_URL,
                "rating": m.get('rating', 0),
                "genres": cat_info.get('genres', []),
                "director": cat_info.get('director', ''),
                "actors": cat_info.get('actors', ''),
                "description": cat_info.get('description', ''),
                "duration": cat_info.get('duration'),
                "avg_vote": cat_info.get('avg_vote'),
                "imdb_id": cat_info.get('imdb_id')
            })
            
    return results

@app.get("/monthly-stats/{year}")
async def get_monthly_stats(year: int, current_user_id: str = Depends(get_current_user_id)):
    """Ottiene le statistiche mensili per un anno specifico (basato sulla data di visione)."""
    movies = list(movies_collection.find(
        {"user_id": current_user_id},
        {"_id": 0, "user_id": 0}
    ))
    
    months = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    monthly_counts = {i: 0 for i in range(1, 13)}
    films_in_year = 0
    
    for movie in movies:
        if movie.get('date'):
            try:
                # Parsing data nel formato YYYY-MM-DD
                date_str = str(movie['date'])
                movie_year = int(date_str.split('-')[0])
                movie_month = int(date_str.split('-')[1])
                
                if movie_year == year:
                    monthly_counts[movie_month] += 1
                    films_in_year += 1
            except (ValueError, IndexError):
                continue
    
    monthly_data = [{"month": months[i], "films": monthly_counts[i+1]} for i in range(12)]
    
    # Trova gli anni disponibili (in cui l'utente ha visto film)
    available_years = set()
    for movie in movies:
        if movie.get('date'):
            try:
                date_str = str(movie['date'])
                movie_year = int(date_str.split('-')[0])
                available_years.add(movie_year)
            except (ValueError, IndexError):
                continue
    
    return {
        "year": year,
        "monthly_data": monthly_data,
        "total_films": films_in_year,
        "available_years": sorted(list(available_years), reverse=True)
    }



def fetch_metadata_from_tmdb(title: str, year: Optional[int]) -> Optional[dict]:
    """
    Cerca metadati su TMDB se mancano nel catalogo locale.
    Esegue una chiamata 'details' per ottenere dati arricchiti (cast, crew, budget, etc).
    Salva il risultato nel catalogo per usi futuri.
    """
    if not title:
        return None
        
    print(f"🌍 Searching TMDB for: {title} ({year})")
    url = "https://api.themoviedb.org/3/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": "it-IT"
    }
    if year:
        params["year"] = year
        
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            
            # Se la ricerca con anno fallisce, riprova senza
            if not results and year:
                 del params["year"]
                 response = requests.get(url, params=params, timeout=5)
                 if response.status_code == 200:
                     results = response.json().get("results", [])
            
            # Se non trova in IT, prova EN
            if not results:
                params.pop("year", None)
                params["language"] = "en-US"
                response = requests.get(url, params=params, timeout=5)
                if response.status_code == 200:
                    results = response.json().get("results", [])

            if results:
                tmdb_movie = results[0]  # Prendi il primo risultato
                movie_id = tmdb_movie['id']
                
                # CHIAMATA DETAILS PER DATI COMPLETI
                details_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
                d_params = {
                     "api_key": TMDB_API_KEY, 
                     "language": "it-IT", 
                     "append_to_response": "credits"
                }
                d_response = requests.get(details_url, params=d_params, timeout=5)
                details = d_response.json() if d_response.status_code == 200 else {}

                # Fallback ai risultati di ricerca se details fallisce
                if not details: 
                    details = tmdb_movie

                credits = details.get("credits", {})
                crew = credits.get("crew", [])
                cast = credits.get("cast", [])
                
                # Estrazione dati arricchiti
                directors = [p['name'] for p in crew if p['job'] == 'Director']
                writers = [p['name'] for p in crew if p['department'] == 'Writing']
                actors_list = [p['name'] for p in cast[:15]]
                production_companies = [c['name'] for c in details.get("production_companies", [])]
                genres_list = [g['name'] for g in details.get("genres", [])]
                
                # Conversione valute (USD default)
                budget = details.get("budget")
                revenue = details.get("revenue")
                budget_str = f"$ {budget}" if budget else None
                revenue_str = f"$ {revenue}" if revenue else None
                
                poster_path = details.get("poster_path") or tmdb_movie.get("poster_path")
                poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else STOCK_POSTER_URL
                
                title_it = details.get("title", title)
                original_title = details.get("original_title")

                # Mappatura completa come movie_final.csv
                new_catalog_entry = {
                    "imdb_id": details.get("imdb_id") if details.get("imdb_id") else f"tmdb_{movie_id}",
                    "imdb_title_id": details.get("imdb_id") if details.get("imdb_id") else f"tmdb_{movie_id}",
                    "title": title_it,
                    "original_title": original_title,
                    "normalized_title": normalize_title(title_it),
                    "normalized_original_title": normalize_title(original_title) if original_title else None,
                    "english_title": details.get("original_title") if details.get("original_language") == "en" else None,
                    "year": int(details.get("release_date", "0")[:4]) if details.get("release_date") else year,
                    "date_published": details.get("release_date"),
                    "genres": genres_list, # Lista, viene gestita bene da MongoDB
                    "genre": ", ".join(genres_list), # Stringa per compatibilità CSV
                    "duration": details.get("runtime"),
                    "country": details.get("origin_country", [None])[0] if details.get("origin_country") else None,
                    "language": details.get("original_language"),
                    "movie_language": details.get("original_language"),
                    "director": ", ".join(directors),
                    "writer": ", ".join(writers),
                    "production_company": ", ".join(production_companies),
                    "actors": ", ".join(actors_list),
                    "description": details.get("overview"),
                    "avg_vote": details.get("vote_average"),
                    "votes": details.get("vote_count"),
                    "budget": budget_str,
                    "usa_gross_income": None, # Difficile da mappare da TMDB generico
                    "worlwide_gross_income": revenue_str,
                    "metascore": None,
                    "reviews_from_users": None,
                    "reviews_from_critics": None,
                    "link_imdb": f"https://www.imdb.com/title/{details.get('imdb_id')}/" if details.get("imdb_id") else None,
                    "poster_url": poster_url,
                    "has_real_poster": bool(poster_path),
                    "loaded_at": datetime.utcnow().isoformat(),
                    "source": "tmdb_enriched_fetch"
                }
                
                # Salva nel DB per cache futura
                # Usa update_one con upsert basato sul titolo per evitare duplicati
                movies_catalog.update_one(
                    {"imdb_id": new_catalog_entry["imdb_id"]},
                    {"$set": new_catalog_entry},
                    upsert=True
                )
                
                return new_catalog_entry
                
    except Exception as e:
        print(f"⚠️ TMDB Error: {e}")
        
    return None




@app.get("/user-movies")
async def get_user_movies(
    current_user_id: str = Depends(get_current_user_id),
    skip: int = 0,
    limit: int = 10000
):
    """
    Ottiene la lista dei film dell'utente con poster dal catalogo.
    Aumentato limite a 10000 per evitare discrepanze tra dashboard e catalogo.
    """
    # Step 1: Recupera i film dell'utente
    user_movies = list(movies_collection.find(
        {"user_id": current_user_id},
        {"_id": 0, "user_id": 0}
    ).sort("added_at", -1).skip(skip).limit(limit))
    
    # Step 2: Raccogli TUTTI i titoli per un batch lookup (per garantire dati aggiornati dal catalogo)
    titles_to_lookup = []
    for movie in user_movies:
        titles_to_lookup.append({
            "title": movie["name"].lower(),
            "year": movie.get("year")
        })
    
    # Step 3: Batch lookup nel catalogo (una sola query)
    if titles_to_lookup:
        # Crea indice per ricerca veloce
        catalog_cache = {}
        
        # Query batch per tutti i titoli - usa $in per ricerca esatta (più veloce)
        title_list = list(set(t["title"] for t in titles_to_lookup))
        
        # Fai chunking se la lista è troppo lunga per evitare regex giganti
        # MongoDB regex ha limiti, meglio fare più query se necessario
        # Ma per ora assumiamo che limit=500 sia gestibile
        import re
        escaped_titles = [re.escape(t) for t in title_list]
        regex_pattern = f"^({'|'.join(escaped_titles)})$"
        
        catalog_movies = movies_catalog.find(
            {
                "$or": [
                    {"title": {"$regex": regex_pattern, "$options": "i"}},
                    {"original_title": {"$regex": regex_pattern, "$options": "i"}},
                    {"english_title": {"$regex": regex_pattern, "$options": "i"}} # Supporto per titoli inglesi
                ]
            },
            {"title": 1, "original_title": 1, "english_title": 1, "year": 1, "poster_url": 1, "imdb_id": 1, "genres": 1, "description": 1, "director": 1, "actors": 1, "votes": 1, "avg_vote": 1, "duration": 1}
        )
        
        # Costruisci cache con chiave title_year
        for cm in catalog_movies:
            # Helper per aggiungere alla cache con logica di "miglior match"
            def add_to_cache(t, y, movie):
                if not t: return
                t_lower = t.lower()
                
                # Helper interno per decidere se sostituire un'entry esistente
                def should_replace(current, new):
                    if not current: return True
                    # Preferiamo entry con poster reale rispetto a stock
                    curr_has_poster = current.get('poster_url') and STOCK_POSTER_URL not in current.get('poster_url', '')
                    new_has_poster = new.get('poster_url') and STOCK_POSTER_URL not in new.get('poster_url', '')
                    if new_has_poster and not curr_has_poster: return True
                    if curr_has_poster and not new_has_poster: return False
                    # A parità di poster, preferiamo quello con più voti
                    return (new.get('votes', 0) or 0) > (current.get('votes', 0) or 0)

                # Key con anno
                key_year = f"{t_lower}_{y}"
                if should_replace(catalog_cache.get(key_year), movie):
                    catalog_cache[key_year] = movie
                
                # Key solo titolo (fallback)
                if should_replace(catalog_cache.get(t_lower), movie):
                    catalog_cache[t_lower] = movie

            # Mappa tutte le varianti di titolo
            add_to_cache(cm.get('title'), cm.get('year', ''), cm)
            add_to_cache(cm.get('original_title'), cm.get('year', ''), cm)
            add_to_cache(cm.get('english_title'), cm.get('year', ''), cm)
        
        # Step 4: Applica i dati del catalogo ai film utente
        for movie in user_movies:
            title_lower = movie["name"].lower()
            year = movie.get("year")
            
            # Cerca con tolleranza sull'anno (±1) per discrepanze Letterboxd/Catalog (molto comuni)
            catalog_movie = catalog_cache.get(f"{title_lower}_{year}")
            
            if not catalog_movie and year:
                catalog_movie = catalog_cache.get(f"{title_lower}_{year-1}") or catalog_cache.get(f"{title_lower}_{year+1}")
            
            if not catalog_movie:
                catalog_movie = catalog_cache.get(title_lower)
            
            if catalog_movie:
                # Sovrascriviamo con i dati del catalogo se il poster è migliore o se mancava
                cat_poster = catalog_movie.get("poster_url")
                if cat_poster and cat_poster != STOCK_POSTER_URL:
                    movie["poster_url"] = cat_poster
                elif not movie.get("poster_url"):
                    movie["poster_url"] = STOCK_POSTER_URL
                
                movie["imdb_id"] = catalog_movie.get("imdb_id")
                movie["genres"] = catalog_movie.get("genres", [])
                # Dettagli extra per popup - sync sempre dal catalogo (la fonte di verità)
                movie["description"] = catalog_movie.get("description")
                movie["director"] = catalog_movie.get("director")
                movie["actors"] = catalog_movie.get("actors")
                movie["duration"] = catalog_movie.get("duration")
                movie["avg_vote"] = catalog_movie.get("avg_vote")
    
    total = movies_collection.count_documents({"user_id": current_user_id})
    
    return {
        "movies": user_movies,
        "total": total,
        "skip": skip,
        "limit": limit
    }



# Modello per aggiungere film
class AddMovieRequest(BaseModel):
    name: str
    year: int
    rating: int
    comment: Optional[str] = None
    imdb_id: Optional[str] = None
    poster_url: Optional[str] = None


class RemoveMovieRequest(BaseModel):
    name: str
    year: int


class UpdateMovieRequest(BaseModel):
    name: str
    year: int
    rating: Optional[int] = None
    comment: Optional[str] = None


@app.post("/user-movies/add")
async def add_movie_to_collection(
    movie: AddMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Aggiunge un film alla collezione dell'utente."""
    # Verifica se il film esiste già
    existing = movies_collection.find_one({
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year
    })
    
    if existing:
        # Se esiste, aggiorna commento e rating
        movies_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "rating": movie.rating,
                "comment": movie.comment,
                "updated_at": datetime.utcnow().isoformat()
            }}
        )
        return {"status": "success", "message": "Film aggiornato"}
    
    # Crea documento film
    new_movie = {
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year,
        "rating": movie.rating,
        "comment": movie.comment,
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "imdb_id": movie.imdb_id,
        "poster_url": movie.poster_url,
        "added_at": datetime.utcnow().isoformat()
    }
    
    movies_collection.insert_one(new_movie)
    return {"status": "success", "message": "Film aggiunto"}


@app.post("/user-movies/update")
async def update_user_movie(
    req: UpdateMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Aggiorna voto o commento di un film nei 'visti'."""
    update_data = {}
    if req.rating is not None: update_data["rating"] = req.rating
    if req.comment is not None: update_data["comment"] = req.comment
    update_data["updated_at"] = datetime.utcnow().isoformat()

    res = movies_collection.update_one(
        {"user_id": current_user_id, "name": req.name, "year": req.year},
        {"$set": update_data}
    )
    
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Film non trovato nei tuoi visti")
        
    return {"status": "success"}


@app.post("/user-movies/remove")
async def remove_movie_from_collection(
    movie: RemoveMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Rimuove un film dalla collezione dell'utente."""
    result = movies_collection.delete_one({
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Film non trovato nella collezione")
    
    # Aggiorna conteggio utente
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$inc": {"movies_count": -1}}
    )
    
    # Ricalcola statistiche
    await recalculate_user_stats(current_user_id)
    
    return {"message": "Film rimosso con successo"}


@app.put("/user-movies/update-rating")
async def update_movie_rating(
    movie: UpdateMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Aggiorna il rating di un film nella collezione."""
    result = movies_collection.update_one(
        {
            "user_id": current_user_id,
            "name": movie.name,
            "year": movie.year
        },
        {"$set": {"rating": movie.rating}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Film non trovato nella collezione")
    
    # Ricalcola statistiche
    await recalculate_user_stats(current_user_id)
    
    return {"message": "Rating aggiornato con successo"}


async def recalculate_user_stats(user_id: str):
    """Ricalcola le statistiche dell'utente."""
    movies = list(movies_collection.find({"user_id": user_id}))
    
    if not movies:
        stats_collection.delete_one({"user_id": user_id})
        return
    
    # Calcola statistiche
    total = len(movies)
    if total == 0:
        return
        
    ratings = [m["rating"] for m in movies]
    avg_rating = sum(ratings) / total
    
    rating_dist = {}
    for r in range(1, 6):
        rating_dist[str(r)] = len([m for m in movies if m["rating"] == r])
    
    top_rated = sorted([m for m in movies if m["rating"] >= 4], key=lambda x: -x["rating"])[:10]
    top_rated_list = [{"Name": m["name"], "Year": m["year"], "Rating": m["rating"]} for m in top_rated]
    
    # Ricalcola anche statistiche avanzate
    advanced = calculate_advanced_stats(movies)
    
    # Aggiorna statistiche
    stats_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "total_watched": total,
            "avg_rating": round(avg_rating, 2),
            "rating_distribution": rating_dist,
            "top_rated_movies": top_rated_list,
            "total_5_stars": rating_dist.get("5", 0),
            "total_4_stars": rating_dist.get("4", 0),
            "total_3_stars": rating_dist.get("3", 0),
            "total_2_stars": rating_dist.get("2", 0),
            "total_1_stars": rating_dist.get("1", 0),
            "genre_data": advanced.get("genre_data", []),
            "favorite_genre": advanced.get("favorite_genre", "Nessuno"),
            "watch_time_hours": advanced.get("total_watch_time_hours", 0),
            "watch_time_minutes": advanced.get("total_watch_time_minutes", 0),
            "avg_duration": advanced.get("avg_duration", 0),
            "top_directors": advanced.get("top_directors", []),
            "top_actors": advanced.get("top_actors", []),
            "best_rated_directors": advanced.get("best_rated_directors", []),
            "best_rated_actors": advanced.get("best_rated_actors", []),
            "rating_vs_imdb": advanced.get("rating_vs_imdb", []),
            "total_unique_directors": advanced.get("total_unique_directors", 0),
            "total_unique_actors": advanced.get("total_unique_actors", 0),
            "updated_at": datetime.utcnow().isoformat()
        }},
        upsert=True
    )


@app.get("/user-history")
async def get_user_history(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene la cronologia delle analisi sentiment."""
    history = list(sentiment_collection.find(
        {"user_id": current_user_id},
        {"_id": 0}
    ).sort("timestamp", -1).limit(50))
    
    return {"history": history}

# ============================================
# SENTIMENT ENDPOINTS
# ============================================
@app.get("/analyze-movie-sentiment/{title}")
async def analyze_sentiment(title: str, current_user_id: str = Depends(get_current_user_id)):
    """Analizza il sentiment di un film (simulato per ora)."""
    # Simula commenti
    mock_comments = [
        f"I absolutely loved {title}! Amazing film!",
        f"{title} was okay, nothing special.",
        f"Didn't enjoy {title}, waste of time.",
        f"One of the best movies I've seen - {title}!",
        f"{title} has great cinematography."
    ]
    
    # Calcola sentiment simulato
    sentiment_score = round(random.uniform(0.3, 0.9), 2)
    
    result = {
        "user_id": current_user_id,
        "movie": title,
        "sentiment_score": sentiment_score,
        "sentiment_label": "positive" if sentiment_score > 0.6 else ("negative" if sentiment_score < 0.4 else "neutral"),
        "comments_analyzed": len(mock_comments),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Salva nella cronologia
    sentiment_collection.insert_one(result)
    
    return {
        "result": {k: v for k, v in result.items() if k not in ["user_id", "_id"]},
        "status": "success"
    }

# ============================================
# ACTIVITY LOG
# ============================================
@app.post("/log-activity")
async def log_activity(activity: dict, current_user_id: str = Depends(get_current_user_id)):
    """Registra un'attività dell'utente."""
    activity["user_id"] = current_user_id
    activity["timestamp"] = datetime.utcnow().isoformat()
    
    activity_collection.insert_one(activity)
    
    return {"status": "success", "message": "Attività registrata"}

# ============================================
# MOVIES CATALOG ENDPOINTS (Catalogo Film IMDb)
# ============================================

# Collezione catalogo
movies_catalog = db.movies_catalog


@app.get("/catalog/movies")
async def get_catalog_movies(
    skip: int = 0,
    limit: int = 50,
    genre: str = None,
    year: int = None,
    min_rating: float = None,
    search: str = None
):
    """
    Ottiene film dal catalogo con filtri opzionali.
    Non richiede autenticazione per la navigazione.
    """
    query = {}
    
    if genre:
        query["genres"] = genre
    if year:
        query["year"] = year
    if min_rating:
        query["avg_vote"] = {"$gte": min_rating}
    if search:
        norm_search = normalize_title(search)
        # Use ^ to match only at the beginning as requested by the user
        regex_search = f"^{re.escape(search)}"
        regex_norm = f"^{re.escape(norm_search)}"
        
        query["$or"] = [
            {"title": {"$regex": regex_search, "$options": "i"}},
            {"original_title": {"$regex": regex_search, "$options": "i"}},
            {"normalized_title": {"$regex": regex_norm, "$options": "i"}},
            {"normalized_original_title": {"$regex": regex_norm, "$options": "i"}},
            {"director": {"$regex": search, "$options": "i"}},
            {"actors": {"$regex": search, "$options": "i"}}
        ]
    
    movies = list(movies_catalog.find(
        query,
        {"_id": 0}
    ).sort([("date_published", -1), ("votes", -1)]).skip(skip).limit(limit))
    
    # Assicura che ogni film abbia un poster_url
    for movie in movies:
        if not movie.get("poster_url"):
            movie["poster_url"] = STOCK_POSTER_URL
    
    total = movies_catalog.count_documents(query)
    
    return {
        "movies": movies,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@app.get("/catalog/movie/{imdb_id}")
async def get_catalog_movie(imdb_id: str):
    """Ottiene i dettagli di un singolo film dal catalogo."""
    movie = movies_catalog.find_one({"imdb_id": imdb_id}, {"_id": 0})
    
    if not movie:
        raise HTTPException(status_code=404, detail="Film non trovato")
    
    # Assicura poster_url
    if not movie.get("poster_url"):
        movie["poster_url"] = STOCK_POSTER_URL
    
    return movie


@app.get("/catalog/search")
async def search_catalog(
    q: str,
    limit: int = 20
):
    """Ricerca film nel catalogo per titolo."""
    norm_q = normalize_title(q)
    # Use ^ to match only at the beginning as requested by the user
    regex_q = f"^{re.escape(q)}"
    regex_norm = f"^{re.escape(norm_q)}"
    
    movies = list(movies_catalog.find(
        {"$or": [
            {"title": {"$regex": regex_q, "$options": "i"}},
            {"original_title": {"$regex": regex_q, "$options": "i"}},
            {"normalized_title": {"$regex": regex_norm, "$options": "i"}},
            {"normalized_original_title": {"$regex": regex_norm, "$options": "i"}}
        ]},
        {"_id": 0, "imdb_id": 1, "title": 1, "year": 1, "poster_url": 1, "avg_vote": 1, "genres": 1, "description": 1, "director": 1, "actors": 1, "duration": 1, "date_published": 1}
    ).sort([("date_published", -1), ("votes", -1)]).limit(limit))
    
    # Assicura poster_url
    for movie in movies:
        if not movie.get("poster_url"):
            movie["poster_url"] = STOCK_POSTER_URL
    
    return {"results": movies, "query": q}


@app.get("/catalog/genres")
async def get_catalog_genres():
    """Ottiene la lista dei generi disponibili nel catalogo."""
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 30}
    ]
    genres = list(movies_catalog.aggregate(pipeline))
    return {"genres": [{"name": g["_id"], "count": g["count"]} for g in genres]}


@app.get("/catalog/poster/{imdb_id}")
async def get_movie_poster(imdb_id: str):
    """Ottiene solo l'URL del poster per un film."""
    movie = movies_catalog.find_one({"imdb_id": imdb_id}, {"_id": 0, "poster_url": 1})
    
    if movie and movie.get("poster_url"):
        return {"poster_url": movie["poster_url"]}
    
    return {"poster_url": STOCK_POSTER_URL}


@app.get("/catalog/stats")
async def get_catalog_stats():
    """Statistiche del catalogo film."""
    total = movies_catalog.count_documents({})
    with_poster = movies_catalog.count_documents({"has_real_poster": True})
    
    # Top generi
    genre_pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_genres = list(movies_catalog.aggregate(genre_pipeline))
    
    # Film per decennio
    decade_pipeline = [
        {"$match": {"year": {"$ne": None}}},
        {"$group": {
            "_id": {"$subtract": ["$year", {"$mod": ["$year", 10]}]},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    by_decade = list(movies_catalog.aggregate(decade_pipeline))
    
    return {
        "total_movies": total,
        "with_real_poster": with_poster,
        "with_stock_poster": total - with_poster,
        "top_genres": [{"name": g["_id"], "count": g["count"]} for g in top_genres],
        "by_decade": [{"decade": d["_id"], "count": d["count"]} for d in by_decade]
    }
