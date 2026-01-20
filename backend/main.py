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
import pytz
from pymongo import MongoClient
from pydantic import BaseModel
from typing import Optional, List, Dict
from auth import get_password_hash, verify_password, create_access_token, get_current_user_id
from quiz_generator import get_daily_questions, run_daily_quiz_generation
from cinema_pipeline import run_full_pipeline
from kafka_producer import get_kafka_producer

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
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# DATABASE CONNECTION
TMDB_API_KEY = "272643841dd72057567786d8fa7f8c5f"
# ============================================
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")

# Timezone italiana (usata in tutto il codice)
italy_tz = pytz.timezone('Europe/Rome')
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

def mongo_to_dict(obj):
    """Converte oggetti MongoDB (come ObjectId) in tipi serializzabili JSON."""
    if obj is None:
        return None
    if isinstance(obj, list):
        return [mongo_to_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {k: (str(v) if k == "_id" else mongo_to_dict(v)) for k, v in obj.items()}
    # Per altri tipi non serializzabili, converti in stringa se necessario
    from bson import ObjectId
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj


def enrich_movie_data(movies: List[Dict]) -> List[Dict]:
    """
    Arricchisce una lista di film con dati dal catalogo (poster, generi, regista).
    Evita duplicazione di logica tra i vari endpoint.
    """
    if not movies:
        return []
        
    enriched = []
    for m in movies:
        title = m.get("name")
        if title:
            norm_title = normalize_title(title)
            # Tenta prima con titolo esatto, poi normalizzato
            cat = movies_catalog.find_one({
                "$or": [
                    {"title": title},
                    {"normalized_title": norm_title}
                ]
            }, {"poster_url": 1, "genres": 1, "director": 1, "duration": 1, "imdb_id": 1, "_id": 0})
            
            if cat:
                m["poster_url"] = cat.get("poster_url") or STOCK_POSTER_URL
                m["genres"] = cat.get("genres", [])
                m["director"] = cat.get("director", "")
                m["duration"] = cat.get("duration")
                m["imdb_id"] = cat.get("imdb_id")
            else:
                m["poster_url"] = m.get("poster_url") or STOCK_POSTER_URL
        
        enriched.append(m)
    return enriched



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

class QuizSubmission(BaseModel):
    correct: int
    wrong: int
    quiz_date: str

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
            "created_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat(),
            "is_active": True,
            "has_data": False
        })
        print("✅ Utente di default creato: pasquale.langellotti")
    else:
        print("✅ Utente pasquale.langellotti già esistente")

    # --- SCHEDULER UPDATE FILM ---
    # Avvia scheduler per aggiornamento automatico film (Cinema/Digital 2026+)
    updater = MovieUpdater(MONGO_URL, TMDB_API_KEY)
    
    def scheduled_movie_updater():
        """Wrapper per MovieUpdater che controlla se è già in esecuzione."""
        italy_tz = pytz.timezone('Europe/Rome')
        status = db["scraper_progress"].find_one({"_id": "movie_updater"})
        is_running = status and status.get("status") == "running"
        if is_running:
            print("⏭️ [MovieUpdater] Già in esecuzione, salto.")
            return
        try:
            db["scraper_progress"].update_one(
                {"_id": "movie_updater"},
                {"$set": {"status": "running", "updated_at": datetime.now(italy_tz).isoformat()}},
                upsert=True
            )
            updater.fetch_new_releases()
        finally:
            db["scraper_progress"].update_one(
                {"_id": "movie_updater"},
                {"$set": {"status": "idle", "updated_at": datetime.now(italy_tz).isoformat()}}
            )
    
    scheduler = BackgroundScheduler()
    # Esegue ogni giorno alle 01:00 ora italiana - con controllo anti-duplicazione
    italy_tz = pytz.timezone('Europe/Rome')
    scheduler.add_job(scheduled_movie_updater, 'cron', hour=1, minute=0, timezone=italy_tz, id='movie_updater')
    
    # --- SCHEDULER CINEMA CAMPANIA ---
    from cinema_pipeline import run_full_pipeline
    
    def scheduled_cinema_pipeline():
        """Esegue il pipeline completo cinema (scrape + sync) con lock anti-duplicazione."""
        italy_tz = pytz.timezone('Europe/Rome')
        scraper_status = db["scraper_progress"].find_one({"_id": "cinema_scraper"})
        is_running = scraper_status and scraper_status.get("status") in ["scraping", "starting", "saving"]
        if is_running:
            print("⏭️ [Scheduler] Pipeline già in esecuzione, salto.")
            return
        
        print("🕐 [Scheduler] Avvio cinema pipeline a mezzanotte...")
        try:
            run_full_pipeline()  # Esegue FASE 1 (scrape) + FASE 2 (sync) automaticamente
            print("✅ [Scheduler] Cinema pipeline completato.")
        except Exception as e:
            print(f"❌ [Scheduler] Errore durante pipeline: {e}")
    
    # Cinema Pipeline alle 00:00 (mezzanotte) ora italiana
    italy_tz = pytz.timezone('Europe/Rome')
    scheduler.add_job(scheduled_cinema_pipeline, 'cron', hour=0, minute=0, timezone=italy_tz, id='cinema_pipeline')
    
    # Incremental Embedding Update alle 00:30 (dopo aggiornamento film)
    def scheduled_embedding_update():
        """Aggiorna embeddings per i nuovi film aggiunti a mezzanotte."""
        import subprocess
        try:
            print("🧬 [Embeddings] Avvio aggiornamento incrementale...")
            result = subprocess.run(
                ["python", "embedding_and_faiss/incremental_embedding_update.py"],
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=3600  # Max 1 ora
            )
            if result.returncode == 0:
                print("✅ [Embeddings] Aggiornamento completato.")
                print(result.stdout)
            else:
                print(f"❌ [Embeddings] Errore: {result.stderr}")
        except Exception as e:
            print(f"❌ [Embeddings] Errore aggiornamento: {e}")
    
    scheduler.add_job(scheduled_embedding_update, 'cron', hour=0, minute=30, timezone=italy_tz, id='embedding_update')
    

    # Quiz AI generation alle 03:00 (ogni notte)
    def scheduled_quiz_generation():
        """Genera 5 domande quiz giornaliere usando Ollama."""
        import asyncio
        try:
            print("🧠 [Quiz] Avvio generazione domande AI...")
            # Import qui per evitare circular imports
            from quiz_generator import run_daily_quiz_generation
            asyncio.run(run_daily_quiz_generation())
            print("✅ [Quiz] Generazione completata.")
        except Exception as e:
            print(f"❌ [Quiz] Errore generazione: {e}")
    
    scheduler.add_job(scheduled_quiz_generation, 'cron', hour=2, minute=50, timezone=italy_tz, id='quiz_generation')
    
    scheduler.start()
    print("🕒 Scheduler avviato: Movie Updater alle 01:00 + Cinema Pipeline a mezzanotte + Quiz AI alle 02:50 (ora italiana).")
    
    # Eseguiamo anche un update SUBITO all'avvio in background (con lock)
    import threading
    t_updater = threading.Thread(target=scheduled_movie_updater)
    t_updater.start()
    
    # --------------------------------------------
    # STALE DATA CHECK (Auto-Start Scraper + Sync)
    # --------------------------------------------
    # Controlla se i dati del cinema sono aggiornati a oggi (ora italiana).
    # Se la data ultima esecuzione != oggi, avvia lo scraper.
    try:
        italy_tz = pytz.timezone('Europe/Rome')
        scraper_status = db["scraper_progress"].find_one({"_id": "cinema_scraper"})
        last_run_str = scraper_status.get("updated_at", "") if scraper_status else ""
        
        # Converti last_run in data italiana
        if last_run_str:
            try:
                last_run_dt = datetime.fromisoformat(last_run_str.replace('Z', '+00:00'))
                # Se non ha timezone, assumiamo UTC
                if last_run_dt.tzinfo is None:
                    last_run_dt = pytz.UTC.localize(last_run_dt)
                last_run_italy = last_run_dt.astimezone(italy_tz)
                last_run_date = last_run_italy.strftime("%Y-%m-%d")
            except:
                last_run_date = last_run_str[:10] if len(last_run_str) >= 10 else ""
        else:
            last_run_date = ""
        
        today_date = datetime.now(italy_tz).strftime("%Y-%m-%d")
        
        print(f"📅 [Startup] Verifica dati cinema: Last run={last_run_date}, Oggi (IT)={today_date}")
        
        if last_run_date != today_date:
            print(f"⚠️ [Startup] Dati cinema vecchi (Last: '{last_run_date}' vs Today: '{today_date}').")
            print("🚀 [Startup] Avvio automatico Cinema Pipeline in background...")
            t_scraper = threading.Thread(target=scheduled_cinema_pipeline)
            t_scraper.start()
        else:
            print("✅ [Startup] Dati cinema già aggiornati a oggi.")
            
    except Exception as e:
        print(f"⚠️ [Startup] Errore check dati vecchi: {e}")
    
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
                rating_val = int(row['Rating'])
                if rating_val == 0: rating_val = 1  # Rating minimo 1 stella
                movie = {
                    "user_id": user_id,
                    "name": row['Name'],
                    "year": int(row['Year']) if pd.notna(row.get('Year')) else None,
                    "rating": rating_val,
                    "date": str(row.get('Date', '')) if pd.notna(row.get('Date')) else None,
                    "letterboxd_uri": row.get('Letterboxd URI', None),
                    "added_at": datetime.now(italy_tz).isoformat()
                }
                movies.append(movie)
            
            # Salva film
            if movies:
                movies_collection.delete_many({"user_id": user_id})
                movies_collection.insert_many(movies)
            
            # RESET STATISTICHE: NECESSARIO PER EVITARE DOPPI CONTEGGI
            # Poiché stiamo caricando l'intero storico (bulk), dobbiamo azzerare
            # i contatori incrementali precedenti, altrimenti Spark sommerà i nuovi dati ai vecchi.
            stats_collection.delete_one({"user_id": user_id})
            db.user_affinities.delete_many({"user_id": user_id})
            print(f"🧹 Stats + Affinities reset for {user_id} before BULK_IMPORT.")
            
            # Pubblica eventi su Kafka per far calcolare le statistiche a Spark
            kafka_producer = get_kafka_producer()
            kafka_producer.send_batch_event("BULK_IMPORT", user_id, movies)
            
            # Aggiorna utente
            users_collection.update_one(
                {"user_id": user_id},
                {"$set": {"has_data": True, "movies_count": len(movies)}}
            )
            
            print(f"✅ Caricati {len(movies)} film per {user_id}. Stats verranno calcolate da Spark.")
        except Exception as e:
            print(f"❌ Errore caricamento CSV: {e}")
    elif existing_stats:
        print(f"✅ Dati già presenti per {user_id}: {existing_stats.get('total_watched', 0)} film")
    else:
        print(f"⚠️ File CSV non trovato: {csv_path}")

# ============================================
# HELPER FUNCTIONS  
# ============================================

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
        "created_at": datetime.now(italy_tz).isoformat(),
        "is_active": True,
        "has_data": False,
        "movies_count": 0
    }
    
    users_collection.insert_one(new_user)
    
    return {"message": "Utente registrato con successo", "username": user.username}

# ============================================
# QUIZ ENDPOINTS
# ============================================
@app.get("/quiz/questions")
async def get_quiz_questions():
    """Ottiene le domande del quiz giornaliero."""
    return get_daily_questions(5)

@app.post("/quiz/submit")
async def submit_quiz(submission: QuizSubmission, current_user_id: str = Depends(get_current_user_id)):
    """Registra il risultato del quiz."""
    # Recupera dati utente attuali per verificare ultimo quiz
    user = users_collection.find_one({"user_id": current_user_id})
    if not user:
        raise HTTPException(status_code=404, detail="Utente non trovato")
        
    current_quiz = user.get("quiz", {})
    
    # Check if quiz was already taken today
    if current_quiz.get("last_date") == submission.quiz_date:
        # Return existing stats without updating
        return {
            "message": "Quiz già completato oggi", 
            "stats": {
                "correct": current_quiz.get("correct_count", 0), 
                "wrong": current_quiz.get("wrong_count", 0)
            },
            "updated": False
        }
    
    # Aggiorna statistiche quiz
    quiz_correct = current_quiz.get("correct_count", 0) + submission.correct
    quiz_wrong = current_quiz.get("wrong_count", 0) + submission.wrong
    quiz_attempts = current_quiz.get("total_attempts", 0) + 1
    
    new_quiz_data = {
        "correct_count": quiz_correct,
        "wrong_count": quiz_wrong,
        "total_attempts": quiz_attempts,
        "last_date": submission.quiz_date
    }
    
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {"quiz": new_quiz_data}}
    )

    return {"message": "Quiz registrato", "stats": {"correct": quiz_correct, "wrong": quiz_wrong}, "updated": True}

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
        {"$set": {"last_login": datetime.now(italy_tz).isoformat()}}
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

@app.post("/user/avatar")
async def update_avatar(file: UploadFile = File(None), avatar_url: str = None, current_user_id: str = Depends(get_current_user_id)):
    """Aggiorna l'avatar dell'utente (upload o URL preset)."""
    import base64
    
    if file:
        # Upload immagine (salva come base64 in MongoDB)
        contents = await file.read()
        # Limit size to 500KB
        if len(contents) > 500000:
            raise HTTPException(status_code=400, detail="Immagine troppo grande (max 500KB)")
        
        avatar_data = base64.b64encode(contents).decode('utf-8')
        avatar_type = file.content_type or 'image/jpeg'
        avatar_value = f"data:{avatar_type};base64,{avatar_data}"
    elif avatar_url:
        # URL preset
        avatar_value = avatar_url
    else:
        raise HTTPException(status_code=400, detail="Fornisci un'immagine o un URL preset")
    
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {"avatar": avatar_value, "avatar_updated_at": datetime.now(italy_tz).isoformat()}}
    )
    
    return {"status": "success", "avatar": avatar_value}

@app.get("/avatars/presets")
async def get_preset_avatars():
    """Restituisce lista di avatar predefiniti (personaggi iconici del cinema)."""
    return [
        {"id": 1, "name": "Al Pacino (Scarface)", "url": "https://wallpapers.com/images/hd/al-pacino-scarface-smoking-cigar-apphc77u4o6toj2p.jpg"},
        {"id": 2, "name": "Joker (Joaquin Phoenix)", "url": "https://wallpapers.com/images/hd/art-of-joaquin-phoenix-joker-pfp-aa4tbtoqa72z6rf4.jpg"},
        {"id": 3, "name": "It", "url": "https://m.media-amazon.com/images/M/MV5BMTg1NTU5NTgwOV5BMl5BanBnXkFtZTgwMTQ1NzMzMzI@._V1_.jpg"},
        {"id": 4, "name": "Leonardo DiCaprio (The wolf of Wall Street)", "url": "https://d13jj08vfqimqg.cloudfront.net/uploads/article/header_marquee/2096/large_WOWS_leodicapriofist.jpg"},
        {"id": 5, "name": "Harrison Ford (Indiana Jones)", "url": "https://www.hollywoodreporter.com/wp-content/uploads/2022/04/Harrison-Ford-Raiders-of-the-Lost-Ark-Everett-MSDRAOF_EC015-H-2022.jpg?w=1296&h=730&crop=1"},
        {"id": 6, "name": "Audrey Hepburn (Colazione da Tiffany)", "url": "https://cdn.artphotolimited.com/images/5b9fc1ecac06024957be8806/1000x1000/audrey-hepburn-dans-breakfast-at-tiffany-s.jpg"},
        {"id": 7, "name": "Uma Thurman (Pulp Fiction)", "url": "https://m.media-amazon.com/images/M/MV5BYzE3YzY4MTItZTQ4MC00MzBkLTk2YWUtZTJkMWY1ZjQ1MWFiXkEyXkFqcGc@._V1_QL75_UY281_CR0,0,500,281_.jpg"},
        {"id": 8, "name": "Marilyn Monroe", "url": "https://www.arttimegallery.org/cdn/shop/files/IMG_7980.heic?v=1700695372&width=1445"},
        {"id": 9, "name": "Ana de Armas", "url": "https://pbs.twimg.com/media/Ec2RTm5XgAISWIh.jpg"},
        {"id": 10, "name": "Cat Woman", "url": "https://hips.hearstapps.com/hmg-prod/images/catwoman-storia-1647942111.jpeg?crop=1.00xw:0.663xh;0,0.0417xh&resize=640:*"}
    ]

# ============================================
# CINEMA ENDPOINTS
# ============================================
# Collection per showtimes cinema
# usiamo sempre db (cinematch_db) coerentemente
showtimes_collection = db["showtimes"]

@app.get("/cinema/dates")
async def get_cinema_dates(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene la lista delle date disponibili che hanno film al cinema per la provincia dell'utente."""
    # Ottieni la provincia dell'utente
    user = users_collection.find_one({"user_id": current_user_id})
    user_province = user.get("province", "napoli") if user else "napoli"
    if not user_province:
        user_province = "napoli"
    user_province = user_province.lower()
    
    today = datetime.now(italy_tz).date().isoformat()
    
    # Schema: regions.<province>.dates.<date>.cinemas.<cinema>
    region_key = f"regions.{user_province}"
    docs = showtimes_collection.find(
        {region_key: {"$exists": True}},
        {"regions": 1}
    )
    
    available_dates = set()
    for doc in docs:
        region_data = doc.get("regions", {}).get(user_province, {})
        # Le date sono direttamente sotto regions.<province>.dates
        dates = region_data.get("dates", {})
        available_dates.update(dates.keys())
    
    # Filtra per non superare oggi e ordina
    available_dates = sorted([d for d in available_dates if d <= today])
    
    return {
        "available_dates": available_dates,
        "oldest_date": available_dates[0] if available_dates else today,
        "newest_date": available_dates[-1] if available_dates else today,
        "today": today,
        "province": user_province
    }

@app.post("/cinema/refresh")
async def refresh_cinema_data(background_tasks: BackgroundTasks, province: str = "napoli"):
    """
    Avvia manualmente il cinema pipeline (scrape + sync) in background.
    """
    async def run_refresh_task(prov_slug: str):
        try:
            from cinema_pipeline import run_full_pipeline
            print(f"🔄 [Manual Refresh] Cinema pipeline started...")
            await run_in_threadpool(run_full_pipeline)
            print(f"✅ [Manual Refresh] Cinema pipeline completed.")
        except Exception as e:
            print(f"❌ [Manual Refresh] Error: {e}")

    background_tasks.add_task(run_refresh_task, province)
    
    return {
        "status": "refreshing",
        "message": "Cinema pipeline avviato in background"
    }

@app.get("/cinema/status")
async def get_cinema_status():
    """Ottiene lo stato dello scraper."""
    try:
        progress = db["scraper_progress"].find_one({"_id": "cinema_scraper"})
        if not progress:
            return {"status": "idle", "percentage": 0}
            
        return {
            "status": progress.get("status", "idle"),
            "percentage": progress.get("percentage", 0),
            "current_province": progress.get("current_province", ""),
            "updated_at": progress.get("updated_at")
        }
    except Exception as e:
        return {"status": "error", "details": str(e)}

@app.get("/cinema/provinces")
async def get_cinema_provinces():
    """Ottiene la lista delle province disponibili nel database showtimes."""
    # Ottieni tutte le province uniche dalla collection showtimes
    docs = showtimes_collection.find({}, {"regions": 1})
    
    provinces = set()
    for doc in docs:
        regions = doc.get("regions", {})
        provinces.update(regions.keys())
    
    # Ordina e formatta
    provinces_list = sorted([
        {"slug": p, "name": p.capitalize()}
        for p in provinces
    ], key=lambda x: x["name"])
    
    return {"provinces": provinces_list}

@app.get("/cinema/films")
async def get_cinema_films(
    background_tasks: BackgroundTasks, 
    current_user_id: str = Depends(get_current_user_id),
    date: str = None,  # Optional: YYYY-MM-DD format
    province: str = None  # Optional: Province slug override
):
    """Ottiene i film in programmazione nella provincia dell'utente con matching robusto."""

    
    # --- FRESHNESS CHECK ---
    # Get the most recent last_updated from showtimes (new schema)
    latest_showtime = showtimes_collection.find_one(
        {},
        {"last_updated": 1, "_id": 0},
        sort=[("last_updated", -1)]
    )
    
    last_update_str = None
    is_refreshing = False
    
    if latest_showtime and latest_showtime.get("last_updated"):
        last_update_str = latest_showtime["last_updated"]
        try:
            # Parse the ISO date string
            last_update_date = datetime.fromisoformat(last_update_str.replace("Z", "+00:00"))
            today = datetime.now(italy_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Check if scraper is already running
            scraper_status = scraper_progress_collection.find_one({"_id": "cinema_scraper"})
            is_scraper_running = scraper_status and scraper_status.get("status") in ["scraping", "starting", "saving"]
            
            # If data is from before today AND scraper is not already running, trigger a background rescrape
            if last_update_date.replace(tzinfo=None) < today and not is_scraper_running:
                print(f"📅 [Cinema] Dati obsoleti ({last_update_date.date()} < {today.date()}), avvio pipeline in background...")
                from cinema_pipeline import run_full_pipeline
                
                def refresh_cinema_data():
                    try:
                        run_full_pipeline()
                        print("✅ [Cinema] Pipeline completato in background.")
                    except Exception as e:
                        print(f"❌ [Cinema] Errore durante il pipeline: {e}")
                
                background_tasks.add_task(refresh_cinema_data)
                is_refreshing = True
            elif is_scraper_running:
                # Scraper is already running, just report it
                is_refreshing = True
        except Exception as e:
            print(f"⚠️ Errore parsing updated_at: {e}")
    else:
        # No showtimes at all, trigger pipeline
        print("📅 [Cinema] Nessun dato presente, avvio pipeline in background...")
        from cinema_pipeline import run_full_pipeline
        
        def refresh_cinema_data():
            try:
                run_full_pipeline()
                print("✅ [Cinema] Pipeline completato in background.")
            except Exception as e:
                print(f"❌ [Cinema] Errore durante il pipeline: {e}")
        
        background_tasks.add_task(refresh_cinema_data)
        is_refreshing = True
        last_update_str = datetime.now(italy_tz).isoformat()
    
    def find_in_catalog(title: str, original_title: str = None) -> dict:
        """Cerca un film nel catalogo con logica robusta usando i campi indicizzati."""
        # Campi da restituire per velocizzare (MOLTO importante)
        projection = {
            "poster_url": 1, "description": 1, "avg_vote": 1, 
            "genres": 1, "year": 1, "duration": 1, "actors": 1, 
            "director": 1, "imdb_id": 1, "_id": 0
        }
        
        # 1. PRIORITY OPTIMIZATION: Check normalized title first (Indexed & Fast)
        norm_title = normalize_title(title) if title else ""
        norm_original = normalize_title(original_title) if original_title else ""
        
        for norm in [norm_title, norm_original]:
            if not norm or len(norm) < 3:
                continue
            
            # Match esatto su campo normalizzato (Index Scan)
            result = movies_catalog.find_one({
                "$or": [
                    {"normalized_title": norm},
                    {"normalized_original_title": norm}
                ]
            }, projection)
            if result:
                return result

        # 2. Fallback: Regex Search (Slower, Scan-likely)
        or_conditions = []
        if title:
            escaped_title = re.escape(title)
            or_conditions.extend([
                {"title": {"$regex": f"^{escaped_title}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{escaped_title}$", "$options": "i"}}
            ])
        
        if original_title:
            escaped_original = re.escape(original_title)
            or_conditions.extend([
                {"title": {"$regex": f"^{escaped_original}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{escaped_original}$", "$options": "i"}}
            ])

        if or_conditions:
            result = movies_catalog.find_one({"$or": or_conditions}, projection)
            if result:
                return result
                
        return None

    # Ottieni provincia utente (default: napoli / Pompei)
    user = users_collection.find_one({"user_id": current_user_id})
    user_province = user.get("province", "napoli") if user else "napoli"
    if not user_province:
        user_province = "napoli"
    
    # Use province override if provided
    if province:
        user_province = province.lower()
    else:
        user_province = user_province.lower()
    
    # Ottieni film visti dall'utente per filtraggio
    user_watched = list(movies_collection.find({"user_id": current_user_id}, {"name": 1}))
    watched_titles = {normalize_title(m['name']) for m in user_watched}
    
    # Determina la data selezionata
    selected_date = date if date else datetime.now(italy_tz).strftime("%Y-%m-%d")
    
    # Schema: regions.<province>.dates.<date>.cinemas.<cinema>.showtimes
    region_key = f"regions.{user_province}"
    
    showtimes_cursor = showtimes_collection.find(
        {region_key: {"$exists": True}},
        {"_id": 0}
    ).limit(100)
    
    # Trasforma i risultati: estrai cinema dalla data selezionata
    showtimes_list = []
    for doc in showtimes_cursor:
        region_data = doc.get("regions", {}).get(user_province, {})
        dates_data = region_data.get("dates", {})
        
        # Controlla se esiste questa data
        if selected_date not in dates_data:
            continue
        
        date_data = dates_data[selected_date]
        cinemas_for_date = []
        
        for cinema_key, cinema_data in date_data.get("cinemas", {}).items():
            cinemas_for_date.append({
                "name": cinema_data.get("cinema_name", cinema_key),
                "address": "",  # Not available in new schema
                "showtimes": cinema_data.get("showtimes", [])
            })
        
        # Solo se ci sono cinema per questa data
        if cinemas_for_date:
            showtimes_list.append({
                "film_id": doc.get("film_id"),
                "film_title": doc.get("film_title"),
                "film_original_title": doc.get("film_original_title", ""),
                "director": doc.get("director", ""),
                "province": user_province.capitalize(),
                "province_slug": user_province,
                "cinemas": cinemas_for_date,
                "updated_at": doc.get("last_updated", "")
            })
    
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
        "director": 1, "normalized_title": 1, "normalized_original_title": 1, "imdb_id": 1, "_id": 0
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

        # Mostra tutti i cinema per film (rimosso limite precedente [:5])
        cinemas = showtime.get("cinemas", [])
        
        # Formatta orari per ogni cinema
        formatted_cinemas = []
        for cinema in cinemas:
            formatted_cinemas.append({
                "name": cinema.get("name", ""),
                "address": cinema.get("address", ""),
                "showtimes": [
                    {"time": s.get("time", ""), "price": s.get("price", ""), "sala": s.get("sala", "")}
                    for s in cinema.get("showtimes", [])[:6]  # Max 6 orari
                ]
            })
        
        # Costruisci oggetto film
        film = {
            "id": str(showtime.get("film_id", "")) if showtime.get("film_id") else "",
            "title": film_title,
            "original_title": film_original_title,
            "director": director,
            "poster": catalog_info.get("poster_url") if (catalog_info and catalog_info.get("poster_url")) else "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster",
            "description": catalog_info.get("description") if catalog_info else "Trama non disponibile per questo film in programmazione.",
            "rating": catalog_info.get("avg_vote") if (catalog_info and catalog_info.get("avg_vote")) else None,
            "genres": catalog_info.get("genres", []) if catalog_info else ["In Sala"],
            "year": catalog_info.get("year") if catalog_info else datetime.now(italy_tz).year,
            "duration": catalog_info.get("duration") if catalog_info else None,
            "actors": catalog_info.get("actors") if catalog_info else None,
            "cinemas": formatted_cinemas,
            "province": showtime.get("province", ""),
            "imdb_id": catalog_info.get("imdb_id") if catalog_info else None
        }
        
        # Fallback poster se non trovato
        if not film["poster"]:
            film["poster"] = "https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster"
        # Salta film senza cinema/sale per questa data
        if not formatted_cinemas:
            continue
        
        films.append(film)
    
    # Ordina film per rating decrescente (film senza rating in fondo)
    films.sort(key=lambda f: (f["rating"] is not None, f["rating"] or 0), reverse=True)
    
    return {
        "province": user_province.capitalize(),
        "films": films,
        "total": len(films),
        "last_update": last_update_str,
        "is_refreshing": is_refreshing
    }


# Collection per il progresso dello scraper
scraper_progress_collection = db["scraper_progress"]

@app.get("/cinema/progress")
async def get_scraper_progress():
    """Ottiene lo stato di avanzamento dello scraper e del sync."""
    # Scraper progress
    scraper_progress = scraper_progress_collection.find_one({"_id": "cinema_scraper"})
    # Sync progress
    sync_progress = scraper_progress_collection.find_one({"_id": "cinema_sync"})
    
    scraper_data = {
        "percentage": scraper_progress.get("percentage", 0) if scraper_progress else 0,
        "status": scraper_progress.get("status", "idle") if scraper_progress else "idle",
        "current_province": scraper_progress.get("current_province", "") if scraper_progress else "",
        "current": scraper_progress.get("current", 0) if scraper_progress else 0,
        "total": scraper_progress.get("total", 0) if scraper_progress else 0
    }
    
    sync_data = {
        "status": sync_progress.get("status", "idle") if sync_progress else "idle",
        "films_added": sync_progress.get("films_added", 0) if sync_progress else 0,
        "current_film": sync_progress.get("current_film", "") if sync_progress else ""
    }
    
    return {
        **scraper_data,
        "sync": sync_data
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
    
    # NOTA: NON inviamo più eventi RECALCULATE qui!
    # Il BULK_IMPORT iniziale ha già inviato tutti i film a Spark.
    # Inviare di nuovo causerebbe DUPLICAZIONE delle statistiche.
    # Se servono i metadati aggiornati (director, actors, etc.), il prossimo
    # evento singolo o un refresh manuale li utilizzerà dal catalogo aggiornato.
    if added_count > 0:
        print(f"✅ [Background] Catalogo arricchito con {added_count} film. Le stats usano già i dati del BULK_IMPORT iniziale.")


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
        rating_val = int(row['Rating'])
        if rating_val == 0: rating_val = 1  # Rating minimo 1 stella
        movie = {
            "user_id": current_user_id,
            "name": row['Name'],
            "year": int(row['Year']) if pd.notna(row.get('Year')) else None,
            "rating": rating_val,
            "date": str(row.get('Date', '')) if pd.notna(row.get('Date')) else None,
            "letterboxd_uri": row.get('Letterboxd URI', None),
            "added_at": datetime.now(italy_tz).isoformat()
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
                "loaded_at": datetime.now(italy_tz).isoformat(),
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

    # RESET STATISTICHE: NECESSARIO PER EVITARE DOPPI CONTEGGI
    # Poiché stiamo ricaricando l'intero storico (bulk), dobbiamo azzerare 
    # i contatori incrementali precedenti, altrimenti Spark sommerà i nuovi dati ai vecchi.
    stats_collection.delete_one({"user_id": current_user_id})
    # V6: Reset anche user_affinities (struttura piatta)
    db.user_affinities.delete_many({"user_id": current_user_id})
    print(f"🧹 Stats + Affinities reset for {current_user_id} before bulk import.")

    # Pubblica batch di eventi su Kafka per Spark (invece di calculate_stats legacy)
    kafka_producer = get_kafka_producer()
    batch_published = kafka_producer.send_batch_event("BULK_IMPORT", current_user_id, movies)
    
    # Aggiorna utente
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {
            "has_data": True,
            "movies_count": len(movies),
            "data_updated_at": datetime.now(italy_tz).isoformat()
        }}
    )
    
    # Avvia task in background per i film mancanti
    titles_years = list(set([(m["name"], m["year"]) for m in movies]))
    background_tasks.add_task(process_missing_movies_background, titles_years, current_user_id)
    
    return {
        "status": "success",
        "filename": file.filename,
        "count": len(movies),
        "kafka_published": batch_published,
        "message": f"Caricati {len(movies)} film. Statistiche in elaborazione da Spark (30s circa)."
    }


@app.post("/recalculate-stats")
async def recalculate_stats(current_user_id: str = Depends(get_current_user_id)):
    """Triggera ricalcolo statistiche via Spark (pubblica evento su Kafka)."""
    movies = list(movies_collection.find({"user_id": current_user_id}))
    
    if not movies:
        raise HTTPException(status_code=404, detail="Nessun film trovato")
    
    # RESET STATISTICHE PRIMA DI RECALCULATE per evitare duplicazioni
    # Spark usa $inc incrementale, quindi dobbiamo partire da zero
    stats_collection.delete_one({"user_id": current_user_id})
    db.user_affinities.delete_many({"user_id": current_user_id})
    print(f"🧹 Stats + Affinities reset for {current_user_id} before RECALCULATE.")
    
    # Pubblica batch su Kafka per far ricalcolare Spark
    kafka_producer = get_kafka_producer()
    batch_published = kafka_producer.send_batch_event("RECALCULATE", current_user_id, movies)
    
    return {
        "message": "Ricalcolo statistiche avviato via Spark",
        "kafka_published": batch_published,
        "movies_count": len(movies),
        "info": "Le statistiche saranno disponibili entro 30 secondi"
    }


async def get_realtime_year_stats(user_id: str, year: int):
    """
    Calcola conteggio mensile reale dalla collezione movies per un anno specifico.
    Restituisce il formato atteso dal frontend per monthly_data e total_films.
    """
    months_map = {1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mag", 6: "Giu", 
                  7: "Lug", 8: "Ago", 9: "Set", 10: "Ott", 11: "Nov", 12: "Dic"}
    
    year_str = str(year)
    pipeline = [
        {"$match": {
            "user_id": user_id, 
            "date": {"$regex": f"^{year_str}"}
        }},
        {"$group": {
            "_id": {"$substr": ["$date", 5, 2]},  # Estrae mese dalla data
            "count": {"$sum": 1}
        }}
    ]
    
    monthly_real = list(movies_collection.aggregate(pipeline))
    real_monthly_map = {int(m["_id"]): m["count"] for m in monthly_real if m["_id"]}
    
    monthly_data = [
        {"month": months_map[i], "films": real_monthly_map.get(i, 0)} 
        for i in range(1, 13)
    ]
    
    return {
        "year": year,
        "monthly_data": monthly_data,
        "total_films": sum(real_monthly_map.values())
    }

@app.get("/user-stats")
async def get_user_stats(current_user_id: str = Depends(get_current_user_id)):
    """
    Ottiene le statistiche dell'utente (Schema V4 incrementale).
    """
    # 1. Leggi stats da DB
    stats = stats_collection.find_one({"user_id": current_user_id}, {"_id": 0})
    
    if not stats:
        # Fallback se le stats non esistono (utente nuovo o primo upload)
        movie_count = movies_collection.count_documents({"user_id": current_user_id})
        if movie_count > 0:
            return {
                "status": "processing",
                "message": "Statistiche in elaborazione...",
                "total_watched": movie_count,
                "avg_rating": 0,
                "genre_data": [],
                "source": "pending"
            }
        raise HTTPException(status_code=404, detail="Nessun dato trovato.")

    # ═══════════════════════════════════════════════════════════
    # SCHEMA INCREMENTALE V4: Calcoli Lazy con Sync Real-time
    # ═══════════════════════════════════════════════════════════
    # A. SYNC REAL-TIME IMMEDIATO dei conteggi chiave (Fonte di verità: collection movies)
    total_watched = movies_collection.count_documents({"user_id": current_user_id})
    
    # Sync Monthly Counts in tempo reale per TUTTI gli anni
    pipeline = [
        {"$match": {"user_id": current_user_id}},
        {"$group": {
            "_id": {"year": {"$substr": ["$date", 0, 4]}, "month": {"$substr": ["$date", 5, 2]}},
            "count": {"$sum": 1}
        }}
    ]
    real_time_monthly_counts = list(movies_collection.aggregate(pipeline))
    
    # Ricostruisci monthly_counts nidificato aggiornato dai dati reali
    synced_monthly_counts = {}
    for item in real_time_monthly_counts:
        y = item["_id"]["year"]
        m = item["_id"]["month"]
        if y and m:
            if y not in synced_monthly_counts:
                synced_monthly_counts[y] = {}
            synced_monthly_counts[y][m] = item["count"]
    
    # Sovrascrivi monthly_counts dalle stats con quelli reali del DB
    # (Spark aggiornerà il resto dei campi asincronamente)
    monthly_counts = synced_monthly_counts
    
    sum_ratings = stats.get("sum_ratings", 0) or 0
    
    # Media rating (lazy) - Usa total_watched reale
    avg_rating = round(sum_ratings / total_watched, 2) if total_watched > 0 else 0
    
    # Ore di visione
    watch_time_hours = (stats.get("watch_time_minutes", 0) or 0) // 60
    
    # Rating distribution -> rating_chart_data
    rating_dist = stats.get("rating_distribution", {})
    rating_chart_data = [
        {"rating": f"⭐{i}", "count": rating_dist.get(str(i), 0), "stars": i}
        for i in range(1, 6)
    ]
    
    # ═══════════════════════════════════════════════════════════════════════
    # NUOVO V6: Leggi directors/actors/genres da user_affinities (struttura piatta)
    # ═══════════════════════════════════════════════════════════════════════
    affinities_collection = db.user_affinities
    
    # --- DIRECTORS ---
    # Most Watched Directors (top 15 per count)
    directors_cursor = list(affinities_collection.find(
        {"user_id": current_user_id, "type": "director", "count": {"$gt": 0}}
    ).sort("count", -1).limit(50))  # Fetch più per i threshold
    
    best_directors = []
    for d in directors_cursor:
        count = d.get("count", 0)
        sum_v = d.get("sum_voti", 0)
        if count > 0:
            best_directors.append({
                "name": d.get("name", d.get("name_key", "").replace("_", " ")),
                "count": count,
                "avg_rating": round(sum_v / count, 1) if count > 0 else 0
            })
    
    # Ordina per avg_rating (per best_rated) 
    best_directors_by_rating = sorted(best_directors, key=lambda x: (-x["avg_rating"], -x["count"]))
    
    # Formatta per threshold come atteso dal frontend
    best_rated_directors = {
        "1": best_directors_by_rating[:10],
        "2": [d for d in best_directors_by_rating if d["count"] >= 2][:10],
        "3": [d for d in best_directors_by_rating if d["count"] >= 3][:10],
        "5": [d for d in best_directors_by_rating if d["count"] >= 5][:10]
    }
    
    # Most watched (già ordinati per count dalla query)
    most_watched_directors = sorted(best_directors, key=lambda x: -x["count"])[:15]
    
    # --- ACTORS ---
    actors_cursor = list(affinities_collection.find(
        {"user_id": current_user_id, "type": "actor", "count": {"$gt": 0}}
    ).sort("count", -1).limit(50))
    
    best_actors = []
    for a in actors_cursor:
        count = a.get("count", 0)
        sum_v = a.get("sum_voti", 0)
        if count > 0:
            best_actors.append({
                "name": a.get("name", a.get("name_key", "").replace("_", " ")),
                "count": count,
                "avg_rating": round(sum_v / count, 1) if count > 0 else 0
            })
    
    best_actors_by_rating = sorted(best_actors, key=lambda x: (-x["avg_rating"], -x["count"]))
    
    best_rated_actors = {
        "1": best_actors_by_rating[:10],
        "2": [a for a in best_actors_by_rating if a["count"] >= 2][:10],
        "3": [a for a in best_actors_by_rating if a["count"] >= 3][:10],
        "5": [a for a in best_actors_by_rating if a["count"] >= 5][:10]
    }
    most_watched_actors = sorted(best_actors, key=lambda x: -x["count"])[:15]
    
    # --- GENRES (ora da user_affinities) ---
    # Fallback: se user_affinities ha generi, usali; altrimenti usa genre_counts da user_stats
    genres_cursor = list(affinities_collection.find(
        {"user_id": current_user_id, "type": "genre", "count": {"$gt": 0}}
    ).sort("count", -1).limit(20))
    
    if genres_cursor:
        # Usa generi da user_affinities (V6)
        genre_counts = {g.get("name_key", g.get("name", "").replace(" ", "_")): g.get("count", 0) for g in genres_cursor}
    else:
        # Fallback a user_stats (retrocompatibilità V5 e precedenti)
        genre_counts = stats.get("genre_counts", {})
    
    total_genres = sum(genre_counts.values()) or 1
    
    # Colori generi
    genre_colors = {
        "Drama": "#E50914", "Comedy": "#FF6B35", "Action": "#00529B",
        "Thriller": "#8B5CF6", "Horror": "#6B21A8", "Romance": "#EC4899",
        "Sci-Fi": "#06B6D4", "Adventure": "#10B981", "Crime": "#F59E0B",
        "Dramma": "#E50914", "Commedia": "#FF6B35", "Azione": "#00529B",
        "Fantascienza": "#06B6D4", "Avventura": "#10B981", "Fantasy": "#8B5CF6",
        "Family": "#F472B6", "Mystery": "#4B5563", "Animation": "#FBBF24"
    }
    
    sorted_genres = sorted(genre_counts.items(), key=lambda x: -x[1])[:8]
    genre_data = [
        {
            "name": g.replace("_", " "),
            "value": round((c / total_genres) * 100, 1),
            "color": genre_colors.get(g.replace("_", " "), "#9CA3AF"),
            "count": c
        }
        for g, c in sorted_genres
    ]
        
    favorite_genre = sorted_genres[0][0].replace("_", " ") if sorted_genres else "Nessuno"
        
    # Monthly counts -> year_data (ricostruisci formato frontend)
    # Struttura ora nidificata: { "2026": { "01": 5, "02": 3 } }
    monthly_counts = stats.get("monthly_counts", {})
    months_map = {1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mag", 6: "Giu", 
                  7: "Lug", 8: "Ago", 9: "Set", 10: "Ott", 11: "Nov", 12: "Dic"}
    
    years_data_map = {}
    for year_key, months in monthly_counts.items():
        try:
            year = int(year_key)
            if year not in years_data_map:
                years_data_map[year] = {i: 0 for i in range(1, 13)}
            for month_key, count in months.items():
                try:
                    month = int(month_key)
                    years_data_map[year][month] = count
                except:
                    pass
        except:
            pass
    
    year_data = []
    for year in sorted(years_data_map.keys(), reverse=True):
        month_counts = years_data_map[year]
        monthly_data = [{"month": months_map[i], "films": month_counts.get(i, 0)} for i in range(1, 13)]
        year_data.append({
            "year": year,
            "monthly_data": monthly_data,
            "total_films": sum(month_counts.values())
        })
        
    available_years = sorted(years_data_map.keys(), reverse=True)
    
    # Top 5 anni più visti (calcolato dalla collection movies)
    # Aggregazione per anno di PRODUZIONE del film (non data visione)
    year_counts = {}
    user_movies = list(movies_collection.find(
        {"user_id": current_user_id}, 
        {"year": 1, "_id": 0}
    ))
    for m in user_movies:
        y = m.get("year")
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1
    
    top_years = [
        {"year": y, "count": c} 
        for y, c in sorted(year_counts.items(), key=lambda x: -x[1])[:5]
    ]
    
    # IMPORTANTE: salva valori prima di sovrascrivere l'oggetto stats
    original_stats_version = stats.get("stats_version", "6.0_flat_affinities")
    original_updated_at = stats.get("updated_at")
    
    # Calcolo conteggi unique da user_affinities
    unique_directors_count = affinities_collection.count_documents(
        {"user_id": current_user_id, "type": "director", "count": {"$gt": 0}}
    )
    unique_actors_count = affinities_collection.count_documents(
        {"user_id": current_user_id, "type": "actor", "count": {"$gt": 0}}
    )
    avg_duration = (stats.get("watch_time_minutes", 0) or 0) // total_watched if total_watched > 0 else 0
    
    # Costruisci risposta finale (formato compatibile frontend)
    stats = {
        "user_id": current_user_id,
        "total_watched": total_watched,
        "avg_rating": avg_rating,
        "watch_time_hours": watch_time_hours,
        "avg_duration": avg_duration,
        "unique_directors_count": unique_directors_count,
        "unique_actors_count": unique_actors_count,
        "favorite_genre": favorite_genre,
        "genre_data": genre_data,
        "rating_chart_data": rating_chart_data,
        "best_rated_directors": best_rated_directors,
        "most_watched_directors": most_watched_directors,
        "best_rated_actors": best_rated_actors,
        "most_watched_actors": most_watched_actors,
        "year_data": year_data,
        "available_years": available_years,
        "top_years": top_years,  # Top 5 anni più visti
        "top_rated_movies": [],  # Lazy: usa /user-stats/top-rated
        "recent_movies": [],     # Lazy: usa /user-stats/recent
        "stats_version": original_stats_version,
        "updated_at": original_updated_at,
        "source": "flat_affinities_v6"
    }
    
    # B. Sync Status and Final formatting (Sync Spark asincrono)    
    # 4. Sync Status Check
    sync_status = "synced"
    user = users_collection.find_one({"user_id": current_user_id}, {"data_updated_at": 1})
    stats_updated = stats.get("updated_at")
    
    if user and user.get("data_updated_at") and stats_updated:
        data_updated = user.get("data_updated_at")
        if data_updated > stats_updated:
            sync_status = "syncing"
    
    stats["sync_status"] = sync_status
    
    # 5. Assicura campi quiz (Prende da users collection nested 'quiz')
    user_doc = users_collection.find_one({"user_id": current_user_id}, {"quiz": 1}) or {}
    quiz_data = user_doc.get("quiz", {})
    
    stats["quiz_correct_count"] = quiz_data.get("correct_count", 0)
    stats["quiz_wrong_count"] = quiz_data.get("wrong_count", 0)
    stats["quiz_total_attempts"] = quiz_data.get("total_attempts", 0)
    stats["last_quiz_date"] = quiz_data.get("last_date")

    return mongo_to_dict(stats)


@app.get("/trends/global")
async def get_global_trends():
    """Restituisce i trend globali calcolati da Spark."""
    trends = db.global_stats.find_one({"type": "global_trends"}, {"_id": 0})
    
    if not trends:
        # Fallback se Spark non ha ancora calcolato
        return {
            "top_movies": [],
            "trending_genres": [],
            "message": "Trend in elaborazione..."
        }
        
    return mongo_to_dict(trends)


@app.get("/user-stats/top-rated")
async def get_top_rated_movies(
    limit: int = 10,
    current_user_id: str = Depends(get_current_user_id)
):
    """
    🚀 LAZY ENDPOINT: Restituisce i film con rating >= 4, ordinati per rating.
    Usato con schema incrementale V4 dove top_rated non è pre-calcolato.
    """
    # Query con indice su (user_id, rating)
    movies = list(movies_collection.find(
        {"user_id": current_user_id, "rating": {"$gte": 4}},
        {"_id": 0, "user_id": 0}
    ).sort("rating", -1).limit(limit))
    
    # Arricchisci con dati catalogo usando l'helper
    return enrich_movie_data(movies)


@app.get("/user-stats/recent")
async def get_recent_movies(
    limit: int = 10,
    current_user_id: str = Depends(get_current_user_id)
):
    """
    🚀 LAZY ENDPOINT: Restituisce i film più recenti per data di visione.
    Usato con schema incrementale V4 dove recent_movies non è pre-calcolato.
    """
    # Query con indice su (user_id, date)
    movies = list(movies_collection.find(
        {"user_id": current_user_id},
        {"_id": 0, "user_id": 0}
    ).sort("date", -1).limit(limit))
    
    # Arricchisci con dati catalogo usando l'helper
    return enrich_movie_data(movies)


@app.get("/recommendations")
async def get_recommendations(current_user_id: str = Depends(get_current_user_id)):
    """
    Get personalized movie recommendations for the user.
    Returns 6 recommended + 3 not-recommended films based on user's taste profile.
    """
    from recommendation_service import get_recommendation_service
    
    try:
        service = get_recommendation_service()
        result = await run_in_threadpool(service.get_recommendations, current_user_id)
        
        if result.get("error"):
            # Return empty lists with error message
            return {
                "recommended": [],
                "not_recommended": [],
                "message": result["error"],
                "matched_films": result.get("matched_films", 0),
                "total_films": result.get("total_films", 0)
            }
        
        return result
    except Exception as e:
        print(f"❌ Recommendation error: {e}")
        return {
            "recommended": [],
            "not_recommended": [],
            "message": f"Error generating recommendations: {str(e)}"
        }

class MovieCreate(BaseModel):
    name: str
    year: Optional[int] = None
    rating: int
    date: Optional[str] = None
    review: Optional[str] = None
    imdb_id: Optional[str] = None # Link forte al catalogo

@app.post("/movies")
async def add_movie(movie: MovieCreate, background_tasks: BackgroundTasks, current_user_id: str = Depends(get_current_user_id)):
    """Aggiunge un singolo film alla lista dei visti."""
    
    entry = {
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year,
        "rating": movie.rating,
        "date": movie.date or datetime.now(italy_tz).strftime("%Y-%m-%d"),
        "review": movie.review,
        "imdb_id": movie.imdb_id,
        "added_at": datetime.now(italy_tz).isoformat()
    }
    
    result = movies_collection.insert_one(entry)
    
    # Aggiorna utente (interazione)
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {
            "has_data": True,
            "data_updated_at": datetime.now(italy_tz).isoformat()
        }}
    )
    
    # O(1) INCREMENTALE: Invia SOLO il film appena aggiunto
    # NON inviare tutti i film (causa doppi incrementi!)
    try:
        kafka_producer = get_kafka_producer()
        kafka_producer.send_movie_event("ADD", current_user_id, entry)
    except Exception as e:
        print(f"⚠️ Errore invio Kafka: {e}")
    
    return {"id": str(result.inserted_id), "message": "Film aggiunto correttamente"}


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
    """
    Ottiene i film dell'utente filtrati per regista o attore.
    V2: Refactoring per allineamento totale con Spark.
    Invece di cercare il regista nel catalogo e poi matchare i film utente,
    facciamo come Spark: prendiamo i film utente, li arricchiamo TUTTI col catalogo,
    e filtriamo quelli che hanno il regista/attore cercato.
    """
    import re
    
    # 1. Recupera TUTTI i film utente
    user_movies = list(movies_collection.find({"user_id": current_user_id}))
    if not user_movies:
        return []

    # 2. Batch Lookup Keys
    titles_to_search = set()
    norm_titles_to_search = set()
    
    for m in user_movies:
        t = m.get('name')
        if t:
            titles_to_search.add(t)
            norm_titles_to_search.add(normalize_title(t))
            
    # 3. Fetch Catalog (Enrichment Source)
    # Cerchiamo nel catalogo qualsiasi film che l'utente possiede
    catalog_docs = list(movies_catalog.find({
        "$or": [
            {"title": {"$in": list(titles_to_search)}},
            {"original_title": {"$in": list(titles_to_search)}},
            {"normalized_title": {"$in": list(norm_titles_to_search)}},
            {"normalized_original_title": {"$in": list(norm_titles_to_search)}}
        ]
    }).collation({"locale": "en", "strength": 2}))
    
    # 4. Build Catalog Map (Key = Normalized Title -> Doc)
    catalog_map = {}
    for cd in catalog_docs:
        # Mappa per tutte le varianti di titolo per garantire il match
        if cd.get('title'): 
            catalog_map[normalize_title(cd['title'])] = cd
        if cd.get('original_title'): 
            catalog_map[normalize_title(cd['original_title'])] = cd
        if cd.get('normalized_title'): 
            catalog_map[cd['normalized_title']] = cd
        if cd.get('normalized_original_title'): 
            catalog_map[cd['normalized_original_title']] = cd
            
    # 5. Enrich & Filter
    results = []
    target_name_norm = normalize_title(name)
    field = "director" if type == "director" else "actors"
    
    for m in user_movies:
        # Enrich logic
        title_norm = normalize_title(m.get('name', ''))
        cat_match = catalog_map.get(title_norm)
        
        # Determine person list
        people_str = ""
        cat_info = {}
        
        if cat_match:
            cat_info = cat_match
            people_str = cat_match.get(field, "")
        else:
            # Fallback (raro se non c'è match, ma usiamo dati utente se presenti)
            cat_info = {}
            people_str = m.get(field, "") # Normalmente vuoto nei film user
            
        # Check Match
        if not people_str:
            continue
            
        # Split tokens (spark logic: split by comma or pipe)
        people_list = [p.strip() for p in re.split(r'[,|]', str(people_str)) if p.strip()]
        
        is_match = False
        for p in people_list:
            if normalize_title(p) == target_name_norm:
                is_match = True
                break
                
        if is_match:
            # Poster construction
            poster = cat_info.get('poster_url')
            if not poster and cat_info.get('poster_path'):
                poster = f"https://image.tmdb.org/t/p/w500{cat_info['poster_path']}"
            if not poster:
                poster = m.get('poster_url') or STOCK_POSTER_URL
                
            results.append({
                "id": str(m.get('_id', random.randint(1, 100000))),
                "title": m.get('name'),
                "year": m.get('year') or cat_info.get('year'),
                "poster": poster,
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
    """
    Ottiene le statistiche mensili per un anno specifico.
    Ora legge da year_data nelle user_stats o fa un sync real-time se richiesto.
    """
    current_year = datetime.now(italy_tz).year
    
    # Se è l'anno corrente, forza sempre il sync real-time per massima precisione
    if year == current_year:
        realtime = await get_realtime_year_stats(current_user_id, year)
        # Recupera available_years dalle stats per completare la risposta
        stats = stats_collection.find_one({"user_id": current_user_id}, {"available_years": 1})
        realtime["available_years"] = stats.get("available_years", [year]) if stats else [year]
        return realtime

    # Per anni passati, leggi da user_stats (che è stato popolato/migrato)
    stats = stats_collection.find_one({"user_id": current_user_id}, {"year_data": 1, "available_years": 1, "_id": 0})
    
    if not stats:
        # Fallback totale
        return await get_realtime_year_stats(current_user_id, year)
    
    year_data = stats.get("year_data", [])
    available_years = stats.get("available_years", [])
    
    # Trova i dati per l'anno richiesto
    year_entry = next((y for y in year_data if y.get("year") == year), None)
    
    if year_entry:
        return {
            "year": year,
            "monthly_data": year_entry.get("monthly_data", []),
            "total_films": year_entry.get("total_films", 0),
            "available_years": available_years
        }
    else:
        # Se non trovato in stats ma anno passato, prova comunque un sync last-resort
        return await get_realtime_year_stats(current_user_id, year)



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
                    "loaded_at": datetime.now(italy_tz).isoformat(),
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
    
    # Step 2: Raccogli TUTTI i titoli e ID per un batch lookup garantito
    titles_to_lookup = []
    ids_to_lookup = []
    
    for movie in user_movies:
        if movie.get("imdb_id"):
            ids_to_lookup.append(movie["imdb_id"])
        
        titles_to_lookup.append({
            "title": movie["name"].lower(),
            "year": movie.get("year")
        })
    
    # Step 3: Batch lookup nel catalogo
    catalog_cache = {}
    
    if titles_to_lookup or ids_to_lookup:
        # Costruisci query
        query_parts = []
        
        # A. Cerca per ID esatto (priorità massima)
        if ids_to_lookup:
            query_parts.append({"imdb_id": {"$in": ids_to_lookup}})
            
        # B. Cerca per Titolo (fallback)
        if titles_to_lookup:
            title_list = list(set(t["title"] for t in titles_to_lookup))
            import re
            escaped_titles = [re.escape(t) for t in title_list]
            regex_pattern = f"^({'|'.join(escaped_titles)})$"
            
            query_parts.append({
                "$or": [
                    {"title": {"$regex": regex_pattern, "$options": "i"}},
                    {"original_title": {"$regex": regex_pattern, "$options": "i"}},
                    {"english_title": {"$regex": regex_pattern, "$options": "i"}}
                ]
            })
        
        catalog_movies = movies_catalog.find(
            {"$or": query_parts} if query_parts else {},
            {"title": 1, "original_title": 1, "english_title": 1, "year": 1, "poster_url": 1, "imdb_id": 1, "genres": 1, "description": 1, "director": 1, "actors": 1, "votes": 1, "avg_vote": 1, "duration": 1}
        )
        
        # Costruisci cache intelligente
        for cm in catalog_movies:
            # Helper per aggiungere alla cache con logica di "miglior match"
            def add_to_cache(key, movie):
                if not key: return
                
                # Helper interno per decidere se sostituire un'entry esistente
                # (Se abbiamo già un match per "Avatar", vogliamo il migliore)
                current = catalog_cache.get(key)
                if not current: 
                    catalog_cache[key] = movie
                    return

                # Preferiamo entry con poster reale
                curr_has_poster = current.get('poster_url') and STOCK_POSTER_URL not in current.get('poster_url', '')
                new_has_poster = movie.get('poster_url') and STOCK_POSTER_URL not in movie.get('poster_url', '')
                
                if new_has_poster and not curr_has_poster: 
                    catalog_cache[key] = movie
                    return
                if curr_has_poster and not new_has_poster: 
                    return
                    
                # A parità di poster, preferiamo quello con più voti
                if (movie.get('votes', 0) or 0) > (current.get('votes', 0) or 0):
                    catalog_cache[key] = movie

            # Mappa tutte le chiavi possibili
            if cm.get('imdb_id'):
                add_to_cache(f"id_{cm['imdb_id']}", cm)
                
            t = cm.get('title')
            y = cm.get('year')
            t_orig = cm.get('original_title')
            t_eng = cm.get('english_title')
            
            if t:
                add_to_cache(f"{t.lower()}_{y}", cm)
                add_to_cache(t.lower(), cm)
            if t_orig:
                add_to_cache(f"{t_orig.lower()}_{y}", cm)
                add_to_cache(t_orig.lower(), cm)
            if t_eng:
                add_to_cache(f"{t_eng.lower()}_{y}", cm)
                add_to_cache(t_eng.lower(), cm)
        
        # Step 4: Applica i dati del catalogo ai film utente
        for movie in user_movies:
            # 1. Prova lookup per ID (Massima precisione)
            catalog_movie = None
            if movie.get("imdb_id"):
                catalog_movie = catalog_cache.get(f"id_{movie['imdb_id']}")
            
            # 2. Se fallisce, prova Titolo + Anno
            title_lower = movie["name"].lower()
            year = movie.get("year")
            
            if not catalog_movie:
                catalog_movie = catalog_cache.get(f"{title_lower}_{year}")
            
            # 3. Fuzzy Year Check e Fallback solo Titolo (come prima)
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
        old_rating = existing.get("rating")
        new_rating = movie.rating if movie.rating and movie.rating > 0 else 1  # Rating minimo 1 stella
        
        movies_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "rating": new_rating,
                "comment": movie.comment,
                "updated_at": datetime.now(italy_tz).isoformat()
            }}
        )
        
        # Aggiorna timestamp per invalidare cache
        users_collection.update_one(
            {"user_id": current_user_id},
            {"$set": {"data_updated_at": datetime.now(italy_tz).isoformat()}}
        )
        
        # O(1) INCREMENTALE: Se cambia il rating, usa UPDATE_RATING
        # NON fare RECALCULATE che causa raddoppio delle statistiche!
        if old_rating != new_rating:
            kafka_producer = get_kafka_producer()
            kafka_producer.send_movie_event("UPDATE_RATING", current_user_id, {
                "name": movie.name,
                "year": movie.year,
                "rating": new_rating,
                "date": existing.get("date"),
                "old_rating": old_rating,
                "new_rating": new_rating
            })
        
        return {"status": "success", "message": "Film aggiornato"}
    
    # Crea documento film
    rating_val = movie.rating if movie.rating and movie.rating > 0 else 1  # Rating minimo 1 stella
    new_movie = {
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year,
        "rating": rating_val,
        "comment": movie.comment,
        "date": datetime.now(italy_tz).strftime("%Y-%m-%d"),
        "imdb_id": movie.imdb_id,
        "poster_url": movie.poster_url,
        "added_at": datetime.now(italy_tz).isoformat()
    }
    
    movies_collection.insert_one(new_movie)
    
    # Pubblica evento Kafka per elaborazione Spark
    kafka_producer = get_kafka_producer()
    kafka_producer.send_movie_event("ADD", current_user_id, new_movie)
    
    # Aggiorna conteggio utente (asincrono, stats da Kafka)
    users_collection.update_one(
        {"user_id": current_user_id},
        {
            "$inc": {"movies_count": 1}, 
            "$set": {
                "has_data": True,
                "data_updated_at": datetime.now(italy_tz).isoformat()
            }
        }
    )
    
    return {"status": "success", "message": "Film aggiunto"}


@app.post("/user-movies/update")
async def update_user_movie(
    req: UpdateMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Aggiorna voto o commento di un film nei 'visti'."""
    
    # O(1) INCREMENTALE: Recupera il film PRIMA per avere old_rating
    existing = movies_collection.find_one({
        "user_id": current_user_id,
        "name": req.name,
        "year": req.year
    })
    
    if not existing:
        raise HTTPException(status_code=404, detail="Film non trovato nei tuoi visti")
    
    old_rating = existing.get("rating")
    
    update_data = {}
    new_rating = None
    if req.rating is not None:
        new_rating = req.rating if req.rating > 0 else 1  # Rating minimo 1 stella
        update_data["rating"] = new_rating
    if req.comment is not None: 
        update_data["comment"] = req.comment
    update_data["updated_at"] = datetime.now(italy_tz).isoformat()

    movies_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": update_data}
    )
    
    # Aggiorna timestamp per invalidare cache
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {"data_updated_at": datetime.now(italy_tz).isoformat()}}
    )
    
    # O(1) INCREMENTALE: Se cambia solo il rating, usa UPDATE_RATING
    # NON fare RECALCULATE che causa raddoppio delle statistiche!
    kafka_producer = get_kafka_producer()
    
    if new_rating is not None and old_rating != new_rating:
        # Invia UPDATE_RATING con old e new rating
        kafka_producer.send_movie_event("UPDATE_RATING", current_user_id, {
            "name": req.name,
            "year": req.year,
            "rating": new_rating,
            "date": existing.get("date"),
            "old_rating": old_rating,
            "new_rating": new_rating
        })
    # Se cambia solo il commento, non serve aggiornare le statistiche
    
    return {"status": "success"}


@app.post("/user-movies/remove")
async def remove_movie_from_collection(
    movie: RemoveMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Rimuove un film dalla collezione dell'utente."""
    
    # O(1) INCREMENTALE: Recupera dati COMPLETI del film PRIMA di eliminarlo
    # (servono rating e date per decrementare i contatori corretti)
    existing_movie = movies_collection.find_one({
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year
    })
    
    if not existing_movie:
        raise HTTPException(status_code=404, detail="Film non trovato nella collezione")
    
    # Ora elimina
    movies_collection.delete_one({"_id": existing_movie["_id"]})
    
    # Pubblica evento DELETE con TUTTI i dati necessari per decrementare
    kafka_producer = get_kafka_producer()
    kafka_producer.send_movie_event("DELETE", current_user_id, {
        "name": existing_movie.get("name"),
        "year": existing_movie.get("year"),
        "rating": existing_movie.get("rating"),  # Necessario per rating_distribution
        "date": existing_movie.get("date")       # Necessario per monthly_counts
    })
    
    # Aggiorna conteggio utente
    users_collection.update_one(
        {"user_id": current_user_id},
        {
            "$inc": {"movies_count": -1},
            "$set": {
                "data_updated_at": datetime.now(italy_tz).isoformat()
            }
        }
    )
    
    # NON fare RECALCULATE - il sistema O(1) decrementa atomicamente
    return {"message": "Film rimosso con successo"}


@app.put("/user-movies/update-rating")
async def update_movie_rating(
    movie: UpdateMovieRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """Aggiorna il rating di un film nella collezione."""
    
    # O(1) INCREMENTALE: recupera dati PRIMA dell'update per calcolare delta
    existing = movies_collection.find_one({
        "user_id": current_user_id,
        "name": movie.name,
        "year": movie.year
    })
    
    if not existing:
        raise HTTPException(status_code=404, detail="Film non trovato nella collezione")
    
    old_rating = existing.get("rating")
    new_rating = movie.rating if movie.rating and movie.rating > 0 else 1
    
    # Aggiorna nel DB
    movies_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": {"rating": new_rating}}
    )
    
    # Aggiorna timestamp
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {"data_updated_at": datetime.now(italy_tz).isoformat()}}
    )
    
    # O(1) INCREMENTALE: Invia evento UPDATE_RATING con old_rating e new_rating
    # Questo modifica SOLO rating_distribution e sum_ratings, non total_watched/genre_counts/etc.
    kafka_producer = get_kafka_producer()
    
    kafka_producer.send_movie_event("UPDATE_RATING", current_user_id, {
        "name": existing.get("name"),
        "year": existing.get("year"),
        "rating": new_rating,
        "date": existing.get("date"),
        "old_rating": old_rating,
        "new_rating": new_rating
    })
    
    return {"message": "Rating aggiornato con successo"}


async def recalculate_user_stats(user_id: str):
    """Ricalcola le statistiche dell'utente (Complete)."""
    movies = list(movies_collection.find({"user_id": user_id}))
    
    if not movies:
        stats_collection.delete_one({"user_id": user_id})
        # V6: Reset anche user_affinities
        db.user_affinities.delete_many({"user_id": user_id})
        # Reset count
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"movies_count": 0, "has_data": False}}
        )
        return
    
    # RESET STATISTICHE PRIMA DI RECALCULATE per evitare duplicazioni
    # Spark usa $inc incrementale, quindi dobbiamo partire da zero
    stats_collection.delete_one({"user_id": user_id})
    db.user_affinities.delete_many({"user_id": user_id})
    print(f"🧹 Stats + Affinities reset for {user_id} before RECALCULATE.")
    
    # 1. Aggiorna movies_count utente
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "movies_count": len(movies),
            "has_data": True,
            "data_updated_at": datetime.now(italy_tz).isoformat()
        }}
    )
    
    # 2. Pubblica evento su Kafka per far calcolare le statistiche a Spark
    kafka_producer = get_kafka_producer()
    kafka_producer.send_batch_event("RECALCULATE", user_id, movies)


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
        "timestamp": datetime.now(italy_tz).isoformat()
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
    activity["timestamp"] = datetime.now(italy_tz).isoformat()
    
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
    """Ricerca film nel catalogo per titolo (supporta italiano, inglese e titolo originale)."""
    norm_q = normalize_title(q)
    # Use ^ to match only at the beginning as requested by the user
    regex_q = f"^{re.escape(q)}"
    regex_norm = f"^{re.escape(norm_q)}"
    
    movies = list(movies_catalog.find(
        {"$or": [
            {"title": {"$regex": regex_q, "$options": "i"}},
            {"original_title": {"$regex": regex_q, "$options": "i"}},
            {"english_title": {"$regex": regex_q, "$options": "i"}},  # Aggiunto per ricerca in inglese
            {"normalized_title": {"$regex": regex_norm, "$options": "i"}},
            {"normalized_original_title": {"$regex": regex_norm, "$options": "i"}},
            {"normalized_english_title": {"$regex": regex_norm, "$options": "i"}}  # Aggiunto per ricerca normalizzata inglese
        ]},
        {"_id": 0, "imdb_id": 1, "title": 1, "year": 1, "poster_url": 1, "avg_vote": 1, "genres": 1, "description": 1, "director": 1, "actors": 1, "duration": 1, "date_published": 1, "english_title": 1, "original_title": 1}
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



# ============================================
# ELASTICSEARCH CONFIGURATION
# ============================================
from elasticsearch import Elasticsearch

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_INDEX_NAME = "movies"
es_client = Elasticsearch(ES_URL)

# Note: We removed the blocking ping() here to avoid startup delays.
# Connection will be verified on first use in search endpoints.


class AdvancedSearchRequest(BaseModel):
    query: str
    fields: List[str] = ["title", "original_title", "actors", "director", "genres", "description"]


@app.post("/catalog/advanced-search")
async def advanced_search_catalog(req: AdvancedSearchRequest):
    """
    Ricerca avanzata film nel catalogo usando Elasticsearch.
    Permette di specificare i campi su cui cercare.
    """
    if not req.query:
        return {"results": [], "query": req.query, "total": 0}

    # Costruisci la query ElasticSearch
    es_query = {
        "size": 20,
        "query": {
            "multi_match": {
                "query": req.query,
                "type": "best_fields",
                "fields": req.fields,
                "fuzziness": "AUTO"
            }
        }
    }

    try:
        response = es_client.search(
            index=ES_INDEX_NAME,
            body=es_query
        )
        
        hits = response["hits"]["hits"]
        results = []
        
        # Collect Titles to fetch posters from Mongo (more reliable than IDs across different imports)
        titles = [hit["_source"].get("title") for hit in hits if hit["_source"].get("title")]
        
        # Fetch posters mapping
        posters_map = {}
        if titles:
            cursor = movies_catalog.find(
                {"title": {"$in": titles}},
                {"title": 1, "poster_url": 1}
            )
            for doc in cursor:
                posters_map[doc["title"]] = doc.get("poster_url")

        for hit in hits:
            src = hit["_source"]
            title = src.get("title")
            
            # Use fetched poster or fallback
            poster = posters_map.get(title) or STOCK_POSTER_URL
            
            # Mappa il risultato ES nel formato CatalogMovie
            movie = {
                 "imdb_id": src.get("imdb_title_id") or src.get("mongo_id"), 
                 "title": src.get("title"),
                 "original_title": src.get("original_title"),
                 "year": src.get("year"),
                 "genres": src.get("genres", []),
                 "poster_url": poster, 
                 "avg_vote": src.get("avg_vote"),
                 "description": src.get("description"),
                 "director": ", ".join(src.get("director", [])),
                 "actors": ", ".join(src.get("actors", [])),
                 "score": hit["_score"]
            }
            results.append(movie)

        return {
            "results": results, 
            "query": req.query, 
            "total": response["hits"]["total"]["value"]
        }

    except Exception as e:
        print(f"❌ ES Search Error: {e}")
        # Fallback alla ricerca MongoDB standard se ES fallisce
        print("⚠️ Fallback to MongoDB search...")
        return await search_catalog(req.query, limit=20)


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


# ============================================
# ADMIN STATS ENDPOINTS (per Grafana Infinity)
# ============================================
@app.get("/admin/stats")
async def get_admin_stats():
    """Statistiche globali per dashboard admin (Grafana Infinity)."""
    # Conteggi principali
    total_users = users_collection.count_documents({})
    total_movies_catalog = movies_catalog.count_documents({})
    total_watched = movies_collection.count_documents({})
    
    # Utenti per provincia
    province_pipeline = [
        {"$group": {"_id": "$province", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    users_by_province = list(users_collection.aggregate(province_pipeline))
    
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
    
    # Rating medio globale
    stats = list(stats_collection.find({}, {"avg_rating": 1}))
    avg_ratings = [s.get("avg_rating", 0) for s in stats if s.get("avg_rating")]
    global_avg_rating = round(sum(avg_ratings) / len(avg_ratings), 2) if avg_ratings else 0
    
    return {
        "total_users": total_users,
        "total_movies_catalog": total_movies_catalog,
        "total_watched": total_watched,
        "global_avg_rating": global_avg_rating,
        "users_by_province": [{"province": p["_id"] or "N/A", "count": p["count"]} for p in users_by_province],
        "top_genres": [{"genre": g["_id"], "count": g["count"]} for g in top_genres],
        "movies_by_decade": [{"decade": d["_id"], "count": d["count"]} for d in by_decade]
    }


@app.get("/admin/stats/users")
async def get_admin_users_stats():
    """Lista utenti per tabella Grafana."""
    users = list(users_collection.find(
        {},
        {"_id": 0, "username": 1, "email": 1, "province": 1, "movies_count": 1, "created_at": 1}
    ).limit(100))
    return users


@app.get("/admin/stats/genres")
async def get_admin_genres_stats():
    """Distribuzione generi per grafico Grafana."""
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 15}
    ]
    genres = list(movies_catalog.aggregate(pipeline))
    return [{"genre": g["_id"], "count": g["count"]} for g in genres]


@app.get("/admin/stats/activity")
async def get_admin_activity_stats():
    """Attività giornaliera (film visti) per time series Grafana."""
    pipeline = [
        {"$match": {"date": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": "$date", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    activity = list(movies_collection.aggregate(pipeline))
    # Grafana Infinity preferisce array di oggetti
    return [{"date": a["_id"], "count": a["count"]} for a in activity]


# ============================================
# QUIZ AI ENDPOINTS
# ============================================
from quiz_generator import (
    get_daily_questions, 
    get_questions_count,
    run_daily_quiz_generation,
    ensure_indexes as ensure_quiz_indexes
)


@app.get("/quiz/questions")
async def get_quiz_questions(n: int = 5):
    """
    Ottiene n domande per il quiz.
    Preferisce domande meno usate e più recenti.
    """
    try:
        questions = get_daily_questions(n)
        
        # Se non ci sono domande, genera al volo
        if not questions:
            return {
                "questions": [],
                "total_available": 0,
                "message": "Nessuna domanda disponibile. Genera nuove domande con /quiz/generate"
            }
        
        # Formatta per il frontend
        formatted = []
        for q in questions:
            formatted.append({
                "id": q.get("movie_id", ""),
                "movie_title": q.get("movie_title", ""),
                "movie_year": q.get("movie_year"),
                "question": q.get("question", ""),
                "answers": q.get("answers", []),
                "explanation": q.get("explanation", ""),
                "category": q.get("category", "plot"),
                "difficulty": q.get("difficulty", "medium")
            })
        
        return {
            "questions": formatted,
            "total_available": get_questions_count()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore recupero domande: {str(e)}")


@app.post("/quiz/generate")
async def generate_quiz_questions(background_tasks: BackgroundTasks, n: int = 5):
    """
    Genera nuove domande quiz usando Ollama.
    Esegue in background per non bloccare la risposta.
    """
    async def generate_task():
        try:
            from quiz_generator import run_daily_quiz_generation
            await run_daily_quiz_generation(force=True)
        except Exception as e:
            print(f"❌ Errore generazione quiz: {e}")
    
    background_tasks.add_task(generate_task)
    
    return {
        "status": "generating",
        "message": f"Generazione di {n} domande avviata in background",
        "current_count": get_questions_count()
    }


@app.get("/quiz/status")
async def get_quiz_generation_status():
    """Ritorna lo stato completo della generazione dei quiz."""
    try:
        status_doc = db.quiz_status.find_one({"_id": "daily_generation"})
        if not status_doc:
            return {
                "status": "FINISHED",
                "last_generated_date": None,
                "needs_generation": True
            }
        
        status = status_doc.get("status", "FINISHED")
        last_date = status_doc.get("last_generated_date")
        today_str = datetime.now(italy_tz).strftime("%Y-%m-%d")
        
        # Frontend può usare needs_generation per decidere se mostrare il pulsante
        needs_generation = (
            status in ["FINISHED", "ERROR", "IDLE"] and 
            last_date != today_str
        )
        
        return {
            "status": status,
            "last_generated_date": last_date,
            "questions_generated": status_doc.get("questions_generated", 0),
            "finished_at": status_doc.get("finished_at"),
            "error_message": status_doc.get("error_message"),
            "needs_generation": needs_generation,
            "today": today_str
        }
    except Exception as e:
        return {"status": "error", "details": str(e)}


@app.get("/quiz/history")
async def get_quiz_history(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene la cronologia quiz dell'utente."""
    # Usa stats_collection che contiene i dati quiz dell'utente
    user_stats = stats_collection.find_one(
        {"user_id": current_user_id},
        {"quiz_correct_count": 1, "quiz_wrong_count": 1, "quiz_total_attempts": 1, "last_quiz_date": 1, "_id": 0}
    )
    
    if not user_stats:
        return {"history": []}
    
    return {"history": [user_stats] if user_stats.get("quiz_total_attempts") else []}


@app.get("/quiz/stats")
async def get_quiz_stats():
    """Statistiche globali del sistema quiz."""
    return {
        "total_questions": get_questions_count(),
        "ollama_model": os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct-q5_K_M"),
        "ollama_url": os.getenv("OLLAMA_URL", "http://ollama:11434")
    }


# Pydantic model for quiz submission
class QuizSubmitRequest(BaseModel):
    correct: int
    wrong: int
    quiz_date: Optional[str] = None

@app.post("/quiz/submit")
async def submit_quiz_results(
    results: QuizSubmitRequest,
    current_user_id: str = Depends(get_current_user_id)
):
    """
    Salva i risultati del quiz nel documento user_stats dell'utente.
    Solo il PRIMO tentativo del giorno viene conteggiato nelle statistiche.
    
    Body: { "correct": 3, "wrong": 2, "quiz_date": "2026-01-12" }
    """
    correct = results.correct
    wrong = results.wrong
    quiz_date = results.quiz_date or datetime.now(italy_tz).strftime("%Y-%m-%d")
    
    # Recupera le stats attuali dell'utente
    user_stats = stats_collection.find_one({"user_id": current_user_id})
    last_quiz_date = user_stats.get("last_quiz_date", "") if user_stats else ""
    
    first_attempt = (last_quiz_date != quiz_date)
    
    if first_attempt:
        # Primo tentativo del giorno: aggiorna i contatori in user_stats
        stats_collection.update_one(
            {"user_id": current_user_id},
            {
                "$inc": {
                    "quiz_correct_count": correct,
                    "quiz_wrong_count": wrong,
                    "quiz_total_attempts": 1
                },
                "$set": {
                    "last_quiz_date": quiz_date
                }
            },
            upsert=True
        )
        print(f"✅ [Quiz] Primo tentativo per {current_user_id}: +{correct} corrette, +{wrong} sbagliate")
    else:
        print(f"🔁 [Quiz] Tentativo ripetuto per {current_user_id} (già completato il {quiz_date})")
    
    # Ritorna le stats aggiornate
    updated_stats = stats_collection.find_one({"user_id": current_user_id})
    
    return {
        "success": True,
        "first_attempt": first_attempt,
        "quiz_correct_count": updated_stats.get("quiz_correct_count", 0) if updated_stats else 0,
        "quiz_wrong_count": updated_stats.get("quiz_wrong_count", 0) if updated_stats else 0,
        "quiz_total_attempts": updated_stats.get("quiz_total_attempts", 0) if updated_stats else 0
    }

