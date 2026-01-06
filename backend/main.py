"""
CineMatch Backend API
Sistema di raccomandazione film con analisi sentiment.
"""
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import os
import random
from datetime import datetime
from pymongo import MongoClient
from pydantic import BaseModel
from typing import Optional
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
# ============================================
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client.cinematch_db

# Collections
users_collection = db.users
movies_collection = db.movies
stats_collection = db.user_stats
sentiment_collection = db.sentiment_history
activity_collection = db.activity_log

# ============================================
# MODELS
# ============================================
class UserAuth(BaseModel):
    username: str
    password: str

class UserRegister(BaseModel):
    username: str
    password: str
    email: Optional[str] = None
    full_name: Optional[str] = None

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
        print("✅ Indici MongoDB creati")
    except Exception as e:
        print(f"⚠️ Indici già esistenti: {e}")
    
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
    
    top_rated = df[df['Rating'] >= 4].nlargest(10, 'Rating')[['Name', 'Year', 'Rating']].to_dict('records')
    
    months = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    if 'Date' in df.columns:
        df_copy = df.copy()
        df_copy['DateParsed'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        df_copy['Month'] = df_copy['DateParsed'].dt.month
        monthly_counts = df_copy.groupby('Month').size().to_dict()
        monthly_data = [{"month": months[i], "films": int(monthly_counts.get(i+1, 0))} for i in range(12)]
        recent = df_copy.nlargest(10, 'DateParsed')[['Name', 'Year', 'Rating', 'Date']].to_dict('records')
    else:
        monthly_data = [{"month": m, "films": 0} for m in months]
        recent = df.head(10)[['Name', 'Year', 'Rating']].to_dict('records')
    
    genre_colors = {
        "Drama": "#E50914", "Comedy": "#FF6B35", "Action": "#00529B",
        "Thriller": "#8B5CF6", "Sci-Fi": "#06B6D4", "Romance": "#EC4899"
    }
    # Distribuzione fissa realistica per i generi
    genre_values = [28, 22, 18, 14, 10, 8]  # Totale = 100
    genre_data = []
    for i, (name, color) in enumerate(genre_colors.items()):
        genre_data.append({"name": name, "value": genre_values[i], "color": color})
    
    return {
        "total_watched": len(movies),
        "avg_rating": round(float(df['Rating'].mean()), 2),
        "rating_distribution": rating_distribution,
        "top_rated_movies": top_rated,
        "recent_movies": recent,
        "monthly_data": monthly_data,
        "genre_data": genre_data,
        "favorite_genre": "Drama",
        "total_5_stars": int(df[df['Rating'] == 5].shape[0]),
        "total_4_stars": int(df[df['Rating'] == 4].shape[0]),
        "total_3_stars": int(df[df['Rating'] == 3].shape[0]),
        "total_2_stars": int(df[df['Rating'] == 2].shape[0]),
        "total_1_stars": int(df[df['Rating'] == 1].shape[0]),
        "watch_time_hours": len(movies) * 2,
        "updated_at": datetime.utcnow().isoformat(),
        "top_years": calculate_top_years(movies)
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
    db_user = users_collection.find_one({"username": user.username})
    
    if not db_user:
        raise HTTPException(status_code=401, detail="Username non trovato")
    
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
    """Ottiene i dati dell'utente corrente."""
    user = users_collection.find_one({"user_id": current_user_id}, {"password": 0, "_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    return user

# ============================================
# DATA ENDPOINTS
# ============================================
@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), current_user_id: str = Depends(get_current_user_id)):
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
    
    # Calcola e salva statistiche
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
    
    return {
        "status": "success",
        "filename": file.filename,
        "count": len(movies),
        "stats": stats,
        "message": f"Caricati {len(movies)} film con successo!"
    }

@app.get("/user-stats")
async def get_user_stats(current_user_id: str = Depends(get_current_user_id)):
    """Ottiene le statistiche dell'utente."""
    stats = stats_collection.find_one({"user_id": current_user_id}, {"_id": 0})
    
    if not stats:
        raise HTTPException(status_code=404, detail="Nessun dato trovato. Carica prima un file CSV.")
    
    # Se top_years non è presente, calcolalo al volo
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

@app.get("/user-movies")
async def get_user_movies(
    current_user_id: str = Depends(get_current_user_id),
    skip: int = 0,
    limit: int = 100
):
    """Ottiene la lista dei film dell'utente."""
    movies = list(movies_collection.find(
        {"user_id": current_user_id},
        {"_id": 0, "user_id": 0}
    ).skip(skip).limit(limit))
    
    total = movies_collection.count_documents({"user_id": current_user_id})
    
    return {
        "movies": movies,
        "total": total,
        "skip": skip,
        "limit": limit
    }

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
