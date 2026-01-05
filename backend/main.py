from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Depends, HTTPException, status
import pandas as pd
import io
import os
import random
from datetime import datetime
from pymongo import MongoClient
from sentiment_analyzer import SentimentAnalyzer
from auth import get_password_hash, verify_password, create_access_token, get_current_user_id
from pydantic import BaseModel

app = FastAPI()


# Inizializza l'analizzatore
analyzer = SentimentAnalyzer()

# Connessione MongoDB (URL preso dalle variabili d'ambiente di Docker)
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client.cinematch_db
users_collection = db.users

# Modelli per la validazione dati
class UserAuth(BaseModel):
    username: str
    password: str

@app.get("/")
def read_root():
    return {"message": "CineMatch API is running", "model": "RoBERTa Loaded"}

@app.post("/register")
async def register(user: UserAuth):
    if users_collection.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username già esistente")
    
    hashed_password = get_password_hash(user.password)
    users_collection.insert_one({
        "username": user.username,
        "password": hashed_password,
        "user_id": user.username, # Per ora usiamo lo username come ID
        "stats": None
    })
    return {"message": "Utente registrato con successo"}

@app.post("/login")
async def login(user: UserAuth):
    db_user = users_collection.find_one({"username": user.username})
    if not db_user or not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Credenziali non valide")
    
    access_token = create_access_token(data={"sub": db_user["user_id"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/analyze-movie-sentiment/{title}")
async def get_sentiment(title: str, current_user_id: str = Depends(get_current_user_id)):
    # Simulazione recupero post da Reddit
    mock_comments = [
        f"I absolutely loved {title}! The visuals were stunning.",
        f"The pacing of {title} was a bit slow, but overall good.",
        f"Didn't like the ending of {title} at all, very disappointing.",
        f"{title} is a masterpiece of modern cinema.",
        f"Typical blockbuster garbage, don't waste your time on {title}."
    ]
    
    score = analyzer.get_aggregate_sentiment(mock_comments)
    result = {
        "movie": title,
        "sentiment_score": round(score, 2),
        "timestamp": datetime.utcnow().isoformat(),
        "type": "reddit_sentiment"
    }

    # Salva il risultato nella history dell'utente
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$push": {"history": result}}
    )
    
    return {
        "result": result,
        "sample_comments": len(mock_comments),
        "status": "success"
    }

@app.get("/user-history")
async def get_user_history(current_user_id: str = Depends(get_current_user_id)):
    user = users_collection.find_one({"user_id": current_user_id})
    if not user or "history" not in user:
        return {"history": []}
    return {"history": user["history"]}


@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), current_user_id: str = Depends(get_current_user_id)):
    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))
    
    # Pulizia dati minima
    movies = df[['Name', 'Year', 'Rating']].dropna().to_dict('records')
    
    # Calcola statistiche avanzate
    months = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    mock_monthly = [{"month": m, "films": random.randint(5, 25)} for m in months]
    
    mock_genres = [
        {"name": "Drama", "value": 35, "color": "#E50914"},
        {"name": "Thriller", "value": 20, "color": "#FF6B35"},
        {"name": "Sci-Fi", "value": 15, "color": "#00529B"},
        {"name": "Action", "value": 30, "color": "#C5A572"}
    ]

    # Salva su MongoDB collegato all'utente
    user_data = {
        "filename": file.filename,
        "movies": movies,
        "stats": {
            "total_watched": len(movies),
            "avg_rating": round(float(df['Rating'].mean()), 1),
            "monthly_data": mock_monthly,
            "genre_data": mock_genres,
            "favorite_genre": "Drama"
        }
    }
    
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$set": {"user_data": user_data, "stats": user_data["stats"]}},
        upsert=True
    )
    
    return {
        "status": "success",
        "filename": file.filename,
        "count": len(movies),
        "message": "Dati salvati correttamente nel tuo profilo"
    }

@app.get("/user-stats")
async def get_user_stats(current_user_id: str = Depends(get_current_user_id)):
    user = users_collection.find_one({"user_id": current_user_id})
    if not user or not user.get("stats"):
        return {"error": "Nessun dato trovato per questo utente"}
    return user["stats"]

@app.post("/log-activity")
async def log_activity(activity: dict, current_user_id: str = Depends(get_current_user_id)):
    """
    Salva genericamente qualsiasi attività (query, raccomandazione, filtro)
    """
    activity["timestamp"] = datetime.utcnow().isoformat()
    users_collection.update_one(
        {"user_id": current_user_id},
        {"$push": {"activity_log": activity}}
    )
    return {"status": "success", "message": "Attività registrata"}
