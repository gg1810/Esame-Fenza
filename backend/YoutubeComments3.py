"""
YoutubeComments3 - Ottiene multipli commenti puliti da un video YouTube
Con salvataggio automatico in MongoDB e calcolo del sentiment.
"""
from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime
import re
import html
import os
import hashlib

# Import del modulo per il calcolo del sentiment
try:
    from YoutubeComments6 import process_pending_comments
    SENTIMENT_ENABLED = True
except ImportError:
    print("[YoutubeComments3] Modulo YoutubeComments6 non disponibile, sentiment disabilitato")
    SENTIMENT_ENABLED = False

# -----------------------------
# CONFIG
# -----------------------------
YOUTUBE_API_KEY = "AIzaSyCWgR9xeE3H2arlD_M8twh82WJ8cc2g6WQ"
MAX_COMMENTS = 5
MIN_CHARS = 80

SPAM_KEYWORDS = ["subscribe"]

# MongoDB Configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DATABASE = "cinematch_db"
MONGODB_COLLECTION = "CommentiVotati"


# -----------------------------
# UTILITY
# -----------------------------
def extract_video_id(youtube_url: str) -> str:
    """Estrae il video ID da un URL YouTube."""
    if "v=" in youtube_url:
        return youtube_url.split("v=")[-1].split("&")[0]
    return youtube_url


def clean_comment(text: str) -> str:
    """Pulisce il testo del commento."""
    text = html.unescape(text)

    # rimuove HTML
    text = re.sub(r"<.*?>", "", text)

    # rimuove URL
    text = re.sub(r"http\S+", "", text)

    # rimuove timestamp tipo 1:23
    text = re.sub(r"\b\d{1,2}:\d{2}\b", "", text)

    # rimuove emoji / caratteri strani (soft)
    text = re.sub(r"[^\w\s.,!?'\"]", "", text)

    # spazi multipli
    text = re.sub(r"\s+", " ", text).strip()

    return text


def is_spam(text: str) -> bool:
    """Controlla se il commento è spam."""
    lower = text.lower()

    # keyword spam
    for k in SPAM_KEYWORDS:
        if k in lower:
            return True

    # troppi link
    if text.count("http") > 0:
        return True

    # tutto maiuscolo
    if text.isupper():
        return True

    # troppe ripetizioni
    words = text.split()
    if len(words) > 10 and len(set(words)) / len(words) < 0.4:
        return True

    return False


# -----------------------------
# MONGODB
# -----------------------------
def get_mongodb_client():
    """Ottiene il client MongoDB."""
    try:
        client = MongoClient(MONGODB_URL)
        return client
    except Exception as e:
        print(f"[MongoDB] Errore connessione: {e}")
        return None




def save_comments_to_mongodb(comments: list) -> bool:
    """
    Salva i commenti nella collezione CommentiYoutube di MongoDB.
    """
    try:
        client = get_mongodb_client()
        if client is None:
            return False
        
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        for c in comments:
            # Genera un ID unico basato sul contenuto del commento per evitare duplicati
            text = c.get("text", "")
            author = c.get("author", "unknown")
            # Usa hash MD5 del testo per ID univoco e stabile
            text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
            comment_id = f"yt3_{author}_{text_hash}"
            
            doc = {
                "_id": comment_id,
                "utente_commento": author,
                "data_ora": c.get("published_at", datetime.now().isoformat()),
                "valore_sentiment": None,  # Sarà calcolato automaticamente
                "commento": text
            }
            
            # Usa upsert per evitare duplicati (basato su _id)
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": doc},
                upsert=True
            )
        
        print(f"[MongoDB] Salvati {len(comments)} commenti dalla sezione 'Commenti Analizzati'")
        client.close()
        return True
        
    except Exception as e:
        print(f"[MongoDB] Errore salvataggio commenti: {e}")
        return False


# -----------------------------
# COMMENTI YOUTUBE
# -----------------------------
def get_multiple_comments(youtube_url: str, max_comments: int = 5, min_chars: int = 5) -> list:
    """
    Ottiene multipli commenti validi da un video YouTube.
    Restituisce una lista di dizionari con author, published_at, text.
    """
    video_id = extract_video_id(youtube_url)

    try:
        youtube = build(
            "youtube",
            "v3",
            developerKey=YOUTUBE_API_KEY
        )

        collected = []

        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText",
            order="relevance"
        )

        while request and len(collected) < max_comments:
            response = request.execute()

            for item in response.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                raw = snippet["textDisplay"]
                cleaned = clean_comment(raw)

                if len(cleaned) < min_chars:
                    continue

                if is_spam(cleaned):
                    continue

                comment_data = {
                    "author": snippet["authorDisplayName"],
                    "published_at": snippet["publishedAt"],
                    "text": cleaned
                }

                collected.append(comment_data)

                if len(collected) >= max_comments:
                    break

            if len(collected) >= max_comments:
                break

            request = youtube.commentThreads().list_next(request, response)

        # Salva i commenti in MongoDB e calcola il sentiment
        if collected:
            save_comments_to_mongodb(collected)
            
            # Calcola automaticamente il sentiment
            if SENTIMENT_ENABLED:
                try:
                    process_pending_comments()
                except Exception as e:
                    print(f"[YoutubeComments3] Errore calcolo sentiment: {e}")
            
            # Recupera i valori di sentiment da MongoDB e aggiungili ai commenti
            try:
                client = get_mongodb_client()
                if client:
                    db = client[MONGODB_DATABASE]
                    collection = db[MONGODB_COLLECTION]
                    
                    for comment in collected:
                        text = comment.get("text", "")
                        author = comment.get("author", "unknown")
                        text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
                        comment_id = f"yt3_{author}_{text_hash}"
                        
                        doc = collection.find_one({"_id": comment_id})
                        if doc:
                            comment["valore_sentiment"] = doc.get("valore_sentiment")
                    
                    client.close()
            except Exception as e:
                print(f"[YoutubeComments3] Errore recupero sentiment: {e}")

        return collected

    except Exception as e:
        print(f"Errore recupero commenti YouTube: {e}")
        return []


def get_trailer_comments(trailer_url: str, max_comments: int = 5) -> list:
    """
    Wrapper per ottenere multipli commenti da un trailer.
    Restituisce lista vuota se il trailer_url è None o non valido.
    """
    if not trailer_url:
        return []
    
    return get_multiple_comments(trailer_url, max_comments=max_comments, min_chars=MIN_CHARS)

