"""
YoutubeComments7 - Calcolo delle medie del sentiment per le collezioni
Questo modulo calcola la media dei valori di sentiment per le collezioni
CommentiLive e CommentiVotati e restituisce i dati per la visualizzazione.
"""
from pymongo import MongoClient
from typing import Dict, Optional
import os

# -----------------------------
# CONFIG
# -----------------------------
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DATABASE = "cinematch_db"

# Collezioni per i commenti
COLLECTION_LIVE = "CommentiLive"
COLLECTION_VOTATI = "CommentiVotati"


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


def calculate_collection_sentiment_average(collection_name: str) -> Dict:
    """
    Calcola la media del valore_sentiment per una collezione specifica.
    
    Args:
        collection_name: Nome della collezione MongoDB
        
    Returns:
        Dizionario con media, conteggio totale, e conteggio con sentiment
    """
    try:
        client = get_mongodb_client()
        if client is None:
            return {
                "average": None,
                "total_comments": 0,
                "comments_with_sentiment": 0,
                "label": "unknown",
                "collection": collection_name
            }
        
        db = client[MONGODB_DATABASE]
        collection = db[collection_name]
        
        # Conta tutti i commenti
        total_comments = collection.count_documents({})
        
        # Trova commenti con valore_sentiment valido (non null)
        comments_with_sentiment = list(collection.find({
            "valore_sentiment": {"$ne": None, "$exists": True}
        }, {"valore_sentiment": 1}))
        
        sentiment_count = len(comments_with_sentiment)
        
        if sentiment_count == 0:
            client.close()
            return {
                "average": None,
                "total_comments": total_comments,
                "comments_with_sentiment": 0,
                "label": "no_data",
                "collection": collection_name
            }
        
        # Calcola la media
        sentiment_sum = sum(c.get("valore_sentiment", 0) for c in comments_with_sentiment)
        average = sentiment_sum / sentiment_count
        
        # Determina l'etichetta
        if average > 0.2:
            label = "positive"
        elif average < -0.2:
            label = "negative"
        else:
            label = "neutral"
        
        client.close()
        
        return {
            "average": round(average, 4),
            "total_comments": total_comments,
            "comments_with_sentiment": sentiment_count,
            "label": label,
            "collection": collection_name
        }
        
    except Exception as e:
        print(f"[YoutubeComments7] Errore calcolo media per {collection_name}: {e}")
        return {
            "average": None,
            "total_comments": 0,
            "comments_with_sentiment": 0,
            "label": "error",
            "collection": collection_name
        }


# -----------------------------
# PUBLIC API
# -----------------------------
def get_sentiment_averages() -> Dict:
    """
    Calcola le medie del sentiment per entrambe le collezioni.
    
    Returns:
        Dizionario con le medie per CommentiLive e CommentiVotati
    """
    live_stats = calculate_collection_sentiment_average(COLLECTION_LIVE)
    votati_stats = calculate_collection_sentiment_average(COLLECTION_VOTATI)
    
    return {
        "status": "success",
        "data": {
            "commenti_live": live_stats,
            "commenti_votati": votati_stats
        }
    }


def get_live_sentiment_average() -> Dict:
    """Restituisce la media del sentiment per CommentiLive."""
    return calculate_collection_sentiment_average(COLLECTION_LIVE)


def get_votati_sentiment_average() -> Dict:
    """Restituisce la media del sentiment per CommentiVotati."""
    return calculate_collection_sentiment_average(COLLECTION_VOTATI)


# -----------------------------
# TEST
# -----------------------------
if __name__ == "__main__":
    print("=== Test YoutubeComments7 - Sentiment Averages ===\n")
    
    result = get_sentiment_averages()
    
    print("Risultati:")
    print("-" * 50)
    
    for key, stats in result["data"].items():
        print(f"\n{key.upper()}:")
        print(f"  Media: {stats['average']}")
        print(f"  Label: {stats['label']}")
        print(f"  Commenti totali: {stats['total_comments']}")
        print(f"  Commenti con sentiment: {stats['comments_with_sentiment']}")
