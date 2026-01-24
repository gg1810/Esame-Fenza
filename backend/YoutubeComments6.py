# Codice che implementa il calcolo della sentiment
"""
YoutubeComments6 - Analisi del sentiment dei commenti YouTube usando roBERTa
Questo modulo accede alla collezione MongoDB CommentiYoutube e calcola il valore
di sentiment per ogni commento usando il modello cardiffnlp/twitter-roberta-base-sentiment-latest.
Il valore calcolato viene inserito nel campo "valore_sentiment".
"""
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
from pymongo import MongoClient
import numpy as np
from scipy.special import softmax
from typing import Dict, List, Optional
import threading
import os


# -----------------------------
# CONFIG
# -----------------------------
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

# MongoDB Configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DATABASE = "cinematch_db"
# Collezioni separate per i commenti
MONGODB_COLLECTIONS = ["CommentiLive", "CommentiVotati"]

# Mapping delle label del modello
LABEL_MAPPING = {
    0: "negative",
    1: "neutral",
    2: "positive"
}

# Valori numerici per il sentiment (usati per valore_sentiment)
# -1 = negativo, 0 = neutro, 1 = positivo
# Il valore effettivo sarà il punteggio ponderato
SENTIMENT_VALUES = {
    "negative": -1.0,
    "neutral": 0.0,
    "positive": 1.0
}


# -----------------------------
# SENTIMENT ANALYZER (Singleton)
# -----------------------------
class SentimentAnalyzer:
    """
    Analizzatore di sentiment singleton che carica il modello roBERTa
    una sola volta e lo riutilizza per tutte le inferenze.
    """
    
    _instance: Optional['SentimentAnalyzer'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        print(f"[SentimentAnalyzer] Caricamento modello {MODEL_NAME}...")
        
        try:
            # Carica tokenizer e modello
            self._tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
            self._config = AutoConfig.from_pretrained(MODEL_NAME)
            self._model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
            
            self._initialized = True
            print(f"[SentimentAnalyzer] Modello caricato con successo!")
            
        except Exception as e:
            print(f"[SentimentAnalyzer] Errore caricamento modello: {e}")
            self._initialized = False
            raise
    
    def _preprocess(self, text: str) -> str:
        """Preprocessa il testo per il modello roBERTa."""
        new_text = []
        for t in text.split(" "):
            t = '@user' if t.startswith('@') and len(t) > 1 else t
            t = 'http' if t.startswith('http') else t
            new_text.append(t)
        return " ".join(new_text)
    
    def analyze(self, text: str) -> float:
        """
        Analizza il sentiment di un testo e restituisce un valore float.
        
        Il valore è calcolato come: (score_positivo - score_negativo)
        Range: da -1.0 (molto negativo) a +1.0 (molto positivo)
        
        Args:
            text: Il testo da analizzare
            
        Returns:
            Float tra -1.0 e 1.0 che rappresenta il sentiment
        """
        if not self._initialized:
            return 0.0
        
        try:
            # Preprocessa il testo
            processed_text = self._preprocess(text)
            
            # Tokenizza
            encoded_input = self._tokenizer(
                processed_text,
                return_tensors='pt',
                truncation=True,
                max_length=512
            )
            
            # Inferenza
            output = self._model(**encoded_input)
            scores = output[0][0].detach().numpy()
            scores = softmax(scores)
            
            # Calcola il valore di sentiment come differenza tra positivo e negativo
            # scores[0] = negative, scores[1] = neutral, scores[2] = positive
            negative_score = float(scores[0])
            positive_score = float(scores[2])
            
            # Il valore finale è la differenza: positivo - negativo
            # Questo dà un range da -1 a +1
            sentiment_value = positive_score - negative_score
            
            return round(sentiment_value, 4)
            
        except Exception as e:
            print(f"[SentimentAnalyzer] Errore analisi: {e}")
            return 0.0
    
    def is_ready(self) -> bool:
        """Verifica se l'analizzatore è pronto."""
        return self._initialized


# Istanza globale (lazy loading)
_analyzer: Optional[SentimentAnalyzer] = None
_analyzer_lock = threading.Lock()


def get_sentiment_analyzer() -> SentimentAnalyzer:
    """Ottiene l'istanza singleton dell'analizzatore di sentiment."""
    global _analyzer
    if _analyzer is None:
        with _analyzer_lock:
            if _analyzer is None:
                _analyzer = SentimentAnalyzer()
    return _analyzer


# -----------------------------
# MONGODB FUNCTIONS
# -----------------------------
def get_mongodb_client():
    """Ottiene il client MongoDB."""
    try:
        client = MongoClient(MONGODB_URL)
        return client
    except Exception as e:
        print(f"[MongoDB] Errore connessione: {e}")
        return None


def get_comments_without_sentiment() -> List[Dict]:
    """
    Recupera i commenti che non hanno ancora un valore di sentiment
    da tutte le collezioni configurate.
    
    Returns:
        Lista di commenti senza sentiment con informazione sulla collezione
    """
    try:
        client = get_mongodb_client()
        if client is None:
            return []
        
        db = client[MONGODB_DATABASE]
        all_comments = []
        
        for collection_name in MONGODB_COLLECTIONS:
            collection = db[collection_name]
            
            # Trova commenti dove valore_sentiment è null o non esiste
            comments = list(collection.find({
                "$or": [
                    {"valore_sentiment": None},
                    {"valore_sentiment": {"$exists": False}}
                ]
            }))
            
            # Aggiungi informazione sulla collezione di origine
            for comment in comments:
                comment["_collection"] = collection_name
            
            all_comments.extend(comments)
        
        client.close()
        return all_comments
        
    except Exception as e:
        print(f"[MongoDB] Errore recupero commenti: {e}")
        return []


def update_comment_sentiment(comment_id: str, sentiment_value: float, collection_name: str = None) -> bool:
    """
    Aggiorna il valore di sentiment per un commento.
    
    Args:
        comment_id: ID del commento
        sentiment_value: Valore di sentiment calcolato (float)
        collection_name: Nome della collezione (se None, cerca in tutte)
        
    Returns:
        True se aggiornato con successo
    """
    try:
        client = get_mongodb_client()
        if client is None:
            return False
        
        db = client[MONGODB_DATABASE]
        
        # Se specificata la collezione, usa quella
        collections_to_check = [collection_name] if collection_name else MONGODB_COLLECTIONS
        
        for coll_name in collections_to_check:
            collection = db[coll_name]
            result = collection.update_one(
                {"_id": comment_id},
                {"$set": {"valore_sentiment": sentiment_value}}
            )
            if result.modified_count > 0:
                client.close()
                return True
        
        client.close()
        return False
        
    except Exception as e:
        print(f"[MongoDB] Errore aggiornamento sentiment: {e}")
        return False


def get_all_comments() -> List[Dict]:
    """
    Recupera tutti i commenti da tutte le collezioni.
    
    Returns:
        Lista di tutti i commenti
    """
    try:
        client = get_mongodb_client()
        if client is None:
            return []
        
        db = client[MONGODB_DATABASE]
        all_comments = []
        
        for collection_name in MONGODB_COLLECTIONS:
            collection = db[collection_name]
            comments = list(collection.find({}))
            
            # Aggiungi informazione sulla collezione di origine
            for comment in comments:
                comment["_collection"] = collection_name
            
            all_comments.extend(comments)
        
        client.close()
        return all_comments
        
    except Exception as e:
        print(f"[MongoDB] Errore recupero commenti: {e}")
        return []


# -----------------------------
# PUBLIC API
# -----------------------------
def process_pending_comments() -> int:
    """
    Processa tutti i commenti che non hanno ancora un valore di sentiment.
    
    Returns:
        Numero di commenti processati
    """
    print("[YoutubeComments6] Avvio elaborazione commenti senza sentiment...")
    
    # Recupera commenti senza sentiment
    comments = get_comments_without_sentiment()
    
    if not comments:
        print("[YoutubeComments6] Nessun commento da processare")
        return 0
    
    print(f"[YoutubeComments6] Trovati {len(comments)} commenti da analizzare")
    
    # Ottieni l'analizzatore
    analyzer = get_sentiment_analyzer()
    
    processed = 0
    for comment in comments:
        comment_id = comment.get("_id")
        text = comment.get("commento", "")
        
        if not text:
            continue
        
        # Calcola il sentiment
        sentiment_value = analyzer.analyze(text)
        
        # Aggiorna nel database (usa la collezione di origine se disponibile)
        collection_name = comment.get("_collection")
        if update_comment_sentiment(comment_id, sentiment_value, collection_name):
            processed += 1
            print(f"[YoutubeComments6] Commento {comment_id}: sentiment = {sentiment_value}")
    
    print(f"[YoutubeComments6] Completato: {processed}/{len(comments)} commenti processati")
    return processed


def analyze_comment(text: str) -> float:
    """
    Analizza il sentiment di un singolo testo.
    
    Args:
        text: Testo da analizzare
        
    Returns:
        Valore di sentiment (float tra -1.0 e 1.0)
    """
    analyzer = get_sentiment_analyzer()
    return analyzer.analyze(text)


def get_comments_with_sentiment() -> List[Dict]:
    """
    Recupera tutti i commenti con i loro valori di sentiment.
    
    Returns:
        Lista di commenti con sentiment
    """
    return get_all_comments()


# -----------------------------
# TEST
# -----------------------------
if __name__ == "__main__":
    print("=== Test YoutubeComments6 - Sentiment Analysis con roBERTa ===\n")
    
    # Test singolo
    test_texts = [
        "This movie looks absolutely amazing! Can't wait to see it!",
        "I'm not sure about this, it looks okay I guess.",
        "This is the worst trailer I've ever seen. So disappointing.",
    ]
    
    print("Test analisi singoli commenti:")
    print("-" * 50)
    
    for text in test_texts:
        result = analyze_comment(text)
        sentiment_label = "POSITIVE" if result > 0.2 else "NEGATIVE" if result < -0.2 else "NEUTRAL"
        print(f"Testo: {text[:50]}...")
        print(f"  → Valore: {result:.4f} ({sentiment_label})")
        print()
    
    # Processa commenti pendenti da MongoDB
    print("\nProcessamento commenti da MongoDB:")
    print("-" * 50)
    processed = process_pending_comments()
    print(f"\nCommenti processati: {processed}")
