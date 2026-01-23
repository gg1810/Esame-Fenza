# file che gestisce gli ultimi 5 commenti live con spark structured Streaming
"""
YoutubeComments5 - Gestisce gli ultimi 5 commenti live con Spark Structured Streaming
Questo modulo implementa un sistema di streaming per raccogliere e processare
commenti YouTube in tempo reale usando Apache Spark Structured Streaming.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from googleapiclient.discovery import build
import re
import html
import json
import threading
import time
from typing import List, Dict, Optional
from collections import deque


# -----------------------------
# CONFIG
# -----------------------------
YOUTUBE_API_KEY = "AIzaSyCWgR9xeE3H2arlD_M8twh82WJ8cc2g6WQ"
MAX_COMMENTS = 5
MIN_CHARS = 80
POLLING_INTERVAL = 30  # Secondi tra ogni polling

SPAM_KEYWORDS = ["subscribe"]


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
# COMMENTS BUFFER (Thread-Safe)
# -----------------------------
class CommentsBuffer:
    """
    Buffer thread-safe per memorizzare gli ultimi 5 commenti.
    Usa un deque con maxlen per mantenere solo gli ultimi N commenti.
    """
    
    def __init__(self, max_size: int = 5):
        self._buffer: deque = deque(maxlen=max_size)
        self._lock = threading.RLock()
        self._last_update = None
    
    def add_comment(self, comment: Dict) -> None:
        """Aggiunge un commento al buffer (thread-safe)."""
        with self._lock:
            # Evita duplicati basandosi su author + text
            existing_keys = {(c["author"], c["text"]) for c in self._buffer}
            if (comment["author"], comment["text"]) not in existing_keys:
                self._buffer.append(comment)
                self._last_update = time.time()
    
    def add_comments(self, comments: List[Dict]) -> None:
        """Aggiunge multipli commenti al buffer."""
        for comment in comments:
            self.add_comment(comment)
    
    def get_comments(self) -> List[Dict]:
        """Restituisce tutti i commenti nel buffer (thread-safe)."""
        with self._lock:
            return list(self._buffer)
    
    def get_comment_at(self, index: int) -> Optional[Dict]:
        """Restituisce il commento all'indice specificato."""
        with self._lock:
            if 0 <= index < len(self._buffer):
                return self._buffer[index]
            return None
    
    def count(self) -> int:
        """Restituisce il numero di commenti nel buffer."""
        with self._lock:
            return len(self._buffer)
    
    def clear(self) -> None:
        """Svuota il buffer."""
        with self._lock:
            self._buffer.clear()
    
    def get_last_update(self) -> Optional[float]:
        """Restituisce il timestamp dell'ultimo aggiornamento."""
        with self._lock:
            return self._last_update


# Buffer globale per i commenti live
_comments_buffer = CommentsBuffer(max_size=MAX_COMMENTS)


# -----------------------------
# YOUTUBE COMMENT FETCHER
# -----------------------------
def fetch_latest_comments(youtube_url: str, max_comments: int = 5, min_chars: int = 80) -> List[Dict]:
    """
    Recupera gli ultimi commenti validi da un video YouTube.
    Usato come sorgente per lo streaming.
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
            maxResults=50,
            textFormat="plainText",
            order="time"  # Ordinati per tempo (più recenti prima)
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
                    "text": cleaned,
                    "comment_id": item["id"]
                }

                collected.append(comment_data)

                if len(collected) >= max_comments:
                    break

            if len(collected) >= max_comments:
                break

            request = youtube.commentThreads().list_next(request, response)

        return collected

    except Exception as e:
        print(f"Errore recupero commenti YouTube: {e}")
        return []


# -----------------------------
# SPARK STRUCTURED STREAMING
# -----------------------------
class YouTubeCommentsStreaming:
    """
    Classe che gestisce lo streaming dei commenti YouTube usando
    Spark Structured Streaming per il processing in tempo reale.
    """
    
    def __init__(self, app_name: str = "YouTubeCommentsStreaming"):
        self._spark: Optional[SparkSession] = None
        self._streaming_query = None
        self._is_running = False
        self._polling_thread: Optional[threading.Thread] = None
        self._current_trailer_url: Optional[str] = None
        self._app_name = app_name
        self._stop_event = threading.Event()
    
    def _init_spark(self) -> SparkSession:
        """Inizializza la sessione Spark."""
        if self._spark is None:
            self._spark = SparkSession.builder \
                .appName(self._app_name) \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/youtube_comments_checkpoint") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .getOrCreate()
            
            # Imposta log level
            self._spark.sparkContext.setLogLevel("WARN")
        
        return self._spark
    
    def _get_schema(self) -> StructType:
        """Schema per i commenti YouTube."""
        return StructType([
            StructField("comment_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("text", StringType(), True),
            StructField("processed_at", TimestampType(), True)
        ])
    
    def _polling_worker(self, trailer_url: str) -> None:
        """
        Worker thread che effettua il polling periodico dei commenti YouTube
        e li processa attraverso Spark Structured Streaming.
        """
        global _comments_buffer
        
        print(f"[YouTubeCommentsStreaming] Avvio polling per: {trailer_url}")
        
        while not self._stop_event.is_set():
            try:
                # Recupera gli ultimi commenti
                comments = fetch_latest_comments(
                    trailer_url, 
                    max_comments=MAX_COMMENTS, 
                    min_chars=MIN_CHARS
                )
                
                if comments:
                    # Aggiorna il buffer con i nuovi commenti
                    _comments_buffer.clear()  # Svuota e riempi con i più recenti
                    _comments_buffer.add_comments(comments)
                    
                    print(f"[YouTubeCommentsStreaming] Buffer aggiornato con {len(comments)} commenti")
                    
                    # Processa con Spark se disponibile
                    if self._spark is not None:
                        self._process_with_spark(comments)
                
            except Exception as e:
                print(f"[YouTubeCommentsStreaming] Errore polling: {e}")
            
            # Attendi prima del prossimo polling
            self._stop_event.wait(POLLING_INTERVAL)
        
        print("[YouTubeCommentsStreaming] Polling terminato")
    
    def _process_with_spark(self, comments: List[Dict]) -> None:
        """
        Processa i commenti usando Spark DataFrame per analisi batch.
        In un setup completo, questo si integrerebbe con un sistema di streaming
        come Kafka per lo streaming vero e proprio.
        """
        try:
            from pyspark.sql import Row
            from datetime import datetime
            
            # Crea RDD e DataFrame dai commenti
            rows = [
                Row(
                    comment_id=c.get("comment_id", ""),
                    author=c["author"],
                    published_at=c["published_at"],
                    text=c["text"],
                    processed_at=datetime.now()
                )
                for c in comments
            ]
            
            if rows:
                df = self._spark.createDataFrame(rows)
                
                # Esempio di elaborazione: ordinamento per tempo
                df_sorted = df.orderBy(desc("published_at"))
                
                # Log del processamento
                print(f"[Spark] Processati {df_sorted.count()} commenti")
                
        except Exception as e:
            print(f"[Spark] Errore processing: {e}")
    
    def start_streaming(self, trailer_url: str) -> bool:
        """
        Avvia lo streaming dei commenti per un trailer specifico.
        
        Args:
            trailer_url: URL del trailer YouTube da monitorare
            
        Returns:
            True se lo streaming è stato avviato con successo
        """
        if self._is_running:
            if self._current_trailer_url == trailer_url:
                print("[YouTubeCommentsStreaming] Streaming già attivo per questo trailer")
                return True
            else:
                self.stop_streaming()
        
        try:
            # Inizializza Spark
            self._init_spark()
            
            # Resetta lo stop event
            self._stop_event.clear()
            
            # Avvia il thread di polling
            self._current_trailer_url = trailer_url
            self._polling_thread = threading.Thread(
                target=self._polling_worker,
                args=(trailer_url,),
                daemon=True
            )
            self._polling_thread.start()
            
            self._is_running = True
            print(f"[YouTubeCommentsStreaming] Streaming avviato per: {trailer_url}")
            
            return True
            
        except Exception as e:
            print(f"[YouTubeCommentsStreaming] Errore avvio: {e}")
            return False
    
    def stop_streaming(self) -> None:
        """Ferma lo streaming corrente."""
        if not self._is_running:
            return
        
        self._stop_event.set()
        
        if self._polling_thread and self._polling_thread.is_alive():
            self._polling_thread.join(timeout=5)
        
        self._is_running = False
        self._current_trailer_url = None
        print("[YouTubeCommentsStreaming] Streaming fermato")
    
    def is_running(self) -> bool:
        """Verifica se lo streaming è attivo."""
        return self._is_running
    
    def shutdown(self) -> None:
        """Arresta completamente il sistema, inclusa la sessione Spark."""
        self.stop_streaming()
        
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
        
        print("[YouTubeCommentsStreaming] Sistema arrestato")


# Istanza globale del streaming manager
_streaming_manager: Optional[YouTubeCommentsStreaming] = None


def get_streaming_manager() -> YouTubeCommentsStreaming:
    """Ottiene l'istanza singleton del manager streaming."""
    global _streaming_manager
    if _streaming_manager is None:
        _streaming_manager = YouTubeCommentsStreaming()
    return _streaming_manager


# -----------------------------
# PUBLIC API
# -----------------------------
def start_live_comments_streaming(trailer_url: str) -> bool:
    """
    Avvia lo streaming dei commenti live per un trailer.
    
    Args:
        trailer_url: URL del trailer YouTube
        
    Returns:
        True se avviato con successo
    """
    if not trailer_url:
        return False
    
    manager = get_streaming_manager()
    return manager.start_streaming(trailer_url)


def stop_live_comments_streaming() -> None:
    """Ferma lo streaming dei commenti live."""
    manager = get_streaming_manager()
    manager.stop_streaming()


def get_live_comments() -> List[Dict]:
    """
    Ottiene gli ultimi 5 commenti live dal buffer.
    
    Returns:
        Lista di commenti (max 5)
    """
    global _comments_buffer
    return _comments_buffer.get_comments()


def get_live_comment_at(index: int) -> Optional[Dict]:
    """
    Ottiene un commento specifico dal buffer live.
    
    Args:
        index: Indice del commento (0-4)
        
    Returns:
        Dizionario con il commento o None se non esiste
    """
    global _comments_buffer
    return _comments_buffer.get_comment_at(index)


def get_live_comments_count() -> int:
    """
    Ottiene il numero di commenti attualmente nel buffer.
    
    Returns:
        Numero di commenti
    """
    global _comments_buffer
    return _comments_buffer.count()


def get_live_comments_for_trailer(trailer_url: str) -> Dict:
    """
    Wrapper principale per ottenere i commenti live da un trailer.
    Avvia lo streaming se non già attivo e restituisce il buffer corrente.
    
    Args:
        trailer_url: URL del trailer YouTube
        
    Returns:
        Dizionario con status, comments, count, e streaming_active
    """
    if not trailer_url:
        return {
            "status": "error",
            "message": "URL trailer non valido",
            "comments": [],
            "count": 0,
            "streaming_active": False
        }
    
    manager = get_streaming_manager()
    
    # Avvia streaming se non attivo
    if not manager.is_running():
        # Prima chiamata: fetch sincrono immediato + avvia streaming
        comments = fetch_latest_comments(trailer_url, max_comments=MAX_COMMENTS, min_chars=MIN_CHARS)
        _comments_buffer.clear()
        _comments_buffer.add_comments(comments)
        
        # Avvia streaming in background
        manager.start_streaming(trailer_url)
    
    # Restituisci il buffer corrente
    comments = get_live_comments()
    
    return {
        "status": "success",
        "comments": comments,
        "count": len(comments),
        "streaming_active": manager.is_running()
    }


# -----------------------------
# TEST
# -----------------------------
if __name__ == "__main__":
    # Test del modulo
    TEST_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Esempio
    
    print("=== Test YoutubeComments5 con Spark Structured Streaming ===")
    
    # Test fetch diretto
    print("\n1. Test fetch commenti:")
    comments = fetch_latest_comments(TEST_URL, max_comments=3, min_chars=10)
    for i, c in enumerate(comments):
        print(f"  {i+1}. {c['author']}: {c['text'][:50]}...")
    
    # Test streaming
    print("\n2. Test streaming:")
    result = get_live_comments_for_trailer(TEST_URL)
    print(f"  Streaming attivo: {result['streaming_active']}")
    print(f"  Commenti nel buffer: {result['count']}")
    
    # Attendi un po' per vedere il polling
    time.sleep(5)
    
    # Stop streaming
    stop_live_comments_streaming()
    print("\n3. Streaming fermato")
