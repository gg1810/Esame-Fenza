"""
Kafka Producer per eventi film utente.
Pubblica eventi quando un utente aggiunge, modifica o elimina un film.
"""
import os
import json
import logging
import pytz
from datetime import datetime
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class MovieEventProducer:
    """
    Producer Kafka per eventi film utente.
    Gestisce la pubblicazione asincrona di eventi sul topic 'user-movie-events'.
    """
    
    TOPIC = "user-movie-events"
    
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer: Optional[KafkaProducer] = None
        self._is_connected = False
    
    def _get_producer(self) -> Optional[KafkaProducer]:
        """Lazy initialization del producer con retry."""
        if self._producer is not None and self._is_connected:
            return self._producer
        
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Garantisce consegna
                retries=3,
                retry_backoff_ms=500,
                request_timeout_ms=10000,
                max_block_ms=10000  # Non bloccare troppo se Kafka è down
            )
            self._is_connected = True
            logger.info(f"✅ Kafka producer connesso a {self.bootstrap_servers}")
            return self._producer
        except KafkaError as e:
            logger.warning(f"⚠️ Kafka non disponibile: {e}. Eventi non verranno pubblicati.")
            self._is_connected = False
            return None
    
    def send_movie_event(self, event_type: str, user_id: str, movie_data: dict) -> bool:
        """
        Pubblica un evento film su Kafka.
        
        Args:
            event_type: Tipo evento (ADD, UPDATE, DELETE)
            user_id: ID utente
            movie_data: Dati del film
            
        Returns:
            True se l'evento è stato pubblicato, False altrimenti
        """
        producer = self._get_producer()
        if not producer:
            logger.debug(f"Kafka non disponibile, evento {event_type} non pubblicato")
            return False
        
        event = {
            "event_type": event_type,
            "user_id": user_id,
            "movie": {
                "name": movie_data.get("name"),
                "year": movie_data.get("year"),
                "rating": movie_data.get("rating"),
                "imdb_id": movie_data.get("imdb_id"),
                "genres": movie_data.get("genres", []),
                "duration": movie_data.get("duration"),
                "director": movie_data.get("director"),
                "actors": movie_data.get("actors"),
                "date": movie_data.get("date"),  # Data visione (importante per stats mensili)
                "old_rating": movie_data.get("old_rating"),  # Per UPDATE_RATING
                "new_rating": movie_data.get("new_rating"),  # Per UPDATE_RATING
            },
            "timestamp": datetime.now(pytz.timezone('Europe/Rome')).isoformat()
        }
        
        try:
            future = producer.send(
                self.TOPIC, 
                key=user_id,  # Partizionamento per user_id
                value=event
            )
            # Attendi conferma (con timeout)
            future.get(timeout=5)
            logger.debug(f"📤 Evento {event_type} pubblicato per user {user_id}")
            return True
        except KafkaError as e:
            logger.error(f"❌ Errore pubblicazione evento: {e}")
            self._is_connected = False
            return False
    
    def send_batch_event(self, event_type: str, user_id: str, movies: list) -> bool:
        """
        Pubblica eventi singoli per ogni film nel batch.
        NOTA: Invia eventi nello STESSO FORMATO di send_movie_event per garantire
        che il processore Spark incrementale O(1) possa gestirli uniformemente.
        """
        producer = self._get_producer()
        if not producer:
            return False
        
        success_count = 0
        
        for movie in movies:
            # Usa lo stesso formato di send_movie_event
            event = {
                "event_type": event_type,  # Es: "ADD", "BULK_IMPORT", "RECALCULATE"
                "user_id": user_id,
                "movie": {
                    "name": movie.get("name"),
                    "year": movie.get("year"),
                    "rating": movie.get("rating"),
                    "imdb_id": movie.get("imdb_id"),
                    "genres": movie.get("genres", []),
                    "duration": movie.get("duration"),
                    "director": movie.get("director"),
                    "actors": movie.get("actors"),
                    "date": movie.get("date"),
                },
                "timestamp": datetime.now(pytz.timezone('Europe/Rome')).isoformat()
            }
            
            try:
                producer.send(self.TOPIC, key=user_id, value=event)
                success_count += 1
            except KafkaError as e:
                logger.error(f"❌ Errore invio evento batch: {e}")
        
        # Flush per assicurarsi che tutti gli eventi siano inviati
        producer.flush()
        
        logger.info(f"📤 Batch: inviati {success_count}/{len(movies)} eventi per user {user_id}")
        return success_count > 0
    
    def flush(self):
        """Forza l'invio di tutti i messaggi in coda."""
        if self._producer:
            self._producer.flush()
    
    def close(self):
        """Chiude il producer."""
        if self._producer:
            self._producer.close()
            self._producer = None
            self._is_connected = False


# Singleton instance
_producer_instance: Optional[MovieEventProducer] = None


def get_kafka_producer() -> MovieEventProducer:
    """Restituisce l'istanza singleton del producer."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = MovieEventProducer()
    return _producer_instance
