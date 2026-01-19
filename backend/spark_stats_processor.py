"""
Spark Structured Streaming application per elaborazione eventi film utente.
Consuma eventi da Kafka e aggiorna statistiche pre-calcolate in MongoDB.
"""
import os
import json
import logging
import pytz
from datetime import datetime
from collections import Counter, defaultdict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, sum as spark_sum,
    collect_list, struct, lit, current_timestamp, explode,
    first, when, size
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    ArrayType, TimestampType, DoubleType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurazione
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
TOPIC = "user-movie-events"


def normalize_title(text: str) -> str:
    """Rimuove accenti e caratteri speciali per matching/ricerca."""
    import unicodedata
    import re
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

# Schema eventi Kafka
movie_schema = StructType([
    StructField("name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("duration", IntegerType(), True),
    StructField("director", StringType(), True),
    StructField("actors", StringType(), True),
    StructField("date", StringType(), True)
])

event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("movie", movie_schema, True),
    StructField("timestamp", StringType(), True)
])


def create_spark_session():
    """Crea la sessione Spark con connettori Kafka e MongoDB."""
    return SparkSession.builder \
        .appName("CineMatch-StatsProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.mongodb.write.connection.uri", f"{MONGODB_URL}/cinematch_db") \
        .config("spark.mongodb.read.connection.uri", f"{MONGODB_URL}/cinematch_db") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .getOrCreate()




def process_partition(iterator):
    """
    Funzione eseguita sui worker Spark (dentro foreachPartition).
    Processa un iteratore di Row e scrive su MongoDB in bulk.
    Ottimizzazione V2: READ BATCHING ($in query)
    """
    from pymongo import MongoClient, UpdateOne, DeleteOne
    import pytz
    from datetime import datetime
    
    # Crea connessione per PARTIZIONE
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    # Parametri batching
    READ_BATCH_SIZE = 500
    
    # Accumula user_ids e rows
    user_rows_buffer = [] 
    
    try:
        # Converti iteratore in lista (necessario per batching, attenzione alla memoria ma le partizioni sono piccole)
        all_rows = list(iterator)
        if not all_rows:
            return iter([])

        # Processa a blocchi di READ_BATCH_SIZE
        for i in range(0, len(all_rows), READ_BATCH_SIZE):
            chunk = all_rows[i : i + READ_BATCH_SIZE]
            user_ids_chunk = [r.user_id for r in chunk]
            
            # --- BATCH READ MOVIES ---
            # Recupera film per TUTTI gli utenti del chunk in UNA query
            movies_cursor = db.movies.find({"user_id": {"$in": user_ids_chunk}})
            
            # Raggruppa film per user_id in memoria e raccogli TUTTI i titoli per il catalogo
            movies_by_user = {}
            all_titles_set = set()
            
            for m in movies_cursor:
                uid = m.get("user_id")
                if uid not in movies_by_user:
                    movies_by_user[uid] = []
                movies_by_user[uid].append(m)
                
                if m.get('name'):
                    all_titles_set.add(m.get('name'))
            
            # --- BATCH READ CATALOG ---
            # Unica query al catalogo per tutti i film del chunk (max 500 utenti * N film)
            # Ottimizzazione V3
            master_catalog_map = {}
            if all_titles_set:
                try:
                    all_titles_list = list(all_titles_set)
                    normalized_titles_list = [normalize_title(t) for t in all_titles_list]
                    
                    catalog_docs = list(db.movies_catalog.find({
                        "$or": [
                            {"title": {"$in": all_titles_list}},
                            {"original_title": {"$in": all_titles_list}},
                            {"normalized_title": {"$in": normalized_titles_list}},
                            {"normalized_original_title": {"$in": normalized_titles_list}}
                        ]
                    }))
                    
                    for d in catalog_docs:
                        if d.get('title'): master_catalog_map[d.get('title')] = d
                        if d.get('original_title'): master_catalog_map[d.get('original_title')] = d
                        if d.get('normalized_title'): master_catalog_map[d.get('normalized_title')] = d
                        if d.get('normalized_original_title'): master_catalog_map[d.get('normalized_original_title')] = d
                        
                    logger.info(f"   📚 Catalog Prefetch: Loaded {len(catalog_docs)} docs for {len(all_titles_set)} unique titles")
                except Exception as e:
                    logger.error(f"❌ Catalog prefetch error: {e}")

            # Preparazione Bulk Writes
            bulk_ops = []
            
            for row in chunk:
                user_id = row.user_id
                user_movies = movies_by_user.get(user_id, [])
                
                if not user_movies:
                    bulk_ops.append(DeleteOne({"user_id": user_id}))
                    continue
                
                try:
                    # Calcola stats passando la mappa pre-caricata
                    stats = compute_user_stats(user_movies, db.movies_catalog, prefetched_map=master_catalog_map)
                    
                    stats["user_id"] = user_id
                    stats["updated_at"] = datetime.now(pytz.timezone('Europe/Rome')).isoformat()
                    stats["source"] = "spark_streaming_bulk_v3"
                    
                    bulk_ops.append(UpdateOne(
                        {"user_id": user_id},
                        {"$set": stats},
                        upsert=True
                    ))
                except Exception as e:
                    logger.error(f"❌ Error computing stats for {user_id}: {e}")
            
            # Esegui Bulk Write per questo chunk
            if bulk_ops:
                try:
                    db.user_stats.bulk_write(bulk_ops, ordered=False)
                    logger.info(f"   ⚡ Processed batch chunk {len(bulk_ops)} users")
                except Exception as e:
                    logger.error(f"❌ Bulk write error: {e}")

    except Exception as e:
        logger.error(f"❌ Partition processing error: {e}")
    finally:
        client.close()
        
    return iter([])



def process_batch(batch_df, batch_id):
    """
    Processa un micro-batch per le statistiche utente.
    Global trends sono gestiti separatamente via Structured Streaming.
    """
    if batch_df.isEmpty():
        return
    
    logger.info(f"Batch {batch_id}: partizionamento e scrittura su Mongo...")
    
    # Raggruppa per user_id (shuffle operation)
    user_events_df = batch_df.groupBy("user_id").agg(
        collect_list(struct(
            col("event_type"),
            col("movie.name").alias("name"),
            col("movie.year").alias("year"),
            col("movie.rating").alias("rating")
        )).alias("events")
    )
    
    # Parallel Write: Esegue process_partition in parallelo sui worker
    user_events_df.foreachPartition(process_partition)
    
    logger.info(f"✅ Batch {batch_id} completato.")



def bootstrap_global_stats():
    """
    Bootstrap: Legge TUTTI i film storici da MongoDB e inizializza global_stats.
    Chiamata UNA VOLTA all'avvio di Spark prima dello streaming.
    Lo streaming poi aggiornerà i conteggi in modo incrementale.
    """
    from pymongo import MongoClient
    from datetime import timedelta
    
    logger.info("🚀 [Bootstrap] Inizializzazione global_stats da dati storici MongoDB...")
    
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    try:
        # Calcola data limite (ultimi 30 giorni per dati più ricchi)
        italy_tz = pytz.timezone('Europe/Rome')
        today = datetime.now(italy_tz)
        thirty_days_ago = today - timedelta(days=30)
        
        # Leggi TUTTI i film aggiunti negli ultimi 30 giorni
        all_recent_movies = list(db.movies.find({
            "$or": [
                {"date": {"$gte": thirty_days_ago.strftime("%Y-%m-%d")}},
                {"added_at": {"$gte": thirty_days_ago.isoformat()}}
            ]
        }))
        
        if not all_recent_movies:
            logger.info("⚠️ [Bootstrap] Nessun film recente trovato, global_stats vuoto")
            db.global_stats.update_one(
                {"type": "global_trends"},
                {"$set": {
                    "type": "global_trends",
                    "top_movies": [],
                    "trending_genres": [],
                    "movie_counts": {},
                    "genre_counts": {},
                    "updated_at": datetime.now(italy_tz).isoformat(),
                    "source": "bootstrap_empty"
                }},
                upsert=True
            )
            client.close()
            return
        
        # Aggregazione conteggi film
        movie_counts = Counter()
        movie_data_map = {}
        genre_counts = Counter()
        
        # Raccogli titoli per batch lookup catalogo
        all_titles = list(set([m.get('name') for m in all_recent_movies if m.get('name')]))
        normalized_titles = [normalize_title(t) for t in all_titles]
        
        # Batch lookup catalogo
        catalog_map = {}
        if all_titles:
            try:
                catalog_docs = list(db.movies_catalog.find({
                    "$or": [
                        {"title": {"$in": all_titles}},
                        {"original_title": {"$in": all_titles}},
                        {"normalized_title": {"$in": normalized_titles}},
                        {"normalized_original_title": {"$in": normalized_titles}}
                    ]
                }))
                for d in catalog_docs:
                    if d.get('title'): catalog_map[d.get('title')] = d
                    if d.get('original_title'): catalog_map[d.get('original_title')] = d
                    if d.get('normalized_title'): catalog_map[d.get('normalized_title')] = d
                    if d.get('normalized_original_title'): catalog_map[d.get('normalized_original_title')] = d
            except Exception as e:
                logger.error(f"⚠️ [Bootstrap] Errore lookup catalogo: {e}")
        
        # Processa ogni film
        for movie in all_recent_movies:
            title = movie.get("name")
            if not title:
                continue
            
            movie_counts[title] += 1
            
            # Lookup catalogo per poster e generi
            norm_title = normalize_title(title)
            catalog_info = catalog_map.get(title) or catalog_map.get(norm_title) or {}
            
            poster = catalog_info.get("poster_url") or movie.get("poster_url")
            if not poster or "placeholder" in str(poster):
                poster = None
            
            if title not in movie_data_map or not movie_data_map[title].get("poster_path"):
                movie_data_map[title] = {"title": title, "poster_path": poster}
            
            # Generi
            genres = catalog_info.get("genres") or movie.get("genres") or []
            if isinstance(genres, str):
                genres = [g.strip() for g in genres.split(',')]
            for g in genres:
                if g:
                    genre_counts[g] += 1
        
        # Formatta Top 10 Movies
        top_movies = []
        for title, cnt in movie_counts.most_common(10):
            m_info = movie_data_map.get(title, {"title": title, "poster_path": None})
            top_movies.append({
                "title": m_info["title"],
                "poster_path": m_info["poster_path"],
                "count": cnt
            })
        
        # Formatta Trending Genres
        total_g = sum(genre_counts.values()) or 1
        trending_genres = [
            {"genre": name, "count": cnt, "percentage": round((cnt / total_g) * 100, 1)}
            for name, cnt in genre_counts.most_common(10)
        ]
        
        # Salva in MongoDB (con conteggi raw per merge streaming + cache poster)
        poster_cache = {title: movie_data_map.get(title, {}).get("poster_path") 
                        for title in movie_counts.keys()}
        
        db.global_stats.update_one(
            {"type": "global_trends"},
            {"$set": {
                "type": "global_trends",
                "top_movies": top_movies,
                "trending_genres": trending_genres,
                "movie_counts": dict(movie_counts),  # Per merge streaming
                "genre_counts": dict(genre_counts),  # Per merge streaming  
                "poster_cache": poster_cache,  # Cache poster per streaming
                "updated_at": datetime.now(italy_tz).isoformat(),
                "total_movies_analyzed": len(all_recent_movies),
                "source": "bootstrap"
            }},
            upsert=True
        )
        
        logger.info(f"✅ [Bootstrap] Global stats inizializzati: {len(all_recent_movies)} film, top {len(top_movies)} titoli")
        
    except Exception as e:
        logger.error(f"❌ [Bootstrap] Errore: {e}")
    finally:
        client.close()


def write_global_trends_to_mongo(batch_df, batch_id):
    """
    Callback per writeStream: INCREMENTA o DECREMENTA i conteggi in base all'event_type.
    Gestisce ADD/UPDATE (incrementa) e DELETE (decrementa).
    Preserva i poster URL esistenti.
    """
    from pymongo import MongoClient
    
    if batch_df.isEmpty():
        return
    
    logger.info(f"📊 [Streaming] Batch {batch_id}: Aggiornamento incrementale global trends...")
    
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    try:
        rows = batch_df.collect()
        if not rows:
            client.close()
            return
        
        # 1. Leggi stato esistente (da bootstrap o precedenti batch)
        existing = db.global_stats.find_one({"type": "global_trends"}) or {}
        movie_counts = Counter(existing.get("movie_counts", {}))
        genre_counts = Counter(existing.get("genre_counts", {}))
        poster_cache = existing.get("poster_cache", {})  # Cache poster esistenti
        
        # 2. Raccogli nuovi titoli per lookup catalogo
        new_titles = set()
        for row in rows:
            if row.movie_name:
                new_titles.add(row.movie_name)
        
        normalized_titles = [normalize_title(t) for t in new_titles]
        
        catalog_map = {}
        if new_titles:
            try:
                catalog_docs = list(db.movies_catalog.find({
                    "$or": [
                        {"title": {"$in": list(new_titles)}},
                        {"original_title": {"$in": list(new_titles)}},
                        {"normalized_title": {"$in": normalized_titles}},
                        {"normalized_original_title": {"$in": normalized_titles}}
                    ]
                }))
                for d in catalog_docs:
                    if d.get('title'): catalog_map[d.get('title')] = d
                    if d.get('original_title'): catalog_map[d.get('original_title')] = d
                    if d.get('normalized_title'): catalog_map[d.get('normalized_title')] = d
                    if d.get('normalized_original_title'): catalog_map[d.get('normalized_original_title')] = d
            except Exception as e:
                logger.error(f"⚠️ [Streaming] Errore lookup catalogo: {e}")
        
        # 3. Processa eventi - gestisce ADD e DELETE
        logger.info(f"   📋 Processando {len(rows)} righe aggregate...")
        for row in rows:
            # Accesso sicuro alle colonne della Row
            row_dict = row.asDict()
            title = row_dict.get('movie_name')
            event_type = row_dict.get('event_type', 'ADD')
            delta = row_dict.get('watch_count', 1)
            
            if not title:
                continue
            
            # Determina se incrementare o decrementare
            if event_type and 'DELETE' in str(event_type).upper():
                # DELETE: decrementa
                old_count = movie_counts.get(title, 0)
                movie_counts[title] = max(0, old_count - delta)
                logger.info(f"   ➖ DELETE {title}: {old_count} - {delta} = {movie_counts[title]}")
                if movie_counts[title] == 0:
                    del movie_counts[title]  # Rimuovi se zero
            else:
                # ADD/UPDATE: incrementa
                old_count = movie_counts.get(title, 0)
                movie_counts[title] = old_count + delta
                logger.info(f"   ➕ ADD {title}: {old_count} + {delta} = {movie_counts[title]}")
            
            # Aggiorna poster cache (solo se abbiamo un poster)
            norm_title = normalize_title(title)
            catalog_info = catalog_map.get(title) or catalog_map.get(norm_title) or {}
            poster = catalog_info.get("poster_url")
            if poster and title not in poster_cache:
                poster_cache[title] = poster
            elif poster and not poster_cache.get(title):
                poster_cache[title] = poster
            
            # Aggiorna generi (solo per ADD/UPDATE)
            if not (event_type and 'DELETE' in str(event_type).upper()):
                genres = catalog_info.get("genres") or []
                if isinstance(genres, str):
                    genres = [g.strip() for g in genres.split(',')]
                for g in genres:
                    if g:
                        genre_counts[g] += delta
        
        # 4. Ricalcola Top 10 con conteggi aggiornati
        top_movies = []
        for title, cnt in movie_counts.most_common(10):
            # Usa poster dalla cache, poi catalogo come fallback
            poster = poster_cache.get(title)
            if not poster:
                norm = normalize_title(title)
                cat = catalog_map.get(title) or catalog_map.get(norm) or {}
                poster = cat.get("poster_url")
                if poster:
                    poster_cache[title] = poster  # Salva in cache
            
            top_movies.append({
                "title": title,
                "poster_path": poster,
                "count": cnt
            })
        
        # 5. Ricalcola Trending Genres
        total_g = sum(genre_counts.values()) or 1
        trending_genres = [
            {"genre": name, "count": cnt, "percentage": round((cnt / total_g) * 100, 1)}
            for name, cnt in genre_counts.most_common(10)
        ]
        
        # 6. Salva con conteggi aggiornati e poster cache
        db.global_stats.update_one(
            {"type": "global_trends"},
            {"$set": {
                "type": "global_trends",
                "top_movies": top_movies,
                "trending_genres": trending_genres,
                "movie_counts": dict(movie_counts),
                "genre_counts": dict(genre_counts),
                "poster_cache": poster_cache,
                "updated_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat(),
                "source": "streaming_incremental"
            }},
            upsert=True
        )
        
        logger.info(f"✅ [Streaming] Top {len(top_movies)} film aggiornati (batch {batch_id})")
        
    except Exception as e:
        logger.error(f"❌ [Streaming] Errore write_global_trends_to_mongo: {e}")
    finally:
        client.close()


def start_global_trends_stream(spark, parsed_stream):
    """
    Avvia uno stream Spark Structured Streaming per i film più visti.
    Include event_type per gestire ADD/UPDATE e DELETE separatamente.
    """
    from pyspark.sql.functions import (
        to_timestamp, count as spark_count, 
        col, first
    )
    
    logger.info("🚀 Avvio Structured Streaming per Global Trends...")
    
    # 1. Prepara lo stream con event_type incluso
    events_with_ts = parsed_stream \
        .withColumn("event_ts", to_timestamp(col("timestamp"))) \
        .filter(col("movie.name").isNotNull()) \
        .select(
            col("movie.name").alias("movie_name"),
            col("event_type"),
            col("event_ts")
        )
    
    # 2. Applica watermark
    watermarked = events_with_ts.withWatermark("event_ts", "1 hour")
    
    # 3. Aggregazione per movie_name + event_type
    #    Questo ci permette di sapere quanti ADD e quanti DELETE per ogni film
    aggregated = watermarked \
        .groupBy(
            col("movie_name"),
            col("event_type")
        ) \
        .agg(spark_count("*").alias("watch_count")) \
        .select(
            col("movie_name"),
            col("event_type"),
            col("watch_count")
        )
    
    # 4. Avvia lo stream con output su MongoDB via foreachBatch
    global_trends_query = aggregated \
        .writeStream \
        .foreachBatch(write_global_trends_to_mongo) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/today-trends") \
        .trigger(processingTime="30 seconds") \
        .queryName("today_trends_stream") \
        .start()
    
    logger.info("✅ Stream Global Trends avviato (trigger: 30s, watermark: 1h)")
    
    return global_trends_query




def compute_user_stats(movies, catalog_collection, prefetched_map=None):
    """
    Calcola le statistiche complete per un utente partendo dalla lista dei suoi film.
    - prefetched_map: Opzionale. Se fornito, evita query al DB.
    """
    from collections import Counter
    import re
    
    # Init contatori
    rating_distribution = Counter()
    genre_counter = Counter()
    director_counter = Counter()
    actor_counter = Counter()
    director_ratings = {}
    actor_ratings = {}
    
    # Batch Enrichment Pre-processing
    # Collect titles to query catalog
    movie_titles = [m.get('name') for m in movies if m.get('name')]
    
    # Build Catalog Map
    catalog_map = {}
    
    if prefetched_map:
        # USA LA MAPPA FORNITA (OTTIMIZZAZIONE)
        catalog_map = prefetched_map
    elif movie_titles:
        # FALLBACK: Query DB (Vecchio metodo lento)
        try:
            # Query catalog for all these titles and their normalized variants
            normalized_titles = [normalize_title(t) for t in movie_titles]
            
            # Using $in for efficiency
            catalog_docs = list(catalog_collection.find({
                "$or": [
                    {"title": {"$in": movie_titles}},
                    {"original_title": {"$in": movie_titles}},
                    {"normalized_title": {"$in": normalized_titles}},
                    {"normalized_original_title": {"$in": normalized_titles}}
                ]
            }))
            
            for d in catalog_docs:
                # Map by all possible identifiers to maximize hit rate
                if d.get('title'): catalog_map[d.get('title')] = d
                if d.get('original_title'): catalog_map[d.get('original_title')] = d
                if d.get('normalized_title'): catalog_map[d.get('normalized_title')] = d
                if d.get('normalized_original_title'): catalog_map[d.get('normalized_original_title')] = d
        except Exception as e:
            logger.error(f"⚠️ Errore build catalog_map: {e}")
    
    
    # Il grafico a torta dei generi è relativo a tutti film visti dell'utente.
    
    processed_movies = []
    
    total_duration = 0
    duration_count = 0
    ratings = []
    
    # Elabora TUTTI i film dell'utente
    for movie in movies:
        movie_title = movie.get('name')
        norm_title = normalize_title(movie_title)
        
        # Rating extraction
        user_rating = movie.get('rating')
        if user_rating is not None:
            ratings.append(user_rating)
            rating_distribution[int(user_rating)] += 1
        
        # Enrich from Catalog Map (using multiple variants)
        catalog_info = catalog_map.get(movie_title) or catalog_map.get(norm_title) or {}
        
        # Enrich the movie object itself for display stats
        enriched_movie = movie.copy()
        
        # Recupera TUTTI i dati dal catalogo: poster, url, genres, director, actors, duration, etc.
        if catalog_info:
            # Poster URL
            if not enriched_movie.get("poster_url") and catalog_info.get("poster_url"):
                enriched_movie["poster_url"] = catalog_info["poster_url"]
            # Genres - PREFERISCI catalogo per generi più accurati
            if catalog_info.get("genres"):
                enriched_movie["genres"] = catalog_info["genres"]
            elif not enriched_movie.get("genres") and catalog_info.get("genre"):
                # Fallback a 'genre' string se 'genres' array non esiste
                genres_str = catalog_info.get("genre", "")
                enriched_movie["genres"] = [g.strip() for g in genres_str.split(',')] if genres_str else []
            # Director
            if catalog_info.get("director"):
                enriched_movie["director"] = catalog_info["director"]
            # Actors
            if catalog_info.get("actors"):
                enriched_movie["actors"] = catalog_info["actors"]
            # Duration
            if catalog_info.get("duration"):
                enriched_movie["duration"] = catalog_info["duration"]
            # Description
            if catalog_info.get("description"):
                enriched_movie["description"] = catalog_info["description"]
            # IMDB ID
            if catalog_info.get("imdb_id"):
                enriched_movie["imdb_id"] = catalog_info["imdb_id"]
            # Year (se mancante)
            if not enriched_movie.get("year") and catalog_info.get("year"):
                enriched_movie["year"] = catalog_info["year"]
            # Avg Vote IMDB
            if catalog_info.get("avg_vote"):
                enriched_movie["avg_vote"] = catalog_info["avg_vote"]
            
        # Duration
        dur = enriched_movie.get('duration', 0)
        if dur:
            try:
                dur = int(dur)
                total_duration += dur
                duration_count += 1
            except (ValueError, TypeError):
                pass
            
        # Genres - usa quelli arricchiti dal catalogo
        genres = enriched_movie.get('genres', [])
        if isinstance(genres, str):
            genres = [g.strip() for g in genres.split(',')]
        for g in genres:
            if g:
                genre_counter[g] += 1

        # --- LOGICA REGISTI E ATTORI ---
        # Registi
        director = enriched_movie.get('director') or movie.get('director')
        if director:
            directors_list = [d.strip() for d in re.split(r'[,|]', str(director)) if d.strip()]
            for d in directors_list:
                director_counter[d] += 1
                if d not in director_ratings: 
                    director_ratings[d] = []
                if user_rating is not None:
                    director_ratings[d].append(user_rating)
        
        # Attori
        actors = enriched_movie.get('actors') or movie.get('actors')
        if actors:
            actors_list = [a.strip() for a in re.split(r'[,|]', str(actors)) if a.strip()]
            for a in actors_list:
                actor_counter[a] += 1
                if a not in actor_ratings: 
                    actor_ratings[a] = []
                if user_rating is not None:
                    actor_ratings[a].append(user_rating)

        if "_id" in enriched_movie:
            enriched_movie["_id"] = str(enriched_movie["_id"])
        processed_movies.append(enriched_movie)

    # --- CALCOLI STATISTICI AVANZATI ---
    
    # 1. Top Rated Movies (Voto >= 4, ordinati per voto decrescente)
    top_rated_candidates = [m for m in processed_movies if m.get('rating', 0) >= 4]
    top_rated_candidates.sort(key=lambda x: x.get('rating', 0), reverse=True)
    top_rated_movies = top_rated_candidates[:10]
    
    # 2. Recent Movies (Ordinati per data visione decrescente)
    def parse_date(d_str):
        if not d_str: return datetime.min
        try:
            return datetime.fromisoformat(d_str.replace('Z', '+00:00'))
        except:
             # Fallback per date semplici YYYY-MM-DD
            try:
                return datetime.strptime(d_str, "%Y-%m-%d")
            except:
                return datetime.min

    processed_movies.sort(key=lambda x: parse_date(x.get('date')), reverse=True)
    recent_movies = processed_movies[:10]
    
    # Per il calcolo degli anni usiamo TUTTI i film dell'utente
    months_map = {1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mag", 6: "Giu", 
                  7: "Lug", 8: "Ago", 9: "Set", 10: "Ott", 11: "Nov", 12: "Dic"}
    
    # YEAR_DATA: Organizza monthly_data per anno
    year_monthly_counts = defaultdict(lambda: Counter())  # {year: {month: count}}
    available_years = set()
    
    for m in processed_movies:
        d = parse_date(m.get('date'))
        if d != datetime.min:
            year_monthly_counts[d.year][d.month] += 1
            available_years.add(d.year)
    
    # Costruisci year_data: lista di oggetti {year, monthly_data, total_films}
    year_data = []
    for year in sorted(available_years, reverse=True):
        month_counts = year_monthly_counts[year]
        monthly_data_for_year = [
            {"month": months_map[i], "films": month_counts.get(i, 0)} 
            for i in range(1, 13)
        ]
        total_films_year = sum(month_counts.values())
        year_data.append({
            "year": year,
            "monthly_data": monthly_data_for_year,
            "total_films": total_films_year
        })
    
    # available_years come lista ordinata decrescente
    available_years_list = sorted(list(available_years), reverse=True)
    
    # 4. Top Years (Anni più visti)
    years = [m.get('year') for m in movies if m.get('year')] # Usa anno originale utente
    year_counts = Counter(years)
    top_years = [{"year": y, "count": c} for y, c in year_counts.most_common(5)]
    
    # 5. Rating Distribution Chart
    rating_chart_data = [
        {"rating": f"⭐{i}", "count": rating_distribution.get(i, 0), "stars": i}
        for i in range(1, 6)
    ]
    
    # 6. Genre Data - Basato su TUTTI i film dell'utente
    total_genres = sum(genre_counter.values()) or 1
    top_genres = genre_counter.most_common(8)
    
    genre_colors = {
        "Drama": "#E50914", "Comedy": "#FF6B35", "Action": "#00529B",
        "Thriller": "#8B5CF6", "Horror": "#6B21A8", "Romance": "#EC4899",
        "Sci-Fi": "#06B6D4", "Adventure": "#10B981", "Crime": "#F59E0B",
        "Dramma": "#E50914", "Commedia": "#FF6B35", "Azione": "#00529B",
        "Fantascienza": "#06B6D4", "Avventura": "#10B981", "Fantasy": "#8B5CF6",
        "Family": "#F472B6", "Mystery": "#4B5563", "History": "#92400E",
        "War": "#7F1D1D", "Music": "#DB2777", "Documentary": "#059669",
        "Western": "#D97706", "Animation": "#FBBF24", "Animazione": "#FBBF24",
        "Thriller": "#8B5CF6", "Giallo": "#F59E0B", "Biography": "#4B5563",
        "Biografia": "#4B5563", "Sport": "#EF4444", "Musical": "#DB2777",
        "Film-Noir": "#111827", "Noir": "#111827"
    }
    
    genre_data = [{
        "name": genre, 
        "value": round((count / total_genres) * 100, 1),
        "color": genre_colors.get(genre, "#9CA3AF"),
        "count": count
    } for genre, count in top_genres]
    
    favorite_genre = top_genres[0][0] if top_genres else "Nessuno"
    
    # 7. Top Directors/Actors - RIMOSSI, derivati lato frontend da best_rated_*
    # Filtra rating None dalla lista prima di calcolare la media
    def safe_avg(ratings_list):
        valid_ratings = [r for r in ratings_list if r is not None]
        if valid_ratings:
            return round(sum(valid_ratings) / len(valid_ratings), 1)
        return 0
    
    # 8. Best Rated Directors & Most Watched Directors
    # Ottimizzazione: Calcola Top 10 per soglie specifiche (1, 2, 3, 5 film)
    # invece di inviare l'intera lista al frontend.
    
    director_stats_list = []
    for d, ratings_list in director_ratings.items():
        valid_ratings = [r for r in ratings_list if r is not None]
        if len(valid_ratings) >= 1:
            avg = round(sum(valid_ratings) / len(valid_ratings), 1)
            director_stats_list.append({"name": d, "count": len(valid_ratings), "avg_rating": avg})
            
    # Most Watched (Top 15 per count)
    director_stats_list.sort(key=lambda x: x["count"], reverse=True)
    most_watched_directors = director_stats_list[:15]
    
    # Best Rated (Top 10 per soglie)
    best_rated_directors = {}
    for threshold in [1, 2, 3, 5]:
        filtered = [d for d in director_stats_list if d["count"] >= threshold]
        # Sort by rating (desc), then count (desc)
        filtered.sort(key=lambda x: (x["avg_rating"], x["count"]), reverse=True)
        best_rated_directors[str(threshold)] = filtered[:10]

    # 9. Best Rated Actors & Most Watched Actors
    actor_stats_list = []
    for a, ratings_list in actor_ratings.items():
        valid_ratings = [r for r in ratings_list if r is not None]
        if len(valid_ratings) >= 1:
            avg = round(sum(valid_ratings) / len(valid_ratings), 1)
            actor_stats_list.append({"name": a, "count": len(valid_ratings), "avg_rating": avg})
            
    # Most Watched (Top 15 per count)
    actor_stats_list.sort(key=lambda x: x["count"], reverse=True)
    most_watched_actors = actor_stats_list[:15]
    
    # Best Rated (Top 10 per soglie)
    best_rated_actors = {}
    for threshold in [1, 2, 3, 5]:
        filtered = [a for a in actor_stats_list if a["count"] >= threshold]
        filtered.sort(key=lambda x: (x["avg_rating"], x["count"]), reverse=True)
        best_rated_actors[str(threshold)] = filtered[:10]

    return {
        "total_watched": len(movies),  # TUTTI i film dell'utente
        "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
        "rating_chart_data": rating_chart_data,
        "top_rated_movies": top_rated_movies,
        "recent_movies": recent_movies,
        "year_data": year_data,  # Organizzato per anno
        "available_years": available_years_list,
        "top_years": top_years,
        "genre_data": genre_data,
        "favorite_genre": favorite_genre,
        "watch_time_hours": total_duration // 60,
        "avg_duration": round(total_duration / duration_count) if duration_count > 0 else 0,
        "best_rated_directors": best_rated_directors,
        "most_watched_directors": most_watched_directors,
        "best_rated_actors": best_rated_actors,
        "most_watched_actors": most_watched_actors,
        "stats_version": "3.2"  # Schema ottimizzato - top lists strutturate
    }


def main():
    """Entry point per l'applicazione Spark Streaming."""
    logger.info("🚀 Avvio CineMatch Stats Processor...")
    logger.info(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"🗃️ MongoDB: {MONGODB_URL}")
    
    # ========== BOOTSTRAP: Inizializza global_stats da dati storici ==========
    bootstrap_global_stats()
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Leggi stream da Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), event_schema).alias("data")) \
        .select("data.*")
    
    # ========== STREAM 1: User Stats (foreachBatch) ==========
    user_stats_query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="1 second") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/user-stats") \
        .queryName("user_stats_stream") \
        .start()
    
    # ========== STREAM 2: Global Trends (Bootstrap + Streaming) ==========
    global_trends_query = start_global_trends_stream(spark, parsed_stream)
    
    logger.info("✅ Tutti gli stream avviati:")
    logger.info("   📊 user_stats_stream: Statistiche utente (trigger 1s)")
    logger.info("   🌍 global_trends_stream: Community Trends (bootstrap + streaming 30s)")
    
    # Attendi terminazione di entrambi gli stream
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
