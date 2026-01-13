"""
Spark Structured Streaming application per elaborazione eventi film utente.
Consuma eventi da Kafka e aggiorna statistiche pre-calcolate in MongoDB.

Per eseguire:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 spark_stats_processor.py
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


def process_batch(batch_df, batch_id):
    """
    Processa un micro-batch di eventi e aggiorna le statistiche.
    Chiamato ogni 30 secondi con gli eventi accumulati.
    """
    # --- GLOBAL TRENDS UPDATE (All Users) ---
    # Run this every batch to ensure global stats are enriched and up-to-date,
    # regardless of whether there are new user events.
    try:
        update_global_trends(spark_session=batch_df.sparkSession)
    except Exception as e:
        logger.error(f"❌ Errore aggiornamento trend globali: {e}")

    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: vuoto, skip events processing")
        return
    
    logger.info(f"Batch {batch_id}: elaborazione {batch_df.count()} eventi")
    
    # Raggruppa per user_id
    user_events = batch_df.groupBy("user_id").agg(
        collect_list(struct(
            col("event_type"),
            col("movie.name").alias("name"),
            col("movie.year").alias("year"),
            col("movie.rating").alias("rating"),
            col("movie.genres").alias("genres"),
            col("movie.duration").alias("duration")
        )).alias("events")
    )
    
    # Per ogni utente, ricalcola le statistiche complete
    # Questa è una versione semplificata - in produzione si farebbe incrementale
    user_ids = [row.user_id for row in user_events.collect()]
    
    from pymongo import MongoClient
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    for user_id in user_ids:
        try:
            # Leggi tutti i film dell'utente da MongoDB
            user_movies = list(db.movies.find({"user_id": user_id}))
            
            if not user_movies:
                # Utente senza film, rimuovi stats
                db.user_stats.delete_one({"user_id": user_id})
                continue
            
            # Calcola statistiche
            stats = compute_user_stats(user_movies, db.movies_catalog)
            stats["user_id"] = user_id
            stats["updated_at"] = datetime.now(pytz.timezone('Europe/Rome')).isoformat()
            stats["source"] = "spark_streaming"
            
            # Salva/aggiorna direttamente nella collection di produzione
            db.user_stats.update_one(
                {"user_id": user_id},
                {"$set": stats},
                upsert=True
            )
            
            logger.info(f"✅ Stats aggiornate per user {user_id}: {stats.get('total_watched', 0)} film")
            
        except Exception as e:
            logger.error(f"❌ Errore elaborazione user {user_id}: {e}")
            

    
    client.close()


def update_global_trends(spark_session):
    """
    Calcola trend globali aggregando i film degli ULTIMI 10 GIORNI di tutti gli utenti.
    I dati della community devono fare riferimento ai dati di 10 giorni.
    """
    logger.info("🌍 Aggiornamento Trend Globali (ultimi 10 giorni)...")
    
    from pymongo import MongoClient
    from datetime import timedelta
    client = MongoClient(MONGODB_URL)
    db = client.cinematch_db
    
    # Calcola data limite (10 giorni fa)
    italy_tz = pytz.timezone('Europe/Rome')
    today = datetime.now(italy_tz)
    ten_days_ago = today - timedelta(days=10)
    
    def parse_date_internal(d_str):
        if not d_str: return datetime.min
        try: return datetime.fromisoformat(str(d_str).replace('Z', '+00:00'))
        except:
            try: return datetime.strptime(str(d_str), "%Y-%m-%d")
            except: return datetime.min
    
    # 1. Recupera TUTTI i film di TUTTI gli utenti degli ultimi 10 giorni
    all_recent_movies = list(db.movies.find({
        "$or": [
            {"date": {"$gte": ten_days_ago.strftime("%Y-%m-%d")}},
            {"added_at": {"$gte": ten_days_ago.isoformat()}}
        ]
    }))
    
    if not all_recent_movies:
        logger.info("⚠️ Nessun film negli ultimi 10 giorni per i trend globali")
        client.close()
        return

    # 2. Aggregazione Film Popolari (basata su visioni negli ultimi 10 giorni)
    movie_counts = Counter()
    movie_data_map = {}
    
    # 3. Aggregazione Generi
    global_genre_counts = Counter()
    
    # Raccogli titoli per query batch al catalogo
    all_titles = list(set([m.get('name') for m in all_recent_movies if m.get('name')]))
    normalized_titles = [normalize_title(t) for t in all_titles]
    
    # Query batch al catalogo
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
            logger.error(f"⚠️ Errore build catalog_map per global trends: {e}")
    
    for movie in all_recent_movies:
        title = movie.get("name")
        if not title:
            continue
            
        # Conta visione
        movie_counts[title] += 1
        
        # Cerca nel catalogo per poster e generi
        norm_title = normalize_title(title)
        catalog_info = catalog_map.get(title) or catalog_map.get(norm_title) or {}
        
        poster = catalog_info.get("poster_url") or movie.get("poster_url")
        if not poster or "placeholder" in str(poster):
            poster = None
            
        if title not in movie_data_map or not movie_data_map[title].get("poster_path"):
            movie_data_map[title] = {
                "title": title,
                "poster_path": poster
            }
        
        # Aggrega Generi (preferisci catalogo, poi film utente)
        genres = catalog_info.get("genres") or movie.get("genres") or []
        if isinstance(genres, str):
            genres = [g.strip() for g in genres.split(',')]
        for g in genres:
            if g:
                global_genre_counts[g] += 1

    # 4. Formatta Top 10 Movies
    top_10_movies = []
    for title, count in movie_counts.most_common(10):
        m_info = movie_data_map.get(title, {"title": title, "poster_path": None})
        top_10_movies.append({
            "title": m_info["title"],
            "poster_path": m_info["poster_path"],
            "watch_count": count
        })

    # 5. Formatta Trending Genres
    trending_genres = []
    total_g_count = sum(global_genre_counts.values()) or 1
    for name, count in global_genre_counts.most_common(10):
        trending_genres.append({
            "genre": name,
            "count": count,
            "percentage": round((count / total_g_count) * 100, 1)
        })

    # 6. Salva risultati globali
    db.global_stats.update_one(
        {}, 
        {"$set": {
            "top_movies": top_10_movies,
            "trending_genres": trending_genres,
            "updated_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat(),
            "total_movies_analyzed": len(all_recent_movies),
            "period_days": 10
        }}, 
        upsert=True
    )
    
    logger.info(f"✅ Trend Globali aggiornati basandosi su {len(all_recent_movies)} film degli ultimi 10 giorni")
    client.close()




def compute_user_stats(movies, catalog_collection):
    """
    Calcola le statistiche complete per un utente partendo dalla lista dei suoi film.
    - Il grafico a torta dei generi è relativo a TUTTI i film visti dell'utente.
    - I dati dei film recuperano tutto (poster, url, etc.) dal database movie_catalog.
    - Directors e attori sono calcolati su TUTTI i film.
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
    if movie_titles:
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
    
    # NOTA: Usiamo TUTTI i film dell'utente (non filtro 10 giorni)
    # Il grafico a torta dei generi deve essere relativo a tutti film visti dell'utente.
    
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
        # Registi: Usa dati dal catalogo (più completi)
        director = enriched_movie.get('director') or movie.get('director')
        if director:
            directors_list = [d.strip() for d in re.split(r'[,|]', str(director)) if d.strip()]
            for d in directors_list:
                director_counter[d] += 1
                if d not in director_ratings: 
                    director_ratings[d] = []
                if user_rating is not None:
                    director_ratings[d].append(user_rating)
        
        # Attori: Usa dati dal catalogo (più completi)
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

    # --- CALCOLI STATISTICI AVANZATI (Ex Pandas logic) ---
    
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
    
    # 7. Top Directors/Actors (Most Frequent)
    # Filtra rating None dalla lista prima di calcolare la media
    def safe_avg(ratings_list):
        valid_ratings = [r for r in ratings_list if r is not None]
        if valid_ratings:
            return round(sum(valid_ratings) / len(valid_ratings), 1)
        return 0
    
    top_directors = [{
        "name": d, 
        "count": c, 
        "avg_rating": safe_avg(director_ratings.get(d, []))
    } for d, c in director_counter.most_common(10)]
    
    # 8. Best Rated Directors (Best Avg Rating, min 1 movie)
    # Restituisci TUTTI i registi con rating per permettere il filtraggio frontend
    director_stats_list = []
    for d, ratings_list in director_ratings.items():
        valid_ratings = [r for r in ratings_list if r is not None]
        if len(valid_ratings) >= 1:
            avg = round(sum(valid_ratings) / len(valid_ratings), 1)
            director_stats_list.append({"name": d, "count": len(valid_ratings), "avg_rating": avg})
            
    # Sort by rating (desc), then count (desc)
    director_stats_list.sort(key=lambda x: (x["avg_rating"], x["count"]), reverse=True)
    best_rated_directors = director_stats_list  # Tutti per permettere filtraggio 1+, 2+, 3+, 5+
    
    top_actors = [{
        "name": a, 
        "count": c, 
        "avg_rating": safe_avg(actor_ratings.get(a, []))
    } for a, c in actor_counter.most_common(15)]
    
    
    # 9. Best Rated Actors (Best Avg Rating, min 1 movie)
    # Restituisci TUTTI gli attori con rating per permettere il filtraggio frontend
    actor_stats_list = []
    for a, ratings_list in actor_ratings.items():
        valid_ratings = [r for r in ratings_list if r is not None]
        if len(valid_ratings) >= 1:
            avg = round(sum(valid_ratings) / len(valid_ratings), 1)
            actor_stats_list.append({"name": a, "count": len(valid_ratings), "avg_rating": avg})
            
    actor_stats_list.sort(key=lambda x: (x["avg_rating"], x["count"]), reverse=True)
    best_rated_actors = actor_stats_list  # Tutti per permettere filtraggio 1+, 2+, 3+, 5+

    return {
        "total_watched": len(movies),  # TUTTI i film dell'utente
        "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
        "rating_distribution": {str(k): v for k, v in rating_distribution.items()},
        "rating_chart_data": rating_chart_data,
        "top_rated_movies": top_rated_movies,
        "recent_movies": recent_movies,
        "year_data": year_data,  # NUOVO: organizzato per anno
        "available_years": available_years_list,  # NUOVO: anni disponibili
        "top_years": top_years,
        "genre_data": genre_data,
        "favorite_genre": favorite_genre,
        "watch_time_hours": total_duration // 60,
        "watch_time_minutes": total_duration % 60,
        "avg_duration": round(total_duration / duration_count) if duration_count > 0 else 0,
        "top_directors": top_directors,
        "best_rated_directors": best_rated_directors,
        "top_actors": top_actors,
        "best_rated_actors": best_rated_actors,
        "total_unique_directors": len(director_counter),
        "total_unique_actors": len(actor_counter),
        "total_5_stars": rating_distribution.get(5, 0),
        "total_4_stars": rating_distribution.get(4, 0),
        "total_3_stars": rating_distribution.get(3, 0),
        "total_2_stars": rating_distribution.get(2, 0),
        "total_1_stars": rating_distribution.get(1, 0),
        "stats_version": "3.0"  # Versione aggiornata - year_data
    }


def main():
    """Entry point per l'applicazione Spark Streaming."""
    logger.info("🚀 Avvio CineMatch Stats Processor...")
    logger.info(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"🗃️ MongoDB: {MONGODB_URL}")
    
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
    
    # Trigger elaborazione ogni 30 secondi
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ Streaming avviato. In attesa di eventi...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
