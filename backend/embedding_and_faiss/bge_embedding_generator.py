"""
E5 Embedding Generator (High Quality)
======================================
Usa il modello multilingual-e5-base - ottimo compromesso qualità/dimensioni.

Output: bert_movies_vectors_bge
"""

import os
import time
import warnings
from typing import List, Dict, Any, Set
from collections import Counter

warnings.filterwarnings("ignore")

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MultiLabelBinarizer
from pymongo import MongoClient

# =============================================================================
# CONFIGURAZIONE
# =============================================================================
MONGO_URI = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
DB_NAME = os.getenv("MONGODB_DB", "cinematch_db")
COLL_INPUT = os.getenv("COLL_INPUT", "movies_catalog")
COLL_OUTPUT = "bert_movies_vectors_bge"  # Stessa collezione output
BERT_MODEL = "intfloat/multilingual-e5-base"  # 768 dim, ~1.1GB
BATCH_SIZE = 32

# Limiti per vocabolario
MAX_ACTORS = 3000
MAX_DIRECTORS = 1000


def parse_comma_separated(value: str) -> List[str]:
    if not value:
        return []
    return [x.strip().lower() for x in value.split(",") if x.strip()]


def main():
    print("=" * 60)
    print("E5 EMBEDDING GENERATOR (High Quality)")
    print("=" * 60)
    print(f"Modello: {BERT_MODEL}")
    print(f"Output: {COLL_OUTPUT}")
    
    # 1. Carica modello E5
    print("\n[1/6] Caricamento modello E5...")
    print("      (Prima volta scarica ~1.1GB)")
    t0 = time.time()
    model = SentenceTransformer(BERT_MODEL)
    print(f"      Caricato in {time.time()-t0:.1f}s")
    print(f"      Dimensione embedding: {model.get_sentence_embedding_dimension()}")
    
    # 2. Leggi da MongoDB
    print("\n[2/6] Lettura da MongoDB...")
    client = MongoClient(MONGO_URI)
    
    cursor = client[DB_NAME][COLL_INPUT].find(
        {"imdb_id": {"$exists": True, "$ne": None, "$ne": ""}},
        {"imdb_id": 1, "title": 1, "description": 1, "genres": 1, "actors": 1, "director": 1, "_id": 0}
    )
    
    movies = list(cursor)
    client.close()
    print(f"      Film caricati: {len(movies)}")
    
    if not movies:
        raise ValueError("Nessun film trovato!")
    
    # 3. Preprocessing
    print("\n[3/6] Preprocessing...")
    all_genres: Set[str] = set()
    actor_counter = Counter()
    director_counter = Counter()
    movies_data = []
    
    for m in movies:
        # genres è già un array, non serve splittare
        raw_genres = m.get("genres", [])
        if isinstance(raw_genres, list):
            g = [x.strip().lower() for x in raw_genres if x and isinstance(x, str)]
        else:
            g = parse_comma_separated(raw_genres) if raw_genres else []
        
        a = parse_comma_separated(m.get("actors", ""))
        d = parse_comma_separated(m.get("director", ""))
        
        all_genres.update(g)
        actor_counter.update(a)
        director_counter.update(d)
        
        movies_data.append({
            "imdb_id": m.get("imdb_id", ""),
            "title": m.get("title", ""),
            "description": m.get("description", ""),
            "genres_list": g, 
            "actors_list": a, 
            "director_list": d
        })
    
    top_actors = [a for a, _ in actor_counter.most_common(MAX_ACTORS)]
    top_directors = [d for d, _ in director_counter.most_common(MAX_DIRECTORS)]
    
    print(f"      Generi: {len(all_genres)}")
    print(f"      Attori: top {len(top_actors)}/{len(actor_counter)}")
    print(f"      Registi: top {len(top_directors)}/{len(director_counter)}")
    
    # 4. Multi-Hot Encoding
    print("\n[4/6] Multi-Hot encodings...")
    
    mlb_genre = MultiLabelBinarizer(classes=sorted(all_genres))
    genre_enc = mlb_genre.fit_transform([m["genres_list"] for m in movies_data]).astype(np.float32)
    print(f"      Genre: {genre_enc.shape[1]} dim")
    
    mlb_actors = MultiLabelBinarizer(classes=top_actors)
    actors_filtered = [[a for a in m["actors_list"] if a in set(top_actors)] for m in movies_data]
    actors_enc = mlb_actors.fit_transform(actors_filtered).astype(np.float32)
    print(f"      Actors: {actors_enc.shape[1]} dim")
    
    mlb_directors = MultiLabelBinarizer(classes=top_directors)
    directors_filtered = [[d for d in m["director_list"] if d in set(top_directors)] for m in movies_data]
    director_enc = mlb_directors.fit_transform(directors_filtered).astype(np.float32)
    print(f"      Directors: {director_enc.shape[1]} dim")
    
    # 5. BGE-M3 embeddings
    print("\n[5/6] Generazione BGE-M3 embeddings...")
    print("      (Potrebbe richiedere 20-30 minuti su CPU)")
    
    descriptions = [m["description"] if m["description"] else "No description available" for m in movies_data]
    
    t0 = time.time()
    desc_emb = model.encode(
        descriptions, 
        batch_size=BATCH_SIZE, 
        show_progress_bar=True, 
        convert_to_numpy=True, 
        normalize_embeddings=True
    )
    elapsed = time.time() - t0
    print(f"      Tempo: {elapsed:.1f}s ({len(descriptions)/elapsed:.1f} doc/s)")
    print(f"      Dimensione: {desc_emb.shape[1]}")
    
    # 6. Salva su MongoDB
    print(f"\n[6/6] Salvataggio su {COLL_OUTPUT}...")
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_OUTPUT]
    coll.drop()
    
    batch_size = 5000
    total_saved = 0
    
    for batch_start in range(0, len(movies_data), batch_size):
        batch_end = min(batch_start + batch_size, len(movies_data))
        docs = []
        
        for i in range(batch_start, batch_end):
            m = movies_data[i]
            docs.append({
                "imdb_id": m["imdb_id"],
                "title": m["title"],
                "embedding_description": desc_emb[i].tolist(),
                "embedding_genre": genre_enc[i].tolist(),
                "embedding_actors": actors_enc[i].tolist(),
                "embedding_director": director_enc[i].tolist(),
                "genres_list": m["genres_list"],
                "actors_list": m["actors_list"],
                "director_list": m["director_list"]
            })
        
        coll.insert_many(docs, ordered=False)
        total_saved += len(docs)
        print(f"      Salvati: {total_saved}/{len(movies_data)}")
    
    coll.create_index("imdb_id", unique=True)
    client.close()
    
    print("\n" + "=" * 60)
    print("COMPLETATO!")
    print(f"Modello: {BERT_MODEL}")
    print(f"Collezione: {DB_NAME}.{COLL_OUTPUT}")
    print(f"Documenti: {total_saved}")
    print(f"Embedding dim: {desc_emb.shape[1]}")
    print("=" * 60)


if __name__ == "__main__":
    main()
