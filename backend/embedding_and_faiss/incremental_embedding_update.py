"""
Incremental Embedding Update
============================
Aggiorna gli embedding solo per i film nuovi in movies_catalog.
Eseguito automaticamente alle 00:30 dopo l'aggiornamento film di mezzanotte.

- Trova film senza embedding
- Genera embeddings incrementali
- Aggiorna indice FAISS
"""

import os
import time
import pickle
import warnings
from typing import List, Set
from collections import Counter

warnings.filterwarnings("ignore")

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MultiLabelBinarizer
from pymongo import MongoClient

try:
    import faiss
except ImportError:
    os.system("pip install faiss-cpu")
    import faiss

# =============================================================================
# CONFIGURAZIONE
# =============================================================================
MONGO_URI = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
DB_NAME = os.getenv("MONGODB_DB", "cinematch_db")
COLL_CATALOG = "movies_catalog"
COLL_VECTORS = "bert_movies_vectors_bge"
BERT_MODEL = "intfloat/multilingual-e5-base"  # 768 dim
BATCH_SIZE = 32

# FAISS files
INDEX_FILE = "/app/data/faiss_bge.index"
MAPPING_FILE = "/app/data/faiss_bge_mapping.pkl"

# Limiti vocabolario (da mantenere coerenti con script iniziale)
MAX_ACTORS = 3000
MAX_DIRECTORS = 1000


def parse_comma_separated(value: str) -> List[str]:
    if not value:
        return []
    return [x.strip().lower() for x in value.split(",") if x.strip()]


def find_missing_films(client, db_name: str) -> List[dict]:
    """Trova film in catalog senza embedding usando aggregation (più efficiente)."""
    
    print("      Conteggio film nel catalogo...")
    catalog_count = client[db_name][COLL_CATALOG].estimated_document_count()
    print(f"      Film nel catalogo: {catalog_count}")
    
    print("      Conteggio embeddings esistenti...")
    try:
        vector_count = client[db_name][COLL_VECTORS].estimated_document_count()
    except:
        vector_count = 0
    print(f"      Embeddings esistenti: {vector_count}")
    
    # Usa aggregation per trovare film mancanti (più efficiente)
    print("      Ricerca film mancanti...")
    pipeline = [
        # 1. Prendi tutti i film dal catalogo
        {"$match": {"imdb_id": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$project": {"imdb_id": 1, "title": 1, "description": 1, "genres": 1, "actors": 1, "director": 1}},
        # 2. Lookup per vedere se esiste embedding
        {"$lookup": {
            "from": COLL_VECTORS,
            "localField": "imdb_id",
            "foreignField": "imdb_id",
            "as": "embedding_exists"
        }},
        # 3. Filtra solo quelli senza embedding
        {"$match": {"embedding_exists": {"$size": 0}}},
        # 4. Rimuovi il campo di join
        {"$project": {"embedding_exists": 0, "_id": 0}}
    ]
    
    missing_films = list(client[db_name][COLL_CATALOG].aggregate(pipeline))
    
    return missing_films


def rebuild_vocabularies(client, db_name: str):
    """
    Ricostruisce i vocabolari COMPLETI da tutti i film.
    Necessario per mantenere coerenza con Multi-hot encoding.
    """
    all_genres = set()
    actor_counter = Counter()
    director_counter = Counter()
    
    # Scan TUTTI i film con embedding (inclusi vecchi)
    all_vectors = client[db_name][COLL_VECTORS].find(
        {},
        {"genres_list": 1, "actors_list": 1, "director_list": 1}
    )
    
    for v in all_vectors:
        all_genres.update(v.get("genres_list", []))
        actor_counter.update(v.get("actors_list", []))
        director_counter.update(v.get("director_list", []))
    
    top_actors = [a for a, _ in actor_counter.most_common(MAX_ACTORS)]
    top_directors = [d for d, _ in director_counter.most_common(MAX_DIRECTORS)]
    
    return sorted(all_genres), top_actors, top_directors


def generate_incremental_embeddings(missing_films: List[dict], all_genres: List[str], 
                                     top_actors: List[str], top_directors: List[str]):
    """Genera embeddings solo per i film mancanti."""
    
    print("\n[2/5] Caricamento modello E5...")
    model = SentenceTransformer(BERT_MODEL)
    print(f"      Dimensione embedding: {model.get_sentence_embedding_dimension()}")
    
    print("\n[3/5] Preprocessing nuovi film...")
    movies_data = []
    
    for m in missing_films:
        # Parse genres
        raw_genres = m.get("genres", [])
        if isinstance(raw_genres, list):
            g = [x.strip().lower() for x in raw_genres if x and isinstance(x, str)]
        else:
            g = parse_comma_separated(raw_genres) if raw_genres else []
        
        a = parse_comma_separated(m.get("actors", ""))
        d = parse_comma_separated(m.get("director", ""))
        
        movies_data.append({
            "imdb_id": m.get("imdb_id", ""),
            "title": m.get("title", ""),
            "description": m.get("description", ""),
            "genres_list": g,
            "actors_list": a,
            "director_list": d
        })
    
    print(f"      Nuovi film processati: {len(movies_data)}")
    
    print("\n[4/5] Multi-Hot encodings...")
    
    # Usa i vocabolari COMPLETI esistenti
    mlb_genre = MultiLabelBinarizer(classes=all_genres)
    genre_enc = mlb_genre.fit_transform([m["genres_list"] for m in movies_data]).astype(np.float32)
    
    mlb_actors = MultiLabelBinarizer(classes=top_actors)
    actors_filtered = [[a for a in m["actors_list"] if a in set(top_actors)] for m in movies_data]
    actors_enc = mlb_actors.fit_transform(actors_filtered).astype(np.float32)
    
    mlb_directors = MultiLabelBinarizer(classes=top_directors)
    directors_filtered = [[d for d in m["director_list"] if d in set(top_directors)] for m in movies_data]
    director_enc = mlb_directors.fit_transform(directors_filtered).astype(np.float32)
    
    print(f"      Genre: {genre_enc.shape[1]} dim")
    print(f"      Actors: {actors_enc.shape[1]} dim")
    print(f"      Directors: {director_enc.shape[1]} dim")
    
    print("\n[5/5] Generazione embeddings descrizioni...")
    descriptions = [m["description"] if m["description"] else "No description" for m in movies_data]
    
    t0 = time.time()
    desc_emb = model.encode(
        descriptions,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True
    )
    print(f"      Tempo: {time.time()-t0:.1f}s")
    
    # Costruisci documenti
    new_docs = []
    for i, m in enumerate(movies_data):
        new_docs.append({
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
    
    return new_docs, desc_emb


def update_faiss_index(new_embeddings: np.ndarray, new_ids: List[dict]):
    """Aggiorna l'indice FAISS aggiungendo nuovi vettori."""
    
    print("\n[6/7] Caricamento indice FAISS esistente...")
    
    if not os.path.exists(INDEX_FILE) or not os.path.exists(MAPPING_FILE):
        print("      ⚠️ Indice FAISS non trovato! Usa faiss_index_builder_bge.py")
        return
    
    # Load existing
    index = faiss.read_index(INDEX_FILE)
    with open(MAPPING_FILE, "rb") as f:
        id_mapping = pickle.load(f)
    
    old_count = index.ntotal
    print(f"      Vettori esistenti: {old_count}")
    
    print("\n[7/7] Aggiunta nuovi vettori...")
    
    # Normalize new embeddings
    new_embeddings_normalized = new_embeddings.copy()
    faiss.normalize_L2(new_embeddings_normalized)
    
    # Add to index
    index.add(new_embeddings_normalized)
    
    # Update mapping
    id_mapping.extend(new_ids)
    
    print(f"      Totale vettori: {index.ntotal} (+{index.ntotal - old_count})")
    
    # Save updated index
    faiss.write_index(index, INDEX_FILE)
    with open(MAPPING_FILE, "wb") as f:
        pickle.dump(id_mapping, f)
    
    print(f"      ✅ Indice aggiornato: {INDEX_FILE}")


def main():
    print("=" * 60)
    print("INCREMENTAL EMBEDDING UPDATE")
    print("=" * 60)
    
    client = MongoClient(MONGO_URI)
    
    # 1. Find missing films
    print("\n[1/5] Ricerca film senza embedding...")
    missing_films = find_missing_films(client, DB_NAME)
    
    if not missing_films:
        print("      ✅ Nessun film nuovo. Indice già aggiornato!")
        client.close()
        return
    
    print(f"      🆕 Nuovi film trovati: {len(missing_films)}")
    
    # 2. Rebuild vocabularies from ALL existing embeddings
    print("\n[2/5] Ricostruzione vocabolari...")
    all_genres, top_actors, top_directors = rebuild_vocabularies(client, DB_NAME)
    print(f"      Generi: {len(all_genres)}")
    print(f"      Top Actors: {len(top_actors)}")
    print(f"      Top Directors: {len(top_directors)}")
    
    # 3. Generate embeddings for new films
    new_docs, desc_embeddings = generate_incremental_embeddings(
        missing_films, all_genres, top_actors, top_directors
    )
    
    # 4. Save to MongoDB
    print("\n[6/7] Salvataggio nuovi embeddings in MongoDB...")
    coll = client[DB_NAME][COLL_VECTORS]
    if new_docs:
        coll.insert_many(new_docs)
        print(f"      ✅ Salvati {len(new_docs)} nuovi documenti")
    
    # 5. Update FAISS index
    new_mapping = [{"imdb_id": doc["imdb_id"], "title": doc["title"]} for doc in new_docs]
    update_faiss_index(desc_embeddings, new_mapping)
    
    client.close()
    
    print("\n" + "=" * 60)
    print("COMPLETATO!")
    print(f"Nuovi film processati: {len(missing_films)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
