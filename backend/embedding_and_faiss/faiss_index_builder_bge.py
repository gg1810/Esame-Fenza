"""
FAISS Index Builder per BGE-M3
==============================
Costruisce indice FAISS dagli embedding BGE-M3.
"""

import os
import pickle
import numpy as np
from pymongo import MongoClient

try:
    import faiss
except ImportError:
    os.system("pip install faiss-cpu")
    import faiss

# --- CONFIGURAZIONE ---
MONGO_URI = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
DB_NAME = os.getenv("MONGODB_DB", "cinematch_db")
COLL_VECTORS = "bert_movies_vectors_bge"  # Collezione BGE

# Output files
INDEX_FILE = "/app/data/faiss_bge.index"
MAPPING_FILE = "/app/data/faiss_bge_mapping.pkl"


def main():
    print("=" * 60)
    print("FAISS INDEX BUILDER (BGE-M3)")
    print("=" * 60)
    
    print("\n[1] Caricamento embedding da MongoDB...")
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_VECTORS]
    
    # Pre-carica original_title da movies_catalog
    catalog_titles = {}
    catalog_cursor = client[DB_NAME]["movies_catalog"].find({}, {"imdb_id": 1, "original_title": 1, "title": 1, "_id": 0})
    for c in catalog_cursor:
        imdb_id = c.get("imdb_id")
        if imdb_id:
            catalog_titles[imdb_id] = c.get("original_title") or c.get("title") or "Unknown"
    print(f"    Titoli da catalogo: {len(catalog_titles)}")
    
    cursor = coll.find({}, {
        "imdb_id": 1,
        "embedding_description": 1,
        "_id": 0
    })
    
    documents = list(cursor)
    client.close()
    
    print(f"    Documenti: {len(documents)}")
    
    if not documents:
        raise ValueError(f"Nessun documento in {COLL_VECTORS}! Esegui prima bge_embedding_generator.py")
    
    print("\n[2] Preparazione matrici...")
    
    embeddings = []
    id_mapping = []
    
    for doc in documents:
        emb = doc.get("embedding_description")
        if emb and len(emb) > 0:
            embeddings.append(emb)
            imdb_id = doc["imdb_id"]
            # Usa original_title da catalogo, con fallback
            title = catalog_titles.get(imdb_id, "Unknown")
            id_mapping.append({
                "imdb_id": imdb_id,
                "title": title
            })
    
    embeddings_matrix = np.array(embeddings, dtype=np.float32)
    print(f"    Matrice: {embeddings_matrix.shape}")
    
    faiss.normalize_L2(embeddings_matrix)
    
    print("\n[3] Costruzione indice FAISS...")
    
    dimension = embeddings_matrix.shape[1]
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings_matrix)
    print(f"    Vettori indicizzati: {index.ntotal}")
    
    print("\n[4] Salvataggio...")
    
    os.makedirs(os.path.dirname(INDEX_FILE), exist_ok=True)
    faiss.write_index(index, INDEX_FILE)
    print(f"    Indice: {INDEX_FILE}")
    
    with open(MAPPING_FILE, "wb") as f:
        pickle.dump(id_mapping, f)
    print(f"    Mapping: {MAPPING_FILE}")
    
    print("\n[5] Test veloce...")
    query = embeddings_matrix[0:1]
    distances, indices = index.search(query, 5)
    
    print(f"    Query: {id_mapping[0]['title']}")
    for i, (dist, idx) in enumerate(zip(distances[0], indices[0])):
        if idx >= 0:
            print(f"      {i+1}. {id_mapping[idx]['title']} ({dist:.4f})")
    
    print("\n" + "=" * 60)
    print("COMPLETATO!")
    print("=" * 60)


if __name__ == "__main__":
    main()
