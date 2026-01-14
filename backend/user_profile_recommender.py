"""
User Profile Recommender
========================
Genera un profilo utente basato sui suoi voti (ratings.csv) e suggerisce film.

Il "taste vector" dell'utente è una media pesata degli embedding dei film che ha votato,
dove il peso è proporzionale al voto dato.
"""

import os
import pickle
import time
import csv
import numpy as np
from pymongo import MongoClient

try:
    import faiss
except ImportError:
    os.system("pip install faiss-cpu")
    import faiss

# --- CONFIGURAZIONE ---
MONGO_URI = "mongodb://mongodb:27017"
DB_NAME = "films"
COLL_CATALOG = "movies_catalog"
COLL_VECTORS = "bert_movies_vectors_bge"

# File ratings utente
RATINGS_FILE = "/app/codice/ratings.csv"

# File indice FAISS
INDEX_FILE = "/app/data/faiss_bge.index"
MAPPING_FILE = "/app/data/faiss_bge_mapping.pkl"

# --- CONFIGURAZIONE RACCOMANDAZIONI ---
TOP_K_CANDIDATES = 500   # Candidati da FAISS
N_RECOMMENDATIONS = 20   # Numero di film da suggerire
N_NOT_RECOMMENDED = 10   # Numero di film meno consigliati
MIN_YEAR_NOT_REC = 2010  # Anno minimo per i non consigliati
MIN_VOTES_NOT_REC = 5000 # Minimo voti per i non consigliati (evita film sconosciuti)

# --- PESI RE-RANKING (similarità) ---
W_DESCRIPTION = 0.4
W_GENRE = 0.1
W_ACTORS = 0.30
W_DIRECTOR = 0.20

# --- QUALITY FACTOR (IMDb) ---
ALPHA_SIMILARITY = 0.70  # Peso della similarità
BETA_QUALITY = 0.30      # Peso del rating IMDb


def cosine_similarity(v1, v2):
    if v1 is None or v2 is None:
        return 0.0
    v1 = np.array(v1, dtype=np.float32)
    v2 = np.array(v2, dtype=np.float32)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(np.dot(v1, v2) / (norm1 * norm2))


def load_user_ratings(filepath: str) -> list:
    """Carica i ratings dell'utente dal CSV."""
    ratings = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('Name', '').strip()
            year = row.get('Year', '')
            rating = row.get('Rating', '')
            
            if name and rating:
                try:
                    rating_val = float(rating)
                    year_val = int(year) if year else None
                    ratings.append({
                        'name': name,
                        'year': year_val,
                        'rating': rating_val
                    })
                except ValueError:
                    continue
    
    return ratings


def build_user_taste_vectors(ratings: list, db, coll_vectors: str) -> dict:
    """
    Costruisce i vettori "gusto utente" come media pesata degli embedding
    dei film votati, dove il peso è il voto normalizzato.
    """
    # Raccogli embedding dei film votati
    desc_vectors = []
    genre_vectors = []
    actors_vectors = []
    director_vectors = []
    weights = []
    matched_films = []
    
    for r in ratings:
        # Cerca il film nel catalogo - prova original_title prima, poi title
        import re
        escaped_name = re.escape(r['name'])
        
        # 1. original_title + anno
        query = {"original_title": {"$regex": f"^{escaped_name}$", "$options": "i"}}
        if r['year']:
            query["year"] = r['year']
        film = db[COLL_CATALOG].find_one(query)
        
        # 2. original_title senza anno
        if not film:
            film = db[COLL_CATALOG].find_one({
                "original_title": {"$regex": f"^{escaped_name}$", "$options": "i"}
            })
        
        # 3. title + anno
        if not film:
            query = {"title": {"$regex": f"^{escaped_name}$", "$options": "i"}}
            if r['year']:
                query["year"] = r['year']
            film = db[COLL_CATALOG].find_one(query)
        
        # 4. title senza anno
        if not film:
            film = db[COLL_CATALOG].find_one({
                "title": {"$regex": f"^{escaped_name}$", "$options": "i"}
            })
        
        if not film:
            continue
        
        # Recupera embedding
        vec = db[coll_vectors].find_one({"imdb_title_id": film["imdb_title_id"]})
        if not vec:
            continue
        
        # Peso = rating normalizzato (0-5 -> 0-1) con bias verso rating alti
        # Rating 5 = peso 1.0, Rating 1 = peso 0.2
        weight = r['rating'] / 5.0
        
        desc_vectors.append(np.array(vec["embedding_description"], dtype=np.float32))
        genre_vectors.append(np.array(vec["embedding_genre"], dtype=np.float32))
        actors_vectors.append(np.array(vec["embedding_actors"], dtype=np.float32))
        director_vectors.append(np.array(vec["embedding_director"], dtype=np.float32))
        weights.append(weight)
        matched_films.append({
            "title": film["title"],
            "year": film.get("year"),
            "rating": r['rating'],
            "imdb_title_id": film["imdb_title_id"]
        })
    
    if not weights:
        return None, []
    
    # Normalizza pesi
    weights = np.array(weights, dtype=np.float32)
    weights = weights / weights.sum()
    
    # Media pesata per ogni tipo di embedding
    user_desc = np.average(desc_vectors, axis=0, weights=weights)
    user_genre = np.average(genre_vectors, axis=0, weights=weights)
    user_actors = np.average(actors_vectors, axis=0, weights=weights)
    user_director = np.average(director_vectors, axis=0, weights=weights)
    
    # Normalizza vettori risultanti
    user_desc = user_desc / (np.linalg.norm(user_desc) + 1e-8)
    
    return {
        "description": user_desc,
        "genre": user_genre,
        "actors": user_actors,
        "director": user_director
    }, matched_films


def main():
    print("=" * 60)
    print("USER PROFILE RECOMMENDER")
    print("=" * 60)
    
    # 1. Carica ratings utente
    print("\n[1] Caricamento ratings utente...")
    
    if not os.path.exists(RATINGS_FILE):
        print(f"    ERRORE: {RATINGS_FILE} non trovato!")
        return
    
    user_ratings = load_user_ratings(RATINGS_FILE)
    print(f"    Ratings caricati: {len(user_ratings)}")
    
    # Statistiche ratings
    avg_rating = sum(r['rating'] for r in user_ratings) / len(user_ratings)
    high_rated = [r for r in user_ratings if r['rating'] >= 4.5]
    print(f"    Rating medio: {avg_rating:.2f}")
    print(f"    Film con rating >= 4.5: {len(high_rated)}")
    
    # 2. Connessione MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 3. Costruisci profilo utente
    print("\n[2] Costruzione profilo utente...")
    
    user_vectors, matched_films = build_user_taste_vectors(
        user_ratings, db, COLL_VECTORS
    )
    
    if not user_vectors:
        print("    ERRORE: Nessun film trovato nel database!")
        client.close()
        return
    
    print(f"    Film matchati: {len(matched_films)}/{len(user_ratings)}")
    
    # Set di film già visti (da escludere)
    seen_ids = set(f["imdb_title_id"] for f in matched_films)
    
    # Mostra top film preferiti
    print("\n    Top film preferiti dall'utente:")
    top_rated = sorted(matched_films, key=lambda x: x['rating'], reverse=True)[:5]
    for f in top_rated:
        print(f"      - {f['title']} ({f['year']}) [Rating: {f['rating']}]")
    
    # 4. Carica indice FAISS
    print("\n[3] Caricamento indice FAISS...")
    
    if not os.path.exists(INDEX_FILE):
        print(f"    ERRORE: {INDEX_FILE} non trovato!")
        client.close()
        return
    
    index = faiss.read_index(INDEX_FILE)
    with open(MAPPING_FILE, "rb") as f:
        id_mapping = pickle.load(f)
    
    print(f"    Indice: {index.ntotal} vettori")
    
    # 5. FAISS retrieval con profilo utente
    print(f"\n[4] FAISS retrieval (top-{TOP_K_CANDIDATES})...")
    
    user_desc_query = user_vectors["description"].reshape(1, -1).copy()
    faiss.normalize_L2(user_desc_query)
    
    t0 = time.time()
    distances, indices = index.search(user_desc_query, TOP_K_CANDIDATES + len(seen_ids))
    faiss_time = (time.time() - t0) * 1000
    
    print(f"    Tempo FAISS: {faiss_time:.1f}ms")
    
    # Filtra film già visti
    faiss_candidates = []
    faiss_scores = {}
    for dist, idx in zip(distances[0], indices[0]):
        if idx >= 0:
            movie_id = id_mapping[idx]["imdb_title_id"]
            if movie_id not in seen_ids:
                faiss_candidates.append(movie_id)
                faiss_scores[movie_id] = float(dist)
    
    print(f"    Candidati (esclusi già visti): {len(faiss_candidates)}")
    
    # 6. Re-ranking
    print("\n[5] Re-ranking...")
    
    t0 = time.time()
    
    # Recupera embedding candidati
    candidate_vecs = {
        doc["imdb_title_id"]: doc 
        for doc in db[COLL_VECTORS].find({"imdb_title_id": {"$in": faiss_candidates}})
    }
    
    # Recupera rating IMDb dal catalogo
    candidate_catalog = {
        doc["imdb_title_id"]: doc 
        for doc in db[COLL_CATALOG].find(
            {"imdb_title_id": {"$in": faiss_candidates}},
            {"imdb_title_id": 1, "avg_vote": 1, "votes": 1, "title": 1, "year": 1, "genre": 1, "director": 1}
        )
    }
    
    reranked = []
    for cand_id in faiss_candidates:
        vec = candidate_vecs.get(cand_id)
        catalog = candidate_catalog.get(cand_id)
        if not vec:
            continue
        
        # Calcola similarità
        sim_desc = faiss_scores.get(cand_id, 0)
        sim_genre = cosine_similarity(user_vectors["genre"], vec.get("embedding_genre"))
        sim_actors = cosine_similarity(user_vectors["actors"], vec.get("embedding_actors"))
        sim_director = cosine_similarity(user_vectors["director"], vec.get("embedding_director"))
        
        similarity_score = (sim_desc * W_DESCRIPTION + 
                            sim_genre * W_GENRE + 
                            sim_actors * W_ACTORS + 
                            sim_director * W_DIRECTOR)
        
        # Quality score con Bayesian Weighted Rating
        # Formula: weighted = (v/(v+m))*R + (m/(v+m))*C
        # Dove: v=votes, m=min_votes, R=avg_vote, C=rating medio globale (es. 6.0)
        avg_vote = 5.0  # Default se mancante
        votes = 0
        if catalog:
            try:
                avg_vote = float(catalog.get("avg_vote", 5.0))
            except (ValueError, TypeError):
                avg_vote = 5.0
            try:
                votes = int(catalog.get("votes", 0))
            except (ValueError, TypeError):
                votes = 0
        
        # Parametri Bayesian
        MIN_VOTES = 1000  # Minimo voti per fiducia piena
        GLOBAL_MEAN = 6.0  # Rating medio globale (benchmark)
        
        # Weighted rating: più voti → più fiducia nel rating reale
        if votes > 0:
            weighted_rating = (votes / (votes + MIN_VOTES)) * avg_vote + (MIN_VOTES / (votes + MIN_VOTES)) * GLOBAL_MEAN
        else:
            weighted_rating = GLOBAL_MEAN  # Nessun voto → usa media globale
        
        quality_score = weighted_rating / 10.0
        
        # Score finale: α * similarity + β * quality
        final_score = (ALPHA_SIMILARITY * similarity_score + 
                       BETA_QUALITY * quality_score)
        
        reranked.append({
            "imdb_title_id": cand_id,
            "title": vec.get("title", "Unknown"),
            "final_score": final_score,
            "similarity_score": similarity_score,
            "quality_score": quality_score,
            "imdb_rating": avg_vote,
            "votes": votes,
            "weighted_rating": weighted_rating,
            "sim_desc": sim_desc,
            "sim_genre": sim_genre,
            "sim_actors": sim_actors,
            "sim_director": sim_director
        })
    
    rerank_time = (time.time() - t0) * 1000
    print(f"    Tempo re-ranking: {rerank_time:.1f}ms")
    
    reranked.sort(key=lambda x: x["final_score"], reverse=True)
    recommendations = reranked[:N_RECOMMENDATIONS]
    
    # 7. Output CONSIGLIATI
    print("\n" + "=" * 60)
    print(f"TOP {N_RECOMMENDATIONS} FILM CONSIGLIATI PER TE")
    print("=" * 60)
    print(f"Basato su {len(matched_films)} film che hai votato")
    print(f"Pesi similarità: desc={W_DESCRIPTION}, genre={W_GENRE}, actors={W_ACTORS}, dir={W_DIRECTOR}")
    print(f"Score finale: {ALPHA_SIMILARITY:.0%} similarità + {BETA_QUALITY:.0%} IMDb")
    print("-" * 60)
    
    for i, rec in enumerate(recommendations, 1):
        details = db[COLL_CATALOG].find_one({"imdb_title_id": rec["imdb_title_id"]})
        
        title = details.get("title", rec["title"]) if details else rec["title"]
        year = details.get("year", "N/A") if details else "N/A"
        genre = details.get("genre", "N/A") if details else "N/A"
        director = details.get("director", "N/A") if details else "N/A"
        
        print(f"\n{i}. {title} ({year})")
        print(f"   Score: {rec['final_score']:.4f} (sim={rec['similarity_score']:.3f})")
        print(f"   IMDb: {rec['imdb_rating']:.1f}/10 ({rec['votes']:,} voti) → weighted: {rec['weighted_rating']:.2f}")
        print(f"   Genre: {genre}")
        print(f"   Director: {director}")
        print(f"   [desc={rec['sim_desc']:.3f}, genre={rec['sim_genre']:.3f}, "
              f"actors={rec['sim_actors']:.3f}, director={rec['sim_director']:.3f}]")
    
    # 8. FILM MENO CONSIGLIATI (approccio scalabile)
    print("\n" + "=" * 60)
    print(f"TOP {N_NOT_RECOMMENDED} FILM MENO CONSIGLIATI PER TE")
    print("=" * 60)
    print(f"(Film recenti dal {MIN_YEAR_NOT_REC}+ con almeno {MIN_VOTES_NOT_REC:,} voti)")
    print("-" * 60)
    
    # Approccio scalabile: campiona film recenti e popolari, calcola solo per quelli
    # Non scansioniamo tutti gli 80k film
    not_rec_candidates = list(db[COLL_CATALOG].aggregate([
        {"$match": {
            "year": {"$gte": MIN_YEAR_NOT_REC},
            "votes": {"$gte": MIN_VOTES_NOT_REC},
            "imdb_title_id": {"$nin": list(seen_ids)}
        }},
        {"$sample": {"size": 500}}  # Campiona 500 film random che soddisfano i criteri
    ]))
    
    not_rec_ids = [c["imdb_title_id"] for c in not_rec_candidates]
    
    # Recupera embedding
    not_rec_vecs = {
        doc["imdb_title_id"]: doc 
        for doc in db[COLL_VECTORS].find({"imdb_title_id": {"$in": not_rec_ids}})
    }
    
    # Calcola score per i candidati
    not_recommended = []
    for cand in not_rec_candidates:
        cand_id = cand["imdb_title_id"]
        vec = not_rec_vecs.get(cand_id)
        if not vec:
            continue
        
        # Calcola similarità
        cand_desc = np.array(vec.get("embedding_description", []), dtype=np.float32)
        if len(cand_desc) > 0:
            cand_desc_norm = cand_desc / (np.linalg.norm(cand_desc) + 1e-8)
            user_desc_flat = user_vectors["description"] / (np.linalg.norm(user_vectors["description"]) + 1e-8)
            sim_desc = float(np.dot(user_desc_flat, cand_desc_norm))
        else:
            sim_desc = 0.0
        
        sim_genre = cosine_similarity(user_vectors["genre"], vec.get("embedding_genre"))
        sim_actors = cosine_similarity(user_vectors["actors"], vec.get("embedding_actors"))
        sim_director = cosine_similarity(user_vectors["director"], vec.get("embedding_director"))
        
        similarity_score = (sim_desc * W_DESCRIPTION + 
                            sim_genre * W_GENRE + 
                            sim_actors * W_ACTORS + 
                            sim_director * W_DIRECTOR)
        
        # Quality score (uguale ai consigliati - film ben votati)
        avg_vote = float(cand.get("avg_vote", 5.0)) if cand.get("avg_vote") else 5.0
        votes = int(cand.get("votes", 0)) if cand.get("votes") else 0
        
        MIN_VOTES = 1000
        GLOBAL_MEAN = 6.0
        if votes > 0:
            weighted_rating = (votes / (votes + MIN_VOTES)) * avg_vote + (MIN_VOTES / (votes + MIN_VOTES)) * GLOBAL_MEAN
        else:
            weighted_rating = GLOBAL_MEAN
        quality_score = weighted_rating / 10.0
        
        # Score per "meno consigliati": BASSA similarità + ALTA qualità
        # Vogliamo film che piacciono agli altri ma probabilmente non all'utente
        # Formula: (1 - similarity) * α + quality * β
        # Film con alta qualità ma bassa affinità → score alto → top della lista
        dissimilarity_score = 1.0 - similarity_score
        not_rec_score = (ALPHA_SIMILARITY * dissimilarity_score + BETA_QUALITY * quality_score)
        
        not_recommended.append({
            "imdb_title_id": cand_id,
            "title": cand.get("title", "Unknown"),
            "year": cand.get("year", "N/A"),
            "genre": cand.get("genre", "N/A"),
            "director": cand.get("director", "N/A"),
            "not_rec_score": not_rec_score,
            "similarity_score": similarity_score,
            "dissimilarity_score": dissimilarity_score,
            "imdb_rating": avg_vote,
            "votes": votes,
            "weighted_rating": weighted_rating
        })
    
    # Ordina per not_rec_score DECRESCENTE (alto = meno consigliato per te ma apprezzato da altri)
    not_recommended.sort(key=lambda x: x["not_rec_score"], reverse=True)
    not_rec_results = not_recommended[:N_NOT_RECOMMENDED]
    
    for i, rec in enumerate(not_rec_results, 1):
        print(f"\n{i}. {rec['title']} ({rec['year']})")
        print(f"   Not-Rec Score: {rec['not_rec_score']:.4f} (dissim={rec['dissimilarity_score']:.3f})")
        print(f"   IMDb: {rec['imdb_rating']:.1f}/10 ({rec['votes']:,} voti) → weighted: {rec['weighted_rating']:.2f}")
        print(f"   Genre: {rec['genre']}")
        print(f"   Director: {rec['director']}")
    
    client.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
