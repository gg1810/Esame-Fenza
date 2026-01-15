"""
Recommendation Service
======================
Generates personalized movie recommendations based on user's rated films.
Uses FAISS for fast similarity search and multi-dimensional scoring.
"""

import os
import pickle
import re
import numpy as np
import pytz
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient

try:
    import faiss
except ImportError:
    os.system("pip install faiss-cpu")
    import faiss

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGODB_URL", "mongodb://mongodb:27017")
DB_NAME = os.getenv("MONGODB_DB", "cinematch_db")
COLL_CATALOG = "movies_catalog"
COLL_VECTORS = "bert_movies_vectors_bge"
COLL_MOVIES = "movies"  # User rated movies collection

# FAISS files
INDEX_FILE = "/app/data/faiss_bge.index"
MAPPING_FILE = "/app/data/faiss_bge_mapping.pkl"

# Recommendation params
TOP_K_CANDIDATES = 500
N_RECOMMENDATIONS = 11
N_NOT_RECOMMENDED = 3
MIN_YEAR_NOT_REC = 2010
MIN_VOTES_NOT_REC = 5000

# Re-ranking weights
W_DESCRIPTION = 0.4
W_GENRE = 0.1
W_ACTORS = 0.30
W_DIRECTOR = 0.20

# Quality factor
ALPHA_SIMILARITY = 0.70
BETA_QUALITY = 0.30


def cosine_similarity(v1, v2) -> float:
    """Calculate cosine similarity between two vectors."""
    if v1 is None or v2 is None:
        return 0.0
    v1 = np.array(v1, dtype=np.float32)
    v2 = np.array(v2, dtype=np.float32)
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(np.dot(v1, v2) / (norm1 * norm2))


def parse_comma_separated(value: str) -> List[str]:
    """Parse comma-separated string into list."""
    if not value:
        return []
    return [x.strip().lower() for x in value.split(",") if x.strip()]


class RecommendationService:
    """Service for generating personalized movie recommendations."""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.index = None
        self.id_mapping = None
        self._initialized = False
    
    def _ensure_initialized(self):
        """Lazy initialization of database and FAISS index."""
        if self._initialized:
            return True
            
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[DB_NAME]
            
            # Load FAISS index
            if os.path.exists(INDEX_FILE) and os.path.exists(MAPPING_FILE):
                self.index = faiss.read_index(INDEX_FILE)
                with open(MAPPING_FILE, "rb") as f:
                    self.id_mapping = pickle.load(f)
                self._initialized = True
                return True
            else:
                print(f"⚠️ FAISS index not found at {INDEX_FILE}")
                return False
        except Exception as e:
            print(f"❌ Error initializing RecommendationService: {e}")
            return False
    
    def _find_film_in_catalog(self, name: str, year: Optional[int]) -> Optional[dict]:
        """Find a film in the catalog by name and year."""
        escaped_name = re.escape(name)
        
        # 1. original_title + year
        query = {"original_title": {"$regex": f"^{escaped_name}$", "$options": "i"}}
        if year:
            query["year"] = year
        film = self.db[COLL_CATALOG].find_one(query)
        
        # 2. original_title without year
        if not film:
            film = self.db[COLL_CATALOG].find_one({
                "original_title": {"$regex": f"^{escaped_name}$", "$options": "i"}
            })
        
        # 3. title + year
        if not film:
            query = {"title": {"$regex": f"^{escaped_name}$", "$options": "i"}}
            if year:
                query["year"] = year
            film = self.db[COLL_CATALOG].find_one(query)
        
        # 4. title without year
        if not film:
            film = self.db[COLL_CATALOG].find_one({
                "title": {"$regex": f"^{escaped_name}$", "$options": "i"}
            })
        
        return film
    
    def _build_user_taste_vectors(self, user_movies: List[dict]) -> Tuple[Optional[dict], List[dict]]:
        """Build user taste vectors from rated movies."""
        desc_vectors = []
        genre_vectors = []
        actors_vectors = []
        director_vectors = []
        weights = []
        matched_films = []
        
        for m in user_movies:
            # Find film in catalog
            film = self._find_film_in_catalog(m.get("name", ""), m.get("year"))
            if not film:
                continue
            
            # Get embedding vector
            vec = self.db[COLL_VECTORS].find_one({"imdb_id": film.get("imdb_id")})
            if not vec:
                continue
            
            # Weight = normalized rating
            rating = m.get("rating", 3)
            weight = rating / 5.0
            
            desc_vectors.append(np.array(vec["embedding_description"], dtype=np.float32))
            genre_vectors.append(np.array(vec["embedding_genre"], dtype=np.float32))
            actors_vectors.append(np.array(vec["embedding_actors"], dtype=np.float32))
            director_vectors.append(np.array(vec["embedding_director"], dtype=np.float32))
            weights.append(weight)
            matched_films.append({
                "title": film.get("original_title") or film.get("title"),
                "year": film.get("year"),
                "rating": rating,
                "imdb_id": film.get("imdb_id")
            })
        
        if not weights:
            return None, []
        
        # Normalize weights
        weights = np.array(weights, dtype=np.float32)
        weights = weights / weights.sum()
        
        # Weighted average for each embedding type
        user_desc = np.average(desc_vectors, axis=0, weights=weights)
        user_genre = np.average(genre_vectors, axis=0, weights=weights)
        user_actors = np.average(actors_vectors, axis=0, weights=weights)
        user_director = np.average(director_vectors, axis=0, weights=weights)
        
        # Normalize
        user_desc = user_desc / (np.linalg.norm(user_desc) + 1e-8)
        
        return {
            "description": user_desc,
            "genre": user_genre,
            "actors": user_actors,
            "director": user_director
        }, matched_films
    
    def _calculate_quality_score(self, avg_vote: float, votes: int) -> float:
        """Calculate Bayesian weighted quality score."""
        MIN_VOTES = 1000
        GLOBAL_MEAN = 6.0
        
        if votes > 0:
            weighted_rating = (votes / (votes + MIN_VOTES)) * avg_vote + (MIN_VOTES / (votes + MIN_VOTES)) * GLOBAL_MEAN
        else:
            weighted_rating = GLOBAL_MEAN
        
        return weighted_rating / 10.0
    
    def get_recommendations(self, user_id: str, force_refresh: bool = False) -> dict:
        """
        Generate personalized recommendations for a user.
        Uses caching: only recalculates if user has rated new films.
        
        Returns:
            dict with 'recommended', 'not_recommended', 'matched_films', 'total_films'
        """
        if not self._ensure_initialized():
            return {
                "error": "Recommendation service not available. FAISS index not found.",
                "recommended": [],
                "not_recommended": []
            }
        
        # 0. Enforce "Spark Stats First" policy
        # Check if user_stats are up-to-date with user data
        user = self.db["users"].find_one({"user_id": user_id})
        
        if user:
            data_updated = user.get("data_updated_at")
            if data_updated:
                # Get stats updated time
                stats = self.db["user_stats"].find_one({"user_id": user_id}, {"updated_at": 1})
                stats_updated = stats.get("updated_at") if stats else None
                
                # If stats are missing OR stats are older than data update -> Processing
                if not stats_updated or stats_updated < data_updated:
                    print(f"⏳ Stats processing: Data {data_updated} > Stats {stats_updated}")
                    return {
                        "status": "processing",
                        "message": "Statistiche in elaborazione. Riprova tra pochi secondi...",
                        "recommended": [],
                        "not_recommended": []
                    }

        # Check for cached recommendations
        if user and not force_refresh:
            cached = user.get("recommendations")
            
            if cached and cached.get("generated_at"):
                cache_time = cached.get("generated_at")
                data_updated = user.get("data_updated_at") or ""
                
                print(f"🔍 Cache: generated_at={cache_time}, data_updated_at={data_updated}")
                
                # Use cache if it exists and is newer than last data update
                if cache_time >= data_updated:
                    print("✅ Returning cached recommendations")
                    # Cache is fresh - enrich with catalog data
                    cached_rec = cached.get("recommended", [])
                    cached_not_rec = cached.get("not_recommended", [])
                    
                    # Get all imdb_ids to fetch from catalog
                    all_ids = [m["imdb_id"] for m in cached_rec + cached_not_rec if m.get("imdb_id")]
                    
                    # Batch fetch from catalog
                    catalog_map = {
                        doc["imdb_id"]: doc
                        for doc in self.db[COLL_CATALOG].find(
                            {"imdb_id": {"$in": all_ids}},
                            {"imdb_id": 1, "title": 1, "original_title": 1, "year": 1, 
                             "poster_url": 1, "genres": 1, "director": 1, "avg_vote": 1}
                        )
                    }
                    
                    # Enrich cached data
                    def enrich_movie(m):
                        catalog = catalog_map.get(m.get("imdb_id"), {})
                        return {
                            "imdb_id": m.get("imdb_id"),
                            "title": m.get("title") or catalog.get("original_title") or catalog.get("title", "Unknown"),
                            "year": catalog.get("year"),
                            "poster": catalog.get("poster_url"),
                            "rating": catalog.get("avg_vote", 0),
                            "genres": catalog.get("genres", []) if isinstance(catalog.get("genres"), list) else [],
                            "director": catalog.get("director", ""),
                            "matchScore": m.get("matchScore", 0)
                        }
                    
                    return {
                        "recommended": [enrich_movie(m) for m in cached_rec],
                        "not_recommended": [enrich_movie(m) for m in cached_not_rec],
                        "matched_films": cached.get("matched_films", 0),
                        "total_films": cached.get("total_films", 0),
                        "cached": True
                    }
        
        # 1. Get user's rated movies from 'movies' collection
        user_movies = list(self.db[COLL_MOVIES].find({"user_id": user_id}))
        if not user_movies:
            return {
                "error": "No movies found. Please upload your Letterboxd CSV first.",
                "recommended": [],
                "not_recommended": []
            }
        
        # 2. Build user taste vectors
        user_vectors, matched_films = self._build_user_taste_vectors(user_movies)
        if not user_vectors:
            return {
                "error": "Could not match any movies in the catalog.",
                "recommended": [],
                "not_recommended": [],
                "matched_films": 0,
                "total_films": len(user_movies)
            }
        
        seen_ids = set(f["imdb_id"] for f in matched_films if f.get("imdb_id"))
        
        # 3. FAISS retrieval
        user_desc_query = user_vectors["description"].reshape(1, -1).copy()
        faiss.normalize_L2(user_desc_query)
        
        distances, indices = self.index.search(user_desc_query, TOP_K_CANDIDATES + len(seen_ids))
        
        # Filter out already seen movies
        faiss_candidates = []
        faiss_scores = {}
        for dist, idx in zip(distances[0], indices[0]):
            if idx >= 0:
                movie_id = self.id_mapping[idx]["imdb_id"]
                if movie_id not in seen_ids:
                    faiss_candidates.append(movie_id)
                    faiss_scores[movie_id] = float(dist)
        
        # 4. Re-ranking
        candidate_vecs = {
            doc["imdb_id"]: doc 
            for doc in self.db[COLL_VECTORS].find({"imdb_id": {"$in": faiss_candidates}})
        }
        
        candidate_catalog = {
            doc["imdb_id"]: doc 
            for doc in self.db[COLL_CATALOG].find(
                {"imdb_id": {"$in": faiss_candidates}},
                {"imdb_id": 1, "avg_vote": 1, "votes": 1, "title": 1, "original_title": 1, 
                 "year": 1, "genres": 1, "director": 1, "poster_url": 1, "description": 1}
            )
        }
        
        reranked = []
        for cand_id in faiss_candidates:
            vec = candidate_vecs.get(cand_id)
            catalog = candidate_catalog.get(cand_id)
            if not vec:
                continue
            
            # Calculate similarities
            sim_desc = faiss_scores.get(cand_id, 0)
            sim_genre = cosine_similarity(user_vectors["genre"], vec.get("embedding_genre"))
            sim_actors = cosine_similarity(user_vectors["actors"], vec.get("embedding_actors"))
            sim_director = cosine_similarity(user_vectors["director"], vec.get("embedding_director"))
            
            similarity_score = (sim_desc * W_DESCRIPTION + 
                                sim_genre * W_GENRE + 
                                sim_actors * W_ACTORS + 
                                sim_director * W_DIRECTOR)
            
            # Quality score
            avg_vote = 5.0
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
            
            quality_score = self._calculate_quality_score(avg_vote, votes)
            final_score = ALPHA_SIMILARITY * similarity_score + BETA_QUALITY * quality_score
            
            # Get title and other info from catalog
            title = catalog.get("original_title") or catalog.get("title") or vec.get("title", "Unknown") if catalog else vec.get("title", "Unknown")
            genres = catalog.get("genres", []) if catalog else []
            
            reranked.append({
                "imdb_id": cand_id,
                "title": title,
                "year": catalog.get("year") if catalog else None,
                "poster": catalog.get("poster_url") if catalog else None,
                "rating": avg_vote,
                "genres": genres if isinstance(genres, list) else [],
                "director": catalog.get("director", "") if catalog else "",
                "matchScore": int(similarity_score * 100),
                "finalScore": final_score,
                "votes": votes
            })
        
        # Sort by final score
        reranked.sort(key=lambda x: x["finalScore"], reverse=True)
        recommended = reranked[:N_RECOMMENDATIONS]
        
        # 5. Not recommended films
        not_rec_candidates = list(self.db[COLL_CATALOG].aggregate([
            {"$match": {
                "year": {"$gte": MIN_YEAR_NOT_REC},
                "votes": {"$gte": MIN_VOTES_NOT_REC},
                "imdb_id": {"$nin": list(seen_ids)}
            }},
            {"$sample": {"size": 500}}
        ]))
        
        not_rec_ids = [c["imdb_id"] for c in not_rec_candidates if c.get("imdb_id")]
        
        not_rec_vecs = {
            doc["imdb_id"]: doc 
            for doc in self.db[COLL_VECTORS].find({"imdb_id": {"$in": not_rec_ids}})
        }
        
        not_recommended_list = []
        for cand in not_rec_candidates:
            cand_id = cand.get("imdb_id")
            if not cand_id:
                continue
            vec = not_rec_vecs.get(cand_id)
            if not vec:
                continue
            
            # Calculate similarity
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
            
            # Quality score
            avg_vote = float(cand.get("avg_vote", 5.0)) if cand.get("avg_vote") else 5.0
            votes = int(cand.get("votes", 0)) if cand.get("votes") else 0
            quality_score = self._calculate_quality_score(avg_vote, votes)
            
            # Not-rec score: low similarity + high quality
            dissimilarity_score = 1.0 - similarity_score
            not_rec_score = ALPHA_SIMILARITY * dissimilarity_score + BETA_QUALITY * quality_score
            
            genres = cand.get("genres", [])
            
            not_recommended_list.append({
                "imdb_id": cand_id,
                "title": cand.get("original_title") or cand.get("title", "Unknown"),
                "year": cand.get("year"),
                "poster": cand.get("poster_url"),
                "rating": avg_vote,
                "genres": genres if isinstance(genres, list) else [],
                "director": cand.get("director", ""),
                "matchScore": int((1 - similarity_score) * 100),  # Inverse for not-rec
                "notRecScore": not_rec_score,
                "votes": votes
            })
        
        # Sort by not_rec_score
        not_recommended_list.sort(key=lambda x: x["notRecScore"], reverse=True)
        not_recommended = not_recommended_list[:N_NOT_RECOMMENDED]
        
        # Build result
        result = {
            "recommended": recommended,
            "not_recommended": not_recommended,
            "matched_films": len(matched_films),
            "total_films": len(user_movies)
        }
        
        # Cache only minimal data (imdb_id, title, matchScore) for scalability
        from datetime import datetime
        
        def minimize_movie(m):
            """Extract only essential fields for caching."""
            return {
                "imdb_id": m.get("imdb_id"),
                "title": m.get("title"),
                "matchScore": m.get("matchScore", 0)
            }
        
        cache_data = {
            "recommended": [minimize_movie(m) for m in recommended],
            "not_recommended": [minimize_movie(m) for m in not_recommended],
            "matched_films": len(matched_films),
            "total_films": len(user_movies),
            "generated_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat()
        }
        
        self.db["users"].update_one(
            {"user_id": user_id},
            {"$set": {"recommendations": cache_data}}
        )
        
        return result
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()


# Singleton instance
_recommendation_service = None

def get_recommendation_service() -> RecommendationService:
    """Get or create the recommendation service singleton."""
    global _recommendation_service
    if _recommendation_service is None:
        _recommendation_service = RecommendationService()
    return _recommendation_service
