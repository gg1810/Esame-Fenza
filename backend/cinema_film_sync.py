"""
Cinema Film Sync - Sincronizza film da ComingSoon a movies_catalog
Cerca i film su TMDB per ottenere tutti i metadati e li aggiunge al catalogo.
"""

import requests
import re
import os
import unicodedata
from datetime import datetime
from pymongo import MongoClient

# Configuration
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
TMDB_API_KEY = "272643841dd72057567786d8fa7f8c5f"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Lingue occidentali permesse (come in MovieUpdater)
ALLOWED_LANGUAGES = ['en', 'it', 'fr', 'de', 'es', 'pt', 'nl', 'da', 'sv', 'no']


class CinemaFilmSync:
    """Sincronizza film da showtimes (ComingSoon) a movies_catalog (CineMatch)."""
    
    def __init__(self, mongo_url: str = MONGO_URL, tmdb_api_key: str = TMDB_API_KEY):
        self.client = MongoClient(mongo_url)
        self.cinematch_db = self.client["cinematch_db"]
        
        # Le collection devono stare tutte in cinematch_db
        self.showtimes = self.cinematch_db["showtimes"]
        self.catalog = self.cinematch_db["movies_catalog"]
        
        self.api_key = tmdb_api_key

    def normalize_title(self, text: str) -> str:
        """Rimuove accenti e caratteri speciali per facilitare il matching."""
        if not text:
            return ""
        # Normalizza in NFD (decomposizione) e rimuove i caratteri non-spacing mark (accenti)
        normalized = unicodedata.normalize('NFD', text)
        result = "".join([c for c in normalized if not unicodedata.combining(c)])
        
        # Gestisce anche caratteri speciali come ā, ē, ī, ō, ū (macron) che non vengono decomposti
        # Mappa caratteri speciali comuni
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
        result = " ".join(result.split())
        return result.lower()
    
    def get_unique_films_from_showtimes(self) -> list:
        """Estrae lista film unici da showtimes."""
        pipeline = [
            {"$group": {
                "_id": {
                    "title": "$film_title",
                    "original_title": "$film_original_title",
                    "director": "$director"
                },
                "film_id": {"$first": "$film_id"},
                "count": {"$sum": 1}
            }}
        ]
        
        results = list(self.showtimes.aggregate(pipeline))
        
        films = []
        for r in results:
            films.append({
                "title": r["_id"]["title"],
                "original_title": r["_id"].get("original_title", ""),
                "director": r["_id"].get("director", ""),
                "film_id": r.get("film_id"),
                "showtimes_count": r["count"]
            })
        
        return films
    
    def film_exists_in_catalog(self, title: str, original_title: str = None, director: str = None) -> dict:
        """
        Verifica se un film esiste già nel catalogo con logica robusta.
        Restituisce il documento trovato o None.
        """
        # 1. Cerca per titolo esatto (case-insensitive)
        escaped_title = re.escape(title)
        query_parts = [
            {"title": {"$regex": f"^{escaped_title}$", "$options": "i"}},
            {"original_title": {"$regex": f"^{escaped_title}$", "$options": "i"}}
        ]
        
        # 2. Cerca per titolo originale
        if original_title:
            escaped_original = re.escape(original_title)
            query_parts.extend([
                {"title": {"$regex": f"^{escaped_original}$", "$options": "i"}},
                {"original_title": {"$regex": f"^{escaped_original}$", "$options": "i"}}
            ])
        
        # Prima ricerca: titoli esatti
        result = self.catalog.find_one({"$or": query_parts})
        if result:
            return result
            
        # 3. Ricerca con titoli normalizzati (senza accenti)
        norm_title = self.normalize_title(title)
        if norm_title and len(norm_title) >= 3:
            # Cerca candidati usando le prime lettere del titolo
            first_letters = norm_title[:3]  # Prime 3 lettere
            
            # Costruisce pattern che cerca titoli che iniziano con le prime lettere
            # Questo è case-insensitive e cattura varianti come Sirāt per "sir"
            candidates = self.catalog.find({
                "$or": [
                    {"title": {"$regex": f"^{re.escape(first_letters)}", "$options": "i"}},
                    {"original_title": {"$regex": f"^{re.escape(first_letters)}", "$options": "i"}}
                ]
            }).limit(100)
            
            for candidate in candidates:
                cand_norm_title = self.normalize_title(candidate.get("title", ""))
                cand_norm_orig = self.normalize_title(candidate.get("original_title", ""))
                
                # Match esatto normalizzato
                if norm_title == cand_norm_title or norm_title == cand_norm_orig:
                    return candidate
                
                # Match parziale (contenimento)
                if len(norm_title) > 5:
                    if norm_title in cand_norm_title or cand_norm_title in norm_title:
                        return candidate
                    if cand_norm_orig and (norm_title in cand_norm_orig or cand_norm_orig in norm_title):
                        return candidate
        
        # 4. Cerca anche per titolo originale normalizzato
        if original_title:
            norm_orig = self.normalize_title(original_title)
            if norm_orig and norm_orig != norm_title:
                search_words = norm_orig.split()[:3]
                if search_words:
                    word_pattern = "|".join([re.escape(w) for w in search_words if len(w) > 2])
                    if word_pattern:
                        candidates = self.catalog.find({
                            "$or": [
                                {"title": {"$regex": word_pattern, "$options": "i"}},
                                {"original_title": {"$regex": word_pattern, "$options": "i"}}
                            ]
                        }).limit(50)
                        
                        for candidate in candidates:
                            cand_norm_title = self.normalize_title(candidate.get("title", ""))
                            cand_norm_orig = self.normalize_title(candidate.get("original_title", ""))
                            
                            if norm_orig == cand_norm_title or norm_orig == cand_norm_orig:
                                return candidate
            
        return None

    def search_tmdb(self, title: str, director: str = None, year: int = None) -> dict:
        """
        Cerca un film su TMDB con logica robusta:
        1. Ricerca per titolo fornito in italiano.
        2. Ricerca in inglese se non trova nulla.
        3. Ricerca per titolo normalizzato.
        4. Verifica i risultati confrontando il regista (se fornito).
        """
        search_queries = [title]
        norm_title = self.normalize_title(title)
        if norm_title != title.lower():
            search_queries.append(norm_title)
        
        # Prova anche senza articoli e parole comuni
        cleaned_title = re.sub(r'^(la |il |lo |le |i |gli |the |a |an )','', title.lower())
        if cleaned_title != title.lower():
            search_queries.append(cleaned_title)

        # Lingue da provare: italiano, inglese, francese (per film europei)
        languages = ["it-IT", "en-US", "fr-FR"]
        
        for lang in languages:
            for query in search_queries:
                url = f"{TMDB_BASE_URL}/search/movie"
                params = {
                    "api_key": self.api_key,
                    "language": lang,
                    "query": query,
                    "include_adult": False
                }
                if year:
                    params["year"] = year
                    
                try:
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        results = response.json().get("results", [])
                        if not results:
                            continue
                            
                        # Se non abbiamo il regista, prendiamo il primo risultato
                        if not director:
                            return results[0]
                            
                        # Se abbiamo il regista, verifichiamo i primi 5 risultati
                        top_results = results[:5]
                        norm_target_director = self.normalize_title(director)
                        # Estrai anche solo il cognome (ultima parola)
                        director_parts = norm_target_director.split()
                        director_surname = director_parts[-1] if director_parts else ""
                        
                        for res in top_results:
                            # Dobbiamo recuperare i crediti per verificare il regista
                            details = self.fetch_full_details(res["id"])
                            if not details:
                                continue
                                
                            crew = details.get("credits", {}).get("crew", [])
                            tmdb_directors = [self.normalize_title(p["name"]) for p in crew if p.get("job") == "Director"]
                            
                            for td in tmdb_directors:
                                # Match completo o parziale (cognome)
                                if norm_target_director in td or td in norm_target_director:
                                    return res # Match trovato!
                                # Match per cognome (ultima parola del nome)
                                if director_surname and len(director_surname) > 3:
                                    if director_surname in td:
                                        return res
                                    
                        # Se nessuno dei top results coincide col regista, 
                        # ma il primo risultato ha un titolo molto simile, usalo come fallback
                        first_result_norm = self.normalize_title(results[0].get("title", ""))
                        if first_result_norm == norm_title or title.lower() == results[0].get("title", "").lower():
                             return results[0]
                        
                        # Fallback ulteriore: se il titolo normalizzato è contenuto o contiene
                        if len(norm_title) >= 4 and (norm_title in first_result_norm or first_result_norm in norm_title):
                            return results[0]
                             
                except Exception as e:
                    print(f"    ⚠️ Errore ricerca TMDB ({query}, {lang}): {e}")
        
        return None
    
    def fetch_full_details(self, tmdb_id: int) -> dict:
        """Ottiene tutti i dettagli di un film da TMDB."""
        url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
        params = {
            "api_key": self.api_key,
            "language": "it-IT",
            "append_to_response": "credits,external_ids"
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"    ⚠️ Errore dettagli TMDB: {e}")
        
        return None
    
    def add_film_to_catalog(self, tmdb_details: dict) -> bool:
        """Aggiunge un film al catalogo. Per i film al cinema NON filtra per lingua."""
        try:
            # Per i film al cinema NON applichiamo il filtro lingua
            # Se un film è in programmazione in Italia, lo aggiungiamo comunque
            original_lang = tmdb_details.get("original_language", "")
            
            # ID
            imdb_id = f"tmdb_{tmdb_details['id']}"
            real_imdb_id = tmdb_details.get("external_ids", {}).get("imdb_id")
            final_id = real_imdb_id if real_imdb_id else imdb_id
            
            # Verifica duplicato
            if self.catalog.find_one({"imdb_id": final_id}):
                # Non è un errore - il film c'è già, basta aggiornare existing count
                return "duplicate"
            
            # Mapping dati
            credits = tmdb_details.get("credits", {})
            crew = credits.get("crew", [])
            cast = credits.get("cast", [])
            
            title = tmdb_details.get("title")
            original_title = tmdb_details.get("original_title")
            
            directors = [p['name'] for p in crew if p.get('job') == 'Director']
            writers = [p['name'] for p in crew if p.get('department') == 'Writing']
            actors_list = [p['name'] for p in cast[:15]]
            genres_list = [g['name'] for g in tmdb_details.get("genres", [])]
            production_companies = [c['name'] for c in tmdb_details.get("production_companies", [])]
            
            poster_path = tmdb_details.get("poster_path")
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
            
            if not poster_url:
                print(f"      - Poster mancante")
                return False
            
            release_date = tmdb_details.get("release_date")
            if not release_date:
                print(f"      - Data rilascio mancante")
                return False
            
            budget = tmdb_details.get("budget")
            revenue = tmdb_details.get("revenue")
            
            new_movie = {
                "imdb_id": final_id,
                "imdb_title_id": final_id,
                "title": title,
                "original_title": original_title,
                "normalized_title": self.normalize_title(title),
                "normalized_original_title": self.normalize_title(original_title) if original_title else None,
                "english_title": original_title if tmdb_details.get("original_language") == "en" else None,
                "year": int(release_date[:4]),
                "date_published": release_date,
                "genres": genres_list,
                "genre": ", ".join(genres_list),
                "duration": tmdb_details.get("runtime"),
                "country": tmdb_details.get("origin_country", [None])[0] if tmdb_details.get("origin_country") else None,
                "language": tmdb_details.get("original_language"),
                "movie_language": tmdb_details.get("original_language"),
                "director": ", ".join(directors),
                "writer": ", ".join(writers),
                "production_company": ", ".join(production_companies),
                "actors": ", ".join(actors_list),
                "description": tmdb_details.get("overview"),
                "avg_vote": tmdb_details.get("vote_average"),
                "votes": tmdb_details.get("vote_count"),
                "budget": f"$ {budget}" if budget else None,
                "usa_gross_income": None,
                "worlwide_gross_income": f"$ {revenue}" if revenue else None,
                "metascore": None,
                "reviews_from_users": None,
                "reviews_from_critics": None,
                "link_imdb": f"https://www.imdb.com/title/{real_imdb_id}/" if real_imdb_id else None,
                "poster_url": poster_url,
                "has_real_poster": True,
                "loaded_at": datetime.utcnow().isoformat(),
                "source": "comingsoon_sync"
            }
            
            self.catalog.update_one(
                {"imdb_id": final_id},
                {"$set": new_movie},
                upsert=True
            )
            
            return True
        
        except Exception as e:
            print(f"    ❌ Errore aggiunta film: {e}")
            return False
    
    def sync(self) -> dict:
        """Esegue la sincronizzazione completa."""
        print("=" * 60)
        print("🔄 Cinema Film Sync")
        print(f"⏰ Avvio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # 1. Ottieni film unici da showtimes
        films = self.get_unique_films_from_showtimes()
        print(f"\n📊 Film unici in programmazione: {len(films)}")
        
        missing = []
        existing = []
        added = []
        failed = []
        
        # 2. Verifica quali mancano nel catalogo
        print("\n🔍 Verifica film nel catalogo...")
        for film in films:
            title = film["title"]
            original_title = film.get("original_title", "")
            director = film.get("director", "")
            
            catalog_match = self.film_exists_in_catalog(title, original_title, director)
            if catalog_match:
                existing.append(film)
            else:
                missing.append(film)
        
        print(f"  ✅ Già presenti: {len(existing)}")
        print(f"  ❌ Mancanti: {len(missing)}")
        
        # 3. Aggiungi film mancanti
        if missing:
            print(f"\n🔄 Aggiunta film mancanti via TMDB...")
            
            for film in missing:
                title = film["title"]
                original_title = film.get("original_title", "")
                director = film.get("director", "")
                
                # Cerca su TMDB con logica robusta
                # Prova prima con originale + regista, poi italiano + regista
                tmdb_result = self.search_tmdb(original_title, director=director) if original_title else None
                
                if not tmdb_result:
                    tmdb_result = self.search_tmdb(title, director=director)
                
                # Se ancora nulla, prova senza regista (fallback meno preciso)
                if not tmdb_result and original_title:
                    tmdb_result = self.search_tmdb(original_title)
                if not tmdb_result:
                    tmdb_result = self.search_tmdb(title)
                
                if tmdb_result:
                    # Ottieni dettagli completi
                    details = self.fetch_full_details(tmdb_result["id"])
                    
                    if details:
                        result = self.add_film_to_catalog(details)
                        if result == True:
                            added.append(film)
                            print(f"  🆕 {title}")
                        elif result == "duplicate":
                            # Film già presente nel catalogo (trovato via TMDB ID)
                            existing.append(film)
                            missing.remove(film)
                            print(f"  ✅ {title} (già nel DB)")
                        else:
                            failed.append(film)
                            print(f"  ❌ Filtrato (poster mancante): {title}")
                    else:
                        failed.append(film)
                        print(f"  ❌ Errore dettagli: {title}")
                else:
                    failed.append(film)
                    print(f"  ⏭️ Non trovato su TMDB: {title}")
        
        # Report
        print("\n" + "=" * 60)
        print("📊 Riepilogo:")
        print("=" * 60)
        print(f"  Film in programmazione: {len(films)}")
        print(f"  Già nel catalogo: {len(existing)}")
        print(f"  Aggiunti ora: {len(added)}")
        print(f"  Non trovati/filtrati: {len(failed)}")
        print(f"\n✅ Completato: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {
            "total": len(films),
            "existing": len(existing),
            "added": len(added),
            "failed": len(failed),
            "missing_titles": [f["title"] for f in failed]
        }


def sync_films_to_catalog():
    """Funzione wrapper per lo scheduler."""
    syncer = CinemaFilmSync()
    return syncer.sync()


if __name__ == "__main__":
    sync_films_to_catalog()
