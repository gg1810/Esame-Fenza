import requests
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

class MovieUpdater:
    # Lingue permesse (include anche giapponese per anime)
    ALLOWED_LANGUAGES = ['en', 'it', 'fr', 'de', 'es', 'pt', 'nl', 'da', 'sv', 'no', 'ja']

    def __init__(self, mongo_url, tmdb_api_key):
        self.client = MongoClient(mongo_url)
        self.db = self.client.cinematch_db
        self.catalog = self.db.movies_catalog
        self.api_key = tmdb_api_key
        self.base_url = "https://api.themoviedb.org/3"

    def normalize_title(self, text: str) -> str:
        """Rimuove accenti e caratteri speciali per matching/ricerca."""
        if not text: return ""
        import unicodedata
        import re
        normalized = unicodedata.normalize('NFD', text)
        result = "".join([c for c in normalized if not unicodedata.combining(c)])
        
        special_chars = {
            'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u',
            'Ā': 'A', 'Ē': 'E', 'Ī': 'I', 'Ō': 'O', 'Ū': 'U',
            'ł': 'l', 'Ł': 'L', 'ø': 'o', 'Ø': 'O', 'æ': 'ae', 'Æ': 'AE',
            'œ': 'oe', 'Œ': 'OE', 'ß': 'ss', 'đ': 'd', 'Đ': 'D',
            'ñ': 'n', 'Ñ': 'N', 'ç': 'c', 'Ç': 'C'
        }
        for char, replacement in special_chars.items():
            result = result.replace(char, replacement)
        
        result = re.sub(r'[^a-zA-Z0-9\s]', ' ', result)
        result = " ".join(result.split()).lower()
        return result

    def fetch_new_releases(self):
        """Scarica i film usciti recentemente (Cinema e Digital)."""
        print("🔄 [Updater] Inizio aggiornamento nuove uscite...")
        count = 0
        
        # 1. Recupera film 'Now Playing' (Cinema)
        print("   📽️ Fetch Now Playing...")
        count += self._fetch_from_tmdb_endpoint("/movie/now_playing", "now_playing")
        
        # 2. Recupera film popolari 2025-2026
        current_year = datetime.now().year
        for year in range(current_year, current_year - 2, -1):
            print(f"   📅 Discover film {year}...")
            count += self._discover_movies(year)
        
        print(f"✅ [Updater] Aggiornamento completato. Aggiunti/Aggiornati {count} film.")
        return count

    def _discover_movies(self, year):
        """Usa discover per trovare film popolari di un anno specifico."""
        url = f"{self.base_url}/discover/movie"
        params = {
            "api_key": self.api_key,
            "language": "it-IT",
            "sort_by": "popularity.desc",
            "primary_release_year": year,
            "with_original_language": "|".join(self.ALLOWED_LANGUAGES),
            "vote_count.gte": 10, # Ridotto per includere film nuovi
            "page": 1
        }
        
        total_added = 0
        # Scarica prime 5 pagine (100 film più popolari dell'anno)
        for i in range(1, 6):
            params["page"] = i
            try:
                data = requests.get(url, params=params).json()
                results = data.get("results", [])
                if not results: break
                
                for movie in results:
                    if self._process_movie(movie, "discover_year"):
                        total_added += 1
            except Exception as e:
                print(f"⚠️ Errore discover page {i}: {e}")
        
        return total_added

    def _fetch_from_tmdb_endpoint(self, endpoint, source_tag):
        url = f"{self.base_url}{endpoint}"
        params = {
            "api_key": self.api_key,
            "language": "it-IT",
            "page": 1,
            "region": "IT" # Focus su uscite italiane/occidentali
        }
        
        total_added = 0
        try:
            # Scarica prime 3 pagine
            for i in range(1, 4):
                params["page"] = i
                data = requests.get(url, params=params).json()
                results = data.get("results", [])
                if not results: break
                
                for movie in results:
                    if self._process_movie(movie, source_tag):
                        total_added += 1
        except Exception as e:
            print(f"⚠️ Errore fetch {endpoint}: {e}")
            
        return total_added

    def _process_movie(self, tmdb_movie, source):
        """Processa e salva un singolo film se rilevante."""
        try:
            # Controllo base esistenza
            imdb_id = f"tmdb_{tmdb_movie['id']}" # Default ID
            
            # Se esiste già, saltiamo (o aggiorniamo solo se necessario, qui saltiamo per velocità)
            if self.catalog.find_one({"imdb_id": imdb_id}):
                return False

            # Fetch dettagli completi (come fatto in main.py) per avere generi, cast, ecc.
            details_url = f"{self.base_url}/movie/{tmdb_movie['id']}"
            params = {
                "api_key": self.api_key,
                "language": "it-IT",
                "append_to_response": "credits,external_ids"
            }
            details_resp = requests.get(details_url, params=params)
            if details_resp.status_code != 200:
                return False
                
            details = details_resp.json()
            
            # FILTRO LINGUE OCCIDENTALI
            if details.get("original_language") not in self.ALLOWED_LANGUAGES:
                # print(f"   ⏩ Saltato (Lingua non occidentale: {details.get('original_language')}): {details.get('title')}")
                return False

            # Filtri di qualità
            # 1. Ignora se non è un film (dovrebbe già esserlo dato l'endpoint)
            # 2. Ignora se non ha data di uscita
            if not details.get("release_date"):
                return False

            # ID IMDB reale preferito
            real_imdb_id = details.get("external_ids", {}).get("imdb_id")
            final_id = real_imdb_id if real_imdb_id else imdb_id

            # Controllo esistenza con ID IMDB reale
            if self.catalog.find_one({"imdb_id": final_id}):
                return False

            # Mapping dati
            credits = details.get("credits", {})
            crew = credits.get("crew", [])
            cast = credits.get("cast", [])
            
            directors = [p['name'] for p in crew if p['job'] == 'Director']
            writers = [p['name'] for p in crew if p['department'] == 'Writing']
            actors_list = [p['name'] for p in cast[:15]]
            genres_list = [g['name'] for g in details.get("genres", [])]
            production_companies = [c['name'] for c in details.get("production_companies", [])]
            
            poster_path = details.get("poster_path")
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
            
            if not poster_url: # Opzionale: ignora film senza poster
                return False

            # Conversione valute
            budget = details.get("budget")
            revenue = details.get("revenue")
            budget_str = f"$ {budget}" if budget else None
            revenue_str = f"$ {revenue}" if revenue else None

            imdb_id_real = details.get("external_ids", {}).get("imdb_id")

            new_movie = {
                "imdb_id": final_id,
                "imdb_title_id": final_id, # Coerenza con movies_final.csv
                "title": details.get("title"),
                "original_title": details.get("original_title"),
                "normalized_title": self.normalize_title(details.get("title")),
                "normalized_original_title": self.normalize_title(details.get("original_title")) if details.get("original_title") else None,
                "english_title": details.get("original_title") if details.get("original_language") == "en" else None,
                "year": int(details.get("release_date")[:4]),
                "date_published": details.get("release_date"),
                "genres": genres_list,
                "genre": ", ".join(genres_list),
                "duration": details.get("runtime"),
                "country": details.get("origin_country", [None])[0] if details.get("origin_country") else None,
                "language": details.get("original_language"),
                "movie_language": details.get("original_language"),
                "director": ", ".join(directors),
                "writer": ", ".join(writers),
                "production_company": ", ".join(production_companies),
                "actors": ", ".join(actors_list),
                "description": details.get("overview"),
                "avg_vote": details.get("vote_average"),
                "votes": details.get("vote_count"),
                "budget": budget_str,
                "usa_gross_income": None,
                "worlwide_gross_income": revenue_str,
                "metascore": None,
                "reviews_from_users": None,
                "reviews_from_critics": None,
                "link_imdb": f"https://www.imdb.com/title/{imdb_id_real}/" if imdb_id_real else None,
                "poster_url": poster_url,
                "has_real_poster": True,
                "loaded_at": datetime.utcnow().isoformat(),
                "source": f"auto_updater_{source}"
            }
            
            self.catalog.update_one(
                {"imdb_id": final_id},
                {"$set": new_movie},
                upsert=True
            )
            print(f"   🆕 Aggiunto: {new_movie['title']} ({new_movie['year']})")
            return True

        except Exception as e:
            print(f"   Error processing movie {tmdb_movie.get('id')}: {e}")
            return False
