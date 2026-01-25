"""
=============================================================================
CINEMA PIPELINE - File unificato per gestione dati cinematografici
=============================================================================

Questo file combina due processi DISTINTI:

┌────────────────────────────────────────────────────────────────────────────┐
│  FASE 1: SCRAPE COMINGSOON                                                 │
│  - Scarica orari, cinema e dettagli film da ComingSoon.it                 │
│  - Salva nella collection "showtimes"                                      │
│  - Dati: orari, prezzi, sale, indirizzi cinema                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌────────────────────────────────────────────────────────────────────────────┐
│  FASE 2: CINEMA FILM SYNC                                                  │
│  - Legge i film dalla collection "showtimes"                              │
│  - Cerca ogni film su TMDB per ottenere metadati completi                 │
│  - Aggiunge i film mancanti alla collection "movies_catalog"              │
│  - Dati: poster, descrizione, cast, generi, rating                        │
└────────────────────────────────────────────────────────────────────────────┘

"""

import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import pytz
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pymongo import MongoClient

# =============================================================================
# CONFIGURAZIONE
# =============================================================================

MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")

# ComingSoon config
BASE_URL = "https://www.comingsoon.it"
REQUEST_DELAY = 1.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}

# TMDB config
TMDB_API_KEY = "272643841dd72057567786d8fa7f8c5f"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Province Campania
CAMPANIA_PROVINCES = [
    {"name": "Napoli", "slug": "napoli"},
    {"name": "Salerno", "slug": "salerno"},
    {"name": "Caserta", "slug": "caserta"},
    {"name": "Avellino", "slug": "avellino"},
    {"name": "Benevento", "slug": "benevento"},
]

# Cache per dettagli film
film_details_cache: Dict[str, Dict] = {}


# =============================================================================
# =============================================================================
# FASE 1: SCRAPE COMINGSOON
# Scarica orari e dettagli film da ComingSoon.it → salva in "showtimes"
# =============================================================================
# =============================================================================

def get_soup(url: str) -> Optional[BeautifulSoup]:
    """Fetch e parse di una pagina."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"  ❌ Errore fetch {url}: {e}")
        return None


def get_film_details(film_url: str, film_id: str) -> Dict:
    """
    Ottiene i dettagli di un film dalla sua scheda.
    
    Args:
        film_url: URL completo della scheda film
        film_id: ID del film (per cache)
    
    Returns:
        Dict con: title, original_title, director
    """
    if film_id in film_details_cache:
        return film_details_cache[film_id]
    
    details = {
        "title": "",
        "original_title": "",
        "director": ""
    }
    
    if not film_url:
        film_details_cache[film_id] = details
        return details
    
    # Assicurati che l'URL punti alla scheda
    if not film_url.endswith('/scheda/'):
        if film_url.endswith('/'):
            film_url += 'scheda/'
        else:
            film_url += '/scheda/'
    
    soup = get_soup(film_url)
    if not soup:
        film_details_cache[film_id] = details
        return details
    
    # Titolo
    title_elem = soup.select_one('h1.titolo')
    if title_elem:
        details["title"] = title_elem.get_text(strip=True)
    
    # Titolo originale
    original_elem = soup.select_one('div.sottotitolo')
    if original_elem:
        original_text = original_elem.get_text(strip=True)
        # Rimuovi parentesi
        original_text = re.sub(r'^\s*\(\s*', '', original_text)
        original_text = re.sub(r'\s*\)\s*$', '', original_text)
        details["original_title"] = original_text
    
    # Regista
    for label in soup.find_all('b'):
        label_text = label.get_text(strip=True).lower()
        if 'regista' in label_text or 'regia' in label_text:
            next_elem = label.find_next_sibling(['a', 'span'])
            if next_elem:
                details["director"] = next_elem.get_text(strip=True)
                break
    
    film_details_cache[film_id] = details
    return details


def get_movies_in_province(province_slug: str) -> List[Dict]:
    """
    Ottiene la lista dei film in programmazione in una provincia.
    Estrae anche l'URL della scheda film.
    """
    url = f"{BASE_URL}/cinema/{province_slug}/"
    soup = get_soup(url)
    
    if not soup:
        return []
    
    movies = []
    
    # Cerca tutti i link ?idf=
    film_links = soup.select('a[href*="?idf="]')
    
    for link in film_links:
        href = link.get("href", "")
        match = re.search(r'idf=(\d+)', href)
        if not match:
            continue
        
        film_id = match.group(1)
        
        # Estrai titolo dal testo del link
        title = None
        text = link.get_text(strip=True)
        title_match = re.search(r'Trova (.+?) nei cinema', text)
        if title_match:
            title = title_match.group(1).strip()
        
        # Se non trovato, cerca nel parent
        if not title:
            parent = link.find_parent(class_=re.compile(r'row|col'))
            if parent:
                title_elem = parent.select_one('a.titolo.h1, a.titolo, .h1')
                if title_elem:
                    title = title_elem.get_text(strip=True)
        
        # Cerca link alla scheda film nel parent
        film_url = ""
        if parent := link.find_parent(class_=re.compile(r'row|col|box')):
            film_link_elem = parent.select_one('a[href*="/film/"]')
            if film_link_elem:
                film_href = film_link_elem.get('href', '')
                if film_href.startswith('/'):
                    film_url = BASE_URL + film_href
                else:
                    film_url = film_href
        
        if title:
            movies.append({
                "id": film_id,
                "title": title,
                "film_url": film_url
            })
    
    # Rimuovi duplicati
    seen = set()
    unique_movies = []
    for m in movies:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique_movies.append(m)
    
    return unique_movies


def get_film_showtimes(province_slug: str, film_id: str) -> List[Dict]:
    """Ottiene i cinema e gli orari per un film specifico in una provincia."""
    url = f"{BASE_URL}/cinema/{province_slug}/?idf={film_id}"
    soup = get_soup(url)
    
    if not soup:
        return []
    
    cinemas = []
    cinema_boxes = soup.select('div.cs-box')
    
    for box in cinema_boxes:
        cinema_link = box.select_one('a[title][href*="/cinema/"]')
        if not cinema_link:
            continue
        
        href = cinema_link.get('href', '')
        if not re.match(r'/cinema/[^/]+/[^/]+/\d+/', href):
            continue
        
        cinema_name = cinema_link.get('title', '') or cinema_link.get_text(strip=True)
        if not cinema_name or len(cinema_name) < 2:
            continue
        
        # Indirizzo
        address = ""
        addr_elem = box.select_one('p.descrizione')
        if addr_elem:
            address = addr_elem.get_text(strip=True)
        
        # Orari
        showtimes_list = []
        
        box_text = box.get_text(' ', strip=True)
        orari_match = re.search(r'Orari e prezzi:\s*(.+?)(?:Acquista|$)', box_text)
        if orari_match:
            orari_str = orari_match.group(1)
            times = re.findall(r'(\d{1,2}[.:]\d{2})\s*/\s*([\d,]+€?)', orari_str)
            
            sala = ""
            sala_match = re.search(r'(Sala\s*\d+)', box_text)
            if sala_match:
                sala = sala_match.group(1).strip()
            
            for time_str, price in times:
                showtimes_list.append({
                    "sala": sala,
                    "time": time_str.replace(".", ":"),
                    "price": price
                })
        
        # Cerca nei fratelli
        sibling = box.find_next_sibling()
        while sibling:
            if sibling.get('class') and 'cs-box' in sibling.get('class', []):
                break
            
            if sibling.name == 'div' and 'meta' in sibling.get('class', []):
                meta_text = sibling.get_text(' ', strip=True)
                
                sala = ""
                sala_match = re.search(r'(Sala\s*\d+)', meta_text)
                if sala_match:
                    sala = sala_match.group(1).strip()
                
                orari_match = re.search(r'Orari e prezzi:\s*(.+)', meta_text)
                if orari_match:
                    orari_str = orari_match.group(1)
                    times = re.findall(r'(\d{1,2}[.:]\d{2})\s*/\s*([\d,]+€?)', orari_str)
                    
                    for time_str, price in times:
                        showtimes_list.append({
                            "sala": sala,
                            "time": time_str.replace(".", ":"),
                            "price": price
                        })
            
            sibling = sibling.find_next_sibling()
        
        if cinema_name and showtimes_list:
            cinemas.append({
                "cinema_name": cinema_name,
                "cinema_url": BASE_URL + href,
                "address": address,
                "showtimes": showtimes_list
            })
    
    return cinemas


def scrape_province(province: Dict) -> List[Dict]:
    """Scarica tutti i dati di una provincia."""
    province_name = province["name"]
    province_slug = province["slug"]
    
    print(f"\n📍 Provincia: {province_name}")
    
    movies = get_movies_in_province(province_slug)
    print(f"  🎬 Film trovati: {len(movies)}")
    
    results = []
    
    for movie in movies:
        # Ottieni dettagli film
        print(f"    📖 {movie['title']}...", end=" ", flush=True)
        
        if movie.get('film_url'):
            time.sleep(REQUEST_DELAY)
            film_details = get_film_details(movie['film_url'], movie['id'])
        else:
            film_details = {"title": "", "original_title": "", "director": ""}
        
        time.sleep(REQUEST_DELAY)
        
        # Ottieni orari
        showtimes = get_film_showtimes(province_slug, movie["id"])
        
        if showtimes:
            italy_tz = pytz.timezone('Europe/Rome')
            result = {
                "province": province_name,
                "province_slug": province_slug,
                "film_id": movie["id"],
                "film_title": movie["title"],
                "film_original_title": film_details.get("original_title", ""),
                "director": film_details.get("director", ""),
                "cinemas": showtimes,
                "updated_at": datetime.now(italy_tz).isoformat()
            }
            results.append(result)
            
            orig = result['film_original_title'] or "-"
            dir_ = result['director'] or "-"
            print(f"✅ Orig: {orig[:20]} | Regista: {dir_[:20]} | {len(showtimes)} cinema")
        else:
            print("⚠️ nessun cinema")
    
    return results


def save_to_mongodb(data: List[Dict]):
    """Salva i dati su MongoDB con schema: Film → Region → Date → Cinema → Showtimes."""
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        db = client["cinematch_db"]
        collection = db["showtimes"]
        
        italy_tz = pytz.timezone('Europe/Rome')
        today_str = datetime.now(italy_tz).strftime("%Y-%m-%d")
        cutoff_date = (datetime.now(italy_tz) - timedelta(days=30)).strftime("%Y-%m-%d")
        
        for record in data:
            film_id = record["film_id"]
            region_slug = record["province_slug"]
            
            # Costruisci gli update per ogni cinema
            update_set = {
                "film_id": film_id,
                "film_title": record["film_title"],
                "film_original_title": record.get("film_original_title", ""),
                "director": record.get("director", ""),
                "last_updated": datetime.now(italy_tz).isoformat()
            }
            
            # Struttura: regions.<region>.dates.<date>.cinemas.<cinema> = {info + showtimes}
            for cinema in record.get("cinemas", []):
                cinema_name = cinema.get("cinema_name", "Unknown")
                # Sanitizza il nome del cinema per usarlo come chiave MongoDB
                cinema_key = cinema_name.replace(".", "_").replace("$", "_")
                
                # Path: regions.<region>.dates.<date>.cinemas.<cinema>
                cinema_path = f"regions.{region_slug}.dates.{today_str}.cinemas.{cinema_key}"
                update_set[f"{cinema_path}.cinema_name"] = cinema_name
                update_set[f"{cinema_path}.cinema_url"] = cinema.get("cinema_url", "")
                update_set[f"{cinema_path}.showtimes"] = cinema.get("showtimes", [])
            
            # Upsert: film_id è la chiave unica (un documento per film)
            collection.update_one(
                {"film_id": film_id},
                {"$set": update_set},
                upsert=True
            )
        
        # Pulizia date vecchie (>30 giorni) per tutti i documenti
        for doc in collection.find({}):
            if "regions" not in doc:
                continue
            
            unset_fields = {}
            for region_slug, region_data in doc.get("regions", {}).items():
                for date_str in region_data.get("dates", {}).keys():
                    if date_str < cutoff_date:
                        unset_fields[f"regions.{region_slug}.dates.{date_str}"] = ""
            
            if unset_fields:
                collection.update_one({"_id": doc["_id"]}, {"$unset": unset_fields})
        
        print(f"💾 Salvati {len(data)} film su MongoDB (schema gerarchico)")
        client.close()
    except Exception as e:
        print(f"❌ Errore MongoDB: {e}")


# Progress tracking collection
def get_progress_collection():
    """Returns the progress tracking collection."""
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    return client["cinematch_db"]["scraper_progress"]

def update_progress(current: int, total: int, status: str, province: str = ""):
    """Updates the scraper progress in MongoDB."""
    try:
        collection = get_progress_collection()
        italy_tz = pytz.timezone('Europe/Rome')
        collection.update_one(
            {"_id": "cinema_scraper"},
            {"$set": {
                "current": current,
                "total": total,
                "percentage": round((current / total) * 100) if total > 0 else 0,
                "status": status,
                "current_province": province,
                "updated_at": datetime.now(italy_tz).isoformat()
            }},
            upsert=True
        )
    except Exception as e:
        print(f"⚠️ Errore aggiornamento progresso: {e}")

def clear_progress():
    """Clears the progress when scraping is complete."""
    try:
        collection = get_progress_collection()
        italy_tz = pytz.timezone('Europe/Rome')
        collection.update_one(
            {"_id": "cinema_scraper"},
            {"$set": {
                "current": 0,
                "total": 0,
                "percentage": 100,
                "status": "completed",
                "current_province": "",
                "updated_at": datetime.now(italy_tz).isoformat()
            }},
            upsert=True
        )
    except Exception as e:
        print(f"⚠️ Errore pulizia progresso: {e}")


def run_scraper():
    """FASE 1: Esegue lo scraping da ComingSoon.it"""
    italy_tz = pytz.timezone('Europe/Rome')
    print("=" * 70)
    print("🎬 FASE 1: SCRAPE COMINGSOON")
    print("   Scarica orari cinema da ComingSoon.it → showtimes")
    print("=" * 70)
    print(f"⏰ Avvio: {datetime.now(italy_tz).strftime('%Y-%m-%d %H:%M:%S')} (ora italiana)")
    
    provinces = CAMPANIA_PROVINCES
    total_provinces = len(provinces)
    print(f"\n🏛️ Campania: {', '.join(p['name'] for p in provinces)}")
    
    # Initialize progress
    update_progress(0, total_provinces, "starting", "")
    
    all_results = []
    
    for idx, province in enumerate(provinces):
        # Update progress start of province
        update_progress(idx, total_provinces, "scraping", province['name'])
        
        results = scrape_province(province)
        
        # Save IMMEDIATELY after each province
        if results:
            print(f"💾 Salvataggio parziale per {province['name']} ({len(results)} film)...")
            save_to_mongodb(results)
            all_results.extend(results)
        
        time.sleep(REQUEST_DELAY)
        
        # Update progress after completing province
        update_progress(idx + 1, total_provinces, "scraping", province['name'])
    
    print(f"\n📊 Totale record accumulati: {len(all_results)}")
    
    # Update progress for completion
    update_progress(total_provinces, total_provinces, "saving", "Finalizzazione")
    
    # Esempio
    if all_results:
        print("\n" + "=" * 70)
        print("📋 Esempio dati estratti:")
        print("=" * 70)
        example = all_results[0]
        print(f"Film: {example['film_title']}")
        print(f"Titolo originale: {example.get('film_original_title') or 'N/A'}")
        print(f"Regista: {example.get('director') or 'N/A'}")
        print(f"Provincia: {example['province']}")
        print(f"Cinema: {len(example['cinemas'])}")
        
        if example['cinemas']:
            cinema = example['cinemas'][0]
            print(f"\n  🎦 {cinema['cinema_name']}")
            print(f"     {cinema.get('address', '')}")
            for show in cinema.get('showtimes', [])[:3]:
                print(f"     - {show.get('sala', 'N/A')}: {show['time']} ({show.get('price', 'N/A')})")
    
    # Mark as completed
    clear_progress()
    
    print(f"\n✅ FASE 1 Completata: {datetime.now(italy_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    
    return all_results


# =============================================================================
# =============================================================================
# FASE 2: CINEMA FILM SYNC
# Legge film da "showtimes" → cerca su TMDB → aggiunge a "movies_catalog"
# =============================================================================
# =============================================================================

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
                
                # Match parziale (contenimento) - Molto rischioso, rimosso per titoli brevi
                # Lo usiamo solo se entrambi i titoli sono lunghi e molto simili
                if len(norm_title) > 15 and len(cand_norm_title) > 15:
                    if norm_title in cand_norm_title or cand_norm_title in norm_title:
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

    def search_by_director(self, director: str, movie_title: str, original_title: str = None) -> dict:
        """
        Fallback "nucleare": Cerca il regista, scarica la sua filmografia completa 
        e cerca il titolo all'interno. Utile per film futuri o con titoli difficili.
        
        Args:
            director: Nome del regista
            movie_title: Titolo del film (italiano)
            original_title: Titolo originale (inglese/altra lingua)
        """
        if not director or not movie_title:
            return None
            
        try:
            # 1. Cerca ID regista
            url_person = f"{TMDB_BASE_URL}/search/person"
            params_p = {"api_key": self.api_key, "query": director}
            resp_p = requests.get(url_person, params=params_p, timeout=10)
            people = resp_p.json().get("results", [])
            
            if not people:
                return None
                
            pid = people[0]['id']
            
            # 2. Scarica filmografia (credits)
            # Proviamo in italiano per massimizzare match con titolo italiano
            url_credits = f"{TMDB_BASE_URL}/person/{pid}/movie_credits"
            params_c = {"api_key": self.api_key, "language": "it-IT"} 
            resp_c = requests.get(url_credits, params=params_c, timeout=10)
            crew = resp_c.json().get('crew', [])
            
            # Filtra solo quelli diretti
            directed_movies = [m for m in crew if m.get('job') == 'Director']
            
            # Crea lista di titoli da cercare (normalizzati)
            search_titles = [self.normalize_title(movie_title)]
            if original_title:
                norm_orig = self.normalize_title(original_title)
                if norm_orig and norm_orig not in search_titles:
                    search_titles.append(norm_orig)
            
            print(f"    🔍 Cercando nella filmografia di {director}: {search_titles}")
            
            # 3. Cerca match per ogni film nella filmografia
            for m in directed_movies:
                m_title = m.get('title', '')
                m_orig = m.get('original_title', '')
                norm_m_title = self.normalize_title(m_title)
                norm_m_orig = self.normalize_title(m_orig)
                
                # Match esatto normalizzato con qualsiasi titolo cercato
                for search_title in search_titles:
                    if norm_m_title == search_title:
                        print(f"    🎯 Trovato via regista: {m_title} (ID: {m['id']})")
                        return m
                    if norm_m_orig == search_title:
                        print(f"    🎯 Trovato via regista: {m_title} [Orig: {m_orig}] (ID: {m['id']})")
                        return m
                    
                    # Match fuzzy/contenimento (solo se lunghezza decente)
                    if len(search_title) > 5:
                        if search_title in norm_m_title or norm_m_title in search_title:
                            print(f"    🎯 Trovato via regista (fuzzy): {m_title} (ID: {m['id']})")
                            return m
                        if search_title in norm_m_orig or norm_m_orig in search_title:
                            print(f"    🎯 Trovato via regista (fuzzy orig): {m_title} (ID: {m['id']})")
                            return m
            
            return None
            
        except Exception as e:
            print(f"    ⚠️ Errore search_by_director: {e}")
            return None

    def search_tmdb(self, title: str, director: str = None, year: int = None, original_title: str = None) -> dict:
        """
        Cerca un film su TMDB con logica robusta:
        1. Ricerca per titolo fornito in italiano.
        2. Ricerca in inglese se non trova nulla.
        3. Ricerca per titolo normalizzato.
        4. Verifica i risultati confrontando il regista (se fornito).
        5. Fallback: cerca via filmografia del regista.
        """
        search_queries = [title]
        norm_title = self.normalize_title(title)
        if norm_title != title.lower():
            search_queries.append(norm_title)
        
        # Prova anche senza articoli e parole comuni
        cleaned_title = re.sub(r'^(la |il |lo |le |i |gli |the |a |an )','', title.lower())
        if cleaned_title != title.lower():
            search_queries.append(cleaned_title)

        # RIMOZIONE LIMITI LINGUA: Aggiungiamo russo e persiano per Ellie e Divine Comedy
        # E proviamo anche senza specificare la lingua per lasciare che TMDB decida
        languages = ["it-IT", "en-US", "ru-RU", "fa-IR", "de-DE", "fr-FR", "es-ES", "ja-JP", None]
        
        for lang in languages:
            for query in search_queries:
                url = f"{TMDB_BASE_URL}/search/movie"
                params = {
                    "api_key": self.api_key,
                    "query": query,
                    "include_adult": False
                }
                if lang:
                    params["language"] = lang
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
        
        # Se tutte le ricerche standard falliscono e abbiamo un regista, tentiamo il fallback
        if director:
            print(f"    ⚠️ Titolo non trovato, provo ricerca via filmografia regista: {director}...")
            director_result = self.search_by_director(director, title, original_title)
            if director_result:
                return director_result

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
            
            # RIMOZIONE LIMITI POSTER: Non filtriamo più i film senza poster.
            # Usiamo un segnaposto per garantire la visualizzazione.
            has_real_poster = True
            if not poster_url:
                print(f"      - Poster mancante, uso segnaposto")
                poster_url = "https://via.placeholder.com/500x750?text=No+Poster"
                has_real_poster = False
            
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
                "has_real_poster": has_real_poster,
                "loaded_at": datetime.now(pytz.timezone('Europe/Rome')).isoformat(),
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

    def safe_add_specific_films(self):
        """Metodo per assicurarsi che i film richiesti dall'utente siano nel catalogo."""
        requested_films = [
            {
                "title": "Ellie e la Città di Smeraldo",
                "original_title": "Volshebnik Izumrudnogo goroda",
                "director": "Igor Voloshin"
            },
            {
                "title": "Divine Comedy",
                "original_title": "Komedie Elahi",
                "director": "Ali Asgari"
            }
        ]
        
        print("\n🔍 Verifica film richiesti dall'utente...")
        for film in requested_films:
            # Verifica se esiste
            match = self.film_exists_in_catalog(film['title'], film['original_title'], film['director'])
            if match:
                print(f"  ✅ {film['title']} già presente.")
                continue
                
            print(f"  🆕 Aggiunta forzata: {film['title']}...")
            tmdb_result = self.search_tmdb(film['original_title'], director=film['director'], original_title=film['original_title'])
            if not tmdb_result:
                tmdb_result = self.search_tmdb(film['title'], director=film['director'])
                
            if tmdb_result:
                details = self.fetch_full_details(tmdb_result['id'])
                if details:
                    self.add_film_to_catalog(details)
                    print(f"    ✨ {film['title']} aggiunto con successo.")
                else:
                    print(f"    ❌ Impossibile ottenere dettagli per {film['title']}.")
            else:
                print(f"    ❌ Impossibile trovare {film['title']} su TMDB.")

    def sync(self) -> dict:
        """FASE 2: Esegue la sincronizzazione completa."""
        print("\n" + "=" * 70)
        print("🔄 FASE 2: CINEMA FILM SYNC")
        print("   Legge showtimes → cerca TMDB → aggiunge a movies_catalog")
        print("=" * 70)
        print(f"⏰ Avvio: {datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 0. Aggiungi film specifici richiesti (safety step)
        self.safe_add_specific_films()
        
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
                tmdb_result = self.search_tmdb(original_title, director=director, original_title=original_title) if original_title else None
                
                if not tmdb_result:
                    tmdb_result = self.search_tmdb(title, director=director, original_title=original_title)
                
                # Se ancora nulla, prova senza regista (fallback meno preciso)
                if not tmdb_result and original_title:
                    tmdb_result = self.search_tmdb(original_title, original_title=original_title)
                if not tmdb_result:
                    tmdb_result = self.search_tmdb(title, original_title=original_title)
                
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
        print("\n" + "=" * 70)
        print("📊 Riepilogo FASE 2:")
        print("=" * 70)
        print(f"  Film in programmazione: {len(films)}")
        print(f"  Già nel catalogo: {len(existing)}")
        print(f"  Aggiunti ora: {len(added)}")
        print(f"  Non trovati/filtrati: {len(failed)}")
        print(f"\n✅ FASE 2 Completata: {datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {
            "total": len(films),
            "existing": len(existing),
            "added": len(added),
            "failed": len(failed),
            "missing_titles": [f["title"] for f in failed]
        }


def sync_films_to_catalog():
    """Wrapper per eseguire solo la FASE 2."""
    syncer = CinemaFilmSync()
    return syncer.sync()


# =============================================================================
# =============================================================================
# MAIN: ESEGUE ENTRAMBE LE FASI IN SEQUENZA
# =============================================================================
# =============================================================================

def run_full_pipeline():
    """
    Esegue il pipeline completo:
    1. Scrape ComingSoon.it → showtimes
    2. Sync showtimes → movies_catalog (via TMDB)
    """
    italy_tz = pytz.timezone('Europe/Rome')
    
    print("\n" + "=" * 70)
    print("🎬 CINEMA PIPELINE - Esecuzione Completa")
    print("=" * 70)
    print(f"⏰ Inizio: {datetime.now(italy_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # FASE 1: Scrape
    print("\n" + "▶" * 35)
    scrape_results = run_scraper()
    
    # FASE 2: Sync
    print("\n" + "▶" * 35)
    sync_results = sync_films_to_catalog()
    
    # Report finale
    print("\n" + "=" * 70)
    print("🏁 PIPELINE COMPLETATO")
    print("=" * 70)
    print(f"  FASE 1 (Scrape): {len(scrape_results)} film scaricati")
    print(f"  FASE 2 (Sync):   {sync_results['added']} film aggiunti al catalogo")
    print(f"⏰ Fine: {datetime.now(italy_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    return {
        "scrape_results": len(scrape_results),
        "sync_results": sync_results
    }


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scrape-only":
            print("Esecuzione SOLO Fase 1 (Scrape)...")
            run_scraper()
        elif sys.argv[1] == "--sync-only":
            print("Esecuzione SOLO Fase 2 (Sync)...")
            sync_films_to_catalog()
        elif sys.argv[1] == "--help":
            print("""
Cinema Pipeline - Gestione dati cinematografici

Uso:
  python cinema_pipeline.py              # Esegue ENTRAMBE le fasi
  python cinema_pipeline.py --scrape-only  # Solo Fase 1 (scrape ComingSoon)
  python cinema_pipeline.py --sync-only    # Solo Fase 2 (sync TMDB → catalogo)
  python cinema_pipeline.py --help         # Mostra questo messaggio
""")
        else:
            print(f"Opzione sconosciuta: {sys.argv[1]}")
            print("Usa --help per vedere le opzioni disponibili.")
    else:
        # Default: esegui tutto
        run_full_pipeline()
