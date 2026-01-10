"""
ComingSoon.it Scraper v6
Scarica cinema, orari E dettagli film (titolo, titolo originale, regista).

Fix: estrae l'URL della scheda film direttamente dalla pagina provincia.
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pymongo import MongoClient

# Configuration
BASE_URL = "https://www.comingsoon.it"
REQUEST_DELAY = 1.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}

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
            result = {
                "province": province_name,
                "province_slug": province_slug,
                "film_id": movie["id"],
                "film_title": movie["title"],
                "film_original_title": film_details.get("original_title", ""),
                "director": film_details.get("director", ""),
                "cinemas": showtimes,
                "updated_at": datetime.now().isoformat()
            }
            results.append(result)
            
            orig = result['film_original_title'] or "-"
            dir_ = result['director'] or "-"
            print(f"✅ Orig: {orig[:20]} | Regista: {dir_[:20]} | {len(showtimes)} cinema")
        else:
            print("⚠️ nessun cinema")
    
    return results


def save_to_mongodb(data: List[Dict]):
    """Salva i dati su MongoDB."""
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        db = client["cinematch_db"]
        collection = db["showtimes"]
        
        for record in data:
            collection.update_one(
                {
                    "province_slug": record["province_slug"],
                    "film_id": record["film_id"]
                },
                {"$set": record},
                upsert=True
            )
        
        print(f"💾 Salvati {len(data)} record su MongoDB")
        client.close()
    except Exception as e:
        print(f"❌ Errore MongoDB: {e}")


def main():
    print("=" * 70)
    print("🎬 ComingSoon.it Scraper v6 (titolo orig. + regista)")
    print(f"⏰ Avvio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    provinces = CAMPANIA_PROVINCES
    print(f"\n🏛️ Campania: {', '.join(p['name'] for p in provinces)}")
    
    all_results = []
    
    for province in provinces:
        results = scrape_province(province)
        all_results.extend(results)
        time.sleep(REQUEST_DELAY)
    
    print(f"\n📊 Totale record: {len(all_results)}")
    
    if all_results:
        save_to_mongodb(all_results)
    
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
    
    print(f"\n✅ Completato: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
