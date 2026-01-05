import pandas as pd
import requests

# Configurazione (Sostituisci con la tua chiave TMDB)
TMDB_API_KEY = "TUA_CHIAVE_API"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def parse_letterboxd_csv(file_path):
    """
    Legge il file CSV di Letterboxd e restituisce una lista di film.
    """
    try:
        df = pd.read_csv(file_path)
        # Letterboxd usa solitamente colonne come 'Name', 'Year', 'Rating'
        # Adattiamo le colonne se necessario
        movies = df[['Name', 'Year', 'Rating']].to_dict('records')
        print(f"Letti {len(movies)} film dal CSV.")
        return movies
    except Exception as e:
        print(f"Errore nella lettura del CSV: {e}")
        return []

def get_movie_details(title, year):
    """
    Cerca un film su TMDB per ottenere poster e generi.
    """
    search_url = f"{TMDB_BASE_URL}/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "year": year
    }
    
    response = requests.get(search_url, params=params)
    if response.status_code == 200:
        results = response.json().get('results')
        if results:
            movie = results[0]
            return {
                "id": movie.get("id"),
                "poster_path": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}",
                "genres": movie.get("genre_ids"), # Nota: TMDB dà ID generi, andranno mappati
                "overview": movie.get("overview")
            }
    return None

# Esempio di utilizzo (commentato):
# movies = parse_letterboxd_csv('ratings.csv')
# for m in movies[:5]:
#     details = get_movie_details(m['Name'], m['Year'])
#     print(f"Film: {m['Name']} - Poster: {details['poster_path'] if details else 'N/A'}")
