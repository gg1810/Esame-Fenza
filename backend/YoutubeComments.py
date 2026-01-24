"""
YoutubeComments - Ottiene trailer YouTube del primo film in uscita il mese prossimo
"""
import requests
from datetime import date, datetime

TMDB_API_KEY = "9c28ec1bf4b6a7b0975768d3e7ccfa4c"
LANGUAGE = "en-US"


def get_first_upcoming_movie_next_month():
    """
    Prende il primo film del mese successivo dalle upcoming movies di TMDB.
    """
    today = date.today()

    # calcolo mese successivo
    if today.month == 12:
        target_month = 1
        target_year = today.year + 1
    else:
        target_month = today.month + 1
        target_year = today.year

    url = "https://api.themoviedb.org/3/movie/upcoming"
    page = 1

    while True:
        params = {
            "api_key": TMDB_API_KEY,
            "language": LANGUAGE,
            "page": page
        }

        response = requests.get(url, params=params)
        data = response.json()

        results = data.get("results", [])
        if not results:
            return None

        for film in results:
            release_date = film.get("release_date")
            if not release_date:
                continue

            release = datetime.strptime(release_date, "%Y-%m-%d").date()

            if release.year == target_year and release.month == target_month:
                return {
                    "movie_id": film["id"],
                    "title": film.get("title"),
                    "release_date": release_date
                }

        if page >= data.get("total_pages", 1):
            break

        page += 1

    return None


def get_youtube_trailer(movie_id: int):
    """
    Ottiene il link al trailer YouTube di un film.
    """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/videos"
    params = {
        "api_key": TMDB_API_KEY,
        "language": LANGUAGE
    }

    response = requests.get(url, params=params)
    data = response.json()

    for video in data.get("results", []):
        if video["site"] == "YouTube" and video["type"] == "Trailer":
            return {
                "url": f"https://www.youtube.com/watch?v={video['key']}",
                "embed_id": video['key']
            }

    return None


def get_upcoming_movie_with_trailer():
    """
    Ottiene il primo film in uscita il mese prossimo con il suo trailer.
    Restituisce un dizionario con title, release_date, trailer_url, embed_id.
    """
    film = get_first_upcoming_movie_next_month()
    
    if not film:
        return None
    
    trailer_data = get_youtube_trailer(film["movie_id"])
    
    return {
        "title": film["title"],
        "release_date": film["release_date"],
        "trailer_url": trailer_data["url"] if trailer_data else None,
        "embed_id": trailer_data["embed_id"] if trailer_data else None
    }
