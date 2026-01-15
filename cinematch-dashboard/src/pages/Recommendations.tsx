import { useState, useEffect } from 'react';
import { MovieCard } from '../components/MovieCard';
import './Recommendations.css';
import { API_BASE_URL } from '../config';

interface Movie {
    id?: number;
    imdb_id?: string;
    title: string;
    year: number;
    poster: string;
    rating: number;
    genres: string[];
    director: string;
    matchScore?: number;
}

const genres = ['Tutti', 'Drama', 'Thriller', 'Sci-Fi', 'Comedy', 'Horror', 'Action'];

export function Recommendations() {
    const [activeGenre, setActiveGenre] = useState('Tutti');
    const [view, setView] = useState<'recommended' | 'not-recommended'>('recommended');
    const [recommendedMovies, setRecommendedMovies] = useState<Movie[]>([]);
    const [notRecommendedMovies, setNotRecommendedMovies] = useState<Movie[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [matchedFilms, setMatchedFilms] = useState(0);
    const [totalFilms, setTotalFilms] = useState(0);

    useEffect(() => {
        const fetchRecommendations = async () => {
            try {
                setLoading(true);
                setError(null);

                const token = localStorage.getItem('token');
                if (!token) {
                    setError('Devi effettuare il login per vedere le raccomandazioni');
                    setLoading(false);
                    return;
                }

                const response = await fetch(`${API_BASE_URL}/recommendations`, {
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                });

                if (!response.ok) {
                    throw new Error('Errore nel caricamento delle raccomandazioni');
                }

                const data = await response.json();

                if (data.message && data.recommended.length === 0) {
                    setError(data.message);
                } else {
                    // Transform API response to match Movie interface
                    const transformMovie = (m: any, index: number): Movie => ({
                        id: index + 1,
                        imdb_id: m.imdb_id,
                        title: m.title || 'Unknown',
                        year: m.year || 0,
                        poster: m.poster || 'https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster',
                        rating: m.rating || 0,
                        genres: Array.isArray(m.genres) ? m.genres : [],
                        director: m.director || '',
                        matchScore: m.matchScore || 0
                    });

                    setRecommendedMovies(data.recommended.map(transformMovie));
                    setNotRecommendedMovies(data.not_recommended.map(transformMovie));
                    setMatchedFilms(data.matched_films || 0);
                    setTotalFilms(data.total_films || 0);
                }
            } catch (err) {
                console.error('Error fetching recommendations:', err);
                setError('Errore nel caricamento delle raccomandazioni');
            } finally {
                setLoading(false);
            }
        };

        fetchRecommendations();
    }, []);

    const filterMovies = (movies: Movie[]) => {
        if (activeGenre === 'Tutti') return movies;
        return movies.filter(m => m.genres.includes(activeGenre));
    };

    const displayedMovies = view === 'recommended'
        ? filterMovies(recommendedMovies)
        : filterMovies(notRecommendedMovies);

    if (loading) {
        return (
            <div className="recommendations-page">
                <div className="page-header">
                    <h1>Raccomandazioni</h1>
                    <p>Caricamento in corso...</p>
                </div>
                <div className="loading-container">
                    <div className="spinner"></div>
                    <p>Analizzando i tuoi gusti cinematografici...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="recommendations-page">
                <div className="page-header">
                    <h1>Raccomandazioni</h1>
                    <p>Film selezionati in base ai tuoi gusti</p>
                </div>
                <div className="error-container">
                    <span className="error-icon">⚠️</span>
                    <p>{error}</p>
                    <p className="error-hint">Carica prima il tuo export Letterboxd dalla dashboard</p>
                </div>
            </div>
        );
    }

    return (
        <div className="recommendations-page">
            <div className="page-header">
                <h1>Raccomandazioni</h1>
                <p>Film selezionati in base ai tuoi gusti ({matchedFilms}/{totalFilms} film analizzati)</p>
            </div>

            <div className="filters-section">
                <div className="view-toggle">
                    <button
                        className={`toggle-btn ${view === 'recommended' ? 'active' : ''}`}
                        onClick={() => setView('recommended')}
                    >
                        <span className="toggle-icon">✓</span>
                        Consigliati
                    </button>
                    <button
                        className={`toggle-btn not-rec ${view === 'not-recommended' ? 'active' : ''}`}
                        onClick={() => setView('not-recommended')}
                    >
                        <span className="toggle-icon">✗</span>
                        Non Consigliati
                    </button>
                </div>

                <div className="genre-filters">
                    {genres.map((genre) => (
                        <button
                            key={genre}
                            className={`genre-btn ${activeGenre === genre ? 'active' : ''}`}
                            onClick={() => setActiveGenre(genre)}
                        >
                            {genre}
                        </button>
                    ))}
                </div>
            </div>

            <div className="movies-section">
                <div className="section-header">
                    <h2>
                        {view === 'recommended' ? '🎯 Film per Te' : '⚠️ Da Evitare'}
                    </h2>
                    <span className="movie-count">{displayedMovies.length} film</span>
                </div>

                <div className="movies-grid">
                    {displayedMovies.map((movie, index) => (
                        <div key={movie.imdb_id || movie.id} style={{ animationDelay: `${index * 0.1}s` }}>
                            <MovieCard
                                movie={movie}
                                showMatchScore={true}
                                variant={view === 'recommended' ? 'recommended' : 'not-recommended'}
                            />
                        </div>
                    ))}
                </div>

                {displayedMovies.length === 0 && (
                    <div className="no-results">
                        <span className="no-results-icon">🎬</span>
                        <p>Nessun film trovato per questo genere</p>
                    </div>
                )}
            </div>
        </div>
    );
}
