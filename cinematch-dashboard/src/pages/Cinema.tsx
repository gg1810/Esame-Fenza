import { useState, useEffect } from 'react';
import { MovieModal } from '../components/MovieModal';
import { catalogAPI } from '../services/api';
import './Cinema.css';

interface CinemaShowtime {
    time: string;
    price: string;
    sala: string;
}

interface CinemaInfo {
    name: string;
    address: string;
    showtimes: CinemaShowtime[];
}

interface CinemaFilm {
    id: string;
    title: string;
    original_title: string;
    director: string;
    poster: string;
    description: string;
    rating: number | null;
    genres: string[];
    year: number | null;
    duration: number | null;
    actors?: string;
    cinemas: CinemaInfo[];
    province: string;
    imdb_id?: string;
}

interface CinemaResponse {
    province: string;
    films: CinemaFilm[];
    total: number;
    last_update: string | null;
    is_refreshing: boolean;
}

// Helper to format date in Italian
function formatDateItalian(isoDate: string | null): string {
    if (!isoDate) return 'Data non disponibile';
    try {
        const date = new Date(isoDate);
        const months = [
            'Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
            'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre'
        ];
        return `${date.getDate()} ${months[date.getMonth()]} ${date.getFullYear()}`;
    } catch {
        return 'Data non disponibile';
    }
}

export function Cinema() {
    const [films, setFilms] = useState<CinemaFilm[]>([]);
    const [selectedFilm, setSelectedFilm] = useState<CinemaFilm | null>(null);
    const [province, setProvince] = useState<string>('');
    const [availableProvinces, setAvailableProvinces] = useState<{ slug: string, name: string }[]>([]);
    const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [isWatchedModalOpen, setIsWatchedModalOpen] = useState(false);
    const [lastUpdate, setLastUpdate] = useState<string | null>(null);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [refreshProgress, setRefreshProgress] = useState(0);
    const [refreshProvince, setRefreshProvince] = useState('');
    const [isSyncing, setIsSyncing] = useState(false);
    const [syncFilm, setSyncFilm] = useState('');
    // Date navigation
    const [selectedDate, setSelectedDate] = useState<string>('');
    const [availableDates, setAvailableDates] = useState<string[]>([]);
    const [todayDate, setTodayDate] = useState<string>('');

    // Fetch available provinces on mount
    useEffect(() => {
        const fetchProvinces = async () => {
            try {
                const response = await fetch('http://localhost:8000/cinema/provinces');
                if (response.ok) {
                    const data = await response.json();
                    setAvailableProvinces(data.provinces || []);
                }
            } catch (err) {
                console.error('Error fetching provinces:', err);
            }
        };
        fetchProvinces();
    }, []);

    useEffect(() => {
        // Fetch available dates on mount
        const fetchDates = async () => {
            try {
                const token = localStorage.getItem('token');
                const response = await fetch('http://localhost:8000/cinema/dates', {
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                });
                if (response.ok) {
                    const data = await response.json();
                    setAvailableDates(data.available_dates || []);
                    setTodayDate(data.today);
                    setSelectedProvince(data.province || '');

                    const newestDate = data.available_dates?.length > 0
                        ? data.available_dates[data.available_dates.length - 1]
                        : data.today;

                    if (newestDate) {
                        setSelectedDate(newestDate);
                    } else {
                        // Edge case: No dates and no today? Stop loading.
                        console.warn("No dates available");
                        setLoading(false);
                        setError("Nessuna data disponibile");
                    }
                } else {
                    throw new Error("Failed to fetch dates");
                }
            } catch (err) {
                console.error('Error fetching dates:', err);
                const today = new Date().toISOString().split('T')[0];
                setSelectedDate(today);
                setTodayDate(today);
                // The useEffect for selectedDate will trigger fetchCinemaFilms -> stops loading.
            }
        };
        fetchDates();
    }, []);

    useEffect(() => {
        if (selectedDate) {
            fetchCinemaFilms(selectedDate, selectedProvince);
        }
    }, [selectedDate, selectedProvince]);

    // Poll for progress when refreshing
    useEffect(() => {
        if (!isRefreshing) return;

        const pollProgress = async () => {
            try {
                const response = await fetch('http://localhost:8000/cinema/status');
                if (response.ok) {
                    const data = await response.json();
                    setRefreshProgress(data.percentage);
                    setRefreshProvince(data.current_province);

                    // If completed, refresh the films list and navigate to today
                    if (data.status === 'completed' || data.status === 'idle') {
                        // Only stop if we were actually ensuring a refresh
                        // But polling runs every 2s. If status is IDLE, we should stop unless we just started.
                        // We need to differentiate "Idle before start" vs "Idle after finish".
                        // Logic: pollProgress is defined. We need to handle "completed" state.

                        // Check if we reached 100% or explicitly confirmation of completion
                        if (data.status === 'completed' || (data.status === 'idle' && refreshProgress > 90)) {
                            setIsRefreshing(false);
                            setIsSyncing(false);
                            setRefreshProgress(0);
                            // Refetch dates and navigate to today
                            setTimeout(async () => {
                                try {
                                    const token = localStorage.getItem('token');
                                    const datesResponse = await fetch('http://localhost:8000/cinema/dates', {
                                        headers: { 'Authorization': `Bearer ${token}` }
                                    });
                                    if (datesResponse.ok) {
                                        const datesData = await datesResponse.json();
                                        setAvailableDates(datesData.available_dates || []);
                                        setTodayDate(datesData.today);
                                        // Auto-navigate to today
                                        setSelectedDate(datesData.today);
                                        // Also fetch films for today
                                        fetchCinemaFilms(datesData.today, selectedProvince);
                                    }
                                } catch (err) {
                                    console.error('Error refreshing dates:', err);
                                }
                            }, 1000);
                        }
                    }
                }
            } catch (err) {
                console.error('Error polling progress:', err);
            }
        };

        pollProgress(); // Initial poll
        const interval = setInterval(pollProgress, 2000); // Poll every 2 seconds

        return () => clearInterval(interval);
    }, [isRefreshing, isSyncing, refreshProgress]);

    const handleRefreshClick = async () => {
        try {
            setIsRefreshing(true);
            setRefreshProgress(0);

            const token = localStorage.getItem('token');
            await fetch(`http://localhost:8000/cinema/refresh?province=${selectedProvince}`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });
        } catch (err) {
            console.error("Refresh failed", err);
            setIsRefreshing(false);
        }
    };

    const fetchCinemaFilms = async (forDate?: string, forProvince?: string) => {
        try {
            const token = localStorage.getItem('token');
            const params = new URLSearchParams();
            if (forDate) params.append('date', forDate);
            if (forProvince) params.append('province', forProvince);
            const queryString = params.toString() ? `?${params.toString()}` : '';

            const response = await fetch(`http://localhost:8000/cinema/films${queryString}`, {
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });

            if (response.ok) {
                const data: CinemaResponse = await response.json();
                setFilms(data.films);
                setProvince(data.province);
                setLastUpdate(data.last_update);
                // Don't override refreshing state from server unless useful, local state is better for immediate feedback
                // setIsRefreshing(data.is_refreshing); 
                if (data.films.length > 0) {
                    setSelectedFilm(data.films[0]);
                } else {
                    setSelectedFilm(null);
                }
            } else {
                setError('Errore nel caricamento dei film');
            }
        } catch (err) {
            setError('Errore di connessione');
        } finally {
            setLoading(false);
        }
    };

    // ... (rest of methods)

    // Helper conditions
    // Show refresh button if today is NOT in available dates
    const isTodayMissing = !availableDates.includes(todayDate);

    // ... (render)

    // Navigation handlers
    const goToPreviousDay = () => {
        const currentIndex = availableDates.indexOf(selectedDate);
        if (currentIndex > 0) {
            setSelectedDate(availableDates[currentIndex - 1]);
        }
    };

    const goToNextDay = () => {
        const currentIndex = availableDates.indexOf(selectedDate);
        if (currentIndex < availableDates.length - 1) {
            setSelectedDate(availableDates[currentIndex + 1]);
        }
    };

    const handleAddToWatched = async (rating: number, review: string) => {
        if (!selectedFilm) return;

        try {
            const token = localStorage.getItem('token');
            const payload = {
                name: selectedFilm.title,
                year: selectedFilm.year,
                rating: rating,
                date: new Date().toISOString().split('T')[0],
                review: review,
                // Link al catalogo per dati completi
                imdb_id: selectedFilm.imdb_id
            };

            const response = await fetch('http://localhost:8000/movies', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (response.ok) {
                setIsWatchedModalOpen(false);

                // Filtra via il film appena visto
                const remainingFilms = films.filter(f => f.id !== selectedFilm.id);
                setFilms(remainingFilms);

                // Seleziona automaticamente il prossimo film con il rating più alto
                if (remainingFilms.length > 0) {
                    // Ordina per rating decrescente
                    const bestFilm = remainingFilms.reduce((prev: CinemaFilm, current: CinemaFilm) => {
                        const prevRating = prev.rating || 0;
                        const currRating = current.rating || 0;
                        return prevRating >= currRating ? prev : current;
                    });

                    setSelectedFilm(bestFilm);
                } else {
                    setSelectedFilm(null);
                }
            } else {
                console.error("Errore salvataggio film");
                alert("Si è verificato un errore durante il salvataggio.");
            }
        } catch (err) {
            console.error(err);
            alert("Errore di connessione.");
        }
    };

    const formatDisplayDate = (isoDate: string) => {
        if (!isoDate) return '';
        const d = new Date(isoDate);
        return d.toLocaleDateString('it-IT', { weekday: 'short', day: 'numeric', month: 'short' });
    };

    const currentIndex = availableDates.indexOf(selectedDate);
    const canGoPrevious = currentIndex > 0;
    const canGoNext = currentIndex < availableDates.length - 1;
    const isToday = selectedDate === todayDate;

    return (
        <div className="cinema-page">
            <div className="page-header">
                <h1>🎭 Al Cinema Ora</h1>
                <div className="date-navigation">
                    <button
                        className="date-nav-btn"
                        onClick={goToPreviousDay}
                        disabled={!canGoPrevious}
                        title="Giorno precedente"
                    >
                        ◀
                    </button>
                    <span className="current-date">
                        {isToday ? 'Oggi' : formatDisplayDate(selectedDate)}
                    </span>
                    <button
                        className="date-nav-btn"
                        onClick={goToNextDay}
                        disabled={!canGoNext}
                        title="Giorno successivo"
                    >
                        ▶
                    </button>

                    {/* Manual Refresh Button - Hidden while refreshing to avoid redundancy */}
                    {(isTodayMissing && !isRefreshing) && (
                        <button
                            className={`refresh-btn ${isRefreshing ? 'spinning' : ''}`}
                            onClick={handleRefreshClick}
                            disabled={isRefreshing}
                            title="Aggiorna programmazione film"
                        >
                            <span className="icon">🔄</span>
                            {isRefreshing ? 'Aggiornamento...' : 'Aggiorna Film'}
                        </button>
                    )}
                </div>
                <div className="province-row">
                    <span>Film in programmazione a </span>
                    <select
                        className="province-selector"
                        value={selectedProvince}
                        onChange={(e) => setSelectedProvince(e.target.value)}
                    >
                        {availableProvinces.map(p => (
                            <option key={p.slug} value={p.slug}>{p.name}</option>
                        ))}
                    </select>
                    {isRefreshing && (
                        <span className="refreshing-badge">
                            🔄 Aggiornamento in corso {refreshProgress}%
                        </span>
                    )}
                    {isSyncing && (
                        <span className="refreshing-badge sync-badge">
                            🎬 Recupero film{syncFilm && `: ${syncFilm}`}
                        </span>
                    )}
                </div>
            </div>

            <div className="cinema-layout">
                {/* Film Principale Selezionato */}
                {selectedFilm && (
                    <div className="featured-movie">
                        <div className="featured-poster-container">
                            <div className="featured-poster">
                                <img
                                    src={selectedFilm.poster}
                                    alt={selectedFilm.title}
                                    loading="eager"
                                />
                                {selectedFilm.rating && (
                                    <div className="poster-overlay">
                                        <div className="rating-badge">
                                            <span className="star">★</span> {selectedFilm.rating.toFixed(1)}
                                        </div>
                                    </div>
                                )}
                            </div>
                            <button className="btn-watched-under-poster" onClick={() => setIsWatchedModalOpen(true)}>
                                Film già visto
                            </button>
                        </div>

                        <div className="featured-info">
                            <h2>{selectedFilm.title}</h2>
                            {selectedFilm.original_title && selectedFilm.original_title !== selectedFilm.title && (
                                <p className="original-title">({selectedFilm.original_title})</p>
                            )}
                            <div className="movie-extra-info">
                                {selectedFilm.director && (
                                    <p className="director-info">
                                        <strong>Regia:</strong> {selectedFilm.director}
                                    </p>
                                )}
                                {selectedFilm.actors && (
                                    <p className="cast-info">
                                        <strong>Cast:</strong> {selectedFilm.actors}
                                    </p>
                                )}
                            </div>

                            {selectedFilm.genres.length > 0 && (
                                <div className="genres-row">
                                    {selectedFilm.genres.slice(0, 4).map((genre) => (
                                        <span key={genre} className="genre-tag">{genre}</span>
                                    ))}
                                </div>
                            )}

                            {selectedFilm.description && (
                                <div className="description-section">
                                    <p className="movie-description">{selectedFilm.description}</p>
                                </div>
                            )}

                            {/* Cinema e Orari */}
                            <div className="cinemas-section">
                                <h4>📍 Cinema Disponibili</h4>
                                {selectedFilm.cinemas.map((cinema, idx) => {
                                    // Sort showtimes by time ascending
                                    const sortedShowtimes = [...cinema.showtimes].sort((a, b) => {
                                        const timeA = a.time.replace(':', '');
                                        const timeB = b.time.replace(':', '');
                                        return parseInt(timeA) - parseInt(timeB);
                                    });
                                    return (
                                        <div key={idx} className="cinema-block">
                                            <div className="cinema-name-header">
                                                🎬 {cinema.name}
                                            </div>
                                            <div className="showtimes-grid">
                                                {sortedShowtimes.map((show, sIdx) => (
                                                    <button key={sIdx} className="showtime-btn">
                                                        <span className="time">{show.time}</span>
                                                        {show.price && <span className="price">{show.price}</span>}
                                                        {show.sala && <span className="sala">{show.sala}</span>}
                                                    </button>
                                                ))}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    </div>
                )}

                {/* Lista Altri Film */}
                <div className="other-movies">
                    <h3>Film in Sala ({films.length})</h3>
                    <div className="movie-list">
                        {films.map((film) => (
                            <div
                                key={film.id}
                                className={`movie-list-item ${selectedFilm?.id === film.id ? 'active' : ''}`}
                                onClick={() => setSelectedFilm(film)}
                            >
                                <img
                                    src={film.poster}
                                    alt={film.title}
                                    className="list-poster"
                                    loading="lazy"
                                />
                                <div className="list-info">
                                    <h4>{film.title}</h4>
                                    <p>{film.genres.slice(0, 2).join(', ')}</p>
                                    <div className="list-meta">
                                        {film.rating && (
                                            <span className="rating">★ {film.rating.toFixed(1)}</span>
                                        )}
                                        <span className="cinemas-count">
                                            🎦 {film.cinemas.length} cinema
                                        </span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {isWatchedModalOpen && selectedFilm && (
                <MovieModal
                    movie={{
                        name: selectedFilm.title,
                        year: selectedFilm.year || new Date().getFullYear(),
                        poster_url: selectedFilm.poster,
                        description: selectedFilm.description,
                        director: selectedFilm.director,
                        actors: selectedFilm.actors,
                        genres: selectedFilm.genres
                    } as any}
                    mode="edit"
                    hideDetailsButton={true}
                    onClose={() => setIsWatchedModalOpen(false)}
                    onSave={handleAddToWatched}
                />
            )}
        </div>
    );
}
