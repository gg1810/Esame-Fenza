import { useState, useEffect, useCallback } from 'react';
import { catalogAPI, dataAPI, type CatalogMovie, type MovieRating } from '../services/api';
import { MovieModal } from '../components/MovieModal';
import './CatalogoFilm.css';

interface UserMovie extends MovieRating { }

interface MoviesByYear {
    [year: string]: UserMovie[];
}

// Immagine stock di fallback
const STOCK_POSTER = 'https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster';

export function CatalogoFilm() {
    // Stati per la ricerca nel catalogo
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState<CatalogMovie[]>([]);
    const [isSearching, setIsSearching] = useState(false);
    const [selectedMovieForModal, setSelectedMovieForModal] = useState<CatalogMovie | UserMovie | null>(null);
    const [modalMode, setModalMode] = useState<'view' | 'edit'>('view');
    const [showAddModal, setShowAddModal] = useState(false);
    const [addingMovie, setAddingMovie] = useState(false);

    // Advanced Search State
    const [isAdvancedSearch, setIsAdvancedSearch] = useState(false);
    const [advancedFilters, setAdvancedFilters] = useState({
        title: '',
        actor: '',
        director: '',
        year: '',
        genre: ''
    });

    // Stato per il ricalcolo e refresh dati
    const [refreshTrigger, setRefreshTrigger] = useState(0);

    // Stati per i film dell'utente (come FilmVisti originale)
    const [movies, setMovies] = useState<UserMovie[]>([]);
    const [loading, setLoading] = useState(true);
    const [groupedMovies, setGroupedMovies] = useState<MoviesByYear>({});
    const [expandedYears, setExpandedYears] = useState<Set<string>>(new Set());
    const [sortOrder, setSortOrder] = useState<'desc' | 'asc'>('desc');
    const [filterRating, setFilterRating] = useState<number | null>(null);

    // Carica i film dell'utente
    useEffect(() => {
        fetchUserMovies();
    }, []);

    const fetchUserMovies = async () => {
        try {
            const data = await dataAPI.getUserMovies(0, 5000);
            const moviesList = data.movies || [];
            setMovies(moviesList);
            groupByYear(moviesList);
        } catch (error) {
            console.error('Errore caricamento film utente:', error);
        } finally {
            setLoading(false);
        }
    };

    // Effetto per il refresh quando cambiano i dati
    useEffect(() => {
        if (refreshTrigger > 0) {
            fetchUserMovies();
        }
    }, [refreshTrigger]);

    const groupByYear = (movieList: UserMovie[]) => {
        const grouped: MoviesByYear = {};
        movieList.forEach(movie => {
            const year = movie.year?.toString() || 'Sconosciuto';
            if (!grouped[year]) {
                grouped[year] = [];
            }
            grouped[year].push(movie);
        });

        // Ordina i film per rating all'interno di ogni anno
        Object.keys(grouped).forEach(year => {
            grouped[year].sort((a, b) => b.rating - a.rating);
        });

        setGroupedMovies(grouped);
        // Espandi i primi 3 anni di default
        const years = Object.keys(grouped).sort((a, b) => parseInt(b) - parseInt(a));
        setExpandedYears(new Set(years.slice(0, 3)));
    };

    const toggleYear = (year: string) => {
        const newExpanded = new Set(expandedYears);
        if (newExpanded.has(year)) {
            newExpanded.delete(year);
        } else {
            newExpanded.add(year);
        }
        setExpandedYears(newExpanded);
    };

    const expandAll = () => {
        setExpandedYears(new Set(Object.keys(groupedMovies)));
    };

    const collapseAll = () => {
        setExpandedYears(new Set());
    };

    const getSortedYears = () => {
        return Object.keys(groupedMovies).sort((a, b) => {
            return sortOrder === 'desc'
                ? parseInt(b) - parseInt(a)
                : parseInt(a) - parseInt(b);
        });
    };

    const getFilteredMovies = (yearMovies: UserMovie[]) => {
        if (filterRating === null) return yearMovies;
        return yearMovies.filter(m => m.rating === filterRating);
    };

    // Ricerca con debounce
    const searchMovies = useCallback(async (query: string) => {
        if (query.length < 2) {
            setSearchResults([]);
            return;
        }

        setIsSearching(true);
        try {
            const results = await catalogAPI.searchMovies(query, 20);
            setSearchResults(results.results);
        } catch (error) {
            console.error('Errore ricerca:', error);
            setSearchResults([]);
        } finally {
            setIsSearching(false);
        }
    }, []);

    // Debounce della ricerca
    useEffect(() => {
        const timer = setTimeout(() => {
            searchMovies(searchQuery);
        }, 300);

        return () => clearTimeout(timer);
    }, [searchQuery, searchMovies]);

    const openAddModal = (movie: CatalogMovie) => {
        // Verifica se il film è già nella collezione
        const existing = movies.find(
            m => m.name.toLowerCase() === movie.title.toLowerCase() && m.year === movie.year
        );

        if (existing) {
            setSelectedMovieForModal(existing);
        } else {
            setSelectedMovieForModal(movie);
        }
        setModalMode('edit');
        setShowAddModal(true);
    };

    const openEditModal = (movie: UserMovie) => {
        setSelectedMovieForModal(movie);
        setModalMode('edit');
        setShowAddModal(true);
    };

    const addOrUpdateMovie = async (rating: number, comment: string) => {
        if (!selectedMovieForModal) return;

        const name = 'title' in selectedMovieForModal ? selectedMovieForModal.title : selectedMovieForModal.name;
        const year = selectedMovieForModal.year || 0;

        setAddingMovie(true);
        try {
            await catalogAPI.addOrUpdateMovie({
                name,
                year,
                rating,
                comment,
                imdb_id: (selectedMovieForModal as any).imdb_id,
                poster_url: (selectedMovieForModal as any).poster_url
            });
            setShowAddModal(false);
            setRefreshTrigger(prev => prev + 1);
        } catch (error) {
            console.error('Errore durante il salvataggio:', error);
            throw error;
        } finally {
            setAddingMovie(false);
        }
    };

    const removeUserMovie = async () => {
        if (!selectedMovieForModal) return;
        const name = 'name' in selectedMovieForModal ? selectedMovieForModal.name : (selectedMovieForModal as CatalogMovie).title;
        const year = selectedMovieForModal.year || 0;

        if (confirm(`Rimuovere "${name}" dai tuoi visti?`)) {
            try {
                await catalogAPI.removeMovie(name, year);
                setShowAddModal(false);
                setRefreshTrigger(prev => prev + 1);
            } catch (error) {
                console.error('Errore durante la rimozione:', error);
                alert("Errore nella rimozione");
            }
        }
    };

    // Renderizza stelle
    const renderStars = (rating: number) => {
        const stars = [];
        for (let i = 1; i <= 5; i++) {
            stars.push(
                <span key={i} className={`star ${i <= rating ? 'filled' : 'empty'}`}>
                    ★
                </span>
            );
        }
        return stars;
    };

    // Statistiche rapide
    const totalMovies = movies.length;
    const avgRating = totalMovies > 0
        ? (movies.reduce((sum, m) => sum + m.rating, 0) / totalMovies).toFixed(2)
        : '0';

    const sortedYears = getSortedYears();

    if (loading) return <div className="loading-screen">Caricamento film...</div>;

    return (
        <div className="catalogo-film-page">
            {/* Header */}
            <div className="page-header">
                <h1>🎬 Catalogo Film</h1>
                <p>Cerca film nel database e aggiungili alla tua collezione</p>
            </div>

            {/* Barra di ricerca */}
            <div className="search-section">
                <div className="search-header-row">
                    {!isAdvancedSearch && (
                        <div className="search-box">
                            <span className="search-icon">🔍</span>
                            <input
                                type="text"
                                placeholder="Cerca un film da aggiungere..."
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                className="search-input"
                            />
                            {searchQuery && (
                                <button
                                    className="clear-search"
                                    onClick={() => {
                                        setSearchQuery('');
                                        setSearchResults([]);
                                    }}
                                >
                                    ✕
                                </button>
                            )}
                        </div>
                    )}

                    {!isAdvancedSearch && (
                        <button
                            className="advanced-search-toggle"
                            onClick={() => setIsAdvancedSearch(true)}
                        >
                            Ricerca Avanzata
                        </button>
                    )}
                </div>

                {isAdvancedSearch && (
                    <div className="advanced-search-panel">
                        <div className="advanced-grid">
                            <div className="input-group">
                                <label>Titolo</label>
                                <input
                                    type="text"
                                    className="advanced-input"
                                    placeholder="Es. Inception"
                                    value={advancedFilters.title}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, title: e.target.value })}
                                />
                            </div>
                            <div className="input-group">
                                <label>Attore</label>
                                <input
                                    type="text"
                                    className="advanced-input"
                                    placeholder="Es. Leonardo DiCaprio"
                                    value={advancedFilters.actor}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, actor: e.target.value })}
                                />
                            </div>
                            <div className="input-group">
                                <label>Regista</label>
                                <input
                                    type="text"
                                    className="advanced-input"
                                    placeholder="Es. Christopher Nolan"
                                    value={advancedFilters.director}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, director: e.target.value })}
                                />
                            </div>
                            <div className="input-group">
                                <label>Anno</label>
                                <input
                                    type="number"
                                    className="advanced-input"
                                    placeholder="Es. 2010"
                                    value={advancedFilters.year}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, year: e.target.value })}
                                />
                            </div>
                        </div>
                        <div className="advanced-actions">
                            <button
                                className="search-btn-secondary"
                                onClick={() => {
                                    setIsAdvancedSearch(false);
                                    setSearchQuery('');
                                    setSearchResults([]);
                                }}
                            >
                                Torna alla ricerca semplice
                            </button>
                            <button
                                className="search-btn-primary"
                                onClick={() => {
                                    console.log("Searching with filters:", advancedFilters);
                                    // Fallback to simple title search for now if title is present
                                    if (advancedFilters.title) {
                                        searchMovies(advancedFilters.title);
                                    }
                                }}
                            >
                                Cerca nel Catalogo
                            </button>
                        </div>
                    </div>
                )}

                {/* Risultati ricerca */}
                {(searchQuery.length >= 2 || searchResults.length > 0) && (
                    <div className="search-results">
                        {isSearching ? (
                            <div className="search-loading">
                                <span className="spinner">🔄</span> Ricerca in corso...
                            </div>
                        ) : searchResults.length > 0 ? (
                            <>
                                <div className="results-header">
                                    <span>
                                        🎯 {searchResults.length} risultati
                                        {isAdvancedSearch ? ' trovati' : ` per "${searchQuery}"`}
                                    </span>
                                </div>
                                <div className="results-grid">
                                    {searchResults.map((movie) => (
                                        <div
                                            key={movie.imdb_id}
                                            className="search-result-card"
                                            onClick={() => openAddModal(movie)}
                                        >
                                            <div className="result-poster">
                                                {movie.poster_url && !movie.poster_url.includes('placeholder') ? (
                                                    <img
                                                        src={movie.poster_url}
                                                        alt={movie.title}
                                                        onError={(e) => {
                                                            e.currentTarget.style.display = 'none';
                                                            const placeholder = e.currentTarget.nextElementSibling as HTMLElement;
                                                            if (placeholder) placeholder.classList.remove('hidden');
                                                        }}
                                                    />
                                                ) : null}
                                                <div className={`poster-placeholder ${movie.poster_url && !movie.poster_url.includes('placeholder') ? 'hidden' : ''}`}>
                                                    <span className="poster-icon">🎬</span>
                                                    <span className="poster-year">{movie.year}</span>
                                                </div>
                                                <div className="add-overlay">
                                                    <span>➕ Aggiungi</span>
                                                </div>
                                            </div>
                                            <div className="result-info">
                                                <h4>{movie.title}</h4>
                                                <span className="result-year">{movie.year || 'N/A'}</span>
                                                {movie.avg_vote && (
                                                    <span className="result-vote">⭐ {movie.avg_vote.toFixed(1)}</span>
                                                )}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </>
                        ) : searchQuery.length >= 2 ? (
                            <div className="no-results">
                                <span>😕</span>
                                <p>Nessun film trovato per "{searchQuery}"</p>
                            </div>
                        ) : null}
                    </div>
                )}
            </div>

            {/* Divider */}
            <div className="section-divider">
                <span>📚 La Tua Collezione</span>
            </div>

            {/* Controlli con Stats a destra */}
            <div className="controls-bar">
                <div className="sort-controls">
                    <button
                        className={`control-btn ${sortOrder === 'desc' ? 'active' : ''}`}
                        onClick={() => setSortOrder('desc')}
                    >
                        Più recenti
                    </button>
                    <button
                        className={`control-btn ${sortOrder === 'asc' ? 'active' : ''}`}
                        onClick={() => setSortOrder('asc')}
                    >
                        Più vecchi
                    </button>
                </div>

                <div className="filter-controls">
                    <span className="filter-label">Filtra per rating:</span>
                    <button
                        className={`rating-filter ${filterRating === null ? 'active' : ''}`}
                        onClick={() => setFilterRating(null)}
                    >
                        Tutti
                    </button>
                    {[5, 4, 3, 2, 1].map(r => (
                        <button
                            key={r}
                            className={`rating-filter ${filterRating === r ? 'active' : ''}`}
                            onClick={() => setFilterRating(r)}
                        >
                            {r}★
                        </button>
                    ))}
                </div>

                <div className="expand-controls">
                    <button className="control-btn" onClick={expandAll}>
                        Espandi tutto
                    </button>
                    <button className="control-btn" onClick={collapseAll}>
                        Comprimi tutto
                    </button>
                </div>

                <div className="stats-inline">
                    <div className="stat-chip">
                        <span className="stat-icon">🎬</span>
                        <span>{totalMovies} film totali</span>
                    </div>
                    <div className="stat-chip">
                        <span className="stat-icon">⭐</span>
                        <span>Media {avgRating}/5</span>
                    </div>
                </div>
            </div>

            {/* Film raggruppati per anno - COME ORIGINALE */}
            {movies.length === 0 ? (
                <div className="empty-collection">
                    <span className="empty-icon">🎬</span>
                    <h3>La tua collezione è vuota</h3>
                    <p>Cerca un film nella barra di ricerca e aggiungilo!</p>
                </div>
            ) : (
                <div className="years-container">
                    {sortedYears.map(year => {
                        const yearMovies = getFilteredMovies(groupedMovies[year]);
                        if (yearMovies.length === 0) return null;

                        const isExpanded = expandedYears.has(year);
                        const yearAvg = (yearMovies.reduce((sum, m) => sum + m.rating, 0) / yearMovies.length).toFixed(1);

                        return (
                            <div key={year} className="year-section">
                                <div
                                    className={`year-header ${isExpanded ? 'expanded' : ''}`}
                                    onClick={() => toggleYear(year)}
                                >
                                    <div className="year-info">
                                        <span className="year-number">{year}</span>
                                        <span className="year-count">{yearMovies.length} film</span>
                                        <span className="year-avg">⭐ {yearAvg}</span>
                                    </div>
                                    <span className="expand-icon">{isExpanded ? '▼' : '▶'}</span>
                                </div>

                                {isExpanded && (
                                    <div className="movies-grid">
                                        {yearMovies.map((movie, index) => (
                                            <div key={index} className="movie-card" onClick={() => openEditModal(movie)}>
                                                <div className="movie-poster">
                                                    {movie.poster_url && movie.poster_url !== STOCK_POSTER ? (
                                                        <img
                                                            src={movie.poster_url}
                                                            alt={movie.name}
                                                            onError={(e) => {
                                                                e.currentTarget.style.display = 'none';
                                                                const placeholder = e.currentTarget.nextElementSibling as HTMLElement;
                                                                if (placeholder) placeholder.classList.remove('hidden');
                                                            }}
                                                        />
                                                    ) : null}
                                                    <div className={`poster-placeholder ${movie.poster_url && movie.poster_url !== STOCK_POSTER ? 'hidden' : ''}`}>
                                                        <span className="poster-icon">🎬</span>
                                                        <span className="poster-year">{movie.year}</span>
                                                    </div>
                                                </div>
                                                <div className="movie-card-info">
                                                    <h4 className="movie-title">{movie.name}</h4>
                                                    <div className="movie-rating">
                                                        {renderStars(movie.rating)}
                                                    </div>
                                                    {movie.comment && (
                                                        <div className="movie-comment-preview">
                                                            "{movie.comment.slice(0, 40)}{movie.comment.length > 40 ? '...' : ''}"
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}

            {/* Modal Film arricchito */}
            {showAddModal && selectedMovieForModal && (
                <MovieModal
                    movie={selectedMovieForModal}
                    mode={modalMode}
                    onClose={() => setShowAddModal(false)}
                    onSave={addOrUpdateMovie}
                    onDelete={'name' in selectedMovieForModal ? removeUserMovie : undefined}
                />
            )}
        </div>
    );
}
