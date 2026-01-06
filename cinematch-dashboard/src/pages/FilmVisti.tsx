import { useState, useEffect } from 'react';
import './FilmVisti.css';

interface Movie {
    name: string;
    year: number;
    rating: number;
    date: string;
    letterboxd_uri?: string;
}

interface MoviesByYear {
    [year: string]: Movie[];
}

export function FilmVisti() {
    const [movies, setMovies] = useState<Movie[]>([]);
    const [loading, setLoading] = useState(true);
    const [groupedMovies, setGroupedMovies] = useState<MoviesByYear>({});
    const [expandedYears, setExpandedYears] = useState<Set<string>>(new Set());
    const [sortOrder, setSortOrder] = useState<'desc' | 'asc'>('desc');
    const [filterRating, setFilterRating] = useState<number | null>(null);

    useEffect(() => {
        fetch('http://localhost:8000/movies', {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => res.json())
            .then(data => {
                setMovies(data);
                groupByYear(data);
                setLoading(false);
            })
            .catch(err => {
                console.error(err);
                setLoading(false);
            });
    }, []);

    const groupByYear = (movieList: Movie[]) => {
        const grouped: MoviesByYear = {};
        movieList.forEach(movie => {
            const year = movie.year.toString();
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

    const getFilteredMovies = (yearMovies: Movie[]) => {
        if (filterRating === null) return yearMovies;
        return yearMovies.filter(m => m.rating === filterRating);
    };

    if (loading) return <div className="loading-screen">Caricamento film...</div>;

    const sortedYears = getSortedYears();
    const totalMovies = movies.length;
    const avgRating = movies.length > 0 
        ? (movies.reduce((sum, m) => sum + m.rating, 0) / movies.length).toFixed(2)
        : '0';

    return (
        <div className="film-visti-page">
            <div className="page-header">
                <h1>🎬 Film Visti</h1>
                <p>Tutti i tuoi {totalMovies} film organizzati per anno di uscita</p>
            </div>

            <div className="film-stats-bar">
                <div className="stat-chip">
                    <span className="stat-icon">🎬</span>
                    <span>{totalMovies} film totali</span>
                </div>
                <div className="stat-chip">
                    <span className="stat-icon">⭐</span>
                    <span>Media {avgRating}/5</span>
                </div>
            </div>

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
            </div>

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
                                        <div key={index} className="movie-card">
                                            <div className="movie-poster">
                                                <div className="poster-placeholder">
                                                    <span className="poster-icon">🎬</span>
                                                    <span className="poster-year">{movie.year}</span>
                                                </div>
                                                {movie.letterboxd_uri && (
                                                    <a 
                                                        href={movie.letterboxd_uri} 
                                                        target="_blank" 
                                                        rel="noopener noreferrer"
                                                        className="letterboxd-link"
                                                        onClick={(e) => e.stopPropagation()}
                                                    >
                                                        🔗
                                                    </a>
                                                )}
                                            </div>
                                            <div className="movie-card-info">
                                                <h4 className="movie-title">{movie.name}</h4>
                                                <div className="movie-rating">
                                                    {renderStars(movie.rating)}
                                                </div>
                                                <div className="movie-date">
                                                    {new Date(movie.date).toLocaleDateString('it-IT', {
                                                        day: 'numeric',
                                                        month: 'short',
                                                        year: 'numeric'
                                                    })}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
