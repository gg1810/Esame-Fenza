import { useState } from 'react';
import { MovieCard } from '../components/MovieCard';
import { recommendedMovies, notRecommendedMovies } from '../data/mockData';
import './Recommendations.css';

const genres = ['Tutti', 'Drama', 'Thriller', 'Sci-Fi', 'Comedy', 'Horror', 'Action'];

export function Recommendations() {
    const [activeGenre, setActiveGenre] = useState('Tutti');
    const [view, setView] = useState<'recommended' | 'not-recommended'>('recommended');

    const filterMovies = (movies: typeof recommendedMovies) => {
        if (activeGenre === 'Tutti') return movies;
        return movies.filter(m => m.genres.includes(activeGenre));
    };

    const displayedMovies = view === 'recommended'
        ? filterMovies(recommendedMovies)
        : filterMovies(notRecommendedMovies);

    return (
        <div className="recommendations-page">
            <div className="page-header">
                <h1>Raccomandazioni</h1>
                <p>Film selezionati in base ai tuoi gusti</p>
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
                        <div key={movie.id} style={{ animationDelay: `${index * 0.1}s` }}>
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
