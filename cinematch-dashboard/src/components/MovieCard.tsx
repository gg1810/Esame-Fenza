import type { Movie } from '../data/mockData';
import './MovieCard.css';

interface MovieCardProps {
    movie: Movie;
    showMatchScore?: boolean;
    variant?: 'recommended' | 'not-recommended' | 'cinema' | 'default';
    onClick?: () => void;
}

export function MovieCard({ movie, showMatchScore = false, variant = 'default', onClick }: MovieCardProps) {
    const getMatchScoreColor = (score: number) => {
        if (score >= 80) return 'score-high';
        if (score >= 50) return 'score-medium';
        return 'score-low';
    };

    return (
        <div className={`movie-card movie-card-${variant}`} onClick={onClick}>
            <div className="movie-poster">
                <img src={movie.poster} alt={movie.title} loading="lazy" />
                {showMatchScore && movie.matchScore !== undefined && (
                    <div className={`match-score ${getMatchScoreColor(movie.matchScore)}`}>
                        {movie.matchScore}%
                    </div>
                )}
            </div>
            <div className="movie-info">
                <h4 className="movie-title">{movie.title}</h4>
                <span className="movie-year">{movie.year}</span>
                <div className="movie-genres">
                    {movie.genres.slice(0, 2).map((genre) => (
                        <span key={genre} className="genre-tag">{genre}</span>
                    ))}
                </div>
                {movie.rating > 0 && (
                    <div className="movie-rating">
                        <span className="star">★</span> {movie.rating.toFixed(1)}
                    </div>
                )}
            </div>
        </div>
    );
}
