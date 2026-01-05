import { useState } from 'react';
import { SentimentGauge } from '../components/SentimentGauge';
import { cinemaMovies } from '../data/mockData';
import './Cinema.css';

export function Cinema() {
    const [selectedMovie, setSelectedMovie] = useState(cinemaMovies[0]);

    return (
        <div className="cinema-page">
            <div className="page-header">
                <h1>🎭 Al Cinema Ora</h1>
                <p>Film in programmazione presso The Space Cinema</p>
            </div>

            <div className="cinema-layout">
                <div className="featured-movie">
                    <div className="featured-poster">
                        <img src={selectedMovie.poster} alt={selectedMovie.title} />
                        <div className="poster-overlay">
                            <div className="rating-badge">
                                <span className="star">★</span> {selectedMovie.rating}
                            </div>
                        </div>
                    </div>

                    <div className="featured-info">
                        <span className="theater-badge">📍 The Space Cinema</span>
                        <h2>{selectedMovie.title}</h2>
                        <p className="movie-meta">
                            {selectedMovie.year} • {selectedMovie.director}
                        </p>
                        <div className="genres-row">
                            {selectedMovie.genres.map((genre) => (
                                <span key={genre} className="genre-tag">{genre}</span>
                            ))}
                        </div>

                        <div className="showtimes-section">
                            <h4>🕐 Orari Disponibili</h4>
                            <div className="showtimes-grid">
                                {selectedMovie.showtimes.map((time) => (
                                    <button key={time} className="showtime-btn">
                                        {time}
                                    </button>
                                ))}
                            </div>
                        </div>

                        <div className="sentiment-section">
                            <h4>💬 Sentiment Reddit</h4>
                            <SentimentGauge score={selectedMovie.redditSentiment} size="medium" />
                            <p className="sentiment-note">
                                Basato su {Math.floor(Math.random() * 500 + 200)} post analizzati con RoBERTa
                            </p>
                        </div>

                        <button className="book-btn">
                            🎟️ Prenota Biglietti
                        </button>
                    </div>
                </div>

                <div className="other-movies">
                    <h3>Altri Film in Sala</h3>
                    <div className="movie-list">
                        {cinemaMovies.map((movie) => (
                            <div
                                key={movie.id}
                                className={`movie-list-item ${selectedMovie.id === movie.id ? 'active' : ''}`}
                                onClick={() => setSelectedMovie(movie)}
                            >
                                <img src={movie.poster} alt={movie.title} className="list-poster" />
                                <div className="list-info">
                                    <h4>{movie.title}</h4>
                                    <p>{movie.genres.slice(0, 2).join(', ')}</p>
                                    <div className="list-meta">
                                        <span className="rating">★ {movie.rating}</span>
                                        <span className="sentiment-mini">
                                            {movie.redditSentiment >= 70 ? '😊' : movie.redditSentiment >= 40 ? '😐' : '😔'}
                                            {movie.redditSentiment}%
                                        </span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
}
