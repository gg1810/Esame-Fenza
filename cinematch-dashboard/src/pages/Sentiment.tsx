import { useState, useEffect } from 'react';
import './Sentiment.css';

interface MovieData {
    title: string;
    release_date: string;
    trailer_url: string | null;
    embed_id: string | null;
}

interface CommentData {
    author: string;
    published_at: string;
    text: string;
    valore_sentiment?: number | null;
}

interface SentimentStats {
    average: number | null;
    total_comments: number;
    comments_with_sentiment: number;
    label: string;
    collection: string;
}

interface SentimentAverages {
    commenti_live: SentimentStats;
    commenti_votati: SentimentStats;
}

export function Sentiment() {
    const [movieData, setMovieData] = useState<MovieData | null>(null);
    const [commentData, setCommentData] = useState<CommentData | null>(null);
    const [commentsData, setCommentsData] = useState<CommentData[]>([]);
    const [loading, setLoading] = useState(true);
    const [commentLoading, setCommentLoading] = useState(true);
    const [commentsLoading, setCommentsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [commentError, setCommentError] = useState<string | null>(null);
    const [commentsError, setCommentsError] = useState<string | null>(null);

    // State per i commenti live con navigazione
    const [liveComments, setLiveComments] = useState<CommentData[]>([]);
    const [liveCommentsLoading, setLiveCommentsLoading] = useState(true);
    const [liveCommentsError, setLiveCommentsError] = useState<string | null>(null);
    const [currentLiveIndex, setCurrentLiveIndex] = useState(0);
    const [streamingActive, setStreamingActive] = useState(false);

    // State per le medie del sentiment
    const [sentimentAverages, setSentimentAverages] = useState<SentimentAverages | null>(null);
    const [sentimentLoading, setSentimentLoading] = useState(true);

    useEffect(() => {
        const fetchMovieData = async () => {
            try {
                const response = await fetch('http://localhost:8000/upcoming-movie-trailer');
                const result = await response.json();

                if (result.status === 'success' && result.data) {
                    setMovieData(result.data);
                } else {
                    setError(result.message || 'Nessun film trovato');
                }
            } catch (err) {
                setError('Errore di connessione al server');
                console.error('Error fetching movie data:', err);
            } finally {
                setLoading(false);
            }
        };

        const fetchCommentData = async () => {
            try {
                const response = await fetch('http://localhost:8000/latest-trailer-comment');
                const result = await response.json();

                if (result.status === 'success' && result.data) {
                    setCommentData(result.data);
                } else {
                    setCommentError(result.message || 'Nessun commento trovato');
                }
            } catch (err) {
                setCommentError('Errore di connessione al server');
                console.error('Error fetching comment data:', err);
            } finally {
                setCommentLoading(false);
            }
        };

        const fetchCommentsData = async () => {
            try {
                const response = await fetch('http://localhost:8000/trailer-comments?max_comments=5');
                const result = await response.json();

                if (result.status === 'success' && result.data) {
                    setCommentsData(result.data);
                } else {
                    setCommentsError(result.message || 'Nessun commento trovato');
                }
            } catch (err) {
                setCommentsError('Errore di connessione al server');
                console.error('Error fetching comments data:', err);
            } finally {
                setCommentsLoading(false);
            }
        };

        const fetchLiveComments = async () => {
            try {
                const response = await fetch('http://localhost:8000/live-trailer-comments');
                const result = await response.json();

                if (result.status === 'success' && result.data) {
                    setLiveComments(result.data);
                    setStreamingActive(result.streaming_active || false);
                } else {
                    setLiveCommentsError(result.message || 'Nessun commento live trovato');
                }
            } catch (err) {
                setLiveCommentsError('Errore di connessione al server');
                console.error('Error fetching live comments:', err);
            } finally {
                setLiveCommentsLoading(false);
            }
        };

        fetchMovieData();
        fetchCommentData();
        fetchCommentsData();
        fetchLiveComments();

        // Fetch sentiment averages
        const fetchSentimentAverages = async () => {
            try {
                const response = await fetch('http://localhost:8000/sentiment-averages');
                const result = await response.json();
                if (result.status === 'success' && result.data) {
                    setSentimentAverages(result.data);
                }
            } catch (err) {
                console.error('Error fetching sentiment averages:', err);
            } finally {
                setSentimentLoading(false);
            }
        };
        fetchSentimentAverages();

        // Polling per aggiornare i commenti live ogni 30 secondi
        const liveInterval = setInterval(fetchLiveComments, 30000);
        // Polling per aggiornare le medie del sentiment ogni 60 secondi
        const sentimentInterval = setInterval(fetchSentimentAverages, 60000);

        return () => {
            clearInterval(liveInterval);
            clearInterval(sentimentInterval);
        };
    }, []);

    const formatDate = (dateStr: string) => {
        const date = new Date(dateStr);
        return date.toLocaleDateString('it-IT', {
            day: 'numeric',
            month: 'long',
            year: 'numeric'
        });
    };

    const formatDateTime = (dateStr: string) => {
        const date = new Date(dateStr);
        return date.toLocaleDateString('it-IT', {
            day: 'numeric',
            month: 'long',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    // Navigazione commenti live
    const goToPreviousComment = () => {
        setCurrentLiveIndex((prev) => (prev > 0 ? prev - 1 : prev));
    };

    const goToNextComment = () => {
        setCurrentLiveIndex((prev) => (prev < liveComments.length - 1 ? prev + 1 : prev));
    };

    const goToComment = (index: number) => {
        setCurrentLiveIndex(index);
    };

    const currentLiveComment = liveComments[currentLiveIndex];

    return (
        <div className="sentiment-page">
            <div className="page-header">
                <h1>💬 Commenti YouTube</h1>
                <p>Scopri i trailer dei film in uscita il mese prossimo</p>
            </div>

            <div className="sentiment-overview">
                <div className="overview-card main-gauge trailer-card">
                    <h3>🎬 Trailer YouTube</h3>
                    {loading ? (
                        <div className="trailer-loading">
                            <span>Caricamento trailer...</span>
                        </div>
                    ) : error ? (
                        <div className="trailer-error">
                            <span>⚠️ {error}</span>
                        </div>
                    ) : movieData?.embed_id ? (
                        <div className="youtube-player">
                            <iframe
                                src={`https://www.youtube.com/embed/${movieData.embed_id}`}
                                title={movieData.title}
                                frameBorder="0"
                                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                                allowFullScreen
                            ></iframe>
                        </div>
                    ) : (
                        <div className="trailer-error">
                            <span>⚠️ Trailer non disponibile</span>
                        </div>
                    )}
                </div>

                <div className="overview-card stats-card movie-info-card">
                    <h3>🎬 Informazioni sul film</h3>
                    {loading ? (
                        <div className="movie-info-loading">
                            <span>Caricamento...</span>
                        </div>
                    ) : error ? (
                        <div className="movie-info-error">
                            <span>Nessuna informazione disponibile</span>
                        </div>
                    ) : movieData ? (
                        <div className="movie-info-content">
                            <div className="movie-info-item">
                                <span className="info-label">Nome Film</span>
                                <span className="info-value">{movieData.title}</span>
                            </div>
                            <div className="movie-info-item">
                                <span className="info-label">Data di Uscita</span>
                                <span className="info-value">{formatDate(movieData.release_date)}</span>
                            </div>
                            <div className="movie-info-item">
                                <span className="info-label">Link Trailer</span>
                                {movieData.trailer_url ? (
                                    <a
                                        href={movieData.trailer_url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="info-link"
                                    >
                                        Apri su YouTube ↗
                                    </a>
                                ) : (
                                    <span className="info-value">Non disponibile</span>
                                )}
                            </div>
                        </div>
                    ) : null}
                </div>
            </div>

            <div className="charts-grid">
                <div className="chart-card live-comments-card">
                    <h3>
                        🔴 Commenti Live (Spark Streaming)
                        {streamingActive && <span className="streaming-indicator"> ● LIVE</span>}
                    </h3>
                    {liveCommentsLoading ? (
                        <div className="comment-loading">
                            <span>Caricamento commenti live...</span>
                        </div>
                    ) : liveCommentsError ? (
                        <div className="comment-error">
                            <span>⚠️ {liveCommentsError}</span>
                        </div>
                    ) : liveComments.length === 0 ? (
                        <div className="comment-error">
                            <span>Nessun commento live disponibile</span>
                        </div>
                    ) : (
                        <div className="live-comment-viewer">
                            <div className="live-comment-content">
                                {currentLiveComment && (
                                    <>
                                        <div className="comment-author">
                                            <span className="author-icon">👤</span>
                                            <span className="author-name">{currentLiveComment.author}</span>
                                        </div>
                                        <div className="comment-date">
                                            <span className="date-icon">📅</span>
                                            <span className="date-value">{formatDateTime(currentLiveComment.published_at)}</span>
                                        </div>
                                        <div className="comment-text live-comment-text">
                                            <p>"{currentLiveComment.text}"</p>
                                        </div>
                                        {currentLiveComment.valore_sentiment !== undefined && currentLiveComment.valore_sentiment !== null && (
                                            <div className="comment-sentiment">
                                                <span className={`sentiment-badge ${currentLiveComment.valore_sentiment > 0.2 ? 'positive' : currentLiveComment.valore_sentiment < -0.2 ? 'negative' : 'neutral'}`}>
                                                    {currentLiveComment.valore_sentiment > 0.2 ? '😊' : currentLiveComment.valore_sentiment < -0.2 ? '😞' : '😐'}
                                                    {' '}{(currentLiveComment.valore_sentiment * 100).toFixed(0)}%
                                                </span>
                                            </div>
                                        )}
                                    </>
                                )}
                            </div>

                            <div className="live-comment-navigation">
                                <button
                                    className="nav-arrow nav-prev"
                                    onClick={goToPreviousComment}
                                    disabled={currentLiveIndex === 0}
                                    aria-label="Commento precedente"
                                >
                                    ◀
                                </button>

                                <div className="pagination-dots">
                                    {liveComments.map((_, index) => (
                                        <button
                                            key={index}
                                            className={`dot ${index === currentLiveIndex ? 'active' : ''}`}
                                            onClick={() => goToComment(index)}
                                            aria-label={`Vai al commento ${index + 1}`}
                                        />
                                    ))}
                                </div>

                                <button
                                    className="nav-arrow nav-next"
                                    onClick={goToNextComment}
                                    disabled={currentLiveIndex === liveComments.length - 1}
                                    aria-label="Commento successivo"
                                >
                                    ▶
                                </button>
                            </div>

                            <div className="live-comment-counter">
                                {currentLiveIndex + 1} / {liveComments.length}
                            </div>
                        </div>
                    )}
                </div>

                <div className="chart-card comment-card">
                    <h3>📊 Sentiment Analysis</h3>
                    {sentimentLoading ? (
                        <div className="sentiment-loading">Caricamento analisi...</div>
                    ) : sentimentAverages ? (
                        <div className="sentiment-averages-container">
                            {/* Commenti Live */}
                            <div className="sentiment-avg-card">
                                <div className="avg-header">
                                    <span className="avg-title">🔴 Commenti Live</span>
                                    <span className="avg-count">
                                        {sentimentAverages.commenti_live.comments_with_sentiment} / {sentimentAverages.commenti_live.total_comments}
                                    </span>
                                </div>
                                <div className="sentiment-gauge">
                                    <div className="gauge-bar">
                                        <div
                                            className={`gauge-fill ${sentimentAverages.commenti_live.label}`}
                                            style={{
                                                width: sentimentAverages.commenti_live.average !== null
                                                    ? `${Math.abs(sentimentAverages.commenti_live.average) * 100}%`
                                                    : '0%'
                                            }}
                                        />
                                    </div>
                                    <div className="gauge-value">
                                        {sentimentAverages.commenti_live.average !== null ? (
                                            <span className={`value-label ${sentimentAverages.commenti_live.label}`}>
                                                {sentimentAverages.commenti_live.label === 'positive' ? '😊' :
                                                    sentimentAverages.commenti_live.label === 'negative' ? '😞' : '😐'}
                                                {' '}{(sentimentAverages.commenti_live.average * 100).toFixed(0)}%
                                            </span>
                                        ) : (
                                            <span className="no-data">Nessun dato</span>
                                        )}
                                    </div>
                                </div>
                            </div>

                            {/* Commenti Votati */}
                            <div className="sentiment-avg-card">
                                <div className="avg-header">
                                    <span className="avg-title">📝 Commenti Analizzati</span>
                                    <span className="avg-count">
                                        {sentimentAverages.commenti_votati.comments_with_sentiment} / {sentimentAverages.commenti_votati.total_comments}
                                    </span>
                                </div>
                                <div className="sentiment-gauge">
                                    <div className="gauge-bar">
                                        <div
                                            className={`gauge-fill ${sentimentAverages.commenti_votati.label}`}
                                            style={{
                                                width: sentimentAverages.commenti_votati.average !== null
                                                    ? `${Math.abs(sentimentAverages.commenti_votati.average) * 100}%`
                                                    : '0%'
                                            }}
                                        />
                                    </div>
                                    <div className="gauge-value">
                                        {sentimentAverages.commenti_votati.average !== null ? (
                                            <span className={`value-label ${sentimentAverages.commenti_votati.label}`}>
                                                {sentimentAverages.commenti_votati.label === 'positive' ? '😊' :
                                                    sentimentAverages.commenti_votati.label === 'negative' ? '😞' : '😐'}
                                                {' '}{(sentimentAverages.commenti_votati.average * 100).toFixed(0)}%
                                            </span>
                                        ) : (
                                            <span className="no-data">Nessun dato</span>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div className="sentiment-error">Nessun dato disponibile</div>
                    )}
                </div>
            </div>

            <div className="posts-section">
                <h3>📝 Commenti Analizzati</h3>
                {commentsLoading ? (
                    <div className="comments-loading">Caricamento commenti...</div>
                ) : commentsError ? (
                    <div className="comments-error">⚠️ {commentsError}</div>
                ) : commentsData.length === 0 ? (
                    <div className="comments-error">Nessun commento trovato</div>
                ) : (
                    <div className="posts-list">
                        {commentsData.map((comment, index) => (
                            <div key={index} className="post-card youtube-comment-card">
                                <div className="post-header">
                                    <span className="subreddit">👤 {comment.author}</span>
                                    <span className="post-date">📅 {formatDateTime(comment.published_at)}</span>
                                </div>
                                <p className="comment-text-content">"{comment.text}"</p>
                                {comment.valore_sentiment !== undefined && comment.valore_sentiment !== null && (
                                    <div className="comment-sentiment">
                                        <span className={`sentiment-badge ${comment.valore_sentiment > 0.2 ? 'positive' : comment.valore_sentiment < -0.2 ? 'negative' : 'neutral'}`}>
                                            {comment.valore_sentiment > 0.2 ? '😊 Positivo' : comment.valore_sentiment < -0.2 ? '😞 Negativo' : '😐 Neutro'}
                                            {' '}({(comment.valore_sentiment * 100).toFixed(0)}%)
                                        </span>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
