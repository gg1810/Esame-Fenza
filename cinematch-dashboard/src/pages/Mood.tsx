import { useState, useEffect } from 'react';
import { MoodSelector } from '../components/MoodSelector';
import { MovieCard } from '../components/MovieCard';
import { MovieModal } from '../components/MovieModal';
import { catalogAPI } from '../services/api';
import { moods } from '../data/mockData';
import type { Movie } from '../data/mockData';
import './Mood.css';

// Mapping frontend mood IDs → backend mood keys
const MOOD_MAPPING: Record<string, string> = {
    'happy': 'felice',
    'sad': 'malinconico',
    'excited': 'eccitato',
    'relaxed': 'rilassato',
    'romantic': 'romantico',
    'thrilling': 'thriller'
};

interface MoodMovie {
    imdb_id: string;
    title: string;
    poster_url?: string;
    year?: number;
    genres?: string[];
    avg_vote?: number;
    director?: string;
}

interface MoodApiResponse {
    status: string;
    generated_at?: string;
    message?: string;
    data: Record<string, MoodMovie[]>;
}

export function Mood() {
    const [selectedMood, setSelectedMood] = useState<string | null>(null);
    const [moodData, setMoodData] = useState<Record<string, Movie[]>>({});
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Modal state
    const [selectedMovie, setSelectedMovie] = useState<any>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);

    // Fetch mood recommendations from API
    useEffect(() => {
        const fetchMoodRecommendations = async () => {
            try {
                const response = await fetch('http://localhost:8000/mood-recommendations');
                const result: MoodApiResponse = await response.json();

                if (result.status === 'success' && result.data) {
                    // Converti i dati backend in Movie objects
                    const enrichedData: Record<string, Movie[]> = {};

                    for (const [frontendMoodId, backendMoodKey] of Object.entries(MOOD_MAPPING)) {
                        const backendMovies = result.data[backendMoodKey] || [];

                        // Converti in Movie objects con dati dal catalogo
                        const movies = backendMovies.map((movie: MoodMovie, index: number) => ({
                            id: index,
                            imdb_id: movie.imdb_id,
                            title: movie.title,
                            year: movie.year || 2024,
                            poster: movie.poster_url || 'https://via.placeholder.com/500x750/1a1a2e/e50914?text=No+Poster',
                            rating: movie.avg_vote || 0,
                            genres: movie.genres || [],
                            director: movie.director || 'Unknown'
                        } as Movie));

                        enrichedData[frontendMoodId] = movies;
                    }

                    setMoodData(enrichedData);
                    setError(null);
                } else if (result.status === 'empty') {
                    setError('I dati mood sono in fase di generazione. Riprova tra qualche istante.');
                } else {
                    setError('Errore nel caricamento delle raccomandazioni mood.');
                }
            } catch (err) {
                console.error('Error fetching mood recommendations:', err);
                setError('Impossibile connettersi al server.');
            } finally {
                setLoading(false);
            }
        };

        fetchMoodRecommendations();
    }, []);

    const handleMoodSelect = (moodId: string) => {
        setSelectedMood(moodId === selectedMood ? null : moodId);
    };

    // Handle movie click - open modal with full details
    const handleMovieClick = async (movie: Movie) => {
        if (!movie.imdb_id) return;

        try {
            // Fetch full details for the modal
            const fullMovie = await catalogAPI.getMovie(movie.imdb_id);
            setSelectedMovie(fullMovie);
            setIsModalOpen(true);
        } catch (err) {
            console.error("Failed to load movie details", err);
            // Fallback: show what we have
            setSelectedMovie(movie);
            setIsModalOpen(true);
        }
    };

    const selectedMoodData = moods.find(m => m.id === selectedMood);
    const movies = selectedMood ? moodData[selectedMood] || [] : [];

    if (loading) {
        return (
            <div className="mood-page">
                <div className="page-header">
                    <h1>😊 Mood-Based Recommendations</h1>
                    <p>Caricamento...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="mood-page">
                <div className="page-header">
                    <h1>😊 Mood-Based Recommendations</h1>
                    <p style={{ color: '#FF6B35' }}>{error}</p>
                </div>
            </div>
        );
    }

    return (
        <div className="mood-page">
            <div className="page-header">
                <h1>😊 Mood-Based Recommendations</h1>
                <p>Trova il film perfetto per il tuo stato d'animo</p>
            </div>

            <MoodSelector
                moods={moods}
                selectedMood={selectedMood}
                onMoodSelect={handleMoodSelect}
            />

            {selectedMood && selectedMoodData && (
                <div className="mood-results">
                    <div
                        className="mood-banner"
                        style={{ '--mood-color': selectedMoodData.color } as React.CSSProperties}
                    >
                        <span className="banner-emoji">{selectedMoodData.emoji}</span>
                        <div className="banner-content">
                            <h2>Film per quando sei {selectedMoodData.label}</h2>
                            <p>Abbiamo selezionato {movies.length} film perfetti per il tuo mood</p>
                        </div>
                    </div>

                    <div className="mood-movies-grid" key={selectedMood}>
                        {movies.map((movie, index) => (
                            <div
                                key={movie.id}
                                style={{ animationDelay: `${index * 0.1}s` }}
                                onClick={() => handleMovieClick(movie)}
                            >
                                <MovieCard movie={movie} />
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {!selectedMood && (
                <div className="mood-empty-state">
                    <div className="empty-icon">🎬</div>
                    <h3>Seleziona il tuo mood</h3>
                    <p>Clicca su un'emozione per scoprire i film più adatti</p>
                </div>
            )}

            <div className="mood-tips">
                <h3>💡 Come funziona?</h3>
                <div className="tips-grid">
                    <div className="tip-card">
                        <span className="tip-icon">1️⃣</span>
                        <h4>Scegli il mood</h4>
                        <p>Seleziona l'emozione che descrive come ti senti</p>
                    </div>
                    <div className="tip-card">
                        <span className="tip-icon">2️⃣</span>
                        <h4>Ricevi consigli</h4>
                        <p>Ogni emozione incontra la sua storia perfetta</p>
                    </div>
                    <div className="tip-card">
                        <span className="tip-icon">3️⃣</span>
                        <h4>Goditi il film</h4>
                        <p>Trova il match perfetto per la tua serata</p>
                    </div>
                </div>
            </div>

            {/* Modal */}
            {isModalOpen && selectedMovie && (
                <MovieModal
                    movie={selectedMovie}
                    mode="view"
                    onClose={() => setIsModalOpen(false)}
                />
            )}
        </div>
    );
}
