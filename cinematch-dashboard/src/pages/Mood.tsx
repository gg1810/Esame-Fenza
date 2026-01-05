import { useState } from 'react';
import { MoodSelector } from '../components/MoodSelector';
import { MovieCard } from '../components/MovieCard';
import { moods, moodMovies } from '../data/mockData';
import './Mood.css';

export function Mood() {
    const [selectedMood, setSelectedMood] = useState<string | null>(null);

    const handleMoodSelect = (moodId: string) => {
        setSelectedMood(moodId === selectedMood ? null : moodId);
    };

    const selectedMoodData = moods.find(m => m.id === selectedMood);
    const movies = selectedMood ? moodMovies[selectedMood] || [] : [];

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

                    <div className="mood-movies-grid">
                        {movies.map((movie, index) => (
                            <div
                                key={movie.id}
                                className="mood-movie-wrapper"
                                style={{ animationDelay: `${index * 0.15}s` }}
                            >
                                <MovieCard movie={movie} />
                                <div
                                    className="mood-match-indicator"
                                    style={{ background: selectedMoodData.color }}
                                >
                                    {selectedMoodData.emoji} Perfetto per te
                                </div>
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
                        <p>Il nostro AI analizza il tono emotivo dei film</p>
                    </div>
                    <div className="tip-card">
                        <span className="tip-icon">3️⃣</span>
                        <h4>Goditi il film</h4>
                        <p>Trova il match perfetto per la tua serata</p>
                    </div>
                </div>
            </div>
        </div>
    );
}
