import './MoodSelector.css';

interface Mood {
    id: string;
    emoji: string;
    label: string;
    color: string;
}

interface MoodSelectorProps {
    moods: Mood[];
    selectedMood: string | null;
    onMoodSelect: (moodId: string) => void;
}

export function MoodSelector({ moods, selectedMood, onMoodSelect }: MoodSelectorProps) {
    return (
        <div className="mood-selector">
            <h3 className="mood-selector-title">Come ti senti oggi?</h3>
            <div className="mood-grid">
                {moods.map((mood) => (
                    <button
                        key={mood.id}
                        className={`mood-button ${selectedMood === mood.id ? 'selected' : ''}`}
                        style={{ '--mood-color': mood.color } as React.CSSProperties}
                        onClick={() => onMoodSelect(mood.id)}
                    >
                        <span className="mood-emoji">{mood.emoji}</span>
                        <span className="mood-label">{mood.label}</span>
                    </button>
                ))}
            </div>
        </div>
    );
}
