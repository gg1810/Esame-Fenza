import './SentimentGauge.css';

interface SentimentGaugeProps {
    score: number; // 0-100
    label?: string;
    size?: 'small' | 'medium' | 'large';
}

export function SentimentGauge({ score, label, size = 'medium' }: SentimentGaugeProps) {
    const getSentimentLabel = (score: number) => {
        if (score >= 70) return { text: 'Positivo', emoji: '😊' };
        if (score >= 40) return { text: 'Neutro', emoji: '😐' };
        return { text: 'Negativo', emoji: '😔' };
    };

    const sentiment = getSentimentLabel(score);
    const rotation = (score / 100) * 180 - 90;

    return (
        <div className={`sentiment-gauge gauge-${size}`}>
            {label && <span className="gauge-label">{label}</span>}
            <div className="gauge-container">
                <div className="gauge-background">
                    <div className="gauge-fill" style={{ '--rotation': `${rotation}deg` } as React.CSSProperties} />
                    <div className="gauge-center">
                        <span className="gauge-emoji">{sentiment.emoji}</span>
                        <span className="gauge-score">{score}%</span>
                    </div>
                </div>
                <div className="gauge-labels">
                    <span>Negativo</span>
                    <span>Positivo</span>
                </div>
            </div>
            <span className="sentiment-text">{sentiment.text}</span>
        </div>
    );
}
