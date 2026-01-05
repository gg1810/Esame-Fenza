import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { boxOfficePredictions } from '../data/mockData';
import './Predictions.css';

const comparisonData = [
    { name: 'Dune: Part Two', predicted: 680, actual: 711, accuracy: 96 },
    { name: 'Oppenheimer', predicted: 890, actual: 952, accuracy: 93 },
    { name: 'Barbie', predicted: 1100, actual: 1442, accuracy: 76 },
    { name: 'Killers of the Flower Moon', predicted: 190, actual: 157, accuracy: 79 },
];

const modelMetrics = [
    { label: 'Accuratezza Media', value: '86%', icon: '🎯' },
    { label: 'Film Analizzati', value: '2,847', icon: '🎬' },
    { label: 'Features Usate', value: '42', icon: '📊' },
    { label: 'Ultimo Training', value: '2 giorni fa', icon: '🔄' },
];

export function Predictions() {
    const formatCurrency = (value: number) => {
        return `$${(value / 1000000).toFixed(0)}M`;
    };

    return (
        <div className="predictions-page">
            <div className="page-header">
                <h1>📈 Previsione Incassi</h1>
                <p>Sistema di machine learning per prevedere il box office</p>
            </div>

            <div className="model-stats">
                {modelMetrics.map((metric) => (
                    <div key={metric.label} className="metric-card">
                        <span className="metric-icon">{metric.icon}</span>
                        <div className="metric-content">
                            <span className="metric-value">{metric.value}</span>
                            <span className="metric-label">{metric.label}</span>
                        </div>
                    </div>
                ))}
            </div>

            <div className="prediction-section">
                <h2>🔮 Prossime Uscite - Previsioni</h2>
                <div className="predictions-grid">
                    {boxOfficePredictions.map((movie) => (
                        <div key={movie.id} className="prediction-card">
                            <img src={movie.poster} alt={movie.title} className="prediction-poster" />
                            <div className="prediction-info">
                                <h3>{movie.title}</h3>
                                <p className="prediction-meta">{movie.year} • {movie.director}</p>
                                <div className="prediction-genres">
                                    {movie.genres.map((genre) => (
                                        <span key={genre} className="genre-tag">{genre}</span>
                                    ))}
                                </div>
                                <div className="prediction-value">
                                    <span className="label">Incasso Previsto</span>
                                    <span className="value">{formatCurrency(movie.predictedBoxOffice || 0)}</span>
                                </div>
                                <div className="confidence-bar">
                                    <div className="confidence-fill" style={{ width: '85%' }}></div>
                                </div>
                                <span className="confidence-label">Confidenza: 85%</span>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            <div className="comparison-section">
                <h2>📊 Previsioni vs Realtà</h2>
                <div className="chart-card">
                    <ResponsiveContainer width="100%" height={350}>
                        <BarChart data={comparisonData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
                            <XAxis dataKey="name" stroke="#6a6b6e" tick={{ fontSize: 12 }} />
                            <YAxis stroke="#6a6b6e" tickFormatter={(v) => `$${v}M`} />
                            <Tooltip
                                contentStyle={{
                                    background: '#1d1d36',
                                    border: '1px solid #2a2a4a',
                                    borderRadius: '8px'
                                }}
                                formatter={(value: number) => [`$${value}M`, '']}
                            />
                            <Legend />
                            <Bar name="Previsto" dataKey="predicted" fill="#00529B" radius={[4, 4, 0, 0]} />
                            <Bar name="Reale" dataKey="actual" fill="#A7A9AC" radius={[4, 4, 0, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            <div className="accuracy-section">
                <h2>🎯 Accuratezza per Film</h2>
                <div className="accuracy-list">
                    {comparisonData.map((movie) => (
                        <div key={movie.name} className="accuracy-item">
                            <span className="accuracy-name">{movie.name}</span>
                            <div className="accuracy-bar-container">
                                <div
                                    className="accuracy-bar"
                                    style={{
                                        width: `${movie.accuracy}%`,
                                        background: movie.accuracy >= 90 ? '#00e676' : movie.accuracy >= 80 ? '#ffc400' : '#ff5252'
                                    }}
                                ></div>
                            </div>
                            <span className="accuracy-value">{movie.accuracy}%</span>
                        </div>
                    ))}
                </div>
            </div>

            <div className="disclaimer">
                <span className="disclaimer-icon">⚠️</span>
                <p>
                    Le previsioni sono basate su un modello di machine learning addestrato su dati storici.
                    I risultati potrebbero non riflettere l'andamento reale del mercato.
                </p>
            </div>
        </div>
    );
}
