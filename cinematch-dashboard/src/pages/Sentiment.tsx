import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line } from 'recharts';
import { SentimentGauge } from '../components/SentimentGauge';
import { sentimentPosts } from '../data/mockData';
import './Sentiment.css';

const sentimentOverTime = [
    { date: '01 Mar', positive: 75, negative: 25 },
    { date: '02 Mar', positive: 82, negative: 18 },
    { date: '03 Mar', positive: 78, negative: 22 },
    { date: '04 Mar', positive: 85, negative: 15 },
    { date: '05 Mar', positive: 88, negative: 12 },
];

const topKeywords = [
    { word: 'visuals', count: 1250 },
    { word: 'soundtrack', count: 980 },
    { word: 'amazing', count: 875 },
    { word: 'epic', count: 720 },
    { word: 'cinematography', count: 650 },
    { word: 'Timothée', count: 590 },
];

export function Sentiment() {
    const overallSentiment = 87;

    const getSentimentColor = (sentiment: string) => {
        switch (sentiment) {
            case 'positive': return '#00c853';
            case 'negative': return '#ef4444';
            default: return '#ffc107';
        }
    };

    return (
        <div className="sentiment-page">
            <div className="page-header">
                <h1>💬 Sentiment Analysis</h1>
                <p>Analisi del sentiment da Reddit usando RoBERTa</p>
            </div>

            <div className="sentiment-overview">
                <div className="overview-card main-gauge">
                    <h3>Sentiment Complessivo</h3>
                    <SentimentGauge score={overallSentiment} size="large" />
                    <div className="model-badge">
                        <span className="model-icon">🤖</span>
                        Powered by RoBERTa
                    </div>
                </div>

                <div className="overview-card stats-card">
                    <h3>📊 Statistiche</h3>
                    <div className="stat-item">
                        <span className="stat-label">Post Analizzati</span>
                        <span className="stat-value">1,247</span>
                    </div>
                    <div className="stat-item">
                        <span className="stat-label">Subreddit Monitorati</span>
                        <span className="stat-value">5</span>
                    </div>
                    <div className="stat-item">
                        <span className="stat-label">Ultimo Aggiornamento</span>
                        <span className="stat-value">2 min fa</span>
                    </div>
                    <div className="stat-item">
                        <span className="stat-label">Commenti Positivi</span>
                        <span className="stat-value positive">87%</span>
                    </div>
                </div>
            </div>

            <div className="charts-grid">
                <div className="chart-card">
                    <h3>📈 Trend Sentiment nel Tempo</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <LineChart data={sentimentOverTime}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis dataKey="date" stroke="#808080" />
                            <YAxis stroke="#808080" />
                            <Tooltip
                                contentStyle={{
                                    background: '#222',
                                    border: '1px solid #333',
                                    borderRadius: '8px'
                                }}
                            />
                            <Line
                                type="monotone"
                                dataKey="positive"
                                stroke="#00c853"
                                strokeWidth={3}
                                dot={{ fill: '#00c853', strokeWidth: 2 }}
                            />
                            <Line
                                type="monotone"
                                dataKey="negative"
                                stroke="#ef4444"
                                strokeWidth={3}
                                dot={{ fill: '#ef4444', strokeWidth: 2 }}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                </div>

                <div className="chart-card">
                    <h3>🔤 Parole Chiave Frequenti</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <BarChart data={topKeywords} layout="vertical">
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis type="number" stroke="#808080" />
                            <YAxis dataKey="word" type="category" stroke="#808080" width={100} />
                            <Tooltip
                                contentStyle={{
                                    background: '#222',
                                    border: '1px solid #333',
                                    borderRadius: '8px'
                                }}
                            />
                            <Bar dataKey="count" fill="#0033A0" radius={[0, 4, 4, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            <div className="posts-section">
                <h3>📝 Post Recenti Analizzati</h3>
                <div className="posts-list">
                    {sentimentPosts.map((post) => (
                        <div key={post.id} className="post-card">
                            <div className="post-header">
                                <span className="subreddit">{post.subreddit}</span>
                                <span
                                    className="sentiment-badge"
                                    style={{ background: getSentimentColor(post.sentiment) }}
                                >
                                    {post.sentiment === 'positive' ? '😊' : post.sentiment === 'negative' ? '😔' : '😐'}
                                    {(post.sentimentScore * 100).toFixed(0)}%
                                </span>
                            </div>
                            <h4 className="post-title">{post.title}</h4>
                            <div className="post-footer">
                                <span className="post-score">⬆️ {post.score.toLocaleString()}</span>
                                <span className="post-date">{post.date}</span>
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
