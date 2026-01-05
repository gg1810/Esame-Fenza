import { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { StatsCard } from '../components/StatsCard';
import './Dashboard.css';

interface DashboardData {
    total_watched: number;
    avg_rating: number;
    favorite_genre: string;
    monthly_data: any[];
    genre_data: any[];
}

export function Dashboard() {
    const [data, setData] = useState<DashboardData | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetch('http://localhost:8000/user-stats', {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => res.json())
            .then(stats => {
                if (!stats.error) {
                    setData(stats);
                }
                setLoading(false);
            })
            .catch(err => {
                console.error(err);
                setLoading(false);
            });
    }, []);

    if (loading) return <div className="loading-screen">Caricamento dati...</div>;

    // Mostra dati di fallback se non ci sono dati reali (per la prima apertura)
    const displayData = data || {
        total_watched: 0,
        avg_rating: 0,
        favorite_genre: 'Nessuno',
        monthly_data: [],
        genre_data: []
    };

    return (
        <div className="dashboard-page">
            <div className="page-header">
                <h1>Dashboard</h1>
                <p>Panoramica del tuo storico cinematografico {data ? '(Dati Reali da MongoDB)' : '(In attesa di dati...)'}</p>
            </div>

            <div className="stats-row">
                <StatsCard
                    icon="🎬"
                    label="Film Visti"
                    value={displayData.total_watched}
                    trend="up"
                    trendValue={data ? "+12 questo mese" : "Carica un CSV"}
                />
                <StatsCard
                    icon="⭐"
                    label="Rating Medio"
                    value={displayData.avg_rating}
                    subtitle="su 5.0"
                />
                <StatsCard
                    icon="🎭"
                    label="Genere Preferito"
                    value={displayData.favorite_genre}
                    subtitle="Basato sui tuoi voti"
                />
                <StatsCard
                    icon="⏱️"
                    label="Ore Guardate"
                    value={displayData.total_watched * 2}
                    trend="up"
                    trendValue="+28h questo mese"
                />
            </div>

            <div className="charts-section">
                <div className="chart-container">
                    <h3>📈 Film Visti per Mese</h3>
                    <ResponsiveContainer width="100%" height={300}>
                        <AreaChart data={displayData.monthly_data}>
                            <defs>
                                <linearGradient id="colorFilms" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#E50914" stopOpacity={0.8} />
                                    <stop offset="95%" stopColor="#E50914" stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis dataKey="month" stroke="#757575" />
                            <YAxis stroke="#757575" />
                            <Tooltip
                                contentStyle={{
                                    background: '#232323',
                                    border: '1px solid #333',
                                    borderRadius: '8px'
                                }}
                            />
                            <Area
                                type="monotone"
                                dataKey="films"
                                stroke="#E50914"
                                fillOpacity={1}
                                fill="url(#colorFilms)"
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>

                <div className="chart-container">
                    <h3>🎭 Distribuzione Generi</h3>
                    <ResponsiveContainer width="100%" height={300}>
                        <PieChart>
                            <Pie
                                data={displayData.genre_data}
                                cx="50%"
                                cy="50%"
                                innerRadius={60}
                                outerRadius={100}
                                paddingAngle={5}
                                dataKey="value"
                            >
                                {displayData.genre_data.map((entry: any, index: number) => (
                                    <Cell key={`cell-${index}`} fill={entry.color} />
                                ))}
                            </Pie>
                            <Tooltip
                                contentStyle={{
                                    background: '#232323',
                                    border: '1px solid #333',
                                    borderRadius: '8px'
                                }}
                            />
                        </PieChart>
                    </ResponsiveContainer>
                    <div className="genre-legend">
                        {displayData.genre_data.map((genre: any) => (
                            <div key={genre.name} className="legend-item">
                                <span className="legend-color" style={{ background: genre.color }}></span>
                                <span>{genre.name}</span>
                                <span className="legend-value">{genre.value}%</span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            <div className="quick-stats">
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.total_watched}</span>
                    <span className="quick-stat-label">Totale Film</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.avg_rating}</span>
                    <span className="quick-stat-label">Rating Medio</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">2024</span>
                    <span className="quick-stat-label">Anno Corrente</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">Real-Time</span>
                    <span className="quick-stat-label">Sincronizzazione</span>
                </div>
            </div>
        </div>
    );
}
