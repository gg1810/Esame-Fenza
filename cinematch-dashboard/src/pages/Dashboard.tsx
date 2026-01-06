import { useState, useEffect, useRef } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, BarChart, Bar } from 'recharts';
import { StatsCard } from '../components/StatsCard';
import './Dashboard.css';

interface DashboardData {
    total_watched: number;
    avg_rating: number;
    favorite_genre: string;
    monthly_data: any[];
    genre_data: any[];
    top_years?: { year: number; count: number }[];
}

interface MonthlyStats {
    year: number;
    monthly_data: any[];
    total_films: number;
    available_years: number[];
}

export function Dashboard() {
    const [data, setData] = useState<DashboardData | null>(null);
    const [loading, setLoading] = useState(true);
    const [noData, setNoData] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [uploadMessage, setUploadMessage] = useState<string | null>(null);
    const [selectedYear, setSelectedYear] = useState<number>(new Date().getFullYear());
    const [monthlyStats, setMonthlyStats] = useState<MonthlyStats | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    const fetchStats = () => {
        fetch('http://localhost:8000/user-stats', {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => {
                if (res.status === 404) {
                    setNoData(true);
                    setLoading(false);
                    return null;
                }
                return res.json();
            })
            .then(stats => {
                if (stats && !stats.detail) {
                    setData(stats);
                    setNoData(false);
                    localStorage.setItem('has_data', 'true');
                }
                setLoading(false);
            })
            .catch(err => {
                console.error(err);
                setLoading(false);
            });
    };

    const fetchMonthlyStats = (year: number) => {
        fetch(`http://localhost:8000/monthly-stats/${year}`, {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => res.json())
            .then(stats => {
                if (stats && !stats.detail) {
                    setMonthlyStats(stats);
                }
            })
            .catch(err => console.error('Errore caricamento stats mensili:', err));
    };

    useEffect(() => {
        fetchStats();
        fetchMonthlyStats(selectedYear);
    }, []);

    useEffect(() => {
        fetchMonthlyStats(selectedYear);
    }, [selectedYear]);

    const handleFileUpload = async (file: File) => {
        if (!file.name.endsWith('.csv')) {
            setUploadMessage('❌ Per favore carica un file CSV');
            return;
        }

        setUploading(true);
        setUploadMessage(null);

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('http://localhost:8000/upload-csv', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('token')}`
                },
                body: formData
            });

            const result = await response.json();

            if (response.ok) {
                setUploadMessage(`✅ Caricati ${result.count} film con successo!`);
                localStorage.setItem('has_data', 'true');
                // Ricarica le statistiche
                setTimeout(() => {
                    fetchStats();
                }, 1000);
            } else {
                setUploadMessage(`❌ ${result.detail || 'Errore durante il caricamento'}`);
            }
        } catch (err) {
            setUploadMessage('❌ Errore di connessione al server');
        } finally {
            setUploading(false);
        }
    };

    if (loading) return <div className="loading-screen">Caricamento dati...</div>;

    // Se non ci sono dati, mostra il form di upload
    if (noData) {
        return (
            <div className="dashboard-page">
                <div className="page-header">
                    <h1>Dashboard</h1>
                    <p>Benvenuto! Carica i tuoi dati Letterboxd per iniziare</p>
                </div>

                <div className="upload-section">
                    <div 
                        className={`upload-zone ${uploading ? 'uploading' : ''}`}
                        onClick={() => fileInputRef.current?.click()}
                        onDragOver={(e) => { e.preventDefault(); }}
                        onDrop={(e) => {
                            e.preventDefault();
                            const file = e.dataTransfer.files[0];
                            if (file) handleFileUpload(file);
                        }}
                    >
                        <input
                            ref={fileInputRef}
                            type="file"
                            accept=".csv"
                            style={{ display: 'none' }}
                            onChange={(e) => {
                                const file = e.target.files?.[0];
                                if (file) handleFileUpload(file);
                            }}
                        />
                        
                        {uploading ? (
                            <div className="upload-loading">
                                <div className="spinner"></div>
                                <p>Analizzando i tuoi film...</p>
                            </div>
                        ) : (
                            <>
                                <div className="upload-icon">📁</div>
                                <h3>Carica il tuo export Letterboxd</h3>
                                <p>Trascina qui il file ratings.csv o clicca per selezionarlo</p>
                            </>
                        )}
                    </div>

                    {uploadMessage && (
                        <div className={`upload-message ${uploadMessage.startsWith('✅') ? 'success' : 'error'}`}>
                            {uploadMessage}
                        </div>
                    )}

                    <div className="upload-instructions">
                        <h4>📋 Come esportare da Letterboxd:</h4>
                        <ol>
                            <li>Vai su <strong>letterboxd.com</strong> → Il tuo profilo</li>
                            <li>Clicca su <strong>Settings</strong> → <strong>Import & Export</strong></li>
                            <li>Scarica <strong>Export your data</strong></li>
                            <li>Carica qui il file <code>ratings.csv</code></li>
                        </ol>
                    </div>
                </div>
            </div>
        );
    }

    // Mostra la dashboard con i dati
    const displayData = data || {
        total_watched: 0,
        avg_rating: 0,
        favorite_genre: 'Nessuno',
        monthly_data: [],
        genre_data: [],
        top_years: []
    };

    // Colori per le barre del grafico anni
    const yearColors = ['#E50914', '#FF6B35', '#00529B', '#8B5CF6', '#06B6D4'];

    // Anno corrente come limite massimo
    const currentYear = new Date().getFullYear();
    const minYear = 2015; // Anno minimo

    // Funzioni per navigare tra gli anni
    const goToPreviousYear = () => {
        if (selectedYear > minYear) {
            setSelectedYear(selectedYear - 1);
        }
    };

    const goToNextYear = () => {
        if (selectedYear < currentYear) {
            setSelectedYear(selectedYear + 1);
        }
    };

    // Dati mensili per l'anno selezionato
    const monthlyDataForYear = monthlyStats?.monthly_data || displayData.monthly_data;
    const filmsInSelectedYear = monthlyStats?.total_films || 0;

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
                    <div className="chart-header">
                        <h3>📈 Film Visti nel {selectedYear} <span className="year-count-badge">{filmsInSelectedYear} film</span></h3>
                        <div className="year-selector">
                            <button
                                className="year-nav-btn"
                                onClick={goToPreviousYear}
                                disabled={selectedYear <= minYear}
                            >
                                ◀
                            </button>
                            <span className="year-display">{selectedYear}</span>
                            <button
                                className="year-nav-btn"
                                onClick={goToNextYear}
                                disabled={selectedYear >= currentYear}
                            >
                                ▶
                            </button>
                        </div>
                    </div>
                    <ResponsiveContainer width="100%" height={300}>
                        <AreaChart data={monthlyDataForYear}>
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
                                    background: '#1a1a1a',
                                    border: '2px solid #E50914',
                                    borderRadius: '8px',
                                    padding: '12px 16px',
                                    boxShadow: '0 4px 20px rgba(0,0,0,0.5)'
                                }}
                                itemStyle={{
                                    color: '#ffffff',
                                    fontSize: '14px',
                                    fontWeight: '600'
                                }}
                                labelStyle={{
                                    color: '#E50914',
                                    fontSize: '14px',
                                    fontWeight: '700',
                                    marginBottom: '4px'
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
                                nameKey="name"
                                label={false}
                            >
                                {displayData.genre_data.map((entry: any, index: number) => (
                                    <Cell key={`cell-${index}`} fill={entry.color} />
                                ))}
                            </Pie>
                            <Tooltip
                                contentStyle={{
                                    background: '#1a1a1a',
                                    border: '2px solid #E50914',
                                    borderRadius: '8px',
                                    padding: '12px 16px',
                                    boxShadow: '0 4px 20px rgba(0,0,0,0.5)'
                                }}
                                itemStyle={{
                                    color: '#ffffff',
                                    fontSize: '14px',
                                    fontWeight: '600'
                                }}
                                labelStyle={{
                                    color: '#E50914',
                                    fontSize: '14px',
                                    fontWeight: '700',
                                    marginBottom: '4px'
                                }}
                                formatter={(value: number, name: string) => [`${value}%`, name]}
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

            <div className="chart-container top-years-chart">
                <h3>🏆 Top 5 Anni Più Visti</h3>
                <ResponsiveContainer width="100%" height={250}>
                    <BarChart 
                        data={displayData.top_years || []} 
                        layout="vertical"
                        margin={{ top: 5, right: 30, left: 60, bottom: 5 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
                        <XAxis type="number" stroke="#757575" />
                        <YAxis 
                            type="category" 
                            dataKey="year" 
                            stroke="#757575"
                            tick={{ fill: '#ffffff', fontSize: 14, fontWeight: 600 }}
                        />
                        <Tooltip
                            contentStyle={{
                                background: '#1a1a1a',
                                border: '2px solid #E50914',
                                borderRadius: '8px',
                                padding: '12px 16px',
                                boxShadow: '0 4px 20px rgba(0,0,0,0.5)'
                            }}
                            itemStyle={{
                                color: '#ffffff',
                                fontSize: '14px',
                                fontWeight: '600'
                            }}
                            labelStyle={{
                                color: '#E50914',
                                fontSize: '14px',
                                fontWeight: '700',
                                marginBottom: '4px'
                            }}
                            formatter={(value: number) => [`${value} film`, 'Visti']}
                        />
                        <Bar 
                            dataKey="count" 
                            radius={[0, 8, 8, 0]}
                        >
                            {(displayData.top_years || []).map((_, index) => (
                                <Cell key={`cell-${index}`} fill={yearColors[index % yearColors.length]} />
                            ))}
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
                <div className="top-years-summary">
                    {(displayData.top_years || []).slice(0, 3).map((item: any, index: number) => (
                        <div key={item.year} className="year-badge">
                            <span className="badge-position">#{index + 1}</span>
                            <span className="badge-year">{item.year}</span>
                            <span className="badge-count">{item.count} film</span>
                        </div>
                    ))}
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
