import React, { useState, useEffect, useRef } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, BarChart, Bar, ScatterChart, Scatter, ZAxis, Legend, ReferenceLine } from 'recharts';
import { StatsCard } from '../components/StatsCard';
import { MovieCard } from '../components/MovieCard';
import { catalogAPI, type CatalogMovie } from '../services/api';
import { useNavigate } from 'react-router-dom';
import './Dashboard.css';

interface DashboardData {
    total_watched: number;
    avg_rating: number;
    favorite_genre: string;
    genre_data: any[];
    top_years?: { year: number; count: number }[];
    // Year data (new structure)
    year_data?: { year: number; monthly_data: any[]; total_films: number }[];
    available_years?: number[];
    // Nuove statistiche
    rating_chart_data?: { rating: string; count: number; stars: number }[];
    watch_time_hours?: number;
    avg_duration?: number;
    // Attori e Registi Ottimizzati (v3.2)
    most_watched_directors?: { name: string; count: number; avg_rating: number }[];
    most_watched_actors?: { name: string; count: number; avg_rating: number }[];
    best_rated_directors?: { [key: string]: { name: string; count: number; avg_rating: number }[] } | { name: string; count: number; avg_rating: number }[];
    best_rated_actors?: { [key: string]: { name: string; count: number; avg_rating: number }[] } | { name: string; count: number; avg_rating: number }[];

    rating_vs_imdb?: { title: string; user_rating: number; user_rating_10: number; imdb_rating: number; difference: number }[];
    // Quiz stats
    quiz_correct_count?: number;
    quiz_wrong_count?: number;
    quiz_total_attempts?: number;
    last_quiz_date?: string;
}

interface MonthlyStats {
    year: number;
    monthly_data: any[];
    total_films: number;
    available_years: number[];
}

interface GlobalTrends {
    top_movies: { title: string; count: number; poster_path: string | null; _id: string }[];
    trending_genres: { genre: string; count: number }[];
}

export function Dashboard() {
    const navigate = useNavigate();
    const [data, setData] = useState<DashboardData | null>(null);
    const [globalTrends, setGlobalTrends] = useState<GlobalTrends | null>(null);
    const [loading, setLoading] = useState(true);
    const [syncing, setSyncing] = useState(false); // Sync status generic
    const [noData, setNoData] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [uploadMessage, setUploadMessage] = useState<string | null>(null);
    const [selectedYear, setSelectedYear] = useState<number>(new Date().getFullYear());
    const [monthlyStats, setMonthlyStats] = useState<MonthlyStats | null>(null);
    const [activeRatingIndex, setActiveRatingIndex] = useState<number | null>(null);
    const [activeYearIndex, setActiveYearIndex] = useState<number | null>(null);
    const [minMoviesFilter, setMinMoviesFilter] = useState<number>(1);
    const [selectedPerson, setSelectedPerson] = useState<{ name: string, type: 'director' | 'actor' } | null>(null);
    const [personMovies, setPersonMovies] = useState<any[]>([]);
    const [loadingPersonMovies, setLoadingPersonMovies] = useState(false);
    const [hideQuizStats, setHideQuizStats] = useState(() => localStorage.getItem('hideQuizStats') === 'true');
    const [selectedTrendMovie, setSelectedTrendMovie] = useState<any | null>(null);
    const [trendMovieDetails, setTrendMovieDetails] = useState<CatalogMovie | null>(null);
    const [loadingTrendMovie, setLoadingTrendMovie] = useState(false);
    const fileInputRef = useRef<HTMLInputElement>(null);

    // Fetch film details when a trend movie is selected
    useEffect(() => {
        if (selectedTrendMovie) {
            setLoadingTrendMovie(true);
            setTrendMovieDetails(null);
            console.log('📽️ Searching for trend movie:', selectedTrendMovie.title);
            catalogAPI.searchMovies(selectedTrendMovie.title, 10)
                .then(result => {
                    console.log('📽️ Search result:', result);
                    if (result.results && result.results.length > 0) {
                        // Find best match by title (case insensitive)
                        const match = result.results.find(m =>
                            m.title.toLowerCase().trim() === selectedTrendMovie.title.toLowerCase().trim()
                        ) || result.results[0];
                        console.log('📽️ Best match:', match);
                        setTrendMovieDetails(match);
                    } else {
                        console.log('📽️ No results found');
                    }
                    setLoadingTrendMovie(false);
                })
                .catch(err => {
                    console.error('📽️ Search error:', err);
                    setLoadingTrendMovie(false);
                });
        } else {
            setTrendMovieDetails(null);
        }
    }, [selectedTrendMovie]);

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
                    console.log('📊 Stats received:', {
                        genre_data_length: stats.genre_data?.length || 0,
                        best_rated_actors_length: stats.best_rated_actors?.length || 0,
                        best_rated_directors_length: stats.best_rated_directors?.length || 0
                    });
                    setData(stats);
                    setNoData(false);
                    localStorage.setItem('has_data', 'true');

                    // Check Sync Status
                    if (stats.sync_status === 'syncing') {
                        setSyncing(true);
                    } else {
                        setSyncing(false);
                    }
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

    const fetchGlobalTrends = () => {
        fetch('http://localhost:8000/trends/global')
            .then(res => res.json())
            .then(trends => {
                setGlobalTrends(trends);
            })
            .catch(err => console.error('Errore trend globali:', err));
    }

    const fetchPersonMovies = (name: string, type: 'director' | 'actor', actualCount?: number) => {
        setLoadingPersonMovies(true);
        setSelectedPerson({ name, type, actualCount });
        setPersonMovies([]);

        fetch(`http://localhost:8000/movies/person?name=${encodeURIComponent(name)}&type=${type}`, {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => res.json())
            .then(movies => {
                if (Array.isArray(movies)) {
                    setPersonMovies(movies);
                }
                setLoadingPersonMovies(false);
            })
            .catch(err => {
                console.error('Errore caricamento film della persona:', err);
                setLoadingPersonMovies(false);
            });
    };

    useEffect(() => {
        fetchStats();
        fetchMonthlyStats(selectedYear);
        fetchGlobalTrends();

        // Polling per sync status se in corso
        const interval = setInterval(() => {
            if (syncing) fetchStats();
        }, 1000); // Check ogni 1s se sta sincronizzando

        return () => clearInterval(interval);
    }, [syncing]);

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
                        onDragOver={(e: React.DragEvent) => { e.preventDefault(); }}
                        onDrop={(e: React.DragEvent) => {
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
        year_data: [],
        available_years: [],
        genre_data: [],
        top_years: [],
        rating_chart_data: [],
        watch_time_hours: 0,
        avg_duration: 0,
        best_rated_directors: [],
        best_rated_actors: [],
        rating_vs_imdb: []
    };

    // Deriva top_actors e top_directors da most_watched_* (se presenti) o fallback su best_rated_* (old)
    const topDirectors = displayData.most_watched_directors
        ? displayData.most_watched_directors.slice(0, 15)
        : (Array.isArray(displayData.best_rated_directors)
            ? [...displayData.best_rated_directors].sort((a, b) => b.count - a.count).slice(0, 15)
            : []);

    const topActors = displayData.most_watched_actors
        ? displayData.most_watched_actors.slice(0, 15)
        : (Array.isArray(displayData.best_rated_actors)
            ? [...displayData.best_rated_actors].sort((a, b) => b.count - a.count).slice(0, 15)
            : []);

    // Colori per le barre
    const yearColors = ['#E50914', '#FF6B35', '#00529B', '#8B5CF6', '#06B6D4'];
    const ratingColors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#10b981'];

    // Anno corrente come limite massimo
    const currentYear = new Date().getFullYear();
    const minYear = 2015;

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

    // Usa monthlyStats se disponibile, altrimenti cerca in year_data
    const getMonthlyDataForYear = () => {
        if (monthlyStats?.monthly_data) return monthlyStats.monthly_data;
        // Fallback: cerca in year_data locale
        const yearEntry = displayData.year_data?.find(y => y.year === selectedYear);
        return yearEntry?.monthly_data || [];
    };

    const monthlyDataForYear = getMonthlyDataForYear();
    const filmsInSelectedYear = monthlyStats?.total_films ||
        displayData.year_data?.find(y => y.year === selectedYear)?.total_films || 0;

    // Formatta ore totali (solo ore, minuti rimossi dallo schema)
    const totalHours = displayData.watch_time_hours || 0;
    const watchTimeDisplay = totalHours > 0 ? `${totalHours}h` : `${displayData.total_watched * 2}h`;

    // Rating distribution data (con fallback)
    const ratingChartData = displayData.rating_chart_data || [
        { rating: "⭐1", count: Math.round(displayData.total_watched * 0.05), stars: 1 },
        { rating: "⭐2", count: Math.round(displayData.total_watched * 0.1), stars: 2 },
        { rating: "⭐3", count: Math.round(displayData.total_watched * 0.25), stars: 3 },
        { rating: "⭐4", count: Math.round(displayData.total_watched * 0.35), stars: 4 },
        { rating: "⭐5", count: Math.round(displayData.total_watched * 0.25), stars: 5 },
    ];

    // Quiz stats helpers
    const today = new Date().toISOString().split('T')[0];
    const hasQuizToday = displayData.last_quiz_date === today;
    const quizCorrect = displayData.quiz_correct_count || 0;
    const quizWrong = displayData.quiz_wrong_count || 0;
    const quizTotal = quizCorrect + quizWrong;
    const quizAccuracy = quizTotal > 0 ? Math.round((quizCorrect / quizTotal) * 100) : 0;

    const toggleHideQuizStats = () => {
        const newValue = !hideQuizStats;
        setHideQuizStats(newValue);
        localStorage.setItem('hideQuizStats', String(newValue));
    };

    // Prepare lists for rendering (Fixing syntax error by moving logic out of JSX)
    const currentFilterKey = String(minMoviesFilter);

    const filteredDirectors = !Array.isArray(displayData.best_rated_directors) && displayData.best_rated_directors
        ? (displayData.best_rated_directors as any)[currentFilterKey] || []
        : (displayData.best_rated_directors as any[] || [])
            .filter(d => d.count >= minMoviesFilter)
            .slice(0, 10);

    const filteredActors = !Array.isArray(displayData.best_rated_actors) && displayData.best_rated_actors
        ? (displayData.best_rated_actors as any)[currentFilterKey] || []
        : (displayData.best_rated_actors as any[] || [])
            .filter(a => a.count >= minMoviesFilter)
            .slice(0, 10);

    return (
        <div className="dashboard-page">
            <div className="page-header">
                <div className="header-title-row">
                    <h1>Dashboard</h1>
                    {syncing && (
                        <div className="sync-badge" title="Stiamo aggiornando le tue statistiche...">
                            <span className="sync-icon">🔄</span> Syncing...
                        </div>
                    )}
                </div>
                <p>Panoramica del tuo storico cinematografico</p>
            </div>

            {/* ============================================
                SEZIONE 1: STATS CARDS PRINCIPALI
                ============================================ */}
            <div className="stats-row">
                <StatsCard
                    icon="🎬"
                    label="Film Visti"
                    value={displayData.total_watched}
                    trend="up"
                    trendValue={`${filmsInSelectedYear} nel ${selectedYear}`}
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
                    subtitle="Basato sui tuoi film"
                />
                <StatsCard
                    icon="⏱️"
                    label="Tempo Totale"
                    value={watchTimeDisplay}
                    subtitle={`Media: ${displayData.avg_duration || 120} min/film`}
                />
            </div>

            {/* ============================================
                SEZIONE QUIZ STATS
                ============================================ */}
            {/* SEZIONE QUIZ MOSSA IN FONDO */}

            {/* ============================================
                SEZIONE 2: GRAFICI PRINCIPALI (esistenti)
                ============================================ */}
            <div className="charts-section">
                {/* Grafico Film per Mese */}
                <div className="chart-container">
                    <div className="chart-header">
                        <h3>📈 Film Visti nel {selectedYear} <span className="year-count-badge">{filmsInSelectedYear} film</span></h3>
                        <div className="year-selector">
                            <button className="year-nav-btn" onClick={goToPreviousYear} disabled={selectedYear <= minYear}>◀</button>
                            <span className="year-display">{selectedYear}</span>
                            <button className="year-nav-btn" onClick={goToNextYear} disabled={selectedYear >= currentYear}>▶</button>
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
                                contentStyle={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px' }}
                                itemStyle={{ color: '#ffffff' }}
                                labelStyle={{ color: '#E50914', fontWeight: '700' }}
                            />
                            <Area type="monotone" dataKey="films" stroke="#E50914" fillOpacity={1} fill="url(#colorFilms)" />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>

                {/* Grafico Distribuzione Generi */}
                <div className="chart-container">
                    <h3>🎭 Distribuzione Generi</h3>
                    {displayData.genre_data && displayData.genre_data.length > 0 ? (
                        <>
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
                                    >
                                        {displayData.genre_data.map((entry: any, index: number) => (
                                            <Cell key={`cell-${index}`} fill={entry.color} />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        contentStyle={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px' }}
                                        itemStyle={{ color: '#ffffff' }}
                                        labelStyle={{ color: '#E50914', fontWeight: '700' }}
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
                        </>
                    ) : (
                        <div className="no-data-placeholder">
                            <span className="placeholder-icon">📊</span>
                            <p>Nessun dato generi disponibile</p>
                        </div>
                    )}
                </div>
            </div>

            {/* ============================================
                SEZIONE 3: DISTRIBUZIONE RATING + TOP ANNI
                ============================================ */}
            <div className="charts-section">
                {/* Distribuzione Rating */}
                <div className="chart-container">
                    <h3>⭐ Come Voti i Film</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <BarChart
                            data={ratingChartData}
                            layout="vertical"
                            onMouseLeave={() => setActiveRatingIndex(null)}
                        >
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
                            <XAxis type="number" stroke="#757575" />
                            <YAxis type="category" dataKey="rating" stroke="#757575" width={50} />
                            <Tooltip
                                contentStyle={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px' }}
                                itemStyle={{ color: '#ffffff' }}
                                labelStyle={{ color: '#E50914', fontWeight: '700' }}
                                formatter={(value: number) => [`${value} film`, 'Totale']}
                                cursor={{ fill: 'transparent' }}
                            />
                            <Bar
                                dataKey="count"
                                radius={[0, 8, 8, 0]}
                                onMouseEnter={(_, index) => setActiveRatingIndex(index)}
                            >
                                {ratingChartData.map((_, index) => (
                                    <Cell
                                        key={`cell-${index}`}
                                        fill={activeRatingIndex === null || activeRatingIndex === index
                                            ? ratingColors[index]
                                            : '#3a3a3a'}
                                        style={{ transition: 'fill 0.2s ease' }}
                                    />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                    <div className="rating-summary">
                        <span className="rating-insight">
                            {(ratingChartData[4]?.count || 0) > (ratingChartData[0]?.count || 0)
                                ? "🎉 Sei un appassionato generoso!"
                                : "🎯 Sei un critico esigente!"}
                        </span>
                    </div>
                </div>

                {/* Top 5 Anni */}
                <div className="chart-container">
                    <h3>🏆 Top 5 Anni Più Visti</h3>
                    <ResponsiveContainer width="100%" height={250}>
                        <BarChart
                            data={displayData.top_years || []}
                            layout="vertical"
                            margin={{ left: 60 }}
                            onMouseLeave={() => setActiveYearIndex(null)}
                        >
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
                            <XAxis type="number" stroke="#757575" />
                            <YAxis type="category" dataKey="year" stroke="#757575" tick={{ fill: '#fff', fontWeight: 600 }} />
                            <Tooltip
                                contentStyle={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px' }}
                                itemStyle={{ color: '#ffffff' }}
                                labelStyle={{ color: '#E50914', fontWeight: '700' }}
                                formatter={(value: number) => [`${value} film`, 'Visti']}
                                cursor={{ fill: 'transparent' }}
                            />
                            <Bar
                                dataKey="count"
                                radius={[0, 8, 8, 0]}
                                onMouseEnter={(_, index) => setActiveYearIndex(index)}
                            >
                                {(displayData.top_years || []).map((_, index) => (
                                    <Cell
                                        key={`cell-${index}`}
                                        fill={activeYearIndex === null || activeYearIndex === index
                                            ? yearColors[index % yearColors.length]
                                            : '#3a3a3a'}
                                        style={{ transition: 'fill 0.2s ease' }}
                                    />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* ============================================
                SEZIONE 4: TUO RATING VS IMDB (NUOVO)
                ============================================ */}
            {displayData.rating_vs_imdb && displayData.rating_vs_imdb.length > 0 && (
                <div className="chart-container full-width">
                    <h3>📊 I Tuoi Voti vs IMDb - Film Più Controversi</h3>
                    <p className="chart-subtitle">Film dove il tuo giudizio differisce di più dal pubblico</p>
                    <ResponsiveContainer width="100%" height={350}>
                        <ScatterChart margin={{ top: 20, right: 30, bottom: 40, left: 50 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis
                                type="number"
                                dataKey="imdb_rating"
                                name="IMDb"
                                stroke="#757575"
                                domain={[0, 10]}
                                label={{ value: 'Rating IMDb', position: 'bottom', fill: '#757575', offset: 0 }}
                            />
                            <YAxis
                                type="number"
                                dataKey="user_rating_10"
                                name="Tuo voto"
                                stroke="#757575"
                                domain={[0, 10]}
                                label={{ value: 'Tuo Rating (scala 10)', angle: -90, position: 'insideLeft', fill: '#757575' }}
                            />
                            <ZAxis type="number" range={[100, 400]} />
                            <ReferenceLine
                                segment={[{ x: 0, y: 0 }, { x: 10, y: 10 }]}
                                stroke="#E50914"
                                strokeDasharray="5 5"
                                strokeWidth={2}
                            />
                            <Tooltip
                                contentStyle={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px', padding: '12px' }}
                                content={({ active, payload }) => {
                                    if (active && payload && payload.length) {
                                        const data = payload[0].payload;
                                        return (
                                            <div style={{ background: '#1a1a1a', border: '2px solid #E50914', borderRadius: '8px', padding: '12px' }}>
                                                <p style={{ color: '#E50914', fontWeight: 700, margin: '0 0 8px 0' }}>{data.title}</p>
                                                <p style={{ color: '#fff', margin: '4px 0' }}>Tuo voto: ⭐{data.user_rating}/5 ({data.user_rating_10}/10)</p>
                                                <p style={{ color: '#fff', margin: '4px 0' }}>IMDb: {data.imdb_rating}/10</p>
                                                <p style={{ color: data.difference > 0 ? '#22c55e' : '#ef4444', margin: '4px 0' }}>
                                                    Differenza: {data.difference > 0 ? '+' : ''}{data.difference}
                                                </p>
                                            </div>
                                        );
                                    }
                                    return null;
                                }}
                            />
                            <Scatter
                                name="Film"
                                data={displayData.rating_vs_imdb.slice(0, 15)}
                                fill="#E50914"
                            />
                        </ScatterChart>
                    </ResponsiveContainer>
                    <div className="scatter-legend">
                        <span>📍 Linea rossa = perfetto accordo con IMDb</span>
                        <span>⬆️ Sopra = ti è piaciuto più del pubblico</span>
                        <span>⬇️ Sotto = ti è piaciuto meno</span>
                    </div>
                </div>
            )}

            {/* ============================================
                SEZIONE 5: REGISTI
                ============================================ */}
            {/* ============================================
                SEZIONE GLOBAL TRENDS (NETFLIX STYLE)
                ============================================ */}
            <h2 className="section-title main-section-divider">🌍 Community Trends</h2>
            <div className="global-trends-section trends-grid-layout">
                {/* Left: Top 10 Films */}
                <div className="trends-scroll-container">
                    <h3>🔥 Top 10 Più Visti</h3>
                    <div className="netflix-row">
                        {globalTrends?.top_movies?.map((movie, index) => {
                            const posterUrl = movie.poster_path && movie.poster_path.startsWith('http')
                                ? movie.poster_path
                                : movie.poster_path
                                    ? `https://image.tmdb.org/t/p/w200${movie.poster_path}`
                                    : 'https://via.placeholder.com/160x240/1a1a2e/e50914?text=No+Poster';

                            return (
                                <div
                                    key={index}
                                    className="trend-card"
                                    title={movie.title}
                                    onClick={() => setSelectedTrendMovie(movie)}
                                >
                                    <div className="trend-rank">{index + 1}</div>
                                    <img
                                        src={posterUrl}
                                        alt={movie.title}
                                        className="trend-poster"
                                        onError={(e: React.SyntheticEvent<HTMLImageElement, Event>) => {
                                            e.currentTarget.src = 'https://via.placeholder.com/160x240/1a1a2e/e50914?text=No+Poster';
                                        }}
                                    />
                                    <div className="trend-info">
                                        <h4>{movie.title}</h4>
                                        <span>{movie.count} visioni</span>
                                    </div>
                                </div>
                            );
                        })}
                        {(!globalTrends?.top_movies || globalTrends.top_movies.length === 0) && (
                            <p>Caricamento trends...</p>
                        )}
                    </div>
                </div>

                {/* Generi di Tendenza - Horizontal Pills */}
                <div className="trends-genres-section">
                    <h3 className="section-subtitle">📈 Generi di Tendenza</h3>
                    <div className="genre-tags-cloud">
                        {globalTrends?.trending_genres?.map((g, i) => (
                            <span key={i} className="genre-tag-chip">
                                {g.genre} <span className="chip-count">({g.count})</span>
                            </span>
                        ))}
                    </div>
                </div>
            </div>

            {/* ============================================
                SEZIONE 5: REGISTI
                ============================================ */}
            <h2 className="section-title main-section-divider">🎬 Analisi Registi</h2>
            <div className="rankings-section">
                {/* Top Registi (Frequenza) */}
                <div className="ranking-card">
                    <h3>Registi Più Visti</h3>
                    <p className="chart-subtitle">I registi di cui hai visto più opere</p>
                    <div className="ranking-list">
                        {topDirectors.slice(0, 10).map((director, index) => (
                            <div key={director.name} className="ranking-item clickable" onClick={() => fetchPersonMovies(director.name, 'director', director.count)}>
                                <span className="rank-position">#{index + 1}</span>
                                <div className="rank-info">
                                    <span className="rank-name">{director.name}</span>
                                    <span className="rank-stats">{director.count} film • ⭐ {director.avg_rating}</span>
                                </div>
                                <div className="rank-bar">
                                    <div
                                        className="rank-bar-fill"
                                        style={{
                                            width: `${(director.count / (topDirectors[0]?.count || 1)) * 100}%`,
                                            background: '#3b82f6'
                                        }}
                                    />
                                </div>
                            </div>
                        ))}
                        {topDirectors.length === 0 && (
                            <p className="no-data">Dati non ancora disponibili</p>
                        )}
                    </div>
                </div>

                {/* Migliori Registi per Voto */}
                <div className="ranking-card">
                    <div className="ranking-card-header">
                        <h3>Migliori per Voto Medio</h3>
                        <div className="header-filter">
                            {[1, 2, 3, 5].map(num => (
                                <button
                                    key={num}
                                    className={`compact-filter-chip ${minMoviesFilter === num ? 'active' : ''}`}
                                    onClick={() => setMinMoviesFilter(num)}
                                >
                                    {num}+
                                </button>
                            ))}
                        </div>
                    </div>
                    <p className="chart-subtitle">Ordinati per la tua media voto</p>
                    <div className="ranking-list">
                        <div className="ranking-list">
                            {filteredDirectors.map((director: any, index: number) => (
                                <div key={director.name} className="ranking-item clickable" onClick={() => fetchPersonMovies(director.name, 'director', director.count)}>
                                    <span className="rank-position">#{index + 1}</span>
                                    <div className="rank-info">
                                        <span className="rank-name">{director.name}</span>
                                        <span className="rank-stats">⭐ {director.avg_rating} media ({director.count} film)</span>
                                    </div>
                                    <div className="rank-bar">
                                        <div
                                            className="rank-bar-fill"
                                            style={{
                                                width: `${(director.avg_rating / 5) * 100}%`,
                                                background: '#10b981'
                                            }}
                                        />
                                    </div>
                                </div>
                            ))}
                            {filteredDirectors.length === 0 && (
                                <p className="no-data">Nessun regista con almeno {minMoviesFilter} film</p>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* ============================================
                SEZIONE 5bis: ATTORI
                ============================================ */}
            <h2 className="section-title main-section-divider">🌟 Analisi Attori</h2>
            <div className="rankings-section">
                {/* Top Attori (Frequenza) */}
                <div className="ranking-card">
                    <h3>Attori Più Visti</h3>
                    <p className="chart-subtitle">I talenti che appaiono più spesso nei tuoi film</p>
                    <div className="ranking-list">
                        {topActors.slice(0, 10).map((actor, index) => (
                            <div key={actor.name} className="ranking-item clickable" onClick={() => fetchPersonMovies(actor.name, 'actor', actor.count)}>
                                <span className="rank-position">#{index + 1}</span>
                                <div className="rank-info">
                                    <span className="rank-name">{actor.name}</span>
                                    <span className="rank-stats">{actor.count} film • ⭐ {actor.avg_rating}</span>
                                </div>
                                <div className="rank-bar">
                                    <div
                                        className="rank-bar-fill"
                                        style={{
                                            width: `${(actor.count / (topActors[0]?.count || 1)) * 100}%`,
                                            background: '#ef4444'
                                        }}
                                    />
                                </div>
                            </div>
                        ))}
                        {topActors.length === 0 && (
                            <p className="no-data">Dati non ancora disponibili</p>
                        )}
                    </div>
                </div>

                {/* Migliori Attori per Voto */}
                <div className="ranking-card">
                    <div className="ranking-card-header">
                        <h3>Migliori per Voto Medio</h3>
                        <div className="header-filter">
                            {[1, 2, 3, 5].map(num => (
                                <button
                                    key={num}
                                    className={`compact-filter-chip ${minMoviesFilter === num ? 'active' : ''}`}
                                    onClick={() => setMinMoviesFilter(num)}
                                >
                                    {num}+
                                </button>
                            ))}
                        </div>
                    </div>
                    <p className="chart-subtitle">Talenti che apprezzi di più</p>
                    <div className="ranking-list">
                        <div className="ranking-list">
                            {filteredActors.map((actor: any, index: number) => (
                                <div key={actor.name} className="ranking-item clickable" onClick={() => fetchPersonMovies(actor.name, 'actor', actor.count)}>
                                    <span className="rank-position">#{index + 1}</span>
                                    <div className="rank-info">
                                        <span className="rank-name">{actor.name}</span>
                                        <span className="rank-stats">⭐ {actor.avg_rating} media ({actor.count} film)</span>
                                    </div>
                                    <div className="rank-bar">
                                        <div
                                            className="rank-bar-fill"
                                            style={{
                                                width: `${(actor.avg_rating / 5) * 100}%`,
                                                background: '#8b5cf6'
                                            }}
                                        />
                                    </div>
                                </div>
                            ))}
                            {filteredActors.length === 0 && (
                                <p className="no-data">Nessun attore con almeno {minMoviesFilter} film</p>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* ============================================
                SEZIONE QUIZ STATS (SPOSTATA QUI)
                ============================================ */}
            <div className={`quiz-stats-widget ${hideQuizStats ? 'blurred-mode' : ''}`} style={{ marginBottom: '30px' }}>
                <div className="quiz-stats-header">
                    <h3>🧠 Quiz Cinematografico</h3>
                    <button className="hide-quiz-btn" onClick={toggleHideQuizStats} title={hideQuizStats ? "Mostra statistiche" : "Nascondi statistiche"}>
                        {hideQuizStats ? '👁️' : '✕'}
                    </button>
                </div>

                <div className="quiz-stats-container">
                    {hideQuizStats && (
                        <div className="quiz-blur-overlay">
                            <button className="show-quiz-btn" onClick={toggleHideQuizStats}>
                                👁️ Mostra Statistiche
                            </button>
                        </div>
                    )}

                    {hasQuizToday ? (
                        <div className="quiz-stats-content">
                            <div className="quiz-stat-item correct">
                                <span className="quiz-stat-value">{quizCorrect}</span>
                                <span className="quiz-stat-label">✓ Corrette</span>
                            </div>
                            <div className="quiz-stat-item wrong">
                                <span className="quiz-stat-value">{quizWrong}</span>
                                <span className="quiz-stat-label">✗ Sbagliate</span>
                            </div>
                            <div className="quiz-stat-item accuracy">
                                <span className="quiz-stat-value">{quizAccuracy}%</span>
                                <span className="quiz-stat-label">Precisione</span>
                            </div>
                            <div className="quiz-stat-item attempts">
                                <span className="quiz-stat-value">{displayData.quiz_total_attempts || 0}</span>
                                <span className="quiz-stat-label">Quiz Completati</span>
                            </div>
                        </div>
                    ) : (
                        <div className="quiz-cta">
                            <p>Non hai ancora fatto il quiz di oggi! Metti alla prova le tue conoscenze cinematografiche 🎬</p>
                            <button className="quiz-cta-btn" onClick={() => navigate('/quiz')}>
                                Fai il Quiz →
                            </button>
                        </div>
                    )}
                </div>
            </div>

            {/* ============================================
                SEZIONE 6: QUICK STATS (esistente)
                ============================================ */}
            <div className="quick-stats">
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.total_watched}</span>
                    <span className="quick-stat-label">Totale Film</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.avg_duration || 0}</span>
                    <span className="quick-stat-label">Durata Media (min)</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.unique_directors_count || 0}</span>
                    <span className="quick-stat-label">Registi Diversi</span>
                </div>
                <div className="quick-stat-card">
                    <span className="quick-stat-value">{displayData.unique_actors_count || 0}</span>
                    <span className="quick-stat-label">Attori Diversi</span>
                </div>
            </div>

            {/* ============================================
                MODAL FILM PER PERSONA
                ============================================ */}
            {selectedPerson && (
                <div className="person-modal-overlay" onClick={() => setSelectedPerson(null)}>
                    <div className="person-modal-content" onClick={e => e.stopPropagation()}>
                        <button className="close-modal" onClick={() => setSelectedPerson(null)}>&times;</button>

                        <div className="modal-header">
                            <h2>{selectedPerson.type === 'director' ? '🎬 Film di' : '🌟 Film con'} {selectedPerson.name}</h2>
                            <p>Hai visto {selectedPerson.actualCount || personMovies.length} film con questo {selectedPerson.type === 'director' ? 'regista' : 'attore'}</p>
                        </div>

                        {loadingPersonMovies ? (
                            <div className="modal-loading">
                                <div className="spinner"></div>
                                <p>Caricamento film...</p>
                            </div>
                        ) : (
                            <div className="modal-movies-grid">
                                {personMovies.map(movie => (
                                    <MovieCard key={movie.id} movie={movie} />
                                ))}
                                {personMovies.length === 0 && !loadingPersonMovies && (
                                    <p className="no-movies">Nessun film trovato.</p>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* ============================================
                MODAL FILM TREND (SAME AS MOVIEMODAL)
                ============================================ */}
            {selectedTrendMovie && (
                <div className="netflix-modal-overlay" onClick={() => setSelectedTrendMovie(null)}>
                    <div className="netflix-modal-content" onClick={e => e.stopPropagation()}>
                        <button className="netflix-modal-close" onClick={() => setSelectedTrendMovie(null)}>
                            ×
                        </button>

                        {loadingTrendMovie ? (
                            <div className="netflix-modal-loading">
                                <div className="spinner"></div>
                                <p>Caricamento dettagli...</p>
                            </div>
                        ) : (
                            <div className="movie-detail-grid">
                                {/* Left: Poster */}
                                <div className="detail-left">
                                    <img
                                        src={trendMovieDetails?.poster_url ||
                                            (selectedTrendMovie.poster_path?.startsWith('http')
                                                ? selectedTrendMovie.poster_path
                                                : selectedTrendMovie.poster_path
                                                    ? `https://image.tmdb.org/t/p/w300${selectedTrendMovie.poster_path}`
                                                    : catalogAPI.STOCK_POSTER_URL)}
                                        alt={selectedTrendMovie.title}
                                        className="detail-poster"
                                    />
                                </div>

                                {/* Right: Details */}
                                <div className="detail-right">
                                    <div className="detail-header">
                                        <h2>{trendMovieDetails?.title || selectedTrendMovie.title}</h2>
                                        <div className="detail-meta">
                                            {trendMovieDetails?.year && <span>{trendMovieDetails.year}</span>}
                                            {trendMovieDetails?.duration && <span>• {trendMovieDetails.duration} min</span>}
                                            {trendMovieDetails?.avg_vote && <span>• ⭐ {trendMovieDetails.avg_vote.toFixed(1)}</span>}
                                        </div>
                                    </div>

                                    <div className="detail-genres">
                                        {trendMovieDetails?.genres?.map(g => (
                                            <span key={g} className="detail-genre-tag">{g}</span>
                                        ))}
                                    </div>

                                    <div className="detail-section">
                                        <h3>Trama</h3>
                                        <p>{trendMovieDetails?.description || "Nessuna descrizione disponibile."}</p>
                                    </div>

                                    <div className="detail-crew">
                                        <div className="crew-item">
                                            <span>Regia</span>
                                            <b>{trendMovieDetails?.director || "N/A"}</b>
                                        </div>
                                        <div className="crew-item">
                                            <span>Cast Principale</span>
                                            <b>{trendMovieDetails?.actors?.split(',').slice(0, 3).join(', ') || "N/A"}</b>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
