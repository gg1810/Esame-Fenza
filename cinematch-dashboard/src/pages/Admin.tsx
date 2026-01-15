import { useState } from 'react';
import './Admin.css';

export function Admin() {
    const [isLoggedIn, setIsLoggedIn] = useState(false);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [activeSection, setActiveSection] = useState('dashboard');

    const handleLogin = (e: React.FormEvent) => {
        e.preventDefault();
        if (username === 'admin' && password === 'admin') {
            setIsLoggedIn(true);
            setError('');
        } else {
            setError('Credenziali non valide');
        }
    };

    const handleLogout = () => {
        setIsLoggedIn(false);
        setUsername('');
        setPassword('');
    };

    // Login screen
    if (!isLoggedIn) {
        return (
            <div className="admin-login-page" data-theme="netflix">
                <div className="admin-login-card">
                    <div className="admin-login-header">
                        <span className="admin-icon">🔐</span>
                        <h1>Admin Panel</h1>
                        <p>Accesso riservato agli amministratori</p>
                    </div>
                    {error && <div className="admin-error">{error}</div>}
                    <form onSubmit={handleLogin}>
                        <div className="admin-form-group">
                            <label>Username</label>
                            <input
                                type="text"
                                value={username}
                                onChange={(e) => setUsername(e.target.value)}
                                placeholder="admin"
                                required
                            />
                        </div>
                        <div className="admin-form-group">
                            <label>Password</label>
                            <input
                                type="password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                placeholder="••••••"
                                required
                            />
                        </div>
                        <button type="submit" className="admin-login-btn">
                            Accedi
                        </button>
                    </form>
                </div>
            </div>
        );
    }

    // Admin Dashboard
    return (
        <div className="admin-container">
            {/* Sidebar a SINISTRA */}
            <aside className="admin-sidebar">
                <div className="admin-sidebar-header">
                    <span className="admin-logo-icon">⚙️</span>
                    <span className="admin-logo-text">Admin Panel</span>
                </div>

                <nav className="admin-nav">
                    <button
                        className={`admin-nav-item ${activeSection === 'dashboard' ? 'active' : ''}`}
                        onClick={() => setActiveSection('dashboard')}
                    >
                        <span>📊</span> Dashboard
                    </button>
                    <button
                        className={`admin-nav-item ${activeSection === 'analytics' ? 'active' : ''}`}
                        onClick={() => setActiveSection('analytics')}
                    >
                        <span>📈</span> Analytics
                    </button>
                </nav>

                <div className="admin-sidebar-footer">
                    <button className="admin-logout-btn" onClick={handleLogout}>
                        🚪 Logout
                    </button>
                </div>
            </aside>

            {/* Main Content */}
            <main className="admin-main">
                <div className="admin-header">
                    <h1>
                        {activeSection === 'dashboard' && '📊 Dashboard Overview'}
                        {activeSection === 'analytics' && '📈 Analytics'}
                    </h1>
                </div>

                <div className="admin-content">
                    {activeSection === 'dashboard' && (
                        <div className="analytics-sections">
                            {/* Sezione Film */}
                            <div className="analytics-section">
                                <h2>🎬 Analytics Film</h2>
                                <div className="grafana-container">
                                    <iframe
                                        src="http://localhost:3001/d-solo/ad5gq57/generi?orgId=1&panelId=1&theme=dark"
                                        className="grafana-iframe"
                                        title="Grafana - Generi Film"
                                    />
                                </div>
                            </div>

                            {/* Sezione Attività (Time Series) */}
                            <div className="analytics-section">
                                <h2>📅 Attività Giornaliera (Recensioni/Voti)</h2>
                                <div className="grafana-container">
                                    <iframe
                                        src="http://localhost:3001/d-solo/ad5gq57/generi?orgId=1&from=1765753200000&to=1772405999000&timezone=browser&tab=queries&panelId=panel-3&__feature.dashboardSceneSolo=true"
                                        className="grafana-iframe"
                                        title="Grafana - Activity"
                                    />
                                </div>
                            </div>

                            {/* Sezione Utenti (Pie Chart Province) */}
                            <div className="analytics-section">
                                <h2>👥 Utenti per Provincia</h2>
                                <div className="grafana-container">
                                    <iframe
                                        src="http://localhost:3001/d-solo/ad5gq57/generi?orgId=1&panelId=2&theme=dark"
                                        className="grafana-iframe"
                                        title="Grafana - Users"
                                    />
                                </div>
                            </div>
                        </div>
                    )}

                    {activeSection === 'analytics' && (
                        <div className="admin-placeholder">
                            <p>🚧 Work in Progress</p>
                            <p className="placeholder-hint">Altre funzionalità in arrivo...</p>
                        </div>
                    )}
                </div>
            </main>
        </div>
    );
}
