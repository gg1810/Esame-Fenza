import { useState, useEffect } from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';
import './Sidebar.css';

const menuItems = [
    { path: '/dashboard', icon: '📊', label: 'Dashboard', theme: 'netflix' },
    { path: '/film-visti', icon: '🎞️', label: 'Film Visti', theme: 'fox' },
    { path: '/recommendations', icon: '🎬', label: 'Raccomandazioni', theme: 'a24' },
    { path: '/cinema', icon: '🎭', label: 'Cinema', theme: 'warner' },
    { path: '/sentiment', icon: '💬', label: 'Sentiment', theme: 'paramount' },
    { path: '/mood', icon: '😊', label: 'Mood', theme: 'lionsgate' },
    { path: '/predictions', icon: '📈', label: 'Previsioni', theme: 'universal' },
];

interface UserData {
    username: string;
    full_name?: string;
    movies_count?: number;
}

export function Sidebar() {
    const location = useLocation();
    const navigate = useNavigate();
    const [userData, setUserData] = useState<UserData | null>(null);

    useEffect(() => {
        // Recupera i dati utente dal backend
        fetch('http://localhost:8000/me', {
            headers: {
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            }
        })
            .then(res => res.json())
            .then(data => {
                if (data && !data.detail) {
                    setUserData(data);
                }
            })
            .catch(err => console.error('Errore caricamento utente:', err));
    }, []);

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('has_data');
        navigate('/login');
    };

    const displayName = userData?.full_name || userData?.username || 'Utente';
    const filmCount = userData?.movies_count || 0;

    return (
        <aside className="sidebar">
            <div className="sidebar-header">
                <NavLink to="/" className="logo">
                    <span className="logo-icon">🎬</span>
                    <span className="logo-text">CineMatch</span>
                </NavLink>
            </div>

            <nav className="sidebar-nav">
                <ul>
                    {menuItems.map((item) => (
                        <li key={item.path}>
                            <NavLink
                                to={item.path}
                                className={({ isActive }) =>
                                    `nav-item ${isActive ? 'active' : ''}`
                                }
                            >
                                <span className="nav-icon">{item.icon}</span>
                                <span className="nav-label">{item.label}</span>
                            </NavLink>
                        </li>
                    ))}
                </ul>
            </nav>

            <div className="sidebar-footer">
                <div className="user-info">
                    <div className="user-avatar">👤</div>
                    <div className="user-details">
                        <span className="user-name">{displayName}</span>
                        <span className="user-films">{filmCount} film visti</span>
                    </div>
                </div>
                <button className="logout-btn" onClick={handleLogout} title="Logout">
                    🚪
                </button>
            </div>
        </aside>
    );
}
