import { NavLink, useLocation } from 'react-router-dom';
import './Sidebar.css';

const menuItems = [
    { path: '/dashboard', icon: '📊', label: 'Dashboard', theme: 'netflix' },
    { path: '/recommendations', icon: '🎬', label: 'Raccomandazioni', theme: 'a24' },
    { path: '/cinema', icon: '🎭', label: 'Cinema', theme: 'warner' },
    { path: '/sentiment', icon: '💬', label: 'Sentiment', theme: 'paramount' },
    { path: '/mood', icon: '😊', label: 'Mood', theme: 'lionsgate' },
    { path: '/predictions', icon: '📈', label: 'Previsioni', theme: 'universal' },
];

export function Sidebar() {
    const location = useLocation();

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
                        <span className="user-name">Utente</span>
                        <span className="user-films">247 film visti</span>
                    </div>
                </div>
            </div>
        </aside>
    );
}
