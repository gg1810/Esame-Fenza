import { useState, useEffect } from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import './Sidebar.css';

const menuItems = [
    { path: '/dashboard', icon: '📊', label: 'Dashboard', theme: 'netflix' },
    { path: '/catalogo', icon: '🎬', label: 'Catalogo Film', theme: 'fox' },
    { path: '/recommendations', icon: '✨', label: 'Raccomandazioni', theme: 'a24' },
    { path: '/cinema', icon: '🎭', label: 'Cinema', theme: 'warner' },
    { path: '/sentiment', icon: '💬', label: 'Sentiment', theme: 'paramount' },
    { path: '/mood', icon: '😊', label: 'Mood', theme: 'lionsgate' },
    { path: '/predictions', icon: '📈', label: 'Previsioni', theme: 'universal' },
    { path: '/quiz', icon: '🌿', label: 'Quiz Ghibli', theme: 'ghibli' },
];

interface UserData {
    username: string;
    full_name?: string;
    movies_count?: number;
    avatar?: string;
}

interface PresetAvatar {
    id: number;
    name: string;
    url: string;
}

export function Sidebar() {
    const navigate = useNavigate();
    const [userData, setUserData] = useState<UserData | null>(null);
    const [showAvatarModal, setShowAvatarModal] = useState(false);
    const [presetAvatars, setPresetAvatars] = useState<PresetAvatar[]>([]);
    const [uploadingAvatar, setUploadingAvatar] = useState(false);

    const fetchUserData = () => {
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
    };

    useEffect(() => {
        fetchUserData();

        // Ascolta eventi di aggiornamento film
        const handleMoviesUpdate = () => {
            fetchUserData();
        };

        window.addEventListener('moviesUpdated', handleMoviesUpdate);

        return () => {
            window.removeEventListener('moviesUpdated', handleMoviesUpdate);
        };
    }, []);

    useEffect(() => {
        if (showAvatarModal) {
            fetch('http://localhost:8000/avatars/presets')
                .then(res => res.json())
                .then(data => setPresetAvatars(data))
                .catch(err => console.error('Errore caricamento avatar:', err));
        }
    }, [showAvatarModal]);

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('has_data');
        navigate('/login');
    };

    const handleAvatarUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setUploadingAvatar(true);
        const formData = new FormData();
        formData.append('file', file);

        try {
            const res = await fetch('http://localhost:8000/user/avatar', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('token')}`
                },
                body: formData
            });
            const data = await res.json();
            if (data.status === 'success') {
                fetchUserData();
                setShowAvatarModal(false);
            }
        } catch (err) {
            console.error('Errore upload avatar:', err);
        } finally {
            setUploadingAvatar(false);
        }
    };

    const handlePresetSelect = async (avatarUrl: string) => {
        setUploadingAvatar(true);
        try {
            const res = await fetch(`http://localhost:8000/user/avatar?avatar_url=${encodeURIComponent(avatarUrl)}`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('token')}`
                }
            });
            const data = await res.json();
            if (data.status === 'success') {
                fetchUserData();
                setShowAvatarModal(false);
            }
        } catch (err) {
            console.error('Errore selezione avatar:', err);
        } finally {
            setUploadingAvatar(false);
        }
    };

    const displayName = userData?.full_name || userData?.username || 'Utente';
    const avatarSrc = userData?.avatar || null;

    return (
        <aside className="sidebar">
            <div className="sidebar-header">
                <NavLink to="/" className="logo">
                    <span className="logo-icon">🎥</span>
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
                    <div className="user-avatar-wrapper" onClick={() => setShowAvatarModal(true)}>
                        {avatarSrc ? (
                            <img src={avatarSrc} alt="Avatar" className="user-avatar-img" />
                        ) : (
                            <div className="user-avatar">👤</div>
                        )}
                        <div className="avatar-edit-icon">+</div>
                    </div>
                    <div className="user-details">
                        <span className="user-name">{displayName}</span>
                    </div>
                </div>
                <button className="logout-btn" onClick={handleLogout} title="Logout">
                    🚪
                </button>
            </div>

            {showAvatarModal && (
                <div className="avatar-modal-overlay" onClick={() => setShowAvatarModal(false)}>
                    <div className="avatar-modal" onClick={(e) => e.stopPropagation()}>
                        <h3>Scegli Avatar</h3>

                        <div className="avatar-upload-section">
                            <label htmlFor="avatar-upload" className="avatar-upload-btn">
                                {uploadingAvatar ? 'Caricamento...' : '📤 Carica Immagine'}
                            </label>
                            <input
                                id="avatar-upload"
                                type="file"
                                accept="image/*"
                                onChange={handleAvatarUpload}
                                style={{ display: 'none' }}
                                disabled={uploadingAvatar}
                            />
                        </div>

                        <div className="avatar-presets">
                            <h4>O scegli un personaggio iconico:</h4>
                            <div className="avatar-grid">
                                {presetAvatars.map(avatar => (
                                    <div
                                        key={avatar.id}
                                        className="avatar-preset-item"
                                        onClick={() => handlePresetSelect(avatar.url)}
                                        title={avatar.name}
                                    >
                                        <img src={avatar.url} alt={avatar.name} />
                                    </div>
                                ))}
                            </div>
                        </div>

                        <button className="modal-close-btn" onClick={() => setShowAvatarModal(false)}>
                            Chiudi
                        </button>
                    </div>
                </div>
            )}
        </aside>
    );
}
