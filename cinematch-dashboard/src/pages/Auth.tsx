import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import './Auth.css';

export function Login() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const navigate = useNavigate();

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            const response = await fetch('http://localhost:8000/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password }),
            });

            if (response.ok) {
                const data = await response.json();
                localStorage.setItem('token', data.access_token);
                localStorage.setItem('username', username);
                navigate('/');
            } else {
                setError('Credenziali non valide');
            }
        } catch (err) {
            setError('Errore di connessione');
        }
    };

    return (
        <div className="auth-page" data-theme="netflix">
            <div className="auth-card">
                <h1>Accedi a CineMatch</h1>
                {error && <div className="auth-error">{error}</div>}
                <form onSubmit={handleSubmit}>
                    <div className="form-group">
                        <label>Username</label>
                        <input
                            type="text"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                            required
                        />
                    </div>
                    <div className="form-group">
                        <label>Password</label>
                        <input
                            type="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            required
                        />
                    </div>
                    <button type="submit" className="auth-button">Entra</button>
                </form>
                <p className="auth-footer">
                    Non hai un account? <Link to="/register">Registrati</Link>
                </p>
            </div>
        </div>
    );
}

export function Register() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [success, setSuccess] = useState(false);
    const navigate = useNavigate();

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            const response = await fetch('http://localhost:8000/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password }),
            });

            if (response.ok) {
                setSuccess(true);
                setTimeout(() => navigate('/login'), 2000);
            } else {
                setError('Errore durante la registrazione');
            }
        } catch (err) {
            setError('Errore di connessione');
        }
    };

    return (
        <div className="auth-page" data-theme="netflix">
            <div className="auth-card">
                <h1>Crea il tuo Account</h1>
                {error && <div className="auth-error">{error}</div>}
                {success && <div className="auth-success">Registrazione completata! Reindirizzamento...</div>}
                <form onSubmit={handleSubmit}>
                    <div className="form-group">
                        <label>Username</label>
                        <input
                            type="text"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                            required
                        />
                    </div>
                    <div className="form-group">
                        <label>Password</label>
                        <input
                            type="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            required
                        />
                    </div>
                    <button type="submit" className="auth-button">Registrati</button>
                </form>
                <p className="auth-footer">
                    Hai già un account? <Link to="/login">Accedi</Link>
                </p>
            </div>
        </div>
    );
}
