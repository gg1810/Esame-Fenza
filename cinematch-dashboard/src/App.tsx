import { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, useLocation, Navigate } from 'react-router-dom';
import { Sidebar } from './components/Sidebar';
import { Dashboard } from './pages/Dashboard';
import { FilmVisti } from './pages/FilmVisti';
import { Recommendations } from './pages/Recommendations';
import { Cinema } from './pages/Cinema';
import { Sentiment } from './pages/Sentiment';
import { Mood } from './pages/Mood';
import { Predictions } from './pages/Predictions';
import { Login, Register } from './pages/Auth';
import './styles/global.css';

// Map routes to studio themes
const routeThemes: Record<string, string> = {
  '/': 'netflix',
  '/dashboard': 'netflix',
  '/film-visti': 'fox',
  '/recommendations': 'a24',
  '/cinema': 'warner',
  '/sentiment': 'paramount',
  '/mood': 'lionsgate',
  '/predictions': 'universal'
};

function ThemeManager({ children }: { children: React.ReactNode }) {
  const location = useLocation();

  useEffect(() => {
    const theme = routeThemes[location.pathname] || 'netflix';
    document.documentElement.setAttribute('data-theme', theme);
  }, [location]);

  return <>{children}</>;
}

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const token = localStorage.getItem('token');
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function App() {
  return (
    <Router>
      <ThemeManager>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          <Route
            path="/*"
            element={
              <ProtectedRoute>
                <div className="app-container">
                  <Sidebar />
                  <main className="main-content">
                    <Routes>
                      <Route path="/" element={<Navigate to="/dashboard" replace />} />
                      <Route path="/dashboard" element={<Dashboard />} />
                      <Route path="/film-visti" element={<FilmVisti />} />
                      <Route path="/recommendations" element={<Recommendations />} />
                      <Route path="/cinema" element={<Cinema />} />
                      <Route path="/sentiment" element={<Sentiment />} />
                      <Route path="/mood" element={<Mood />} />
                      <Route path="/predictions" element={<Predictions />} />
                      <Route path="*" element={<Navigate to="/dashboard" replace />} />
                    </Routes>
                  </main>
                </div>
              </ProtectedRoute>
            }
          />
        </Routes>
      </ThemeManager>
    </Router>
  );
}

export default App;
