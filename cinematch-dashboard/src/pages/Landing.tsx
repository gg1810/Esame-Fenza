import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import './Landing.css';

export function Landing() {
    const [isDragging, setIsDragging] = useState(false);
    const [fileName, setFileName] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const navigate = useNavigate();

    // Se l'utente ha già dati, vai direttamente alla dashboard
    useEffect(() => {
        const hasData = localStorage.getItem('has_data');
        if (hasData === 'true') {
            navigate('/dashboard');
        }
    }, [navigate]);

    const handleDragOver = (e: React.DragEvent) => {
        e.preventDefault();
        setIsDragging(true);
    };

    const handleDragLeave = () => {
        setIsDragging(false);
    };

    const uploadFile = async (file: File) => {
        setFileName(file.name);
        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('http://localhost:8000/upload-csv', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('token')}`
                },
                body: formData,
            });

            if (response.ok) {
                localStorage.setItem('has_data', 'true'); // Aggiorna stato
                setTimeout(() => navigate('/dashboard'), 1000);
            } else {
                alert('Errore durante il caricamento');
                setFileName(null);
            }
        } catch (error) {
            console.error('Error:', error);
            alert('Errore di connessione al server');
            setFileName(null);
        }
    };

    const handleDrop = (e: React.DragEvent) => {
        e.preventDefault();
        setIsDragging(false);
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith('.csv')) {
            uploadFile(file);
        }
    };

    const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (file && file.name.endsWith('.csv')) {
            uploadFile(file);
        }
    };

    const handleClick = () => {
        fileInputRef.current?.click();
    };

    return (
        <div className="landing-page" data-theme="netflix">
            <div className="landing-background">
                <div className="bg-gradient"></div>
                <div className="bg-pattern"></div>
            </div>

            <div className="landing-content">
                <div className="landing-header">
                    <span className="logo-icon">🎬</span>
                    <h1 className="logo-title">CineMatch</h1>
                    <p className="logo-tagline">Il tuo sistema di raccomandazione film personalizzato</p>
                </div>

                <div
                    className={`upload-zone ${isDragging ? 'dragging' : ''} ${fileName ? 'uploaded' : ''}`}
                    onDragOver={handleDragOver}
                    onDragLeave={handleDragLeave}
                    onDrop={handleDrop}
                    onClick={handleClick}
                >
                    <input
                        ref={fileInputRef}
                        type="file"
                        accept=".csv"
                        onChange={handleFileSelect}
                        hidden
                    />

                    {fileName ? (
                        <>
                            <div className="upload-success">
                                <span className="success-icon">✓</span>
                            </div>
                            <p className="upload-filename">{fileName}</p>
                            <p className="upload-loading">Caricamento in corso...</p>
                        </>
                    ) : (
                        <>
                            <div className="upload-icon">
                                <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                                    <polyline points="17 8 12 3 7 8" />
                                    <line x1="12" y1="3" x2="12" y2="15" />
                                </svg>
                            </div>
                            <p className="upload-text">Trascina il tuo file Letterboxd qui</p>
                            <p className="upload-subtext">oppure clicca per selezionare</p>
                            <span className="upload-format">Formato supportato: .csv</span>
                        </>
                    )}
                </div>

                <div className="landing-features">
                    <div className="feature">
                        <span className="feature-icon">📊</span>
                        <span className="feature-text">Analisi completa</span>
                    </div>
                    <div className="feature">
                        <span className="feature-icon">🎯</span>
                        <span className="feature-text">Raccomandazioni AI</span>
                    </div>
                    <div className="feature">
                        <span className="feature-icon">💬</span>
                        <span className="feature-text">Commenti YouTube</span>
                    </div>
                    <div className="feature">
                        <span className="feature-icon">😊</span>
                        <span className="feature-text">Match per Mood</span>
                    </div>
                </div>
            </div>
        </div>
    );
}
