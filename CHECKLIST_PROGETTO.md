# ✅ Checklist Operativa: CineMatch Full-Stack

Segui questi punti in ordine per costruire il sistema completo.

## 1. 🔑 Preparazione Account e Chiavi
- [ ] **TMDB API**: Registrati su [themoviedb.org](https://www.themoviedb.org/) e richiedi una API Key (è istantanea e gratuita). Serve per scaricare i poster dei film.
- [ ] **Reddit API (PRAW)**: Vai su [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps), crea una "script app" e segnati `client_id` e `client_secret`. Serve per lo scraper del sentiment.
- [ ] **Docker Desktop**: Installa [Docker Desktop](https://www.docker.com/products/docker-desktop/) sul tuo PC e assicurati che sia avviato.

## 2. 📂 Organizzazione Cartelle
- [ ] Crea una cartella `backend` nella root del progetto.
- [ ] Crea un file `requirements.txt` dentro `backend` con:
  ```text
  fastapi
  uvicorn
  pandas
  requests
  praw
  transformers
  torch
  pymongo
  ```

## 3. 🐳 Setup Infrastruttura (Docker)
- [ ] Crea un file `docker-compose.yml` nella cartella principale.
- [ ] Configura il servizio `mongodb`.
- [ ] Configura il servizio `backend` (Python).
- [ ] Configura il servizio `frontend` (React).
- [ ] Lancia tutto con il comando: `docker-compose up --build`.

## 4. 🐍 Sviluppo Backend (Python)
- [ ] **CSV Parser**: Completa lo script per leggere il file Letterboxd.
- [ ] **DB Integration**: Scrivi le funzioni per salvare i film caricati su MongoDB.
- [ ] **RoBERTa Script**: Scarica il modello `cardiffnlp/twitter-roberta-base-sentiment-latest` via HuggingFace.
- [ ] **API Endpoints**: Crea i "punti di contatto" (es. `/upload-csv`, `/get-recommendations`).

## 5. 🔗 Collegamento Finale
- [ ] **API Fetch**: In React, sostituisci i dati finti in `mockData.ts` con chiamate `fetch()` al tuo backend.
- [ ] **Stato di Caricamento**: Aggiungi un messaggio "Analisi AI in corso..." durante il processing del CSV.

---

### Prossimo Passo Consigliato:
Iniziamo dalla **Punto 3 (Docker)**? Posso scriverti il file `docker-compose.yml` completo così hai subito il database attivo. 🎬
