# 📕 CineMatch: La Bibbia Tecnica del Progetto

Questa guida contiene ogni singolo dettaglio tecnico, comando e configurazione necessaria per passare dal prototipo attuale ad un sistema di produzione completo. Non sottovalutiamo nulla.

---

## 🏗️ 1. INFRASTRUTTURA DOCKER (Il Cuore)
Il progetto gira su 5 contenitori isolati che comunicano tra loro tramite una rete interna Docker.

### 🐳 Servizi nel `docker-compose.yml`:
1.  **`mongodb`**: Database NoSQL. Porta `27017`. Salva i dati fisicamente nella cartella `mongo_data` (volume persistente).
2.  **`spark-master`**: Orchestratore per il Big Data. Interfaccia WEB su `localhost:8080`.
3.  **`spark-worker`**: Il "braccio" che esegue i calcoli ML (ALS per raccomandazioni).
4.  **`backend`**: FastAPI (Python). Carica i modelli AI, gestisce Auth e scrive su MongoDB. Porta `8000`.
5.  **`frontend`**: Vite + React. L'interfaccia utente. Porta `5173`.

### 🚀 Comandi Docker Fondamentali:
- `docker-compose up --build`: Costruisce le immagini da zero (obbligatorio se modifichi `requirements.txt` o i `Dockerfile`).
- `docker-compose up -d`: Avvia in background.
- `docker-compose logs -f backend`: Per vedere gli errori di Python in tempo reale.
- `docker exec -it cinematch_db mongosh`: Per entrare nel database e fare query manuali.

---

## 🐍 2. BACKEND & LOGICA AI (Python)
Il backend è diviso in moduli per essere manutenibile.

### 🔑 Autenticazione (JWT):
- Le password sono salvate come **Hash BCrypt**. Mai salvare password in chiaro.
- Il server genera un `access_token` (JWT). Il Frontend deve inviarlo in ogni richiesta nell'Header: `Authorization: Bearer <token>`.

### 🧠 Sentiment Analysis (RoBERTa):
- Usiamo `cardiffnlp/twitter-roberta-base-sentiment-latest`. 
- **⚠️ Attenzione**: Al primo avvio, Docker scaricherà circa 500MB di modello. Assicurati di avere spazio su disco.
- **Flusso**: Testo -> Tokenizer -> Modello -> Punteggi (Pos/Neg/Neu) -> Media Voti.

### 📂 Gestione CSV (Pandas):
- Il file scaricato da Letterboxd viene letto in memoria (`io.BytesIO`).
- I dati vengono filtrati (`dropna`) e convertiti in dizionari Python per MongoDB.

---

## 💾 3. PERSISTENZA TOTALE (MongoDB)
Ogni informazione generata deve essere salvata nella collezione `users`.

### 📂 Struttura Documento Utente:
```json
{
  "_id": "659...",
  "username": "luca",
  "password": "hashed_password",
  "stats": { "total_watched": 450, "avg_rating": 4.2 },
  "user_data": { "movies": [...] },
  "history": [
    { "movie": "Dune", "sentiment": 85.5, "timestamp": "2026-01-05..." }
  ],
  "activity_log": [
    { "action": "filter_genre", "value": "Sci-Fi", "timestamp": "..." }
  ]
}
```

---

## ⚡ 4. BIG DATA PROCESSING (Apache Spark)
Spark non risponde alle richieste del sito, ma pulisce i dati in background.

1.  **Connessione**: Spark usa il connettore `mongo-spark-connector` per leggere da MongoDB.
2.  **Job di Raccomandazione**:
    - Legge i rating di *tutti* gli utenti.
    - Crea una matrice Utente-Film.
    - Usa **MLlib ALS** per prevedere quali film piaceranno a un utente basandosi su utenti simili.
    - Scrive i 10 film consigliati direttamente nel profilo MongoDB dell'utente.

---

## 🎨 5. FRONTEND & TEMI (React)
L'estetica è fondamentale per il progetto.

- **Dynamic Theming**: Avviene in `App.tsx` tramite la funzione `ThemeManager`. Cambia l'attributo `data-theme` del tag `<html>`.
- **CSS Variables**: Tutte le pagine leggono i colori da `themes.css` (es: `var(--accent-primary)`).
- **Integrazione API**:
    - Usare `fetch()` con l'header di autorizzazione.
    - Gestire lo stato di `loading`: l'App deve mostrare uno spinner mentre RoBERTa calcola lo score.

---

## 🛠️ 6. CHECKLIST DI SVILUPPO (Ordine di Esecuzione)

1.  **Configura `.env`**: Crea un file con `TMDB_KEY` e `REDDIT_SECRET`.
2.  **Sostituisci Mock**: Cerca in tutto il frontend dove viene importato `mockData` e sostituiscilo con una chiamata `fetch` al backend.
3.  **Ottimizzazione Docker**: Se il caricamento di RoBERTa è lento, assicurati di aver dato almeno 4GB di RAM a Docker Desktop.
4.  **Implementa Scraper Reali**: Sostituisci i commenti finti in `main.py` con una funzione che usa la libreria `praw`.
5.  **Verifica MongoDB**: Controlla periodicamente con *MongoDB Compass* che i dati vengano realmente salvati nelle sotto-cartelle `history` e `activity_log`.

Questa guida è la tua roadmap definitiva. Ogni riga di codice che abbiamo scritto finora è stata progettata per incastrarsi perfettamente in questo sistema. 🎬🚀✨
