# CineMatch | Roadmap per l'Integrazione Full-Stack

Questo documento delinea i passaggi necessari per trasformare il prototipo React in un'applicazione completa con Backend Python, MongoDB e Docker.

---

## 🏗️ FASE 1: Infrastruttura (Docker)
L'obiettivo è creare un ambiente dove tutti i pezzi comunicano senza problemi di installazione locale.

1.  **Creazione `docker-compose.yml`**: Configurare i 3 servizi (frontend, backend, database).
2.  **Configurazione MongoDB**: Setup di una rete interna Docker per permettere a Python di salvare i dati nel DB.
3.  **Persistenza Dati**: Configurare i "volumi" in modo che se spegni Docker, i tuoi film salvati non vadano persi.

## 🐍 FASE 2: Backend & Dati (Python)
Il "cervello" dell'applicazione. Useremo **FastAPI** perché è moderno, ultra-veloce e gestisce bene il caricamento file.

1.  **Parser CSV (Pandas)**: Creare la funzione per leggere il CSV di Letterboxd ed estrarre titoli e voti.
2.  **Scraper Reddit & AMDB**:
    *   Uno script per scaricare post su film specifici.
    *   Integrazione API (o file AMDB) per recuperare i poster e gli ID dei film.
3.  **Database Manager (PyMongo)**: Funzioni per salvare/leggere i profili utente e i risultati delle analisi in MongoDB.

## 🤖 FASE 3: AI & Modelli (Sentiment)
Implementazione del sistema di intelligenza artificiale.

1.  **Integrazione RoBERTa**: Caricamento del modello tramite la libreria `transformers` di HuggingFace.
2.  **Pipeline di Analisi**: Funzione che prende i commenti di Reddit, li passa al modello e restituisce un punteggio da 0 a 100 per il frontend.
3.  **Previsione Incassi**: Creazione del modello (anche semplice regressione lineare all'inizio) per il sistema di previsione.

## 🔗 FASE 4: Collegamento (React ↔ Python)
Sostituire i dati finti con quelli reali provenienti dal tuo backend.

1.  **Fetch API**: Modificare il frontend per fare richieste `GET` e `POST` verso Python.
2.  **Gestione Caricamento**: Mostrare una barra di progresso o uno spinner mentre Python elabora il CSV e l'analisi AI.

## ⚡ FASE 5: Big Data & Machine Learning (Apache Spark)
Per gestire milioni di dati dai database AMDB e Reddit.

1.  **Spark Integration**: Aggiungere Spark (Master e Worker) nel `docker-compose.yml`.
2.  **Recommendation Engine (PySpark)**: Usare Spark MLlib per creare un modello di raccomandazione (es. ALS - Alternating Least Squares) basato sui ratings degli utenti.
3.  **Data Processing**: Processare grandi volumi di commenti Reddit in parallelo per estrarre trend globali.
4.  **Batch Jobs**: Creare script che ogni notte aggiornano le raccomandazioni in MongoDB leggendo i dati grezzi.

---

## 🛠️ Cosa ti serve installare subito?
Se vuoi iniziare ad esplorare questa parte, assicurati di avere:
- **Docker Desktop** installato.
- **Python 3.10+** (per testare gli script prima di metterli in Docker).
- Una **Chiave API** di TMDB (gratuita) per i poster dei film.

---

**Quale di questi punti vuoi approfondire per primo?** Posso scriverti uno schema del file `docker-compose.yml` o farti vedere come Python legge il CSV. 🎬✨
