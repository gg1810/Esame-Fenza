# 🎯 CineMatch: Piano di Sviluppo da Demo a Produzione

Questo documento serve come guida definitiva per trasformare la demo attuale in un sistema completo e scalabile. L'obiettivo è passare dai dati simulati ad un'architettura **Data-Driven** dove ogni analisi viene salvata permanentemente su MongoDB.

---

## 🏗️ 1. ARCHITETTURA DATI (MongoDB Deep Integration)
Dobbiamo smettere di usare `mockData.ts` e far sì che ogni azione scriva nel DB.

- [ ] **Data Model Evolution**: Espandere lo schema dell'utente in MongoDB per includere:
    *   `history`: Lista completa dei film dal CSV.
    *   `recommendations_cache`: Risultati generati da Spark/TMDB (per non ricalcolarli ogni volta).
    *   `sentiment_results`: Storico delle analisi fatte su film specifici.
    *   `queries_log`: Salvataggio di ogni ricerca o filtro applicato dall'utente.
- [ ] **Repository Pattern (Python)**: Creare un file `database.py` nel backend per centralizzare tutte le funzioni di lettura/scrittura (es. `save_recommendation()`, `get_user_history()`).

---

## 🎬 2. INTEGRAZIONE API REALI (Sostituzione Mock)
Passaggio cruciale per avere contenuti veri.

- [ ] **TMDB (The Movie Database)**:
    *   Sostituire gli URL statici delle immagini con chiamate dinamiche.
    *   Utilizzare l'endpoint `/recommendation` di TMDB per alimentare la pagina **A24**.
- [ ] **Reddit Scrapper (PRAW)**:
    *   Implementare la raccolta reale dei commenti da `/r/movies` o `/r/boxoffice`.
    *   Passare i commenti crudi al nostro modello **RoBERTa** salvato localmente nel container.

---

## ⚡ 3. BIG DATA & ANALYTICS (Apache Spark)
Qui il progetto fa il salto di qualità accademico/tecnico.

- [ ] **Batch Processing**: Creare un Job Spark che:
    *   Legge tutti i ratigns degli utenti da MongoDB.
    *   Applica l'algoritmo **ALS (Collaborative Filtering)**.
    *   Scrive i "Top 10 consigliati" nella collezione MongoDB dell'utente.
- [ ] **Parallel Sentiment**: Se un utente carica 1000 film, Spark può analizzare il sentiment di migliaia di commenti Reddit in parallelo molto più velocemente di Python puro.

---

## 🛡️ 4. SICUREZZA E ROBUSTEZZA
- [ ] **JWT Refresh Tokens**: Implementare una sicurezza più solida per non far scadere la sessione improvvisamente.
- [ ] **Error Handling**: Gestire i casi in cui un film nel CSV non viene trovato o le API esterne sono offline.
- [ ] **Validation (Pydantic)**: Assicurarsi che ogni dato inserito nel database sia formattato correttamente.

---

## 🚀 ORDINE DI OPERAZIONI (Roadmap Pratica)

1.  **STEP 1 (Persistenza Totale)**: Modificare `main.py` per salvare *ogni* risultato di analisi su MongoDB prima di inviarlo al frontend.
2.  **STEP 2 (Clean Frontend)**: Rimuovere gradualmente i file di `mockData` e sostituirli con `fetch()` verso il backend.
3.  **STEP 3 (API Keys)**: Inserire le chiavi TMDB e Reddit nelle variabili d'ambiente di Docker.
4.  **STEP 4 (Spark ML)**: Implementare il primo vero algoritmo di raccomandazione su Spark.
5.  **STEP 5 (UI Polish)**: Aggiungere feedback visivi (spinner, messaggi di successo) per ogni interazione col database.

---

**NOTA**: La "Demo" che abbiamo ora è la fondamenta. Seguendo questi punti, il progetto diventerà una piattaforma pronta per essere presentata come esame o portfolio professionale. 🎬✨
