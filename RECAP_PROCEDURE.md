# 📋 Recap Procedurale: Da Prototipo a Sistema Reale

Questo file riassume i passaggi esatti da seguire, in ordine cronologico, per completare lo sviluppo del progetto CineMatch e garantire la persistenza di ogni dato generato.

---

### 🟢 FASE 1: Consolidamento Database (Persistenza Totale)
*Obiettivo: Ogni azione fatta sul sito deve lasciare una traccia su MongoDB.*

1.  **Salvataggio Raccomandazioni**: Modificare la logica che genera film consigliati per scrivere i risultati in una collezione `recommendations` legata all'ID utente.
2.  **Tracking delle Query**: Ogni volta che l'utente filtra film (es. "Mostra commedie del 1990"), inviare una chiamata al backend per salvare i parametri di ricerca in `queries_history`.
3.  **Audit Log**: Registrare data e ora di ogni caricamento CSV per mostrare all'utente l'evoluzione dello storico nel tempo.

### 🟡 FASE 2: Integrazione Live API (Dati Veri)
*Obiettivo: Eliminare i dati simulati.*

4.  **TMDB Connection**: Inserire la chiave API in un file `.env`. Sostituire la logica di `mockData.ts` con chiamate `fetch` che prendono poster e trame in tempo reale.
5.  **Scraper Reddit**: Completare lo script `praw` in Python per pescare i commenti reali invece di usare quelli generati dallo script.
6.  **Environment Variables**: Configurare Docker per passare tutte le chiavi segrete in modo sicuro.

### 🔵 FASE 3: Intelligence con Apache Spark
*Obiettivo: Scalabilità e Machine Learning.*

7.  **Training ALS**: Far girare il Job Spark sui dati salvati in MongoDB per creare raccomandazioni personalizzate (Collaborative Filtering).
8.  **Sentiment su Larga Scala**: Usare Spark per analizzare migliaia di post contemporaneamente (Batch Processing) invece di uno alla volta.
9.  **Data Warehouse**: Usare Spark per pulire i dati grezzi dei file AMDB massivi e caricarli in MongoDB in formato ottimizzato per il frontend.

### 🔴 FASE 4: Raffinamento Frontend & UI
*Obiettivo: Esperienza utente fluida.*

10. **Global State**: Usare React Context o Redux per gestire il token di login e i dati utente in tutto il sito.
11. **Feedback di Elaborazione**: Poiché l'analisi RoBERTa e Spark richiedono tempo, aggiungere barre di caricamento reali che mostrano lo stato dell'elaborazione nel backend.
12. **Dashboard Storica**: Creare una pagina "Le mie attività" dove l'utente può rivedere tutte le query e le analisi fatte in passato, caricate da MongoDB.

---

### 🛠️ Come muoversi ora:
1.  **Avvia Docker**: `docker-compose up --build`
2.  **Registrati**: Crea un account per testare la persistenza.
3.  **Carica CSV**: Verifica che i dati appaiano su MongoDB Compass.
4.  **Sviluppa**: Segui i punti 1-3 della Fase 1 per completare la persistenza delle query.

Questo progetto ha ora tutte le fondamenta tecnicamente solide (Docker, Auth, DB, AI, Big Data). Buon lavoro! 🎬🚀
