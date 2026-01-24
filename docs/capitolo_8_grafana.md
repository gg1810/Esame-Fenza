# Capitolo 8: Dashboard Analitica con Grafana

Grafana rappresenta il livello di visualizzazione ("Presentation Layer") dell'architettura Big Data di CineMatch. Attraverso una connessione diretta alle sorgenti dati (MongoDB via plugin Infinity/MongoDB e API Backend), offre una panoramica in tempo reale dello stato del sistema e dei trend cinematografici.

Il dashboard configurato è suddiviso in tre sezioni logiche principali, progettate per monitorare sia le metriche di business (film, utenti) che quelle tecniche (sentiment, performance).

---

## 8.1 Sezione Global Trends (Analisi Mercato)

Questa sezione visualizza i dati calcolati da Spark Streaming e persistiti nella collezione `global_stats`. Serve per comprendere quali contenuti stanno performando meglio sulla piattaforma.

### 1. Top 10 Movies (Bar Chart)
*   **Descrizione**: Visualizza i 10 film con il maggior numero di interazioni (visualizzazioni/voti) accumulate.
*   **Sorgente Dati**: MongoDB (`global_stats` -> `top_movies`).
*   **Visualizzazione**: Grafico a barre orizzontali. Sull'asse Y i titoli dei film, sull'asse X il numero di interazioni.
*   **Insight**: Permette di identificare istantaneamente i "Blockbuster" del momento.

### 2. Trending Genres (Pie Chart)
*   **Descrizione**: Mostra la distribuzione percentuale dei generi più popolari basata sull'attività recente degli utenti.
*   **Sorgente Dati**: MongoDB (`global_stats` -> `trending_genres`).
*   **Visualizzazione**: Grafico a torta (Donut Chart) con legenda laterale e percentuali.
*   **Insight**: Utile per capire se l'utenza preferisce generi specifici (es. Drama vs Sci-Fi) in un determinato periodo.

---



## 8.2 Sezione Monitoraggio Utenti e Sistema

Questa sezione offre metriche operative sulla base utenti e sullo stato del sistema.

### 3. Nuovi Utenti Giornalieri (Stat)
*   **Descrizione**: Visualizza il numero di nuovi utenti registrati nelle ultime 24 ore.
*   **Sorgente Dati**: MongoDB (`users` collection).
*   **Visualizzazione**: Stat panel (Single Value).

### 4. Distribuzione Rating (Histogram)
*   **Descrizione**: Un istogramma che mostra come gli utenti distribuiscono i loro voti (da 1 a 5 stelle).
*   **Sorgente Dati**: MongoDB (`user_stats` aggregate o `global_stats`).
*   **Visualizzazione**: Istogramma verticale.
*   **Insight**: Evidenzia se la community è tendenzialmente generosa (molti 4-5) o critica (molti 1-2).

---

## 8.3 Architettura di Collegamento

Grafana non si collega direttamente a Spark, ma legge i risultati "cristallizzati" che Spark scrive su MongoDB. L'architettura utilizza il plugin **Infinity Datasource** (per chiamate API JSON al backend) o un connettore MongoDB nativo.

*   **Vantaggio**: Disaccoppiamento totale. Se Spark è sotto carico pesante per il processing, la dashboard Grafana continua a leggere velocemente l'ultimo stato valido dal database, senza rallentamenti.
