# Analisi Schema `user_stats` - Ridondanze e Proposte di Ottimizzazione

## 📊 Stato Attuale

La collection `user_stats` contiene **38 campi** per ogni documento utente. Molti di questi sono **ridondanti** o contengono **dati duplicati**.

---

## 🔴 RIDONDANZE CRITICHE DA RIMUOVERE

### 1. **Dati Rating Duplicati (3 strutture diverse per gli stessi dati!)**

| Campo | Contenuto | Proposta |
|-------|-----------|----------|
| `total_1_stars`, `total_2_stars`, `total_3_stars`, `total_4_stars`, `total_5_stars` | Conteggio per stella (5 campi separati) | ❌ **RIMUOVERE** |
| `rating_distribution` | `{"0":1, "1":8, "2":18, "3":33, "4":34, "5":21}` | ❌ **RIMUOVERE** |
| `rating_chart_data` | `[{rating: "⭐1", count: 8, stars: 1}, ...]` | ✅ **MANTENERE** (usato dal frontend) |

**Problema**: Stessi dati memorizzati 3 volte in formati diversi!

---

### 2. **Dati Mensili Duplicati (3 strutture diverse!)**

| Campo | Contenuto | Proposta |
|-------|-----------|----------|
| `monthly_data` | Array mesi dell'anno corrente `[{month: "Gen", films: 1}, ...]` | ❌ **RIMUOVERE** |
| `yearly_monthly_data` | Oggetto per anno `{"2025": [{month: "Gen", films: 0}, ...]}` | ❌ **RIMUOVERE** |
| `year_data` | Array strutturato `[{year: 2026, monthly_data: [...], total_films: 1}]` | ✅ **MANTENERE** (più completo, forse aggiungere fino al 2010 o se un film è stato aggiunto nel 2010 lo fa da solo?) |

**Problema**: `monthly_data` è un subset di `year_data`, e `yearly_monthly_data` è la stessa cosa in formato diverso.

---

### 3. **Attori/Registi Duplicati**

| Campo | Contenuto | Proposta |
|-------|-----------|----------|
| `top_actors` | Top 15 attori per numero film | ❌ **RIMUOVERE** (derivabile da `best_rated_actors`) |
| `best_rated_actors` | TUTTI gli attori (1232!) con count e avg_rating | ✅ **MANTENERE** |
| `top_directors` | Top 10 registi per numero film | ❌ **RIMUOVERE** (derivabile da `best_rated_directors`) |
| `best_rated_directors` | TUTTI i registi (103) con count e avg_rating | ✅ **MANTENERE** |

**Problema**: `top_actors` e `top_directors` sono semplicemente subset ordinati diversamente di `best_rated_*`. Il frontend può filtrarli/ordinarli al volo.

---

### 4. **Timestamp/Source Duplicati**

| Campo | Valore Esempio | Proposta |
|-------|----------------|----------|
| `updated_at` | `2026-01-12T23:44:01.941018` | ✅ **MANTENERE** |
| `last_update` | `2026-01-12T09:58:45.421287` | ❌ **RIMUOVERE** (duplicato) |
| `source` | `spark_streaming` | ✅ **MANTENERE** |
| `source_file` | `ratings.csv` | ❌ **RIMUOVERE** (legacy, non più usato) |

---

### 5. **Watch Time Duplicato**

| Campo | Valore | Proposta |
|-------|--------|----------|
| `watch_time_hours` | `196` | ✅ **MANTENERE** (intero, più leggibile) |
| `watch_time_minutes` | `20` | ❌ **RIMUOVERE** (solo i minuti extra, confusionario) |

**Problema**: `watch_time_minutes = 20` non sono 20 minuti totali ma i minuti extra oltre le ore. È confusionario. Meglio un solo campo.

---

### 6. **Contatori Ridondanti**

| Campo | Valore | Proposta |
|-------|--------|----------|
| `total_unique_actors` | `1232` | ❌ **RIMUOVERE** (= `best_rated_actors.length` ok se veniva usato fallo con spark, se non è usato aggiungilo in dashboard nel box in basso al posto di qualche info ridondante) |
| `total_unique_directors` | `103` | ❌ **RIMUOVERE** (= `best_rated_directors.length` vedi sopra) |

---

## ✅ SCHEMA PROPOSTO (Ottimizzato)

```javascript
{
  "_id": ObjectId,
  "user_id": "Pswie",
  
  // === STATISTICHE BASE ===
  "total_watched": 115,
  "avg_rating": 3.3,
  "favorite_genre": "Comedy",
  "watch_time_hours": 196,        // Tempo totale in ore (minuti come decimali se necessario)
  "avg_duration": 102,            // Durata media film in minuti
  
  // === GENERI ===
  "genre_data": [                 // Per grafico a torta
    {"genre": "Comedy", "count": 45, "percentage": 39},
    {"genre": "Romance", "count": 30, "percentage": 26},
    ...
  ],
  
  // === RATING ===
  "rating_chart_data": [          // Per grafico barre rating
    {"rating": "⭐1", "count": 8, "stars": 1},
    {"rating": "⭐2", "count": 18, "stars": 2},
    ...
  ],
  
  // === TIMELINE ===
  "year_data": [                  // Per grafico mensile con selezione anno
    {
      "year": 2026,
      "monthly_data": [{month: "Gen", films: 1}, ...],
      "total_films": 1
    },
    {
      "year": 2025,
      "monthly_data": [{month: "Gen", films: 0}, ...],
      "total_films": 114
    }
  ],
  "available_years": [2026, 2025],
  "top_years": [{year: 2025, count: 114}, ...],
  
  // === FILM ===
  "top_rated_movies": [           // Top 10 film per voto
    {"title": "...", "year": 2020, "rating": 5},
    ...
  ],
  "recent_movies": [              // Ultimi 10 film visti
    {"title": "...", "year": 2024, "rating": 4, "date": "2026-01-12"},
    ...
  ],
  "rating_vs_imdb": [             // Top 20 differenze con IMDb
    {"title": "...", "user_rating": 5, "imdb_rating": 4.5, "difference": 0.5},
    ...
  ],
  
  // === PERSONE ===
  "best_rated_actors": [          // TUTTI gli attori (frontend filtra)
    {"name": "...", "count": 6, "avg_rating": 4.5},
    ...
  ],
  "best_rated_directors": [       // TUTTI i registi (frontend filtra)
    {"name": "...", "count": 4, "avg_rating": 4.8},
    ...
  ],
  
  // === QUIZ ===
  "quiz_correct_count": 5,
  "quiz_wrong_count": 5,
  "quiz_total_attempts": 2,
  "last_quiz_date": "2026-01-12",
  
  // === METADATA ===
  "updated_at": "2026-01-12T23:44:01.941018",
  "source": "spark_streaming",
  "stats_version": "3.1"
}
```

---

## 📉 RIEPILOGO RIDUZIONE

| Metrica | Prima | Dopo | Risparmio |
|---------|-------|------|-----------|
| **Campi totali** | 38 | 24 | **-37%** |
| **Dati rating** | 3 strutture | 1 struttura | **-67%** |
| **Dati mensili** | 3 strutture | 1 struttura | **-67%** |
| **Dati attori/registi** | 4 array | 2 array | **-50%** |

---

## ⚠️ CAMPI DA RIMUOVERE (Lista completa)

1. `total_1_stars`
2. `total_2_stars`
3. `total_3_stars`
4. `total_4_stars`
5. `total_5_stars`
6. `rating_distribution`
7. `monthly_data`
8. `yearly_monthly_data`
9. `top_actors`
10. `top_directors`
11. `total_unique_actors`
12. `total_unique_directors`
13. `watch_time_minutes`
14. `last_update`
15. `source_file`

---

## 🔧 AZIONI RICHIESTE

Se approvi le modifiche:

1. **Aggiornare `spark_stats_processor.py`** - Rimuovere calcolo campi ridondanti
2. **Verificare frontend Dashboard.tsx** - Assicurarsi che usi solo i campi mantenuti
3. **Migrare dati esistenti** - Script per rimuovere campi vecchi da MongoDB
4. **Aggiornare versione stats** - `stats_version: "3.1"`

---

**Confermi le modifiche proposte?** (Sì/No/Modifica)
