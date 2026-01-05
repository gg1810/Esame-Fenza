# 👥 CineMatch: Guida alla Collaborazione in Team

Per lavorare in più persone sullo stesso progetto e permettere a tutti di modificare il codice senza creare conflitti, dobbiamo adottare un workflow professionale basato su **Git** e **Docker**.

---

## 🏎️ 1. Setup del Repository (GitHub)
Il modo migliore per collaborare è usare GitHub. Uno di voi deve fare il primo passo:

1.  **Inizializza Git**: Nella cartella principale `Esame Fenza`, apri il terminale e scrivi:
    ```bash
    git init
    ```
2.  **Crea il file `.gitignore`**: Per evitare di caricare file inutili (come `node_modules` o dati pesanti di MongoDB).
    *(Ti ho già preparato il file nella cartella principale).*
3.  **Carica su GitHub**:
    - Crea un repo vuoto su GitHub.
    - `git add .`
    - `git commit -m "First team commit"`
    - `git branch -M main`
    - `git remote add origin <URL_DEL_TUO_REPO>`
    - `git push -u origin main`

---

## 🛠️ 2. Come devono lavorare i tuoi Collaboratori
Ogni volta che qualcuno vuole contribuire, deve seguire questo schema:

1.  **Clone**: `git clone <URL_DEL_REPO>`
2.  **Avvio Ambiente**: `docker-compose up --build`
    *(Grazie a Docker, non dovranno installare Python, Node o MongoDB sul loro PC. Avranno tutto pronto in 1 secondo).*
3.  **Modifica**: Scrivono il codice.
4.  **Sync**: 
    - `git pull` (per prendere le modifiche degli altri)
    - `git add .`
    - `git commit -m "Descrizione della modifica"`
    - `git push`

---

## 🔐 3. Gestione dei Segreti (`.env`)
**IMPORTANTE**: Non caricate mai le API Key (TMDB, Reddit) su GitHub. 
1.  Condividete il file `.env` privatamente (es. su Slack o WhatsApp).
2.  Ognuno lo mette nella cartella `backend/`.
3.  Docker lo leggerà automaticamente.

---

## 📊 4. Database Condiviso vs Locale
Avete due opzioni per i dati:

*   **Opzione A (Locale - Consigliata)**: Ognuno ha il suo database MongoDB dentro il proprio Docker. È più veloce e sicuro per lo sviluppo. Se carichi un CSV, lo vedi solo tu.
*   **Opzione B (Condiviso)**: Se volete vedere tutti gli stessi dati, dovete usare **MongoDB Atlas** (Cloud). 
    - Create un cluster gratuito su Atlas.
    - Cambiate `MONGODB_URL` nel `docker-compose.yml` con l'indirizzo di Atlas.

---

## 🚦 5. Regole d'oro per il Team
- **Mai modificare `node_modules`**: Se serve una libreria nuova, aggiungetela al `package.json` o `requirements.txt` e fate rifare il `build` di Docker agli altri.
- **Commit piccoli**: Meglio 10 commit piccoli che uno gigante che rompe tutto.
- **Docker è la legge**: Se a qualcuno "non funziona", la soluzione è quasi sempre fermare e riavviare Docker (`docker-compose down` e poi `up`).

Questa struttura permette a 2, 5 o 10 persone di lavorare contemporaneamente senza mai calpestarsi i piedi. 🎬🚀✨
