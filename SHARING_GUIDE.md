# 🚀 Come Condividere la Demo di CineMatch Rapidamente

Hai diverse opzioni per mostrare il tuo lavoro ad altri, dalla più semplice alla più professionale.

---

## 1. Condivisione in Locale (Stessa rete Wi-Fi)
*Ideale per mostrare la demo a colleghi o amici che sono nella tua stessa stanza.*

1.  Trova l'indirizzo IP del tuo computer:
    - Su Windows, apri il terminale e scrivi `ipconfig`. Cerca `Indirizzo IPv4` (es. `192.168.1.15`).
2.  Assicurati che Docker sia attivo (`docker-compose up -d`).
3.  Gli altri possono accedere al sito digitando nel loro browser:
    - `http://<IL_TUO_IP>:5173`
4.  **Nota**: Devi disattivare temporaneamente il Firewall di Windows o autorizzare la porta 5173.

---

## 2. Condivisione Pubblica con Ngrok (La più veloce)
*Ideale per dare un link funzionante a chiunque su Internet in 2 minuti.*

1.  Scarica [Ngrok](https://ngrok.com/).
2.  Apri un terminale e avvia il tunnel sulla porta del frontend:
    ```bash
    ngrok http 5173
    ```
3.  Copia l'URL pubblico che ti fornisce Ngrok (es. `https://a1b2-c3d4.ngrok-free.app`).
4.  Chiunque clicchi su quel link vedrà il tuo frontend!

---

## 3. Condivisione tramite GitHub (Professionale)
*Ideale per permettere ad altri sviluppatori di scaricare e avviare il progetto.*

1.  Crea un nuovo repository su GitHub.
2.  Invia il codice:
    ```bash
    git init
    git add .
    git commit -m "Initial commit"
    git remote add origin <url_del_tuo_repo>
    git push -u origin main
    ```
3.  Chiunque scarichi il progetto dovrà solo digitare un comando per vedere tutto funzionante:
    ```bash
    docker-compose up --build
    ```

---

## 4. Esportazione "Portable" (Senza Git)
*Se vuoi dare il materiale su una chiavetta USB.*

1.  Zippa l'intera cartella `Esame Fenza`.
2.  Assicurati che chi riceve il file abbia **Docker Desktop** installato.
3.  Una volta scompattato, basterà aprire il terminale nella cartella e fare `docker-compose up`.

---

### ⚠️ Consiglio per la Demo:
Se condividi il link (Ngrok o IP locale), assicurati che nel file `Landing.tsx` l'URL del backend punti correttamente al tuo IP o sia un percorso relativo, altrimenti il frontend non riuscirà a parlare con il server Python se acceduto da un altro dispositivo. 

Per la demo rapida, **Ngrok** è imbattibile! 🎬🚀
