Progettazione Schema MongoDB: Architettura Dati Quiz
Decisione
Abbiamo scelto di mantenere Documenti Separati (Basati su Collezione) per le domande del quiz piuttosto che incorporarle in un singolo documento "Box Giornaliero".

1. Approccio Attuale: Documenti Separati (Standard)
Ogni domanda è un documento indipendente nella collezione 
quiz_questions
.

Esempio di Schema:

{ "_id": "...", "quiz_date": "2026-01-12", "question": "...", "movie_id": "tt12345" }
{ "_id": "...", "quiz_date": "2026-01-12", "question": "...", "movie_id": "tt67890" }
✅ Vantaggi (Perché abbiamo scelto questo):

Flessibilità delle Query: Permette di cercare le domande secondo vari criteri (per Film, per Categoria, per Difficoltà) senza dover "srotolare" (unwind) array complessi.
Esempio: "Trova tutte le domande su 'Inception'" è una semplice 
find({movie_title: "Inception"})
.
Analisi Granulare: È più facile tracciare le statistiche per una domanda specifica (es. "Quale domanda precisa gli utenti sbagliano più spesso?").
Riutilizzabilità: Se la generazione del quiz giornaliero fallisce, è banale selezionare "3 nuove domande + 2 vecchie domande a caso" senza dover manipolare array nidificati complessi.
Performance: Leggere 5 piccoli documenti ha una latenza trascurabile rispetto alla lettura di un unico documento grande, ma l'indicizzazione è molto più potente sui documenti separati.
2. Approccio Alternativo: Box Giornaliero (Incorporato)
Memorizzare tutte le 5 domande del giorno all'interno di un singolo documento.

Esempio di Schema:

{
  "_id": "2026-01-12",
  "questions": [ {q1}, {q2}, {q3}, {q4}, {q5} ]
}
❌ Svantaggi:

Silos di Dati: Le domande sono "intrappolate" all'interno dell'oggetto giornaliero. Cercare "Tutte le domande sui film di Fantascienza" diventa costoso (richiede la scansione di tutti i giorni + unwind degli array).
Complessità: Correggere un singolo errore di battitura in una domanda richiede l'aggiornamento dell'intero array/documento giornaliero.
Indicizzazione Limitata: Non è facile indicizzare campi specifici delle domande (come movie_id) per ricerche efficienti su tutta la cronologia.
Conclusione
L'approccio a Documenti Separati è la best practice standard per questo tipo di dati ibridi relazionali/documentali in MongoDB. Offre il miglior equilibrio tra prestazioni, flessibilità e manutenibilità per la piattaforma CineMatch.