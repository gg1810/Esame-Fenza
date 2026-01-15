"""
Quiz Generator Module - VERSIONE FINALE CON QWEN 2.5 7B
Massima qualità per domande quiz in italiano perfetto
"""
import json
import re
import logging
import os
import pytz
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
import asyncio
import httpx

from pymongo import MongoClient
from pydantic import BaseModel, Field

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ⭐ MODELLO MIGLIORE - QWEN 2.5 7B
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct-q5_K_M")

# MongoDB connection
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client.cinematch_db
movies_catalog = db.movies_catalog
quiz_questions = db.quiz_questions

# Schemi Pydantic
class AnswerSchema(BaseModel):
    id: str = Field(description="letter identifier (a, b, c, or d)")
    text: str = Field(description="answer text")

class QuizQuestionSchema(BaseModel):
    question: str = Field(description="The quiz question text")
    answers: List[AnswerSchema] = Field(description="Exactly 4 answer options")
    correct: str = Field(description="The id of the correct answer (a, b, c, or d)")
    explanation: str = Field(description="Detailed explanation of why the answer is correct")
    category: str = Field(description="Category: plot, cast, trivia, or year")
    difficulty: str = Field(description="Difficulty level: easy, medium, or hard")

def ensure_indexes():
    """Crea gli indici per ottimizzare le query."""
    quiz_questions.create_index([("movie_id", 1)])
    quiz_questions.create_index([("created_at", -1)])
    quiz_questions.create_index([("used_count", 1), ("created_at", -1)])
    quiz_questions.create_index([("category", 1), ("difficulty", 1)])
    logger.info("Quiz indexes created/verified")

def get_random_movies(n: int = 5) -> List[Dict]:
    """Seleziona n film casuali dal catalogo."""
    pipeline = [
        {
            "$match": {
                "description": {"$exists": True, "$ne": ""},
                "title": {"$exists": True, "$ne": ""}
            }
        },
        {"$sample": {"size": n * 2}},
        {
            "$project": {
                "imdb_id": 1,
                "title": 1,
                "year": 1,
                "description": 1,
                "genre": 1,
                "director": 1,
                "actors": 1
            }
        }
    ]
    movies = list(movies_catalog.aggregate(pipeline))
    
    week_ago = datetime.now(pytz.timezone('Europe/Rome')) - timedelta(days=7)
    recent_movie_ids = set(
        q.get("movie_id") for q in quiz_questions.find(
            {"created_at": {"$gte": week_ago}},
            {"movie_id": 1}
        ) if q and q.get("movie_id")
    )
    
    available_movies = [m for m in movies if m.get("imdb_id") not in recent_movie_ids]
    if len(available_movies) < n:
        available_movies = movies
        
    return available_movies[:n]

def validate_question(q: Dict) -> Tuple[bool, str]:
    """
    Validazione rigorosa della qualità.
    Returns: (is_valid, reason)
    """
    # 1. Template generici (ZERO TOLERANCE)
    bad_phrases = [
        "domanda sul film", "risposta 1", "risposta 2", "risposta 3", "risposta 4",
        "breve spiegazione", "qual è il titolo", "quale film", "testo della domanda",
        "risposta plausibile", "opzione", "terza opzione", "quarta opzione"
    ]
    
    question_text = q["question"].lower()
    answers_text = " ".join(a["text"].lower() for a in q["answers"])
    explanation_text = q["explanation"].lower()
    
    for phrase in bad_phrases:
        if phrase in question_text:
            return False, f"Template nella domanda: '{phrase}'"
        if phrase in answers_text:
            return False, f"Template nelle risposte: '{phrase}'"
        if phrase in explanation_text:
            return False, f"Template nella spiegazione: '{phrase}'"
    
    # 2. Lunghezza minima (qualità)
    if len(q["question"]) < 30:
        return False, "Domanda troppo corta (<30 caratteri)"
    
    if len(q["question"]) > 200:
        return False, "Domanda troppo lunga (>200 caratteri)"
    
    if any(len(a["text"]) < 8 for a in q["answers"]):
        return False, "Alcune risposte troppo corte (<8 caratteri)"
    
    if len(q["explanation"]) < 25:
        return False, "Spiegazione troppo corta (<25 caratteri)"
    
    # 3. Risposte uniche
    answer_texts = [a["text"] for a in q["answers"]]
    if len(set(answer_texts)) < 4:
        return False, "Risposte duplicate trovate"
    
    # 4. Spiegazione non ripetitiva
    if len(q["question"]) > 50 and q["question"][:50].lower() in q["explanation"].lower():
        return False, "Spiegazione ripete la domanda"
    
    # 5. Risposta corretta esatta
    correct_answers = [a for a in q["answers"] if a.get("isCorrect")]
    if len(correct_answers) != 1:
        return False, f"Numero errato di risposte corrette: {len(correct_answers)}"
    
    # 6. Errori grammaticali italiani
    italian_errors = {
        "quale è": "qual è",
        "qual'è": "qual è",
        "un'altro": "un altro",
        "un'assassino": "un assassino"
    }
    
    full_text = (question_text + " " + answers_text + " " + explanation_text).lower()
    for wrong, correct in italian_errors.items():
        if wrong in full_text:
            return False, f"Errore italiano: '{wrong}' → usa '{correct}'"
    
    # 7. Parole sospette (possibili allucinazioni)
    suspicious = ["incertezza durante", "bambinastra", "sovrapposte"]
    for word in suspicious:
        if word in full_text:
            logger.warning(f"⚠️  Parola sospetta: '{word}' - possibile allucinazione")
    
    return True, "OK"

async def call_ollama_generate(prompt: str) -> str:
    """Chiamata API Ollama ottimizzata per Qwen 2.5."""
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {
                        "temperature": 0.7,      # Bilanciato
                        "top_p": 0.9,
                        "top_k": 40,
                        "num_predict": 800,      # Spazio sufficiente per qualità
                        "repeat_penalty": 1.1,   # Evita ripetizioni
                    }
                }
            )
            response.raise_for_status()
            data = response.json()
            
            # Qwen 2.5 usa "response"
            text = data.get("response", "").strip()
            
            if not text:
                # Fallback per altri campi
                text = data.get("thinking", "").strip() or data.get("message", {}).get("content", "").strip()
            
            return text
            
    except Exception as e:
        logger.error(f"Ollama API error: {e}")
        return ""

async def _generate_single_question(movie: Dict) -> Optional[Dict]:
    """Genera una singola domanda (interno)."""
    
    # ⭐ PROMPT OTTIMIZZATO PER QWEN 2.5 7B
    prompt = f"""Sei un esperto di cinema. Genera un quiz di ECCELLENTE QUALITÀ in ITALIANO PERFETTO.

FILM: "{movie.get('title')}" ({movie.get('year')})
TRAMA: {(movie.get('description') or 'N/A')[:350]}

REGOLE CRITICHE:
1. Usa SOLO informazioni presenti nella trama (non inventare dettagli!)
2. Domanda chiara e specifica su un evento, personaggio o dettaglio della trama
3. NON chiedere il titolo del film o informazioni troppo ovvie
4. 4 risposte PLAUSIBILI e DIVERSE (basate sulla trama, non generiche)
5. Spiegazione che AGGIUNGE informazioni (non ripete la domanda)
6. Italiano grammaticalmente perfetto: "Qual è" (non "Quale è")
7. Lunghezza ottimale: domanda 30-150 caratteri, risposte 10-80 caratteri
8. Valuta la difficoltà: "easy", "medium", o "hard"

ESEMPIO ECCELLENTE:
{{
  "question": "Perché il protagonista decide di tradire il suo migliore amico?",
  "answers": [
    {{"id": "a", "text": "Per salvare la propria famiglia dalla rovina"}},
    {{"id": "b", "text": "Dopo aver scoperto un segreto del passato"}},
    {{"id": "c", "text": "Costretto da un ricatto dell'antagonista"}},
    {{"id": "d", "text": "Per conquistare l'amore di una donna"}}
  ],
  "correct": "c",
  "explanation": "Il protagonista viene ricattato dall'antagonista con prove compromettenti, costringendolo a tradire il suo amico per proteggere la sua reputazione.",
  "category": "plot",
  "difficulty": "medium"
}}

GENERA ORA IL TUO QUIZ (output SOLO JSON valido, nessun altro testo):
"""

    try:
        response_text = await call_ollama_generate(prompt)
        
        if not response_text:
            logger.error("Empty response from Ollama")
            return None
        
        # Pulizia robusta del JSON
        text = response_text.strip()
        
        # Rimuovi markdown
        text = re.sub(r'```json\s*|\s*```', '', text)
        
        # Rimuovi eventuali prefissi
        text = re.sub(r'^(Okay|Sure|Here|Ecco)[^\{]*', '', text, flags=re.IGNORECASE)
        
        # Trova JSON
        json_match = re.search(r'\{[^{]*"question".*?\}(?=\s*$)', text, re.DOTALL)
        if json_match:
            text = json_match.group(0)
        
        # Valida con Pydantic
        data = QuizQuestionSchema.model_validate_json(text)
        
        # Costruisci risposta
        return {
            "movie_id": movie.get("imdb_id", ""),
            "movie_title": movie.get("title", "Unknown"),
            "movie_year": movie.get("year"),
            "question": data.question,
            "answers": [
                {
                    "id": ans.id.lower(),
                    "text": ans.text,
                    "isCorrect": ans.id.lower() == data.correct.lower()
                } for ans in data.answers
            ],
            "explanation": data.explanation,
            "category": data.category,
            "difficulty": data.difficulty,
            "quiz_date": datetime.now(pytz.timezone('Europe/Rome')).strftime("%Y-%m-%d"),
            "created_at": datetime.now(pytz.timezone('Europe/Rome')),
            "used_count": 0,
            "last_used": None
        }
        
    except Exception as e:
        logger.error(f"Generation error: {e}")
        if 'response_text' in locals():
            logger.debug(f"Raw response: {response_text[:200]}")
        return None

async def generate_quiz_question(movie: Dict, max_retries: int = 5) -> Optional[Dict]:
    """Genera con retry logic."""
    for attempt in range(max_retries):
        q = await _generate_single_question(movie)
        
        if not q:
            logger.warning(f"  Attempt {attempt + 1}/{max_retries}: Generation failed")
            await asyncio.sleep(1)
            continue
        
        is_valid, reason = validate_question(q)
        
        if is_valid:
            logger.info(f"  ✓ Valid question (attempt {attempt + 1})")
            return q
        else:
            logger.warning(f"  Attempt {attempt + 1}/{max_retries}: {reason}")
            await asyncio.sleep(1)
    
    logger.error(f"  ✗ Failed after {max_retries} attempts")
    return None

async def generate_daily_questions(n: int = 5) -> List[Dict]:
    """Genera n domande giornaliere."""
    logger.info(f"Generating {n} questions with {OLLAMA_MODEL}...")
    movies = get_random_movies(n * 3)
    
    questions = []
    for i, movie in enumerate(movies):
        if len(questions) >= n:
            break
            
        logger.info(f"[{i+1}] Processing: {movie.get('title')}...")
        q = await generate_quiz_question(movie)
        
        if q:
            questions.append(q)
    
    return questions

def save_questions_to_db(questions: List[Dict]) -> int:
    """
    Salva nel database con gestione duplicati per data.
    Usa 'quiz_date' e 'movie_id' come chiave unica per evitare doppioni nello stesso giorno.
    """
    if not questions:
        return 0
    
    saved_count = 0
    for q in questions:
        # Upsert: se esiste già una domanda per questo film in questa data, aggiornala
        result = quiz_questions.update_one(
            {
                "quiz_date": q["quiz_date"],
                "movie_id": q["movie_id"] 
            },
            {"$set": q},
            upsert=True
        )
        if result.upserted_id or result.modified_count > 0:
            saved_count += 1
            
    return saved_count

async def run_daily_quiz_generation(force: bool = False):
    """Main task per produzione."""
    ensure_indexes()
    italy_tz = pytz.timezone('Europe/Rome')
    today_str = datetime.now(italy_tz).strftime("%Y-%m-%d")
    
    logger.info(f"🔍 [Quiz] run_daily_quiz_generation called. force={force}, today={today_str}")
    
    # Check status
    current_status = db.quiz_status.find_one({"_id": "daily_generation"})
    
    if current_status:
        # 1. Check if already finished today (and has questions)
        is_done = current_status.get("status") in ["FINISHED", "GENERATED"]
        has_questions = current_status.get("questions_generated", 0) > 0
        
        if not force and current_status.get("last_generated_date") == today_str and is_done and has_questions:
            logger.info(f"✅ [Quiz] Quiz already generated for {today_str} ({current_status.get('questions_generated')} q), skipping.")
            return

        # 2. Check if currently generating
        if current_status.get("status") == "GENERATING":
             # EXTRA CHECK: If we already have 5 questions for today, just mark as FINISHED
            existing_count = quiz_questions.count_documents({"quiz_date": today_str})
            if existing_count >= 5:
                logger.info(f"✅ [Quiz] Found {existing_count} questions despite GENERATING status. Marking as FINISHED.")
                db.quiz_status.update_one(
                    {"_id": "daily_generation"},
                    {
                        "$set": {
                            "status": "FINISHED",
                            "last_generated_date": today_str,
                            "questions_generated": existing_count,
                            "finished_at": datetime.now(italy_tz).isoformat()
                        }
                    },
                    upsert=True
                )
                return

            # Check for stale lock (older than 30 mins)
            started_at_str = current_status.get("started_at", "")
            try:
                started_at = datetime.fromisoformat(started_at_str)
                if (datetime.now(italy_tz) - started_at).total_seconds() > 1800:
                    logger.warning("⚠️ Stale generation lock detected (>30m), resetting...")
                else:
                    logger.warning("⚠️ Quiz generation already in progress, skipping...")
                    return
            except:
                pass
    
    # 1. Update status to GENERATING
    try:
        db.quiz_status.update_one(
            {"_id": "daily_generation"},
            {
                "$set": {
                    "status": "GENERATING",
                    "questions_generated": 0,
                    "error_message": None,
                    "started_at": datetime.now(italy_tz).isoformat()
                }
            },
            upsert=True
        )
        
        questions = await generate_daily_questions(5)
        saved_count = save_questions_to_db(questions)
        
        # 2. Update status to FINISHED with date
        db.quiz_status.update_one(
            {"_id": "daily_generation"},
            {
                "$set": {
                    "status": "FINISHED",
                    "last_generated_date": today_str,
                    "questions_generated": saved_count,
                    "finished_at": datetime.now(italy_tz).isoformat()
                }
            },
            upsert=True
        )
        logger.info(f"✅ Quiz generation completed: {saved_count} questions.")
        
    except Exception as e:
        logger.error(f"Error during quiz generation: {e}")
        # Set status to ERROR so frontend knows something went wrong
        db.quiz_status.update_one(
            {"_id": "daily_generation"},
            {
                "$set": {
                    "status": "ERROR",
                    "error_message": str(e),
                    "finished_at": datetime.now(italy_tz).isoformat()
                }
            },
            upsert=True
        )

def get_daily_questions(n: int = 5) -> List[Dict]:
    """
    Ritorna le domande di oggi.
    Se mancano, cerca le prime disponibili nel futuro (es. domani).
    Se mancano anche quelle, torna lista vuota (segnale per il frontend di mostrare 'Genera').
    """
    italy_tz = pytz.timezone('Europe/Rome')
    today_str = datetime.now(italy_tz).strftime("%Y-%m-%d")
    
    # 1. Cerca quiz di OGGI
    questions = list(quiz_questions.find({"quiz_date": today_str}, {"_id": 0}).limit(n))
    
    if questions:
        return questions

    # 2. Se non ci sono quiz oggi, cerca quiz FUTURI (es. domani)
    # Ordinati per data crescente -> prendi il primo gruppo disponibile (next available day)
    # Nota: questo prende le prime N domande future in assoluto. 
    # Se ci sono 5 domande per domani, prenderà quelle.
    future_questions = list(quiz_questions.find(
        {"quiz_date": {"$gt": today_str}}, 
        {"_id": 0}
    ).sort([("quiz_date", 1), ("created_at", 1)]).limit(n))
    
    if future_questions:
        return future_questions
        
    # 3. Nessun quiz -> Ritorna vuoto per attivare il pulsante "Genera" nel frontend
    return []

def get_questions_count() -> int:
    """Ritorna il numero totale di domande nel DB."""
    return quiz_questions.count_documents({})

if __name__ == "__main__":
    # Test wrapper that uses the same logic as production
    async def test():
        print(f"\n{'='*70}")
        print(f"  🎬 QUIZ GENERATOR - TEST RUN")
        print(f"{'='*70}")
        
        # Use force=True to ensure we can test even if already generated today
        await run_daily_quiz_generation(force=True)
        
    asyncio.run(test())