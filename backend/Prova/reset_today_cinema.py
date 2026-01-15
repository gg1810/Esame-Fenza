from pymongo import MongoClient
import os
from datetime import datetime
import pytz

# Configurazione (allineata a main.py)
MONGODB_URL = "mongodb://localhost:27017"
DB_NAME = "cinematch_db"

def reset_today():
    try:
        client = MongoClient(MONGODB_URL)
        db = client[DB_NAME]
        showtimes_collection = db["showtimes"]
        
        # Data di oggi (simulata o reale)
        italy_tz = pytz.timezone("Europe/Rome")
        today = datetime.now(italy_tz).strftime("%Y-%m-%d")
        
        print(f"🔄 Rimozione programmazione per la data: {today}")
        
        # Recupera tutti i documenti
        cursor = showtimes_collection.find({})
        count = 0
        
        # Itera e rimuovi la data specifica dalle regioni
        for doc in cursor:
            updated = False
            regions = doc.get("regions", {})
            
            for province in list(regions.keys()):
                dates = regions[province].get("dates", {})
                # DEBUG: Stampa le date trovate per i primi 5 film
                if count < 5:
                    print(f"  [DEBUG] Film {doc.get('film_title', 'Unknown')} - Provincia {province} - Date disponibili: {list(dates.keys())}")
                
                if today in dates:
                    print(f"  - Rimuovo {today} per film id: {doc.get('film_id')} (Provincia: {province})")
                    del dates[today]
                    updated = True
            
            if updated:
                showtimes_collection.replace_one({"_id": doc["_id"]}, doc)
                count += 1
                
        print(f"✅ Completato! Rimossi orari di oggi da {count} film.")
        
    except Exception as e:
        print(f"❌ Errore: {e}")

if __name__ == "__main__":
    reset_today()
