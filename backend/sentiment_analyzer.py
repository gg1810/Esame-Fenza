from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch

# Modello RoBERTa ottimizzato per il sentiment
MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"

class SentimentAnalyzer:
    def __init__(self):
        print("Caricamento modello RoBERTa... Potrebbe volerci un po' la prima volta.")
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL)
        print("Modello caricato con successo!")

    def analyze(self, text: str):
        """
        Analizza un testo e restituisce i punteggi di Negative, Neutral, Positive.
        """
        if not text:
            return {"negative": 0, "neutral": 1, "positive": 0}

        # Pre-processing (tronca se troppo lungo per il modello)
        encoded_input = self.tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
        
        with torch.no_state():
            output = self.model(**encoded_input)
        
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)
        
        return {
            "negative": float(scores[0]),
            "neutral": float(scores[1]),
            "positive": float(scores[2])
        }

    def get_aggregate_sentiment(self, comments: list):
        """
        Analizza una lista di commenti e restituisce una media.
        """
        if not comments:
            return 0
            
        total_positive = 0
        for comment in comments:
            res = self.analyze(comment)
            total_positive += res['positive']
            
        return (total_positive / len(comments)) * 100
