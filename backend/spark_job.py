from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col

def run_recommendation_job():
    # Crea la sessione Spark con il connettore per MongoDB
    spark = SparkSession.builder \
        .appName("CineMatchRecommendationEngine") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/cinematch_db.users") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/cinematch_db.recommendations") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    # 1. Carica i dati da MongoDB
    # (In un caso reale caricheresti i rating di tutti gli utenti)
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    
    # 2. Pre-processing
    # ALS richiede ID numerici per utenti e film. Dovrai mapparli.
    # ratings = df.select("user_numeric_id", "movie_numeric_id", "rating")

    # 3. Addestramento del modello (Alternating Least Squares)
    # als = ALS(maxIter=10, regParam=0.01, userCol="user_numeric_id", itemCol="movie_numeric_id", ratingCol="rating")
    # model = als.fit(ratings)

    # 4. Genera raccomandazioni (es: top 10 per ogni utente)
    # userRecs = model.recommendForAllUsers(10)

    # 5. Salva i risultati su MongoDB
    # userRecs.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
    
    print("Job Spark completato con successo (Placeholder)")
    spark.stop()

if __name__ == "__main__":
    run_recommendation_job()
