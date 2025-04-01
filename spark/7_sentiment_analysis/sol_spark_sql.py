from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Iniciar SparkSession
spark = SparkSession.builder \
    .appName("Sentiment Analysis with Spark SQL") \
    .getOrCreate()

# Cargar el dataset de tweets
tweets_path = "/home/alejandro/Documents/USB/pregrado/big-data/data/us_news_tweets.json"
tweets_df = spark.read.json(tweets_path)
tweets_df.createOrReplaceTempView("tweets")  # Registrar como tabla temporal

# 2. Extraer los campos necesarios: ID y texto del tweet
extract_query = """
SELECT id AS tweet_id, content AS tweet_content
FROM tweets
"""
extract_details_df = spark.sql(extract_query)
extract_details_df.createOrReplaceTempView("extract_details")

# Limitar la cantidad de registros (opcional para pruebas)
small_extract_query = """
SELECT *
FROM extract_details
LIMIT 10
"""
small_extract_details_df = spark.sql(small_extract_query)
small_extract_details_df.createOrReplaceTempView("small_extract_details")

# Dividir el texto en palabras (tokenización)
tokens_query = """
SELECT 
    tweet_id, 
    tweet_content, 
    EXPLODE(SPLIT(tweet_content, '\\s+')) AS word
FROM small_extract_details
"""
tokens_df = spark.sql(tokens_query)
tokens_df.createOrReplaceTempView("tokens")

# Cargar el léxico AFINN
afinn_path = "/home/thebug-code/Documents/big-data/AFINN/AFINN-111.txt"
afinn_df = spark.read.csv(afinn_path, sep="\t", inferSchema=True, header=False).toDF("word", "rating")
afinn_df.createOrReplaceTempView("afinn")

# Unir tokens con el léxico para asignar calificaciones
word_rating_query = """
SELECT 
    t.tweet_id, 
    t.tweet_content, 
    t.word, 
    a.rating AS word_rating
FROM tokens t
LEFT JOIN afinn a
ON t.word = a.word
"""
word_rating_df = spark.sql(word_rating_query)
word_rating_df.createOrReplaceTempView("word_rating")

# Agrupar las calificaciones por cada tweet y calcular el promedio
avg_rate_query = """
SELECT 
    tweet_id, 
    tweet_content, 
    AVG(word_rating) AS tweet_rating
FROM word_rating
GROUP BY tweet_id, tweet_content
"""
avg_rate_df = spark.sql(avg_rate_query)
avg_rate_df.createOrReplaceTempView("avg_rate")

# Clasificar tweets en positivos y negativos
classified_tweets_query = """
SELECT 
    tweet_id, 
    tweet_content, 
    tweet_rating, 
    CASE 
        WHEN tweet_rating >= 0 THEN 'positive'
        ELSE 'negative'
    END AS sentiment
FROM avg_rate
"""
classified_tweets_df = spark.sql(classified_tweets_query)
classified_tweets_df.createOrReplaceTempView("classified_tweets")

# Almacenar los resultados en archivos separados
positive_tweets_query = """
SELECT *
FROM classified_tweets
WHERE sentiment = 'positive'
"""
positive_tweets_df = spark.sql(positive_tweets_query)
positive_tweets_output = "/home/thebug-code/Documents/big-data/output/positive_tweets"
positive_tweets_df.write.csv(positive_tweets_output, header=True, mode="overwrite")

negative_tweets_query = """
SELECT *
FROM classified_tweets
WHERE sentiment = 'negative'
"""
negative_tweets_df = spark.sql(negative_tweets_query)
negative_tweets_output = "/home/thebug-code/Documents/big-data/output/negative_tweets"
negative_tweets_df.write.csv(negative_tweets_output, header=True, mode="overwrite")

# Finalizar SparkSession
spark.stop()
