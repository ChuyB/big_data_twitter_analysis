from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, when

# Iniciar SparkSession
spark = SparkSession.builder \
    .appName("Sentiment Analysis with Spark") \
    .getOrCreate()

# 1. Cargar el dataset de tweets (archivo JSON)
tweets_path = "/home/thebug-code/Documents/big-data/data/us_news_tweets.json"
tweets_df = spark.read.json(tweets_path)

# 2. Extraer los campos necesarios: ID y texto del tweet
extract_details_df = tweets_df.select(
    col("id").alias("tweet_id"),
    col("content").alias("tweet_content")
)

# Limitar el número de registros (opcional, para pruebas)
small_extract_details_df = extract_details_df.limit(10)

# 3. Dividir el texto en palabras (tokenización)
tokens_df = small_extract_details_df.withColumn("word", explode(split(col("tweet_content"), "\\s+")))

# 4. Cargar el léxico AFINN con las palabras y sus calificaciones
afinn_path = "/home/thebug-code/Documents/big-data/AFINN/AFINN-111.txt"
afinn_df = spark.read.csv(afinn_path, sep="\t", inferSchema=True, header=False).toDF("word", "rating")

# 5. Unir tokens de los tweets con el léxico para asignar calificaciones
word_rating_df = tokens_df.join(afinn_df, tokens_df.word == afinn_df.word, how="left_outer")

# 6. Extraer ID del tweet, texto y la calificación de las palabras
rating_df = word_rating_df.select(
    col("tweet_id"),
    col("tweet_content"),
    col("rating").alias("word_rating")
)

# 7. Agrupar las calificaciones por tweet
word_group_df = rating_df.groupBy("tweet_id", "tweet_content") \
    .agg(avg("word_rating").alias("tweet_rating"))

# 8. Clasificar tweets en positivos y negativos
classified_tweets_df = word_group_df.withColumn(
    "sentiment",
    when(col("tweet_rating") >= 0, "positive").otherwise("negative")
)

# 9. Almacenar los resultados en archivos separados
positive_tweets_df = classified_tweets_df.filter(col("sentiment") == "positive")
negative_tweets_df = classified_tweets_df.filter(col("sentiment") == "negative")

positive_tweets_output = "/home/thebug-code/Documents/big-data/output/positive_tweets"
negative_tweets_output = "/home/thebug-code/Documents/big-data/output/negative_tweets"

positive_tweets_df.write.csv(positive_tweets_output, header=True, mode="overwrite")
negative_tweets_df.write.csv(negative_tweets_output, header=True, mode="overwrite")

# Finalizar SparkSession
spark.stop()

