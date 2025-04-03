from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, when

# Inicia SparkSession
spark = SparkSession.builder \
    .appName("Sentiment Analysis with Spark") \
    .getOrCreate()

# Carga el dataset de tweets
tweets_path = "/home/alejandro/Documents/USB/pregrado/big-data/data/us_news_tweets.json"
tweets_df = spark.read.json(tweets_path)

# Extrae los campos necesarios: ID y texto del tweet
extract_details_df = tweets_df.select(
    col("id").alias("tweet_id"),
    col("content").alias("tweet_content")
)

# Limita el número de registros (opcional para pruebas)
small_extract_details_df = extract_details_df.limit(10)

# Divide el texto en palabras (tokenización)
tokens_df = small_extract_details_df.withColumn("word", explode(split(col("tweet_content"), "\\s+")))

# Carga el léxico AFINN con las palabras y sus calificaciones
afinn_path = "/home/thebug-code/Documents/big-data/AFINN/AFINN-111.txt"
afinn_df = spark.read.csv(afinn_path, sep="\t", inferSchema=True, header=False).toDF("word", "rating")

# Une los tokens de los tweets con el léxico para asignar calificaciones
word_rating_df = tokens_df.join(afinn_df, tokens_df.word == afinn_df.word, how="left_outer")

# Extrae ID del tweet, texto y la calificación de las palabras
rating_df = word_rating_df.select(
    col("tweet_id"),
    col("tweet_content"),
    col("rating").alias("word_rating")
)

# Agrupa las calificaciones por tweet
word_group_df = rating_df.groupBy("tweet_id", "tweet_content") \
    .agg(avg("word_rating").alias("tweet_rating"))

# Clasifica los tweets en positivos y negativos
classified_tweets_df = word_group_df.withColumn(
    "sentiment",
    when(col("tweet_rating") >= 0, "positive").otherwise("negative")
)

# Almacena los resultados en archivos separados
positive_tweets_df = classified_tweets_df.filter(col("sentiment") == "positive")
negative_tweets_df = classified_tweets_df.filter(col("sentiment") == "negative")

positive_tweets_output = "/home/thebug-code/Documents/big-data/output/positive_tweets"
negative_tweets_output = "/home/thebug-code/Documents/big-data/output/negative_tweets"

positive_tweets_df.write.csv(positive_tweets_output, header=True, mode="overwrite")
negative_tweets_df.write.csv(negative_tweets_output, header=True, mode="overwrite")

# Finaliza SparkSession
spark.stop()

