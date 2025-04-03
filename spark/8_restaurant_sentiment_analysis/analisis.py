from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, when, explode, array, coalesce, lit, when, count

# Inicializar sesión de Spark
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Restaurantes a analizar
restaurants = ["kfc", "mcdonald's", "starbucks"]

# Palabras clave para análisis de sentimiento
positive_words = {"love", "great", "awesome", "amazing", "good", "happy"}
negative_words = {"hate", "bad", "terrible", "awful", "horrible", "sad"}

# Cargar datos desde un archivo JSON
df = spark.read.json("data/us_news_tweets.json")

# Convertir contenido del tweet a minúsculas
df = df.withColumn("content", lower(col("content")))

# Identificar múltiples restaurantes mencionados en un mismo tweet
df = df.withColumn("restaurant_array", array([when(col("content").contains(r), r) for r in restaurants]))
df = df.withColumn("restaurant", explode(df.restaurant_array))

# Filtrar los tweets que contienen al menos un restaurante
df = df.filter(col("restaurant").isNotNull())

# Analizar sentimiento basado en palabras clave
df = df.withColumn(
    "sentiment",
    when(col("content").rlike("|".join(positive_words)), "positive")
    .when(col("content").rlike("|".join(negative_words)), "negative")
    .otherwise("neutral")
)

# Contar sentimientos por restaurante
df_filtered = df.groupBy("restaurant", "sentiment").agg(count("*").alias("count"))

# Ordenar los resultados por restaurante y sentimiento
df_filtered = df_filtered.orderBy("restaurant", "sentiment")

# Mostrar resultados
df_filtered.show()

# Guardar resultados si es necesario
df_filtered.write.csv("sentiment_analysis_results.csv", header=True, mode="overwrite")

# Finalizar sesión de Spark
spark.stop()