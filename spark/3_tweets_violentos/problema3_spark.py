from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower
from pyspark.sql.functions import split
from pyspark.sql.functions import collect_set
from pyspark.sql.functions import struct
from pyspark.sql.functions import size
from pyspark.sql.functions import expr
from pyspark.sql.functions import desc

#spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark = SparkSession.builder.appName("problema_3").getOrCreate()

# Cargar el archivo JSON y las palabras
df = spark.read.json("twitter/us_news_tweets.json")
vio_keywords = spark.read.text("vio_keywords.txt")

# Tomar lo que queremos de los tweets
tweets = df.select("content","user.username","likeCount","retweetCount","place.country","place.fullName")

# Clasificar por pais
paises_tweets = tweets.filter(tweets["country"] == "United States")

# Realizar el producto cruzado para luego ver si las palabras estan en los tweets
producto_cruz = paises_tweets.crossJoin(vio_keywords)
tweets_violentos = producto_cruz.filter(lower(col("content")).contains(col("value")))

# Extraer el estado y eliminamos columnas innecesarias
tweets_estados = tweets_violentos.withColumn("state", split(tweets_violentos["fullName"], ", ")[1])
tweets_estados = tweets_estados.drop("country").drop("fullName").drop("value")

# Agrupamos por estado sin eliminar los tweets del grupo y los contamos
tweets_agrupados = tweets_estados.groupBy("state").agg(collect_set(struct("*")).alias("tweets"))
num_tweets = tweets_agrupados.withColumn("cantidad_tweets", size("tweets"))

# Para mostrar mejor los resultados, eliminamos los states de cada tweet (ya que ya esta en el grupo) y reorganizamos las columnas
tweets_no_state = num_tweets.withColumn(
    "tweets",
    expr("transform(tweets, x -> struct(x.content as content, x.username as username, x.likeCount as likeCount, x.retweetCount as retweetCount))")
)
reordenamiento_cols = tweets_no_state.select("state", "cantidad_tweets", "tweets")

# Ordenamos por cantidad_tweets
tweets_ordenados = reordenamiento_cols.orderBy(desc("cantidad_tweets"))

# Guardamos los resultados
tweets_ordenados.write.json("salida_problema3_spark")

