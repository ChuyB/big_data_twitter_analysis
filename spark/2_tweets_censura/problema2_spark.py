from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower

#spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark = SparkSession.builder.appName("problema_2").getOrCreate()

# Cargar el archivo JSON y las palabras
df = spark.read.json("twitter/us_news_tweets.json")
cat_keywords = spark.read.text("cat_keywords.txt")

# Tomar lo que queremos de los tweets
tweets = df.select("content","user.username","likeCount","retweetCount")

# Realizar el producto cruzado para luego ver si las palabras estan en los tweets
producto_cruz = tweets.crossJoin(cat_keywords)
tweets_gatos = producto_cruz.filter(lower(col("content")).contains(col("value")))
tweets_gatos = tweets_gatos.drop("value")

# Eliminar los tweets de gatos del conjunto principal
diferencia = tweets.subtract(tweets_gatos)

# Guardamos los resultados
# Guardamos los tweets relacionados a temas de gatos que se eliminaron
tweets_gatos.write.json("salida_problema2_gatos_spark")
# Guardamos el numero de tweets eliminados
numero_de_tweets = tweets_gatos.count()
numero = [(numero_de_tweets,)]  # El entero a guardar
columns = ["numero"]
num = spark.createDataFrame(numero, columns)
num.write.json("conteo/salida_problema2_num_spark")

# Guardamos el archivo principal resultante (no tiene tweets de gatos)
diferencia.write.json("salida_problema2_no_gatos_spark")

