from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

SparkSession.builder.master("local[*]").getOrCreate().stop()
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Cargar el archivo JSON y las palabras claves
df = spark.read.json("twitter/us_news_tweets.json")
pro_keywords = spark.read.text("pro_vax_keywords.txt")
anti_keywords = spark.read.text("anti_vax_keywords.txt")

# Tomar lo que queremos de los tweets
tweets = df.select("content","user.username","likeCount","retweetCount")

# Realizar el producto cruz para luego ver si las palabras estan en los tweets
producto_cruz_pro = tweets.crossJoin(pro_keywords)
tweets_pro_vax = producto_cruz_pro.filter(lower(col("content")).contains(col("value")))
producto_cruz_anti = tweets.crossJoin(anti_keywords)
tweets_anti_vax = producto_cruz_anti.filter(lower(col("content")).contains(col("value")))

# Guardamos contadores de tweets pro y anti vacunas
numero_de_tweets_pro = tweets_pro_vax.count()
numero_de_tweets_anti = tweets_anti_vax.count()
# Definir el esquema del DataFrame
esquema = StructType([
    StructField("tipo", StringType(), True),
    StructField("numero", IntegerType(), True)
])
# Dataframe contadores
datos = [("pro-vacunas", numero_de_tweets_pro), ("anti-vacunas", numero_de_tweets_anti)]
conteo = spark.createDataFrame(datos, esquema)

usuarios_anti = tweets_anti_vax.groupBy("username").count().orderBy(col("count").desc())
usuarios_pro = tweets_pro_vax.groupBy("username").count().orderBy(col("count").desc())

# Guardamos los resultados
tweets_pro_vax.write.json("salida_problema4_provax_spark")
tweets_anti_vax.write.json("salida_problema4_antivax_spark")
conteo.write.json("conteo/total_tweets")
usuarios_anti.write.json("conteo/usuarios_anti")
usuarios_pro.write.json("conteo/usuarios_pro")