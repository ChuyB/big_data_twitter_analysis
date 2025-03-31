from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower

#spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark = SparkSession.builder.appName("problema_4").getOrCreate()

# Cargar el archivo JSON y las palabras claves
df = spark.read.json("twitter/us_news_tweets.json")
pro_keywords = spark.read.text("pro_vax_keywords.txt")
anti_keywords = spark.read.text("anti_vax_keywords.txt")

# Tomar lo que queremos de los tweets
tweets = df.select("content","user.username","likeCount","retweetCount")

# Realizar el producto cruzado para luego ver si las palabras estan en los tweets
producto_cruz_pro = tweets.crossJoin(pro_keywords)
tweets_pro_vax = producto_cruz_pro.filter(lower(col("content")).contains(col("value")))
producto_cruz_anti = tweets.crossJoin(anti_keywords)
tweets_anti_vax = producto_cruz_anti.filter(lower(col("content")).contains(col("value")))

# Guardamos los resultados
tweets_pro_vax.write.json("salida_problema4_provax_spark")
tweets_anti_vax.write.json("salida_problema4_antivax_spark")

# Guardamos contadores de tweets pro y anti vacunas
numero_de_tweets_pro = tweets_pro_vax.count()
numero = [(numero_de_tweets_pro,)]  # El entero a guardar
columns = ["numero"]
num = spark.createDataFrame(numero, columns)
num.write.json("conteo/salida_problema4_num_pro_spark")

numero_de_tweets_anti = tweets_anti_vax.count()
numero = [(numero_de_tweets_anti,)]  # El entero a guardar
columns = ["numero"]
num = spark.createDataFrame(numero, columns)
num.write.json("conteo/salida_problema4_num_anti_spark")