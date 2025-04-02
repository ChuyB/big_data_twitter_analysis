from pyspark.sql import SparkSession

SparkSession.builder.master("local[*]").getOrCreate().stop()

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Cargar el archivo JSON y las palabras
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets AS
SELECT content, user.username AS username, likeCount, retweetCount, 
       place.country AS country, place.fullName AS fullName
FROM json.`twitter/us_news_tweets.json`
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW vio_keywords AS
SELECT value
FROM text.`vio_keywords.txt`
""")

# Mostrar los datos de la vista 'tweets'
spark.sql("SELECT * FROM tweets").show(10)  # Muestra las primeras 10 filas sin truncar el contenido

# Mostrar los datos de la vista 'vio_keywords'
spark.sql("SELECT * FROM vio_keywords").show(10)  # Muestra las primeras 10 filas sin truncar el contenido

# Filtrar por país
spark.sql("""
CREATE OR REPLACE TEMP VIEW paises_tweets AS
SELECT *
FROM tweets
WHERE country = 'United States'
""")
spark.sql("SELECT * FROM paises_tweets").show(10) # Mostrar resultado

# Realizar el producto cruz y buscar palabras clave en los tweets
spark.sql("""
CREATE OR REPLACE TEMP VIEW producto_cruz AS
SELECT *
FROM paises_tweets
CROSS JOIN vio_keywords
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_violentos AS
SELECT *
FROM producto_cruz
WHERE LOWER(content) LIKE CONCAT('%', value, '%')
""")
# Mostrar 10 primeros resultados
spark.sql("SELECT * FROM producto_cruz").show(10)

# Extraer el estado y eliminar columnas innecesarias
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_estados AS
SELECT SPLIT(fullName, ', ')[1] AS state, content, username, likeCount, retweetCount
FROM tweets_violentos
""")# Agrupar por estado y contar los tweets
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_agrupados AS
SELECT state, COLLECT_LIST(struct(content, username, likeCount, retweetCount)) AS tweets
FROM tweets_estados
GROUP BY state
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW num_tweets AS
SELECT state, SIZE(tweets) AS cantidad_tweets, tweets
FROM tweets_agrupados
""")

# Reorganizar las columnas y ordenar los resultados
resultados = spark.sql("""
SELECT state, cantidad_tweets, 
       TRANSFORM(tweets, x -> struct(x.content, x.username, x.likeCount, x.retweetCount)) AS tweets
FROM num_tweets
ORDER BY cantidad_tweets DESC
""")

# Mostrar los primeros 10 resultados
resultados.show(10)

# Guardar los resultados en un archivo JSON
resultados.write.json("salida_problema3_spark_sql")
