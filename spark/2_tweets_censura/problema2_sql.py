from pyspark.sql import SparkSession

# Crear la SparkSession conectando al maestro remoto
spark = SparkSession.builder.appName("problema2_sql").getOrCreate()

# Cargar el archivo JSON y las palabras clave directamente como vistas SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets AS
SELECT content, user.username AS username, likeCount, retweetCount
FROM json.`twitter/us_news_tweets.json`
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW cat_keywords AS
SELECT value
FROM text.`cat_keywords.txt`
""")

# Mostrar los contenidos a valuar (tweets y palabras claves).
spark.sql("SELECT * FROM tweets").show(5)
spark.sql("SELECT * FROM cat_keywords").show(5)

# Realizar el producto cruzado y buscar las palabras clave en los tweets
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_gatos AS
SELECT t.content, t.username, t.likeCount, t.retweetCount
FROM tweets t
CROSS JOIN cat_keywords c
WHERE LOWER(t.content) LIKE CONCAT('%', LOWER(c.value), '%')
""")

spark.sql("SELECT * FROM tweets_gatos").show(5)

# Eliminar los tweets de gatos del conjunto principal
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_no_gatos AS
SELECT t.*
FROM tweets t
EXCEPT
SELECT tg.*
FROM tweets_gatos tg
""")

spark.sql("SELECT * FROM tweets_no_gatos").show(5)

# Guardar los resultados
spark.sql("SELECT * FROM tweets_gatos").write.json("salida_problema2_gatos_sql")
spark.sql("SELECT * FROM tweets_no_gatos").write.json("salida_problema2_no_gatos_sql")
# Guardamos contador de numero de tweets de gatos
spark.sql("SELECT COUNT(*) AS numero_sql FROM tweets_gatos").write.json("conteo/salida_problema2_num_sql")

# Cerramos la sesion
spark.stop()