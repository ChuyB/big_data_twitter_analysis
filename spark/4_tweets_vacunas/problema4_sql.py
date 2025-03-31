from pyspark.sql import SparkSession

# Crear la SparkSession
spark = SparkSession.builder.appName("problema4_sql").getOrCreate()

# Cargar los datos directamente como vistas SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets AS
SELECT content, user.username AS username, likeCount, retweetCount
FROM json.`twitter/us_news_tweets.json`
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW pro_keywords AS
SELECT value
FROM text.`pro_vax_keywords.txt`
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW anti_keywords AS
SELECT value
FROM text.`anti_vax_keywords.txt`
""")

# Mostramos los contenidos a evaluar (tweets y palabras claves).
spark.sql("SELECT * FROM tweets").show(5)
spark.sql("SELECT * FROM pro_keywords").show(5)
spark.sql("SELECT * FROM anti_keywords").show(5)


# Realizar el producto cruzado y filtrar los tweets relacionados con pro-vax
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_pro_vax AS
SELECT t.content, t.username, t.likeCount, t.retweetCount
FROM tweets t
CROSS JOIN pro_keywords pk
WHERE LOWER(t.content) LIKE CONCAT('%', LOWER(pk.value), '%')
""")

spark.sql("SELECT * FROM tweets_pro_vax").show(5)

# Realizar el producto cruzado y filtrar los tweets relacionados con anti-vax
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets_anti_vax AS
SELECT t.content, t.username, t.likeCount, t.retweetCount
FROM tweets t
CROSS JOIN anti_keywords ak
WHERE LOWER(t.content) LIKE CONCAT('%', LOWER(ak.value), '%')
""")

spark.sql("SELECT * FROM tweets_anti_vax").show(5)

# Guardar los resultados en archivos JSON
spark.sql("SELECT * FROM tweets_pro_vax").write.json("salida_problema4_provax_sql")
spark.sql("SELECT * FROM tweets_anti_vax").write.json("salida_problema4_antivax_sql")
# Guardamos los contadores de tweets pro y anti vacunas
spark.sql("SELECT COUNT(*) AS num_pro_sql FROM tweets_pro_vax").write.json("conteo/salida_problema4_num_pro_sql")
spark.sql("SELECT COUNT(*) AS num_anti_sql FROM tweets_anti_vax").write.json("conteo/salida_problema4_num_anti_sql")

# Cerramos la sesion
spark.stop()