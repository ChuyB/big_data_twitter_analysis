from pyspark.sql import SparkSession

# Crear la SparkSession
spark = SparkSession.builder.appName("RestaurantSentimentAnalysis").getOrCreate()

# Crear una vista temporal para los tweets
spark.sql("""
CREATE OR REPLACE TEMP VIEW tweets AS
SELECT content
FROM json.`data/us_news_tweets.json`
""")

# Crear una vista temporal para los restaurantes
spark.sql("""
CREATE OR REPLACE TEMP VIEW restaurants AS
SELECT 'kfc' AS restaurant
UNION ALL
SELECT 'mcdonald''s'
UNION ALL
SELECT 'starbucks'
""")

# Crear una vista temporal para palabras positivas
spark.sql("""
CREATE OR REPLACE TEMP VIEW positive_words AS
SELECT 'love' AS word
UNION ALL
SELECT 'great'
UNION ALL
SELECT 'awesome'
UNION ALL
SELECT 'amazing'
UNION ALL
SELECT 'good'
UNION ALL
SELECT 'happy'
""")

# Crear una vista temporal para palabras negativas
spark.sql("""
CREATE OR REPLACE TEMP VIEW negative_words AS
SELECT 'hate' AS word
UNION ALL
SELECT 'bad'
UNION ALL
SELECT 'terrible'
UNION ALL
SELECT 'awful'
UNION ALL
SELECT 'horrible'
UNION ALL
SELECT 'sad'
""")

# Identificar menciones de restaurantes en los tweets
spark.sql("""
CREATE OR REPLACE TEMP VIEW mentions AS
SELECT t.content, r.restaurant
FROM tweets t
CROSS JOIN restaurants r
WHERE LOWER(t.content) LIKE CONCAT('%', LOWER(r.restaurant), '%')
""")

# Analizar sentimientos
spark.sql("""
CREATE OR REPLACE TEMP VIEW sentiment_analysis AS
SELECT m.restaurant,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM positive_words p
            WHERE LOWER(m.content) LIKE CONCAT('%', LOWER(p.word), '%')
        ) THEN 'positive'
        WHEN EXISTS (
            SELECT 1 FROM negative_words n
            WHERE LOWER(m.content) LIKE CONCAT('%', LOWER(n.word), '%')
        ) THEN 'negative'
        ELSE 'neutral'
    END AS sentiment
FROM mentions m
""")

# Contar el número de sentimientos por restaurante
spark.sql("""
CREATE OR REPLACE TEMP VIEW sentiment_counts AS
SELECT restaurant, sentiment, COUNT(*) AS count
FROM sentiment_analysis
GROUP BY restaurant, sentiment
""")

# Generar combinaciones completas de restaurantes y sentimientos
spark.sql("""
CREATE OR REPLACE TEMP VIEW all_combinations AS
SELECT r.restaurant, s.sentiment
FROM restaurants r
CROSS JOIN (
    SELECT 'positive' AS sentiment
    UNION ALL
    SELECT 'negative'
    UNION ALL
    SELECT 'neutral'
) s
""")

# Completar combinaciones faltantes con un conteo de 0
spark.sql("""
CREATE OR REPLACE TEMP VIEW complete_results AS
SELECT ac.restaurant, ac.sentiment, COALESCE(sc.count, 0) AS count
FROM all_combinations ac
LEFT JOIN sentiment_counts sc
ON ac.restaurant = sc.restaurant AND ac.sentiment = sc.sentiment
ORDER BY ac.restaurant, ac.sentiment
""")

# Mostrar los resultados finales
spark.sql("SELECT * FROM complete_results").show()

# Guardar los resultados en un archivo
spark.sql("SELECT * FROM complete_results").write.csv("output_sentiment_analysis_results.csv", header=True, mode="overwrite")

# Finalizar sesión de Spark
spark.stop()
