from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import nltk
from nltk.corpus import stopwords
import json
import logging
import gensim
from gensim import corpora, models
import os
import sys

# Configure logging
logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Initialize Spark with SQL support
spark = SparkSession.builder \
    .appName("NewsTopicAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# NLTK Setup
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# Custom stopwords
stop_words = set(stopwords.words("english"))
stop_words.update({"https", "news"})

# Define UDF for text cleaning
def clean_text(text):
    words = nltk.word_tokenize(text)
    return [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]

clean_text_udf = udf(clean_text, ArrayType(StringType()))

schema = StructType([
    StructField("content", StringType(), True)
])

# Load data using Spark SQL
logger.info("Loading and preprocessing data")
df = spark.read.text("news.json") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(col("data.content").alias("text")) \
    .withColumn("tokens", clean_text_udf(col("text"))) \
    .cache()

# Collect tokens to driver for Gensim processing
logger.info("Preparing data for Gensim")
tokens = df.select("tokens").rdd.flatMap(lambda x: x).collect()

# Build dictionary and corpus using Gensim
dictionary = corpora.Dictionary(tokens)
dictionary.filter_extremes(no_below=5, no_above=0.5)
corpus = [dictionary.doc2bow(text) for text in tokens]

# Train LDA model
logger.info("Training LDA model")
lda_model = models.LdaModel(
    corpus=corpus,
    id2word=dictionary,
    num_topics=20,
    alpha="auto",
    per_word_topics=True,
    chunksize=10000,
    update_every=1,
    passes=1
)

# Define UDFs for topic processing
def get_dominant_topic(bow):
    if not bow:  
        return -1
    if isinstance(bow, dict):
        bow = list(bow.items())  # Convert dict back to list of tuples if needed
    topics = lda_model.get_document_topics(bow)
    if topics:
        return max(topics, key=lambda x: x[1])[0]
    return -1

def get_topic_words(topic_id):
    return ' '.join([word for word, _ in lda_model.show_topic(topic_id)])

get_dominant_topic_udf = udf(get_dominant_topic, IntegerType())
get_topic_words_udf = udf(get_topic_words, StringType())

# Convert corpus to DataFrame
corpus_df = spark.createDataFrame(
    [(i, bow) for i, bow in enumerate(corpus)],  
    ["doc_id", "bow"]
)

# Add explicit ID columns to both DataFrames before joining
df = df.withColumn("row_id", monotonically_increasing_id())
corpus_df = corpus_df.withColumn("row_id", monotonically_increasing_id())

# Join with original data using the explicit IDs
processed_df = df.join(corpus_df, "row_id")

# Get dominant topics
result_df = processed_df \
    .withColumn("topic_id", get_dominant_topic_udf(col("bow"))) \
    .filter(col("topic_id") >= 0) \
    .withColumn("topic_words", get_topic_words_udf(col("topic_id"))) \
    .groupBy("topic_words") \
    .count() \
    .orderBy("count", ascending=False)

logger.info("Saving results")

results = result_df.collect()

output_path = "output.txt"
with open(output_path, 'w') as f:
    for row in results:
        f.write(f"{row['topic_words']}\t{row['count']}\n")

logger.info(f"Results saved to {output_path}")
spark.stop()