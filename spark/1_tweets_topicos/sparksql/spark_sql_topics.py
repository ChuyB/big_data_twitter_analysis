from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import json
import nltk
from gensim import corpora, models
import logging

# Set up logging
logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO
)

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

# Stopwords (without "https" and "news")
stop_words = set(nltk.corpus.stopwords.words("english"))
stop_words.update({"https", "news"})

def clean_text(text):
    """Tokenize and clean text"""
    words = nltk.word_tokenize(text)
    return [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]

# Register UDFs
clean_text_udf = udf(clean_text, ArrayType(StringType()))

def get_dominant_topic(text, dictionary, lda_model):
    """Get dominant topic for a text"""
    cleaned = clean_text(text)
    bow = dictionary.doc2bow(cleaned)
    topics = lda_model.get_document_topics(bow)
    if topics:
        dominant_topic = max(topics, key=lambda x: x[1])
        topic_words = [word for word, _ in lda_model.show_topic(dominant_topic[0])]
        return ' '.join(topic_words)
    return None

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TopicModelingSQL") \
        .getOrCreate()

    # Read input JSON files
    df = spark.read.json("input/*.json")  # Adjust path as needed
    
    # Build dictionary (collect to driver)
    dictionary = corpora.Dictionary(df.select("content").rdd
                                  .flatMap(lambda x: clean_text(x['content']))
                                  .collect())
    
    # Load LDA model
    lda_model = models.LdaModel.load("lda.model")
    
    # Broadcast dictionary and model to workers
    dictionary_bc = spark.sparkContext.broadcast(dictionary)
    lda_model_bc = spark.sparkContext.broadcast(lda_model)
    
    # Register UDF for topic extraction
    def get_topic_udf(text):
        return get_dominant_topic(text, dictionary_bc.value, lda_model_bc.value)
    
    spark.udf.register("get_topic", get_topic_udf)
    
    # Process documents and get dominant topics
    result_df = df.withColumn("topic", get_topic_udf(df["content"]))
    
    # Count topics and sort by frequency
    topic_counts = result_df.groupBy("topic") \
                          .count() \
                          .orderBy("count", ascending=False)
    
    # Show results
    topic_counts.show(truncate=False)
    
    # Save results
    topic_counts.write.mode("overwrite").csv("output/topic_counts")
    
    spark.stop()