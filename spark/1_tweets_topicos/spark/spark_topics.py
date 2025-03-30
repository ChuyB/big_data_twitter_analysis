from pyspark import SparkContext
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

def process_partition(partition):
    """Process each partition to get topic distributions"""
    # Load the LDA model once per partition
    lda_model = models.LdaModel.load("lda.model")
    
    for text in partition:
        cleaned = clean_text(text)
        bow = dictionary.doc2bow(cleaned)
        topics = lda_model.get_document_topics(bow)
        if topics:
            dominant_topic = max(topics, key=lambda x: x[1])
            topic_words = [word for word, _ in lda_model.show_topic(dominant_topic[0])]
            yield ' '.join(topic_words)

if __name__ == "__main__":
    sc = SparkContext(appName="TopicModeling")
    
    # Read input files
    lines = sc.textFile("input/*.json")  # Adjust path as needed
    
    # Parse JSON and extract content
    documents = lines.map(lambda x: json.loads(x)) \
                     .map(lambda x: x.get("content", ""))
    
    # Build dictionary (requires collecting all texts)
    all_texts = documents.map(clean_text).collect()
    dictionary = corpora.Dictionary(all_texts)
    
    # Process documents and get dominant topics
    topics = documents.mapPartitions(process_partition)
    
    # Count topics and sort by frequency
    topic_counts = topics.map(lambda x: (x, 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda x: x[1], ascending=False)
    
    # Save results
    topic_counts.saveAsTextFile("output/topic_counts")
    
    sc.stop()