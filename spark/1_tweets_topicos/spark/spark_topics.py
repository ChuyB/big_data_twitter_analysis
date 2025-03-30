from pyspark import SparkContext
import nltk
from nltk.corpus import stopwords
import json
import logging
import gensim
from gensim import corpora, models
import os
import sys

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

sc = SparkContext(appName="NewsTopicAnalysis", master="local[*]")

nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('punkt_tab', quiet=True)

stop_words = set(stopwords.words("english"))
stop_words.update({"https", "news"})

def clean_text(text):
    words = nltk.word_tokenize(text)
    return [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]

# Load and preprocess data
logger.info("Mapper: starting processing")
input_rdd = sc.textFile("news.json") \
    .map(lambda line: json.loads(line.strip())) \
    .map(lambda doc: clean_text(doc.get("content", ""))) \
    .cache()

# Build dictionary
sampled_texts = input_rdd.takeSample(False, min(10000, input_rdd.count()))
dictionary = corpora.Dictionary(sampled_texts)
dictionary.filter_extremes(no_below=5, no_above=0.5)

# Create corpus
corpus_rdd = input_rdd.map(lambda text: dictionary.doc2bow(text))

# Train LDA 
lda_model = models.LdaModel(
    corpus=corpus_rdd.collect(),
    id2word=dictionary,
    num_topics=20,
    alpha="auto",
    per_word_topics=True,
    chunksize=10000,
    update_every=1,
    passes=1
)

def process_document(bow):
    topics = lda_model.get_document_topics(bow)
    if topics:
        dominant_topic = max(topics, key=lambda x: x[1])
        topic_id, _ = dominant_topic
        topic_words = [word for word, _ in lda_model.show_topic(topic_id)]
        return (' '.join(topic_words), 1)
    return ("no_topic", 0)

# Process and count topics
topic_counts = corpus_rdd \
    .map(process_document) \
    .filter(lambda x: x[0] != "no_topic") \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

# Save output
def save_output():
    ordered_results = topic_counts.collect()
    
    with open("output.txt", "w") as outfile:
        for topic, count in ordered_results:
            outfile.write(f"{topic}\t{count}\n")
    
    logger.info(f"Output saved to output.txt with {len(ordered_results)} entries")

save_output()

# Clean up
sc.stop()
logger.info("Processing complete")