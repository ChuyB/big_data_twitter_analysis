import sys
import json
import nltk
import logging
from gensim import corpora, models

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('punkt_tab')

# Stopwords (without "https" and "news")
stop_words = set(nltk.corpus.stopwords.words("english"))
stop_words.update({"https", "news"})

def clean_text(text):
    # Tokenize the text into words
    words = nltk.word_tokenize(text)
    # Remove punctuation and common words (stopwords)
    words = [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]
    return words


# Read from stdin
texts = []
line_counter = 0
for line in sys.stdin:
    line_counter += 1
    if (line_counter % 1000) == 0:
        logging.info(f"Mapper: processed {line_counter} lines")
    data = json.loads(line.strip())
    content = data.get("content", "")
    cleaned_text = clean_text(content)
    texts.append(cleaned_text)

# Dictionary from the text data
dictionary = corpora.Dictionary(texts)

# Corpus from data
corpus = [dictionary.doc2bow(text) for text in texts]

# Training the LatentDirichletAllocation model
# ldamodel = models.LdaModel(corpus, id2word=dictionary, num_topics=20, alpha="auto", per_word_topics=True, chunksize=10000, update_every=1, passes=1)

# # Save model
# ldamodel.save("lda.model")

# Load model
ldamodel = models.LdaModel.load("lda.model")

# Extract topics
batch_size = 1000
doc_count = 0
for start_idx in range(0, len(corpus), batch_size):
    logging.info(f"Mapper (topics): processed {doc_count} entries")
    doc_count += batch_size

    batch = corpus[start_idx:start_idx + batch_size]
    topic_distributions = [ldamodel.get_document_topics(bow) for bow in batch]

    for doc_topics in topic_distributions:
        dominant_topic = max(doc_topics, key=lambda x: x[1])
        topic_id, _ = dominant_topic
        topic_words = [word for word, _ in ldamodel.show_topic(topic_id)]
        concat_words = ' '.join(topic_words)

        # Write to stdout
        print(f"{concat_words}\t1")
