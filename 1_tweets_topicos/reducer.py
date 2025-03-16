import sys
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

topic_counts = {}

# Read from stdin
line_counter = 0
for line in sys.stdin:
    line_counter += 1
    if line_counter % 1000 == 0:
        logging.info(f"Reducer: processed {line_counter} lines")
    # Parse input
    line = line.strip()
    topic, count = line.rsplit('\t', 1)

    # Convert count to integer
    count = int(count)

    # Increment the count for this topic in the dictionary
    if topic in topic_counts:
        topic_counts[topic] += count
    else:
        topic_counts[topic] = count

# Write to stdout already sorted
for topic, count in sorted(topic_counts.items(), key=lambda item: item[1], reverse=True):
    print(f"{topic}\t{count}")
