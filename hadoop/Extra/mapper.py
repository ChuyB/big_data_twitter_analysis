import sys
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

with open('./restaurantes.txt', 'r') as f:
    restaurantes = set(line.strip() for line in f.readlines())

line_counter = 0

for line in sys.stdin:
    line_counter += 1
    if line_counter % 1000 == 0:
        logging.info(f"Procesando línea {line_counter}")
    
    import json
    try:
        tweet = json.loads(line)
    except json.JSONDecodeError:
        logging.error("Error al decodificar JSON")
        continue

    data = tweet.get("content", "").lower()
    location = tweet.get("user", {}).get("location", "desconocida").lower()

    for restaurante in restaurantes:
        if restaurante in data:
            print(f"{restaurante}\t{location}\t1")
