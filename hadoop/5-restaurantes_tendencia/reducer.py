import sys
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

"""
 - Quitamos espacios en blanco al principio y al final de cada linea, ademas de separar
   restaurante y count por una tabulacion (Asi fue el formato de salida del mapper)
 - Si dos restaurantes tienen el mismo nombre suma sus contadores (+=1)
 - Ordena la salida de mayor a menor por sus contadores.
"""

conteo_restaurantes = {}
line_counter = 0
for line in sys.stdin:
    line_counter += 1
    if line_counter % 1000 == 0:
        logging.info(f"Reducer: procesando {line_counter} líneas")

    line = line.strip()
    restaurante, count = line.split('\t')
    count = int(count)

    if restaurante in conteo_restaurantes:
        conteo_restaurantes[restaurante] += count
    else:
        conteo_restaurantes[restaurante] = count

for restaurante, count in sorted(conteo_restaurantes.items(), key=lambda item: item[1], reverse=True):
    print(f"{restaurante:<15}\t{count}")

