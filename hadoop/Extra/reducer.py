import sys
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

"""
 - Procesa la salida del mapper (restaurante, ciudad, count).
 - Calcula el total de menciones sumando todas las ciudades.
 - Ordena los resultados alfabéticamente por el nombre del restaurante.
"""

conteo_restaurantes_ciudades = {}
line_counter = 0

for line in sys.stdin:
    line_counter += 1
    if line_counter % 1000 == 0:
        logging.info(f"Reducer: procesando {line_counter} líneas")

    line = line.strip()
    try:
        restaurante, ciudad, count = line.split('\t')
        count = int(count)
    except ValueError:
        logging.error(f"Línea no válida: {line}")
        continue


    clave = (restaurante, ciudad)

    if clave in conteo_restaurantes_ciudades:
        conteo_restaurantes_ciudades[clave] += count
    else:
        conteo_restaurantes_ciudades[clave] = count

for (restaurante, ciudad), count in sorted(conteo_restaurantes_ciudades.items(), key=lambda item: item[0][0]):
    print(f"{restaurante:<15}\t{ciudad:<15}\t{count}")




