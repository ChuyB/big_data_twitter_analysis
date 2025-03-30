import sys
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    stream=sys.stderr
)

"""
 - Leer la lista de restaurantes desde un archivo.
 - Se abre el archivo 'restaurantes.txt' ubicado en la carpeta '../data/'.
 - Cada línea del archivo representa el nombre de un restaurante, que se convierte 
   en un elemento de un conjunto (`set`) para optimizar la búsqueda.
"""
with open('./restaurantes.txt', 'r') as f:
    restaurantes = set(line.strip() for line in f.readlines())

"""
 - Lee cada linea del dataset quita espacios en blancos al principio y al final, ademas de 
   convertir la linea a minusculas
 - Recorre la lista de restaurantes y busca en la linea coincidencias, en caso de haberlas
   retorna el nombre del restaurante encontrado con un 1 tabulado
"""
line_counter = 0
for line in sys.stdin:
    line_counter += 1
    if line_counter % 1000 == 0:
        logging.info(f"Procesando línea {line_counter}")
    
    data = line.strip().lower()
    
    for restaurante in restaurantes:
        if restaurante in data:
            print(f"{restaurante}\t1")
