from pyspark import SparkContext
import csv
import re
from collections import Counter

# Crear SparkContext
sc = SparkContext(appName="CityYearPriceRange")

# === 1. Leer CSV limpio desde carpeta (salida de Spark) ===
lines = sc.textFile("/data/base_datos_cars_BD/cars_clean/*")

# === 2. Eliminar encabezado ===
header = lines.first()
data = lines.filter(lambda l: l != header)

# === 3. Parsear CSV a filas ===
rows = data.map(lambda line: next(csv.reader([line])))

# === 4. Procesar filas ===
def process_row(row):
    try:
        make = row[0]
        model = row[1]
        price = float(row[2])
        year = row[3]
        city = row[4]
        days = float(row[5])
        desc = row[6].lower()

        # Clasificación por rango de precios
        if price > 50000:
            price_range = 'high'
        elif price >= 20000:
            price_range = 'medium'
        else:
            price_range = 'low'

        # Extraer palabras clave de descripción
        words = re.findall(r'\b[a-z]{3,}\b', desc)

        key = (city, year, price_range)
        return (key, (1, days, words))
    except:
        return None

filtered = rows.map(process_row).filter(lambda x: x is not None)

# === 5. Reducir por clave (ciudad, año, rango de precio) ===
def reduce_func(a, b):
    return (
        a[0] + b[0],         # total coches
        a[1] + b[1],         # total días
        a[2] + b[2]          # lista combinada de palabras
    )

reduced = filtered.reduceByKey(reduce_func)

# === 6. Generar salida final con top 3 palabras y promedio de días ===
def final_output(kv):
    key, (count, total_days, words) = kv
    top_words = [w for w, _ in Counter(words).most_common(3)]
    avg_days = round(total_days / count, 1)
    return key, count, avg_days, top_words

results = reduced.map(final_output)

# === 7. Mostrar primeros resultados ===
for r in results.take(10):
    print(r)

# === 8. Finalizar contexto Spark ===
sc.stop()
