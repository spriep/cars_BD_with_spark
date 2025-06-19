from pyspark import SparkContext
import csv
import re
from collections import Counter
import os
import time

os.environ['USER'] = 'saioa'
os.environ['LOGNAME'] = 'saioa'

print("Inicio del script")
sc = SparkContext(appName="CityYearPriceRange")

# Leer dataset
lines = sc.textFile("/data/cars_clean/cars.csv")
header = lines.first()
data = lines.filter(lambda l: l != header)

# Total de líneas sin encabezado
total_lines = data.count()
print(f"Total líneas dataset: {total_lines}")

# Porcentajes a evaluar
percentages = [0.5, 0.3, 0.1]

def process_subset(sample_fraction):
    print(f"\nProcesando {int(sample_fraction*100)}% del dataset...")
    start_time = time.time()

    # Tomar muestra sin reemplazo y sin semilla para que sea aleatorio
    sample_data = data.sample(withReplacement=False, fraction=sample_fraction)

    # Parsear CSV a filas
    rows = sample_data.map(lambda line: next(csv.reader([line])))

    def process_row(row):
        try:
            make = row[0]
            model = row[1]
            price = float(row[2])
            year = row[3]
            city = row[4]
            days = float(row[5])
            desc = row[6].lower()

            if price > 50000:
                price_range = 'high'
            elif price >= 20000:
                price_range = 'medium'
            else:
                price_range = 'low'

            words = re.findall(r'\b[a-z]{3,}\b', desc)

            key = (city, year, price_range)
            return (key, (1, days, words))
        except:
            return None

    filtered = rows.map(process_row).filter(lambda x: x is not None)

    def reduce_func(a, b):
        return (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2]
        )

    reduced = filtered.reduceByKey(reduce_func)

    def final_output(kv):
        key, (count, total_days, words) = kv
        top_words = [w for w, _ in Counter(words).most_common(3)]
        avg_days = round(total_days / count, 1)
        return key, count, avg_days, top_words

    results = reduced.map(final_output)

    # Forzar evaluación y mostrar primeros 5 resultados para validar
    output = results.take(5)
    for r in output:
        print(r)

    elapsed_time = time.time() - start_time
    print(f"Tiempo total para {int(sample_fraction*100)}%: {elapsed_time:.2f} segundos")

# Ejecutar para cada porcentaje
for p in percentages:
    process_subset(p)

sc.stop()
print("Fin del script")
