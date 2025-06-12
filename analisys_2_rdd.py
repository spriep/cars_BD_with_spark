from pyspark import SparkContext
import csv
import re
from collections import Counter

sc = SparkContext(appName="CityYearPriceRange")

# Leer CSV (ajusta la ruta a tu archivo)
lines = sc.textFile("cars.csv")

# Convertir CSV a filas
header = lines.first()
data = lines.filter(lambda l: l != header)
rows = data.map(lambda line: next(csv.reader([line])))

def process_row(row):
    try:
        city = row[5]
        year = row[6]
        price = float(row[7])
        days = float(row[8])
        desc = row[9].lower()
        
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
        a[0] + b[0],             # total cars
        a[1] + b[1],             # total days
        a[2] + b[2]              # merged words
    )

reduced = filtered.reduceByKey(reduce_func)

def final_output(kv):
    key, (count, total_days, words) = kv
    top_words = [w for w, _ in Counter(words).most_common(3)]
    avg_days = round(total_days / count, 1)
    return key, count, avg_days, top_words

results = reduced.map(final_output)

for r in results.take(10):
    print(r)
