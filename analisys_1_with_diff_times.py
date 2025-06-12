from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import time

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Used Cars Preprocessing - Limited Memory") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.shuffle.partitions", "4000") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Ruta del CSV
path_csv = r"/data/base_datos_cars_BD/used_cars_data.csv"

# Cargar CSV
df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("multiLine", True) \
               .option("escape", "\"") \
               .option("mode", "PERMISSIVE") \
               .csv(path_csv)

# Selección de columnas relevantes
columns_to_keep = ["make_name", "model_name", "price", "year", "city", "daysonmarket", "description"]
df = df.select(*columns_to_keep)

# Limpieza de datos
df = df.withColumn("price", when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price")).otherwise(None)) \
       .withColumn("year", when(col("year").rlike("^[0-9]{4}$"), col("year")).otherwise(None)) \
       .withColumn("price", col("price").cast("double")) \
       .withColumn("year", col("year").cast("int"))

threshold = 0.7 * df.count()
cols_to_drop = [c for c in df.columns if df.filter(col(c).isNull()).count() > threshold]
df = df.drop(*cols_to_drop)

df = df.replace("None", None).replace("NULL", None).replace("", None)

df = df.filter(
    col("make_name").isNotNull() & col("model_name").isNotNull()
).filter(
    ~col("model_name").rlike("^[0-9.\\-]+$")
).filter(
    ~col("model_name").rlike("(?i)(am/fm|sirius|volume control|equipment|console|rear window|wiper|preferred group|--|in)")
)

# Función para ejecutar el análisis y medir el tiempo
def run_analysis(df, fraction):
    df_sample = df.sample(withReplacement=False, fraction=fraction, seed=42)
    df_sample.createOrReplaceTempView("cars")
    
    start_time = time.time()
    
    resultado = spark.sql("""
    SELECT 
      make_name,
      model_name,
      COUNT(*) AS total_cars,
      MIN(price) AS min_price,
      MAX(price) AS max_price,
      ROUND(AVG(price), 2) AS avg_price,
      COLLECT_SET(year) AS years
    FROM cars
    GROUP BY make_name, model_name
    ORDER BY make_name
    """)
    
    resultado.show(5, truncate=False)
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"Tiempo para {int(fraction*100)}% del dataset: {elapsed:.2f} segundos")
    
    return elapsed

# Probar con distintas fracciones de datos
fractions = [0.1, 0.3, 0.5]
tiempos = {}

for f in fractions:
    print(f"\nEjecutando con {int(f*100)}% del dataset...")
    tiempos[f] = run_analysis(df, f)

print("\nTiempos medidos (fracción : segundos):")
for f, t in tiempos.items():
    print(f"{int(f*100)}% -> {t:.2f} s")

spark.stop()
