########HE UTILIZADO SPARK SQL
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, min, max, avg, collect_set, round as spark_round
import os
'''
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
'''

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

# === 1. Cargar CSV de forma robusta ===
path_csv = r"/data/base_datos_cars_BD/used_cars_data.csv"

df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("multiLine", True) \
               .option("escape", "\"") \
               .option("mode", "PERMISSIVE") \
               .csv(path_csv)

# === 2. Selección de columnas relevantes ===
columns_to_keep = ["make_name", "model_name", "price", "year", "city", "daysonmarket", "description"]
df = df.select(*columns_to_keep)

# === 3. Limpieza de datos ===
df = df.withColumn("price", when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price")).otherwise(None)) \
       .withColumn("year", when(col("year").rlike("^[0-9]{4}$"), col("year")).otherwise(None)) \
       .withColumn("price", col("price").cast("double")) \
       .withColumn("year", col("year").cast("int"))

threshold = 0.7 * df.count()
cols_to_drop = [c for c in df.columns if df.filter(col(c).isNull()).count() > threshold]
df = df.drop(*cols_to_drop)

df = df.replace("None", None).replace("NULL", None).replace("", None)

# === 3.5 Filtro de registros inválidos o con ruido textual ===
df = df.filter(
    col("make_name").isNotNull() & col("model_name").isNotNull()
).filter(
    ~col("model_name").rlike("^[0-9.\\-]+$")
).filter(
    ~col("model_name").rlike("(?i)(am/fm|sirius|volume control|equipment|console|rear window|wiper|preferred group|--|in)")
)

# === 4. Registrar como vista temporal ===
df.createOrReplaceTempView("cars")

# === 5. Consulta SQL agregada ===
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

# === 6. Mostrar resultados agregados ===
resultado.show(10, truncate=False)

# === 7. Guardar en Parquet ===
output_path = "/data/base_datos_cars_BD/resultado_analisis"
resultado.write.mode("overwrite").parquet(output_path)

# === 9. Finalizar sesión Spark ===
spark.stop()
