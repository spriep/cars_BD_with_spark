from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, min, max, avg, collect_set, round as spark_round

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisAutos") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

# === 1. Cargar CSV ===
path_csv = r"C:\BDs_grandes\base_datos_cars_BD\used_cars_data.csv"
df = spark.read.csv(path_csv, header=True, inferSchema=True)

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

# === 4. Registrar como vista temporal para usar SQL ===
df.createOrReplaceTempView("cars")

# === 5. Ejecutar consulta SQL ===
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

# === 6. Mostrar resultados ===
resultado.show(truncate=False)

# === 7. (Opcional) Guardar el resultado en Parquet o CSV ===
output_path = r"C:\BDs_grandes\base_datos_cars_BD\resultado_analisis"
resultado.write.mode("overwrite").parquet(output_path)

# Finalizar sesión de Spark
spark.stop()
