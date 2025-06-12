from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# === 0. Crear la sesión de Spark con parámetros avanzados ===
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

# === 1. Cargar CSV original ===
path_csv = "/data/base_datos_cars_BD/used_cars_data.csv"

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
       .withColumn("year", col("year").cast("int")) \
       .withColumn("daysonmarket", col("daysonmarket").cast("double"))

# Eliminar columnas con >70% nulos
threshold = 0.7 * df.count()
cols_to_drop = [c for c in df.columns if df.filter(col(c).isNull()).count() > threshold]
df = df.drop(*cols_to_drop)

# Reemplazar valores vacíos por null
df = df.replace("None", None).replace("NULL", None).replace("", None)

# Filtros de registros inválidos
df = df.filter(
    col("make_name").isNotNull() & col("model_name").isNotNull()
).filter(
    ~col("model_name").rlike("^[0-9.\\-]+$")
).filter(
    ~col("model_name").rlike("(?i)(am/fm|sirius|volume control|equipment|console|rear window|wiper|preferred group|--|in)")
)

# === 4. Guardar archivo limpio para análisis RDD ===
output_path = "/data/base_datos_cars_BD/cars_clean"  # o donde prefieras
df.write.mode("overwrite").option("header", True).csv(output_path)

print(f"\n✔ Datos limpios guardados en: {output_path}\n")
