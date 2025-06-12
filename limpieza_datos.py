from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import to_date
from pyspark.sql.functions import regexp_replace




spark = SparkSession.builder \
    .appName("MiProyectoSpark") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()


# Usa tu CSV
path_csv = r"C:\BDs_grandes\base_datos_cars_BD\used_cars_data.csv"
df = spark.read.csv(path_csv, header=True, inferSchema=True)
#df.printSchema()
#df.show(5)

########## ELIMINACION DE ATRIBUTOS IRRELEVANTES PARA EL TASK1 Y 2
columns_to_keep = ["make_name", "model_name", "price", "year", "city", "daysonmarket", "description"]
df = df.select(*columns_to_keep)


##########      MUCHOS DATOS ESTAN EN TIPOS RAROS (NUMEROS EN STRINGS) --> CAMBIAMOS
# Primero, limpiamos valores no numéricos en 'price' y 'year' usando regex
df = df.withColumn("price", when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price")).otherwise(None)) \
       .withColumn("year", when(col("year").rlike("^[0-9]{4}$"), col("year")).otherwise(None))

# Ahora sí hacemos el cast seguro
df = df.withColumn("price", col("price").cast("double")) \
       .withColumn("year", col("year").cast("int"))


##########      LIMPIEZA DE VALORES NULOS por columnas --> THRESHOLD 70% DE VALORES NULOS OUT
threshold = 0.7 * df.count()
cols_to_drop = [c for c in df.columns if df.filter(col(c).isNull()).count() > threshold]
df = df.drop(*cols_to_drop)

##########      LIMPIEZA VALORES INCONSISTENTES --> Campos como "None", "NULL", "", o similares deben normalizarse como null.
df = df.replace("None", None)
df = df.replace("NULL", None)
df = df.replace("", None)


# Guardar el DataFrame limpio como CSV
# Guardar el DataFrame limpio como archivo Parquet en modo append
output_path = "hdfs://localhost:9000/user/spark/output/used_cars_data_limpio"

df.write \
    .mode("append") \
    .parquet(output_path)

