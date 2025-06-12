from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Car Project") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv("cars.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("cars")
