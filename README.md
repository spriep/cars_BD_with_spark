# Proyecto de Análisis de Autos

## Herramientas utilizadas:
- Spark SQL
- Apache Hive
- Spark Core (RDDs)

## Cómo ejecutar con el docker
docker run -it --rm -v C:\BDs_grandes:/data -v C:\Users\saioa\Desktop\ROMA_TRE\Big_data\my_proyect:/scripts bitnami/spark /bin/bash

spark-submit /scripts/analisys_1.py






### 1. Análisis 1 (por marca y modelo)

**Con Spark SQL:**
```bash
spark-submit --master local analysis1.sql

**Con Hive**
hive -f analysis1.hql

### 2. Análisis 2 (Reporte por ciudad y año con rangos de precio)
**Con Spark Core (RDD) y MapReduce:**
```bash
spark-submit analysis2_rdd.py
