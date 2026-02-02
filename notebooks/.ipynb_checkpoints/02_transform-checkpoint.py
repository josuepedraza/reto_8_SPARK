# %% [markdown]
# # 2. Transformación (Capa Plata)
# Limpieza de datos y tipado fuerte.

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import *

builder = SparkSession.builder \
    .appName("Lab_SECOP_Silver") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer Bronce
df_bronze = spark.read.format("delta").load("data/lakehouse/bronze/secop")

# %%
# Transformar
df_silver = df_bronze \
    .withColumnRenamed("Precio Base", "precio_base") \
    .withColumnRenamed("Departamento", "departamento") \
    .withColumnRenamed("Fecha de Firma", "fecha_firma_str") \
    .withColumn("fecha_firma", to_date(col("fecha_firma_str"), "yyyy-MM-dd")) \
    .filter(col("precio_base") > 0) \
    .select("Entidad", "departamento", "precio_base", "fecha_firma")

# %%
# Escribir Plata
df_silver.write.format("delta").mode("overwrite").save("data/lakehouse/silver/secop")
print("Capa Plata generada exitosamente.")
df_silver.show(5)
