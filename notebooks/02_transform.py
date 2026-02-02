# %% [markdown]
# # 2. Transformación (Capa Plata)
# Limpieza, tipado fuerte y Quality Gate con bifurcación

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit
from delta import *

# %%
# Spark Session con Delta
builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Silver")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer capa Bronce
df_bronze = spark.read.format("delta").load("data/lakehouse/bronze/secop")

# %%
# Renombrar y tipar columnas relevantes
df_typed = (
    df_bronze
    .withColumnRenamed("Precio Base", "precio_base")
    .withColumnRenamed("Departamento", "departamento")
    .withColumnRenamed("Fecha de Firma", "fecha_firma_str")
    .withColumn("fecha_firma", to_date(col("fecha_firma_str"), "yyyy-MM-dd"))
)

# %%
# Definición de reglas de calidad y motivo de rechazo
df_qc = df_typed.withColumn(
    "motivo_rechazo",
    when(col("precio_base").isNull(), lit("precio_base_nulo"))
    .when(col("precio_base") <= 0, lit("precio_base_no_positivo"))
    .when(col("fecha_firma").isNull(), lit("fecha_firma_nula"))
    .otherwise(lit(None))
)

# %%
# Split de datos
df_validos = df_qc.filter(col("motivo_rechazo").isNull())
df_invalidos = df_qc.filter(col("motivo_rechazo").isNotNull())

# %%
# Escribir capa Silver (solo registros válidos)
(
    df_validos
    .select("Entidad", "departamento", "precio_base", "fecha_firma")
    .write
    .format("delta")
    .mode("overwrite")
    .save("data/lakehouse/silver/secop")
)

# %%
# Escribir capa Quarantine (registros inválidos con motivo)
(
    df_invalidos
    .select(
        "Entidad",
        "departamento",
        "precio_base",
        "fecha_firma",
        "motivo_rechazo"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .save("data/lakehouse/quarantine/secop_errors")
)

# %%
print("✅ Capa Silver generada:", df_validos.count(), "registros")
print("⚠️ Registros en Quarantine:", df_invalidos.count())
