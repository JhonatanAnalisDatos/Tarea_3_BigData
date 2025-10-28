from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count, lit
from pyspark.sql.types import StructType, StructField, StringType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaAnaliticoPatrimonioLorica") \
    .getOrCreate()

# Reducir nivel de logs
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos
schema = StructType([
    StructField("Nombre", StringType(), True),
    StructField("Direccion", StringType(), True),
    StructField("Descripcion", StringType(), True),
    StructField("Tipo de acceso", StringType(), True),
    StructField("Tipo de Patrimonio", StringType(), True),
    StructField("Grupo", StringType(), True),
    StructField("Componente", StringType(), True),
    StructField("Elemento", StringType(), True),
    StructField("Georefenciacion", StringType(), True),
    StructField("Tipo Propiedad", StringType(), True)
])

# Leer flujo de datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sitios_lorica") \
    .load()

# Parsear JSON y enriquecer datos
df_analitico = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").withColumn(
    "Zona",
    when(col("Direccion").contains("rural") |
         col("Direccion").contains("Zona rural"), "RURAL"
    ).otherwise("URBANO")
).withColumn(
    "Prioridad_Conservacion",
    when(col("Tipo Propiedad") == "Pública", "ALTA - Acceso directo"
    ).when(col("Tipo de Patrimonio").contains("material"), "MEDIA - Intervenció>
    ).otherwise("BAJA - Monitoreo")
).withColumn(
    "Categoria_Uso",
    when(col("Tipo de Patrimonio").contains("Protegidas"), "ECOTURISMO"
    ).when(col("Elemento").contains("Vivienda"), "RESIDENCIAL"
    ).when(col("Elemento").contains("Plaza") |
           col("Elemento").contains("Parque"), "ESPACIO PÚBLICO"
    ).otherwise("CULTURAL")
)
# TABLA 1: Resumen ejecutivo por categorías
resumen_categorias = df_analitico.groupBy(
    "Tipo de Patrimonio", "Tipo Propiedad", "Zona"
).agg(
    count("Nombre").alias("Total_Bienes"),
    lit("").alias("---")
).select(
    col("Tipo de Patrimonio").alias("CATEGORIA"),
    col("Tipo Propiedad").alias("PROPIEDAD"),
    col("Zona"),
    col("Total_Bienes"),
    col("---")
)

# TABLA 2: Detalle de bienes con prioridad
detalle_bienes = df_analitico.select(
    col("Nombre").alias("BIEN_CULTURAL"),
    col("Tipo de Patrimonio").alias("CATEGORIA"),
    col("Tipo Propiedad").alias("PROPIEDAD"),
    col("Zona"),
    col("Prioridad_Conservacion").alias("PRIORIDAD"),
    col("Categoria_Uso").alias("USO_RECOMENDADO")
)

# Mostrar TABLA 1 - Resumen ejecutivo
print("=" * 80)
print("📊 RESUMEN EJECUTIVO - PATRIMONIO LORICA")
print("=" * 80)
query_resumen = resumen_categorias.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Mostrar TABLA 2 - Detalle de bienes
print("\n" + "=" * 80)
print("🏛️ DETALLE DE BIENES CON PRIORIZACIÓN")
print("=" * 80)
query_detalle = detalle_bienes.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Mantener streams activos
query_resumen.awaitTermination()
query_detalle.awaitTermination()
