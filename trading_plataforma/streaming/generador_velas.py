from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configuración de Spark
spark = SparkSession.builder \
    .appName("GeneradorVelas") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Estructura de los ticks
schema = StructType([
    StructField("symbol", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", StringType())
])

# Consumir ticks de Kafka
datos = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ticks") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir JSON a columnas
ticks = datos.select(
    F.from_json(
        F.col("value").cast("string"), schema
    ).alias("data"),
    "partition",
    "offset"
).select("data.*", "partition", "offset")

# Convertir timestamp y calcular precio medio
ticks = ticks.withColumn(
    "timestamp", F.to_timestamp("timestamp")
).filter(
    "symbol IS NOT NULL AND timestamp IS NOT NULL "
    "AND bid > 0 AND ask >= bid"
).withColumn(
    "precio", (F.col("bid") + F.col("ask")) / 2
)

# Orden cronológico de los ticks
orden = F.struct("timestamp", "partition", "offset")

# Generar velas de un minuto
velas = ticks.withWatermark("timestamp", "10 seconds") \
    .groupBy(
        F.col("symbol"),
        F.window("timestamp", "1 minute")
    ).agg(
        F.min_by("precio", orden).alias("open"),
        F.max("precio").alias("high"),
        F.min("precio").alias("low"),
        F.max_by("precio", orden).alias("close"),
        F.count("*").alias("num_ticks")
    )

# Estructura solicitada por el profesor
resultado = velas.select(
    "symbol",
    F.col("window.start").alias("inicio"),
    "open",
    "high",
    "low",
    "close",
    "num_ticks"
)

# Convertir las velas a JSON
salida = resultado.select(
    F.col("symbol").alias("key"),
    F.to_json(F.struct("*")).alias("value")
)

# Publicar las velas en Kafka
consulta = salida.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "velas") \
    .option("checkpointLocation", "checkpoints/velas") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

print("Generando velas de un minuto...")

consulta.awaitTermination()