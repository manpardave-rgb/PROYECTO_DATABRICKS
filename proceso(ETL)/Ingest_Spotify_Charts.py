# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, BooleanType
from pyspark.sql.functions import current_timestamp, to_date, col, lit

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_au")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adlsantodata1703")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/spotify_wrapped_2025_top50_songs.csv"

# COMMAND ----------

wrapped_full_schema = StructType(fields=[StructField("wrapped_2025_rank", IntegerType(), True),
                                    StructField("song_title", StringType(), True),
                                    StructField("artist", StringType(), True),
                                    StructField("streams_2025_billions", DoubleType(), True),
                                    StructField("primary_genre", StringType(), True),
                                    StructField("bpm", IntegerType(), True),
                                    StructField("duration_seconds", IntegerType(), True),
                                    StructField("release_year", IntegerType(), True),
                                    StructField("artist_country", StringType(), True),
                                    StructField("explicit", BooleanType(), True),
                                    StructField("danceability", DoubleType(), True),
                                    StructField("energy", DoubleType(), True),
                                    StructField("valence", DoubleType(), True),
                                    StructField("acousticness", DoubleType(), True),
                                    StructField("peak_global_chart_position", IntegerType(), True),
                                    StructField("dataset_part", StringType(), True)
])

# COMMAND ----------

charts_df = spark.read \
            .option("header", True) \
            .schema(wrapped_full_schema) \
            .csv(ruta)

# COMMAND ----------

charts_with_timestamp_df = charts_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

charts_selected_df = charts_with_timestamp_df.select(
                                                        col("wrapped_2025_rank").cast("string").alias("track_id"),
                                                        col("wrapped_2025_rank").alias("rank"),
                                                        col("artist_country").alias("country"),
                                                        col("streams_2025_billions").alias("streams"),
                                                        to_date(lit("2025-12-31")).alias("snapshot_date"),
                                                        col("ingestion_date")
)

# COMMAND ----------

destino = f"{catalogo}.{esquema}.spotify_daily_charts"

charts_selected_df.write.mode('append') \
    .partitionBy('snapshot_date') \
    .saveAsTable(destino)
print(f"Carga exitosa en {destino}")
