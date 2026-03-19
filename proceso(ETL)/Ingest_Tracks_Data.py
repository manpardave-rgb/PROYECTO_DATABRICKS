# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/spotify_alltime_top100_songs.csv"

# COMMAND ----------

tracks_schema = StructType(fields=[StructField("alltime_rank", IntegerType(), True),
                                    StructField("song_title", StringType(), True),
                                    StructField("artist", StringType(), True),
                                    StructField("total_streams_billions", DoubleType(), True),
                                    StructField("primary_genre", StringType(), True),
                                    StructField("bpm", IntegerType(), True),
                                    StructField("release_year", IntegerType(), True),
                                    StructField("artist_country", StringType(), True),
                                    StructField("explicit", BooleanType(), True),
                                    StructField("danceability", DoubleType(), True),
                                    StructField("energy", DoubleType(), True),
                                    StructField("valence", DoubleType(), True),
                                    StructField("acousticness", DoubleType(), True),
                                    StructField("dataset_part", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_tracks = spark.read.option('header', True).schema(tracks_schema).csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
df_tracks_final = df_tracks.select(col("alltime_rank").cast("string").alias("track_id"),
                                    col("song_title").alias("track_name"),
                                    col("artist").alias("artist_name"),
                                    lit(None).cast("string").alias("album_name"),
                                    col("total_streams_billions").cast("double").alias("popularity"),
                                    lit(0).cast("long").alias("duration_ms"),
                                    col("explicit").cast("boolean"),
                                    col("release_year").cast("string").alias("release_date"),
                                    col("danceability").cast("double"),
                                    col("energy").cast("double"),
                                    current_timestamp().alias("ingestion_date")
)

# COMMAND ----------

tracks_renamed_df = df_tracks_final.withColumnRenamed("trackId", "track_id") \
                                            .withColumnRenamed("trackName", "track_name") \
                                            .withColumnRenamed("artistName", "artist_name") \
                                            .withColumnRenamed("albumName", "album_name") \
                                            .withColumnRenamed("pop", "popularity") \
                                            .withColumnRenamed("duration", "duration_ms") \
                                            .withColumnRenamed("dance", "danceability")

# COMMAND ----------

print(f"Number of columns: {len(df_tracks_final.columns)}")

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
tracks_final_df = df_tracks_final.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

destino = f"{catalogo}.{esquema}.spotify_tracks"

# Verificación de seguridad antes de insertar
if len(df_tracks_final.columns) == 11:
    df_tracks_final.write.mode("append").insertInto(destino)
    print(f"Carga exitosa en {destino}")
else:
    print("Error: El número de columnas no coincide con la tabla Bronze (deben ser 11)")
