# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

for nombre, defecto in [("catalogo", "lending"), ("esquema", "datalab_zone"), ("ruta_origen", "/Volumes/lending/landing_zone/archivos/lending"), ("tabla", "clientes")]:
    dbutils.widgets.text(nombre, defecto)
c, e, ro, t = (dbutils.widgets.get(x) for x in ("catalogo", "esquema", "ruta_origen", "tabla"))

origen = f"{ro}/{t}/"
checkpoint = f"/Volumes/{c}/{e}/checkpoints/ui_{t}_bronze"

q = (spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "csv").option("header", "true")
     .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
     .option("cloudFiles.inferColumnTypes", "true")
     .load(origen)
     .withColumn("_ingested_at", F.current_timestamp())
     .withColumn("_source_file", F.col("_metadata.file_path"))
     .writeStream
     .option("checkpointLocation", checkpoint)
     .option("mergeSchema", "true")
     .trigger(availableNow=True)
     .toTable(f"{c}.{e}.ui_{t}_bronze"))
q.awaitTermination()