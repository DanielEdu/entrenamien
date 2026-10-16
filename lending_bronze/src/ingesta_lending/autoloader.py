"""Auto Loader: opciones, columnas de metadata, lectura y escritura hacia Bronze."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingesta_lending.config import Config

COLUMNA_RESCATADA = "_rescued_data"
PATRON_LOTE = r"lote=(\d+)"
MARCAS_COLUMNA_NUEVA = ("UnknownFieldException", "UNKNOWN_FIELD_EXCEPTION")


@dataclass(frozen=True)
class Resultado:
    filas_leidas: int
    filas_totales: int


def opciones_lectura(config: Config) -> dict[str, str]:
    return {
        "cloudFiles.format": "csv",
        "header": "true",
        "cloudFiles.inferColumnTypes": "false",  # todo STRING; los tipos se resuelven en Silver
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.schemaLocation": config.ruta_schema,
        "rescuedDataColumn": COLUMNA_RESCATADA,  # lo que no encaje no se descarta
    }


def agregar_metadata(df: DataFrame, config: Config) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modified", F.col("_metadata.file_modification_time"))
        .withColumn("_lote", F.regexp_extract(F.col("_metadata.file_path"), PATRON_LOTE, 1).cast("int"))
        .withColumn("_job_run_id", F.lit(config.job_run_id))
    )


def leer(spark: SparkSession, config: Config) -> DataFrame:
    lectura = spark.readStream.format("cloudFiles").options(**opciones_lectura(config))
    return agregar_metadata(lectura.load(config.ruta_tabla_origen), config)


def _ejecutar(spark: SparkSession, config: Config) -> int:
    """Procesa lo pendiente y termina. Devuelve las filas leídas del origen."""
    query = (
        leer(spark, config)
        .writeStream.option("checkpointLocation", config.ruta_checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config.tabla_destino)
    )
    query.awaitTermination()
    return sum(p["numInputRows"] for p in query.recentProgress)


def _es_columna_nueva(error: Exception) -> bool:
    return any(marca in str(error) for marca in MARCAS_COLUMNA_NUEVA)


def ingestar(spark: SparkSession, config: Config, log) -> Resultado:
    """Con addNewColumns el stream se detiene al ver una columna nueva; se reintenta una vez y la columna entra."""
    try:
        filas_leidas = _ejecutar(spark, config)
    except Exception as error:
        if not _es_columna_nueva(error):
            raise
        log.warning("Columna nueva en el origen; esquema actualizado, se reintenta una vez")
        filas_leidas = _ejecutar(spark, config)
    filas_totales = spark.table(config.tabla_destino).count()
    return Resultado(filas_leidas, filas_totales)
