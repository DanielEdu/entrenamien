"""Punto de entrada: ingesta una tabla de lending a Bronze.

Uso:
    main.py --tabla clientes --catalogo lending --esquema datalab_zone \
            --ruta_origen /Volumes/lending/landing_zone/archivos/lending --job_run_id 123
"""

import argparse
import os
import sys
import time


def _agregar_paquete_al_path() -> None:
    """Hace importable ingesta_lending.

    En Databricks (serverless, git_source) el script puede ejecutarse sin __file__, así que se prueban varias
    ubicaciones y se usa la primera que contenga el paquete.
    """
    candidatos = []
    if "__file__" in globals():
        candidatos.append(os.path.dirname(os.path.abspath(__file__)))
    if sys.argv and sys.argv[0]:
        candidatos.append(os.path.dirname(os.path.abspath(sys.argv[0])))
    cwd = os.getcwd()
    candidatos += [os.path.join(cwd, "lending_bronze", "src"), os.path.join(cwd, "src"), cwd]
    for carpeta in candidatos:
        if os.path.isdir(os.path.join(carpeta, "ingesta_lending")):
            sys.path.insert(0, carpeta)
            return
    raise RuntimeError(f"No se encontró el paquete ingesta_lending. Rutas probadas: {candidatos}")


_agregar_paquete_al_path()

from ingesta_lending.autoloader import ingestar  # noqa: E402
from ingesta_lending.config import JOB_RUN_ID_LOCAL, Config  # noqa: E402
from ingesta_lending.log import obtener_logger  # noqa: E402
from ingesta_lending.tablas import obtener_tabla  # noqa: E402


def leer_argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingesta de una tabla de lending a Bronze")
    parser.add_argument("--tabla", required=True)
    parser.add_argument("--catalogo", required=True)
    parser.add_argument("--esquema", required=True)
    parser.add_argument("--ruta_origen", required=True)
    parser.add_argument("--job_run_id", default=JOB_RUN_ID_LOCAL)
    return parser.parse_args(argv)


def crear_spark():
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:  # dentro de Databricks no hace falta Databricks Connect
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


def main(argv: list[str] | None = None) -> None:
    args = leer_argumentos(argv)
    tabla = obtener_tabla(args.tabla)
    config = Config(
        tabla=tabla.nombre,
        catalogo=args.catalogo,
        esquema=args.esquema,
        ruta_origen=args.ruta_origen,
        job_run_id=args.job_run_id,
    )
    log = obtener_logger(config.tabla)
    inicio = time.monotonic()
    log.info(
        f"Inicio | origen={config.ruta_tabla_origen} | destino={config.tabla_destino} "
        f"| checkpoint={config.ruta_checkpoint} | schema={config.ruta_schema} | job_run_id={config.job_run_id}"
    )
    try:
        resultado = ingestar(crear_spark(), config, log)
    except Exception as error:
        log.error(f"Fin ERROR | duracion={time.monotonic() - inicio:.1f}s | {error}")
        raise
    log.info(f"Filas leídas: {resultado.filas_leidas}")
    log.info(f"Filas totales en destino: {resultado.filas_totales}")
    log.info(f"Fin OK | duracion={time.monotonic() - inicio:.1f}s")


if __name__ == "__main__":
    main()
