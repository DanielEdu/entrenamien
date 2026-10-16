"""Logger con formato 'AAAA-MM-DD HH:MM:SS | nivel | tabla | mensaje'."""

import logging
import sys

FORMATO = "%(asctime)s | %(levelname)s | %(tabla)s | %(message)s"
FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"


def obtener_logger(tabla: str) -> logging.LoggerAdapter:
    logger = logging.getLogger("ingesta_lending")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(FORMATO, FORMATO_FECHA))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logging.LoggerAdapter(logger, {"tabla": tabla})
