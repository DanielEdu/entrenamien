"""Configuración de una ingesta y construcción de rutas y nombres. Sin valores fijos de ambiente."""

from dataclasses import dataclass

JOB_RUN_ID_LOCAL = "local"


@dataclass(frozen=True)
class Config:
    tabla: str
    catalogo: str
    esquema: str
    ruta_origen: str
    job_run_id: str = JOB_RUN_ID_LOCAL

    @property
    def tabla_destino(self) -> str:
        """<catalogo>.<esquema>.<tabla>_bronze, con el nombre de la tabla origen en minúsculas."""
        return f"{self.catalogo}.{self.esquema}.{self.tabla.lower()}_bronze"

    @property
    def ruta_tabla_origen(self) -> str:
        """<ruta_origen>/<tabla>/ (contiene las subcarpetas lote=<N>)."""
        return f"{self.ruta_origen.rstrip('/')}/{self.tabla}/"

    @property
    def ruta_base_checkpoints(self) -> str:
        return f"/Volumes/{self.catalogo}/{self.esquema}/checkpoints/{self.tabla.lower()}_bronze"

    @property
    def ruta_checkpoint(self) -> str:
        return f"{self.ruta_base_checkpoints}/checkpoint"

    @property
    def ruta_schema(self) -> str:
        return f"{self.ruta_base_checkpoints}/schema"
