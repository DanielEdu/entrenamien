from ingesta_lending.config import Config


def _config(**kw) -> Config:
    base = dict(
        tabla="clientes",
        catalogo="lending",
        esquema="datalab_zone",
        ruta_origen="/Volumes/lending/landing_zone/archivos/lending",
    )
    return Config(**{**base, **kw})


def test_tabla_destino_lleva_sufijo_bronze():
    assert _config().tabla_destino == "lending.datalab_zone.clientes_bronze"


def test_tabla_destino_en_minusculas():
    assert _config(tabla="Clientes").tabla_destino == "lending.datalab_zone.clientes_bronze"


def test_tabla_destino_sigue_el_esquema():
    assert _config(esquema="bronze_zone").tabla_destino == "lending.bronze_zone.clientes_bronze"


def test_ruta_tabla_origen():
    assert _config().ruta_tabla_origen == "/Volumes/lending/landing_zone/archivos/lending/clientes/"


def test_ruta_tabla_origen_ignora_barra_final():
    assert _config(ruta_origen="/Volumes/x/y/z/").ruta_tabla_origen == "/Volumes/x/y/z/clientes/"


def test_rutas_de_checkpoint_y_schema():
    c = _config(tabla="pagos_cuotas", esquema="bronze_zone")
    assert c.ruta_checkpoint == "/Volumes/lending/bronze_zone/checkpoints/pagos_cuotas_bronze/checkpoint"
    assert c.ruta_schema == "/Volumes/lending/bronze_zone/checkpoints/pagos_cuotas_bronze/schema"


def test_job_run_id_por_defecto_es_local():
    assert _config().job_run_id == "local"


def test_job_run_id_explicito():
    assert _config(job_run_id="987").job_run_id == "987"
