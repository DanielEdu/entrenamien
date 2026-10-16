# Spec: lending_bronze — ingesta de lending a Bronze

## Objetivo
Ingerir a la capa Bronze, de forma incremental, idempotente y exactly-once, los archivos CSV del módulo de
préstamos (lending) que llegan a un Volume. El bundle NO genera datos: los archivos los deja otro proceso.

## Fuente
- Ruta raíz: /Volumes/lending/landing_zone/archivos/lending/  (variable `ruta_origen`, en el esquema landing_zone)
- Una carpeta por tabla y una subcarpeta por entrega: <ruta_origen>/<tabla>/lote=<N>/*.csv
- Formato: CSV con encabezado, separador coma, UTF-8. Llegan entregas nuevas; las anteriores no cambian.

| Tabla origen          | Qué es                                                        | Llave          |
|-----------------------|---------------------------------------------------------------|----------------|
| productos_prestamo    | Catálogo de productos de crédito: montos, plazos y tasa       | id_producto    |
| clientes              | Clientes: segmento, ciudad, ingreso y score; se actualizan    | id_cliente     |
| solicitudes_prestamo  | Solicitudes de crédito: monto, plazo, estado y canal          | id_solicitud   |
| desembolsos           | Dinero entregado por cada solicitud aprobada                  | id_desembolso  |
| pagos_cuotas          | Cuotas pagadas por desembolso, con días de mora               | id_pago        |

## Destino
- Tabla Delta por tabla origen: <catalogo>.<esquema>.<tabla>_bronze (sufijo _bronze, nombre de la tabla origen en minúsculas).
- Catálogo = producto de datos: `lending` en todos los ambientes. Esquema = capa: `datalab_zone` en dev (desarrollo) y `bronze_zone` en prod.
- Checkpoint:        /Volumes/<catalogo>/<esquema>/checkpoints/<tabla>_bronze/checkpoint
- Esquema inferido:  /Volumes/<catalogo>/<esquema>/checkpoints/<tabla>_bronze/schema

## Reglas de ingesta (Auto Loader)
1. readStream con format cloudFiles, cloudFiles.format=csv, header=true.
2. Todas las columnas de negocio como STRING: cloudFiles.inferColumnTypes=false. Los tipos se resuelven en Silver.
3. Evolución de esquema: cloudFiles.schemaEvolutionMode=addNewColumns y mergeSchema=true en la escritura.
   Si el stream se detiene por una columna nueva, se reintenta una vez y la columna entra.
4. Lo que no encaje queda en _rescued_data (no se descarta).
5. trigger(availableNow=True): procesa lo pendiente y termina.
6. Columnas de metadata en cada tabla:
   - _ingested_at            timestamp de la carga
   - _source_file            ruta completa del archivo (_metadata.file_path)
   - _source_file_modified   fecha de modificación del archivo (_metadata.file_modification_time)
   - _lote                   número de entrega, tomado de la carpeta lote=N
   - _job_run_id             id de la corrida del job (parámetro), "local" si se corre a mano

## Código
- Sin notebooks. Punto de entrada src/main.py, que recibe --tabla, --catalogo, --esquema, --ruta_origen, --job_run_id.
- Paquete modular y parametrizado en src/ingesta_lending/:
  - config.py    → dataclass de configuración y construcción de rutas y nombres (sin valores fijos)
  - tablas.py    → catálogo de las 5 tablas: nombre, descripción y llave
  - autoloader.py→ opciones de Auto Loader, columnas de metadata, lectura y escritura
  - log.py       → logger con formato "AAAA-MM-DD HH:MM:SS | nivel | tabla | mensaje"
- Agregar una tabla nueva = una entrada en tablas.py y una tarea en el YAML; nada más.
- Logs mínimos por tabla: inicio con rutas, filas leídas, filas totales en destino, duración, fin OK/ERROR.
- tests/ con pytest sobre config.py y tablas.py, sin pyspark.

## Bundle
- databricks.yml con variables catalogo, esquema, ruta_origen, git_url y git_branch; targets dev (mode development, default) y
  prod (mode production).
- resources/ingesta_lending_bronze.job.yml: un job "ingesta-lending-bronze" con UNA TAREA POR TABLA (5 tareas, sin depends_on
  entre ellas), cada una spark_python_task sobre src/main.py en serverless (environment del job).
- El código del job se lee de GitHub, del mismo repositorio del bundle (job con git_source), no de archivos sincronizados al workspace:
  - git_url: https://github.com/DanielEdu/entrenamien.git (variable `git_url`).
  - git_branch (variable `git_branch`): en dev la rama local actual (${bundle.git.branch}) para probar antes de mergear; en prod, `main`.
  - El proyecto vive en la subcarpeta lending_bronze/ del repo, así que python_file es lending_bronze/src/main.py (relativo a la raíz del repo).
  - Consecuencia: el job ejecuta lo que está PUSHEADO en la rama, no lo que hay en local. Hay que hacer push antes de correrlo.

## Criterios de aceptación
1. databricks bundle validate -t dev pasa.
2. Primera corrida con el lote 1: existen las 5 tablas <tabla>_bronze en lending.datalab_zone y todas las columnas de negocio son string.
3. Idempotencia: una segunda corrida sin archivos nuevos lee 0 filas y el conteo de cada tabla no cambia.
4. Incremental: tras generar el lote 2, la corrida lee solo los archivos lote=2 (_lote = 2 en las filas nuevas).
5. Evolución de esquema: si un archivo trae una columna nueva, la tabla la incorpora sin perder filas.
6. Cada fila tiene las 5 columnas de metadata no nulas (salvo _job_run_id al correr a mano).
7. Los checkpoints y esquemas quedan en las rutas indicadas, uno por tabla.
8. El log de cada tarea muestra inicio, filas leídas, total en destino y fin.
9. bundle deploy -t prod escribe en lending.bronze_zone, no en datalab_zone.
10. El job desplegado usa git_source con el repo de GitHub: dev apunta a la rama local y prod a main, y las tareas ejecutan
    lending_bronze/src/main.py desde el checkout de Git.
