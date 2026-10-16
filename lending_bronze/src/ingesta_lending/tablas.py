"""Catálogo de las tablas de lending. Una tabla nueva = una entrada aquí y una tarea en el YAML del job."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Tabla:
    nombre: str
    descripcion: str
    llave: str


TABLAS: dict[str, Tabla] = {
    t.nombre: t
    for t in (
        Tabla("productos_prestamo", "Catálogo de productos de crédito: montos, plazos y tasa", "id_producto"),
        Tabla("clientes", "Clientes: segmento, ciudad, ingreso y score; se actualizan", "id_cliente"),
        Tabla("solicitudes_prestamo", "Solicitudes de crédito: monto, plazo, estado y canal", "id_solicitud"),
        Tabla("desembolsos", "Dinero entregado por cada solicitud aprobada", "id_desembolso"),
        Tabla("pagos_cuotas", "Cuotas pagadas por desembolso, con días de mora", "id_pago"),
    )
}


def obtener_tabla(nombre: str) -> Tabla:
    try:
        return TABLAS[nombre]
    except KeyError:
        raise ValueError(f"Tabla desconocida '{nombre}'. Válidas: {', '.join(TABLAS)}") from None
