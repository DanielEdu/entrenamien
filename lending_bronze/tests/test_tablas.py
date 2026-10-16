import pytest

from ingesta_lending.tablas import TABLAS, obtener_tabla

LLAVES = {
    "productos_prestamo": "id_producto",
    "clientes": "id_cliente",
    "solicitudes_prestamo": "id_solicitud",
    "desembolsos": "id_desembolso",
    "pagos_cuotas": "id_pago",
}


def test_son_las_cinco_tablas():
    assert set(TABLAS) == set(LLAVES)


@pytest.mark.parametrize("nombre,llave", LLAVES.items())
def test_llave_de_cada_tabla(nombre, llave):
    assert obtener_tabla(nombre).llave == llave


def test_todas_tienen_descripcion():
    assert all(t.descripcion for t in TABLAS.values())


def test_nombre_coincide_con_la_clave():
    assert all(clave == t.nombre for clave, t in TABLAS.items())


def test_tabla_desconocida():
    with pytest.raises(ValueError, match="desconocida"):
        obtener_tabla("inexistente")
