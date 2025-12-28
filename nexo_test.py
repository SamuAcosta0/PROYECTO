"""Cliente de consola para consultar precios en la base de datos nexo_precios."""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import psycopg2

DB_CONFIG = {
    "dbname": "nexo_precios",
    "user": "TU_USUARIO_AQUI",
    "password": "TU_PASSWORD_AQUI",
    "host": "localhost",
    "port": 5432,
}


def get_connection() -> Optional[psycopg2.extensions.connection]:
    """Crea y devuelve una nueva conexión a la base de datos o None si falla."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as exc:
        print(f"[ERROR] No se pudo conectar a la base de datos: {exc}")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Error inesperado al conectar a la base de datos: {exc}")
    return None


def run_query(query: str, params: Optional[Sequence[object]] = None) -> List[Tuple]:
    """Ejecuta una consulta y devuelve todas las filas, o una lista vacía si hay error."""
    connection = get_connection()
    if connection is None:
        return []

    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    except psycopg2.Error as exc:
        print(f"[ERROR] Falló la ejecución de la consulta: {exc}")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Error inesperado durante la consulta: {exc}")
    finally:
        cursor.close()
        connection.close()
    return []


def print_prices_table(rows: List[Tuple]) -> None:
    """Imprime una tabla con los precios recibidos."""
    headers = [
        "ID",
        "PRODUCTO",
        "MARCA",
        "SUCURSAL",
        "BARRIO",
        "COMERCIO",
        "FUENTE",
        "PRECIO",
        "FECHA",
    ]

    # Determinar el ancho máximo de cada columna
    column_count = len(headers)
    widths = [len(header) for header in headers]
    for row in rows:
        for idx in range(column_count):
            widths[idx] = max(widths[idx], len(str(row[idx]) if row[idx] is not None else ""))

    # Construir líneas de encabezado y separador
    header_line = " ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    separator_line = " ".join("-" * widths[idx] for idx in range(column_count))

    print(header_line)
    print(separator_line)

    for row in rows:
        formatted_row = []
        for idx, value in enumerate(row):
            text = "" if value is None else str(value)
            if headers[idx] == "PRECIO":
                formatted_row.append(text.rjust(widths[idx]))
            else:
                formatted_row.append(text.ljust(widths[idx]))
        print(" ".join(formatted_row))


def list_all_prices() -> None:
    """Lista todos los precios con detalles de producto, sucursal y fuente."""
    query = """
        SELECT
            p.id_precio,
            pr.nombre,
            pr.marca,
            s.nombre_sucursal,
            b.nombre_barrio,
            c.nombre_comercio,
            f.nombre_fuente,
            p.precio_lista,
            p.fecha_captura
        FROM precio AS p
        JOIN producto AS pr ON p.id_producto = pr.id_producto
        JOIN sucursal AS s ON p.id_sucursal = s.id_sucursal
        JOIN barrio AS b ON s.id_barrio = b.id_barrio
        JOIN comercio AS c ON s.id_comercio = c.id_comercio
        JOIN fuente_datos AS f ON p.id_fuente = f.id_fuente
        ORDER BY p.fecha_captura DESC;
    """
    rows = run_query(query)
    if not rows:
        print("No hay precios cargados todavía.")
        return

    print_prices_table(rows)


def search_prices_by_product_name() -> None:
    """Busca precios filtrando por nombre de producto (búsqueda parcial)."""
    term = input("Ingresá el nombre (o parte) del producto: ").strip()
    if not term:
        print("Debes ingresar al menos un carácter para buscar.")
        return

    query = """
        SELECT
            p.id_precio,
            pr.nombre,
            pr.marca,
            s.nombre_sucursal,
            b.nombre_barrio,
            c.nombre_comercio,
            f.nombre_fuente,
            p.precio_lista,
            p.fecha_captura
        FROM precio AS p
        JOIN producto AS pr ON p.id_producto = pr.id_producto
        JOIN sucursal AS s ON p.id_sucursal = s.id_sucursal
        JOIN barrio AS b ON s.id_barrio = b.id_barrio
        JOIN comercio AS c ON s.id_comercio = c.id_comercio
        JOIN fuente_datos AS f ON p.id_fuente = f.id_fuente
        WHERE pr.nombre ILIKE %s
        ORDER BY p.fecha_captura DESC;
    """
    rows = run_query(query, (f"%{term}%",))
    if not rows:
        print("No se encontraron precios para ese producto.")
        return

    print_prices_table(rows)


def search_prices_by_neighborhood() -> None:
    """Busca precios filtrando por nombre de barrio (búsqueda parcial)."""
    term = input("Ingresá el nombre (o parte) del barrio: ").strip()
    if not term:
        print("Debes ingresar al menos un carácter para buscar.")
        return

    query = """
        SELECT
            p.id_precio,
            pr.nombre,
            pr.marca,
            s.nombre_sucursal,
            b.nombre_barrio,
            c.nombre_comercio,
            f.nombre_fuente,
            p.precio_lista,
            p.fecha_captura
        FROM precio AS p
        JOIN producto AS pr ON p.id_producto = pr.id_producto
        JOIN sucursal AS s ON p.id_sucursal = s.id_sucursal
        JOIN barrio AS b ON s.id_barrio = b.id_barrio
        JOIN comercio AS c ON s.id_comercio = c.id_comercio
        JOIN fuente_datos AS f ON p.id_fuente = f.id_fuente
        WHERE b.nombre_barrio ILIKE %s
        ORDER BY p.fecha_captura DESC;
    """
    rows = run_query(query, (f"%{term}%",))
    if not rows:
        print("No se encontraron precios para ese barrio.")
        return

    print_prices_table(rows)


def main_menu() -> None:
    """Muestra el menú principal y maneja las opciones del usuario."""
    while True:
        print("\n==============================")
        print("NEXO - Motor de Precios (MVP)")
        print("==============================")
        print("\n1) Listar todos los precios")
        print("2) Buscar precios por nombre de producto")
        print("3) Buscar precios por barrio")
        print("0) Salir\n")

        option = input("Elegí una opción: ").strip()

        if option == "1":
            list_all_prices()
        elif option == "2":
            search_prices_by_product_name()
        elif option == "3":
            search_prices_by_neighborhood()
        elif option == "0":
            print("Saliendo de NEXO...")
            break
        else:
            print("Opción inválida. Por favor, elegí una opción válida.")


if __name__ == "__main__":
    main_menu()
