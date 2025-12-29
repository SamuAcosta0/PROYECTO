from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, render_template, request

DB_CONFIG: Dict[str, Any] = {
    "dbname": "nexo_precios",
    "user": "TU_USUARIO_AQUI",
    "password": "TU_PASSWORD_AQUI",
    "host": "localhost",
    "port": 5432,
}

app = Flask(__name__)


def get_connection() -> Optional[psycopg2.extensions.connection]:
    """Crea y devuelve una conexión a la base de datos o None si falla."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as exc:
        app.logger.error("No se pudo conectar a la base de datos: %s", exc)
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("Error inesperado al conectar a la base de datos: %s", exc)
    return None


def run_query(
    query: str,
    params: Optional[Sequence[object]] = None,
    fetch: str = "all",
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Ejecuta una consulta y devuelve filas y un posible mensaje de error."""
    connection = get_connection()
    if connection is None:
        return None, "No se pudo conectar a la base de datos."

    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch == "one":
                    return cursor.fetchone(), None
                return list(cursor.fetchall()), None
    except psycopg2.Error as exc:
        app.logger.error("Falló la ejecución de la consulta: %s", exc)
        return None, "Ocurrió un problema al consultar la base de datos."
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("Error inesperado durante la consulta: %s", exc)
        return None, "Ocurrió un error inesperado durante la consulta."
    finally:
        connection.close()


FILTER_FIELDS = {
    "producto": "pr.nombre",
    "barrio": "b.nombre_barrio",
    "comercio": "c.nombre_comercio",
}


def build_filter_clause(args: Dict[str, str]) -> Tuple[str, List[object]]:
    conditions: List[str] = []
    params: List[object] = []

    for field, column in FILTER_FIELDS.items():
        value = args.get(field, "").strip()
        if value:
            conditions.append(f"{column} ILIKE %s")
            params.append(f"%{value}%")

    fecha_desde = args.get("fecha_desde", "").strip()
    if fecha_desde:
        conditions.append("p.fecha_captura >= %s")
        params.append(fecha_desde)

    fecha_hasta = args.get("fecha_hasta", "").strip()
    if fecha_hasta:
        conditions.append("p.fecha_captura <= %s")
        params.append(fecha_hasta)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    return where_clause, params


def fetch_price_rows(filters: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    where_clause, params = build_filter_clause(filters)
    query = f"
        SELECT
            p.id_precio,
            pr.nombre AS producto,
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
        {where_clause}
        ORDER BY p.fecha_captura DESC, p.id_precio DESC;
    ""

    rows, error = run_query(query, params)
    return rows or [], error


def fetch_price_summary(filters: Dict[str, str]) -> Tuple[Dict[str, Any], Optional[str]]:
    where_clause, params = build_filter_clause(filters)
    query = f"
        SELECT
            MIN(p.precio_lista) AS precio_minimo,
            MAX(p.precio_lista) AS precio_maximo,
            AVG(p.precio_lista) AS precio_promedio,
            COUNT(*) AS total_precios
        FROM precio AS p
        JOIN producto AS pr ON p.id_producto = pr.id_producto
        JOIN sucursal AS s ON p.id_sucursal = s.id_sucursal
        JOIN barrio AS b ON s.id_barrio = b.id_barrio
        JOIN comercio AS c ON s.id_comercio = c.id_comercio
        {where_clause};
    ""

    summary, error = run_query(query, params, fetch="one")
    return summary or {}, error


@app.route("/")
def index() -> str:
    filters = {
        "producto": request.args.get("producto", ""),
        "barrio": request.args.get("barrio", ""),
        "comercio": request.args.get("comercio", ""),
        "fecha_desde": request.args.get("fecha_desde", ""),
        "fecha_hasta": request.args.get("fecha_hasta", ""),
    }

    precios, price_error = fetch_price_rows(filters)
    resumen, summary_error = fetch_price_summary(filters)
    error = price_error or summary_error

    return render_template(
        "index.html",
        precios=precios,
        resumen=resumen,
        filtros=filters,
        error=error,
    )


@app.route("/api/sucursales")
def api_sucursales() -> Any:
    filters = {
        "producto": request.args.get("producto", ""),
        "barrio": request.args.get("barrio", ""),
        "comercio": request.args.get("comercio", ""),
        "fecha_desde": request.args.get("fecha_desde", ""),
        "fecha_hasta": request.args.get("fecha_hasta", ""),
    }

    where_clause, params = build_filter_clause(filters)

    precio_minimo_expr = "MIN(p.precio_lista) AS precio_minimo" if filters.get("producto") else "NULL AS precio_minimo"

    query = f"
        SELECT
            s.id_sucursal,
            s.nombre_sucursal,
            b.nombre_barrio,
            c.nombre_comercio,
            COUNT(p.id_precio) AS total_precios,
            MAX(p.fecha_captura) AS ultima_captura,
            {precio_minimo_expr}
        FROM sucursal AS s
        JOIN barrio AS b ON s.id_barrio = b.id_barrio
        JOIN comercio AS c ON s.id_comercio = c.id_comercio
        JOIN precio AS p ON p.id_sucursal = s.id_sucursal
        JOIN producto AS pr ON p.id_producto = pr.id_producto
        {where_clause}
        GROUP BY s.id_sucursal, s.nombre_sucursal, b.nombre_barrio, c.nombre_comercio
        HAVING COUNT(p.id_precio) > 0
        ORDER BY ultima_captura DESC, total_precios DESC;
    ""

    rows, error = run_query(query, params)
    if error is not None:
        return jsonify({"error": error}), 500

    return jsonify({"sucursales": rows})


@app.route("/api/precios")
def api_precios() -> Any:
    filters = {
        "producto": request.args.get("producto", ""),
        "barrio": request.args.get("barrio", ""),
        "comercio": request.args.get("comercio", ""),
        "fecha_desde": request.args.get("fecha_desde", ""),
        "fecha_hasta": request.args.get("fecha_hasta", ""),
    }

    precios, price_error = fetch_price_rows(filters)
    resumen, summary_error = fetch_price_summary(filters)
    error = price_error or summary_error
    if error is not None:
        return jsonify({"error": error}), 500

    return jsonify({"precios": precios, "resumen": resumen})


if __name__ == "__main__":
    app.run(debug=True)
