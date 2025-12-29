from __future__ import annotations

from typing import Optional, Sequence

import psycopg2
from flask import Flask, render_template, request

DB_CONFIG = {
    "dbname": "nexo_precios",
    "user": "TU_USUARIO_AQUI",
    "password": "TU_PASSWORD_AQUI",
    "host": "localhost",
    "port": 5432,
}

app = Flask(__name__)


def get_connection() -> Optional[psycopg2.extensions.connection]:
    """Return a new PostgreSQL connection using DB_CONFIG, or None on failure."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error al conectar a la base de datos: {exc}")
        return None


def run_query(sql: str, params: Optional[Sequence[object]] = None) -> list[tuple]:
    """Execute a SQL query and return all rows; handle errors gracefully."""
    rows: list[tuple] = []
    conn = get_connection()
    if conn is None:
        return rows

    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error al ejecutar la consulta: {exc}")
    finally:
        cur.close()
        conn.close()
    return rows


@app.route("/", methods=["GET"])
def index():
    """Render the main dashboard with optional filters for producto and barrio."""
    producto = request.args.get("producto", "").strip()
    barrio = request.args.get("barrio", "").strip()

    sql = """
        SELECT
            p.id_precio,
            pr.nombre AS producto,
            pr.marca AS marca,
            s.nombre AS sucursal,
            b.nombre_barrio AS barrio,
            c.nombre AS comercio,
            f.nombre AS fuente,
            p.precio_lista AS precio,
            p.fecha_captura AS fecha
        FROM precio p
        JOIN producto pr      ON pr.id_producto = p.id_producto
        JOIN sucursal s       ON s.id_sucursal  = p.id_sucursal
        JOIN barrio b         ON b.id_barrio    = s.id_barrio
        JOIN comercio c       ON c.id_com       = s.id_com
        JOIN fuente_datos f   ON f.id_fuente    = p.id_fuente
        WHERE 1=1
    """

    params: list[object] = []
    if producto:
        sql += " AND pr.nombre ILIKE %s"
        params.append(f"%{producto}%")
    if barrio:
        sql += " AND b.nombre_barrio ILIKE %s"
        params.append(f"%{barrio}%")

    sql += " ORDER BY p.fecha_captura DESC"

    rows = run_query(sql, tuple(params))

    return render_template("index.html", rows=rows, producto=producto, barrio=barrio)


if __name__ == "__main__":
    app.run(debug=True)
