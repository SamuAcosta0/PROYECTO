from typing import List, Optional, Sequence, Tuple

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
    """Create a new PostgreSQL connection using DB_CONFIG or return None on failure."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as exc:
        print(f"Error al conectar a la base de datos: {exc}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error inesperado al conectar: {exc}")
    return None


def run_query(sql: str, params: Optional[Sequence[object]] = None) -> List[Tuple]:
    """Execute a SELECT query and return all rows, handling errors gracefully."""
    rows: List[Tuple] = []
    conn = get_connection()
    if conn is None:
        return rows

    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    except psycopg2.Error as exc:
        print(f"Error al ejecutar la consulta: {exc}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error inesperado al ejecutar la consulta: {exc}")
    finally:
        try:
            cur.close()
        except Exception:  # pylint: disable=broad-except
            pass
        try:
            conn.close()
        except Exception:  # pylint: disable=broad-except
            pass
    return rows


@app.route("/", methods=["GET"])
def index():
    """Render the price listing with optional filters for producto and barrio."""
    producto = request.args.get("producto", "").strip()
    barrio = request.args.get("barrio", "").strip()

    sql = """
        SELECT
            p.id_precio,
            pr.nombre       AS producto,
            pr.marca        AS marca,
            s.nombre        AS sucursal,
            b.nombre_barrio AS barrio,
            c.nombre        AS comercio,
            f.nombre        AS fuente,
            p.precio_lista  AS precio,
            p.fecha_captura AS fecha
        FROM precio p
        JOIN producto pr      ON pr.id_producto = p.id_producto
        JOIN sucursal s       ON s.id_sucursal  = p.id_sucursal
        JOIN barrio b         ON b.id_barrio    = s.id_barrio
        JOIN comercio c       ON c.id_com       = s.id_com
        JOIN fuente_datos f   ON f.id_fuente    = p.id_fuente
        WHERE 1 = 1
    """

    params: List[object] = []
    if producto:
        sql += " AND pr.nombre ILIKE %s"
        params.append(f"%{producto}%")
    if barrio:
        sql += " AND b.nombre_barrio ILIKE %s"
        params.append(f"%{barrio}%")

    sql += " ORDER BY p.fecha_captura DESC"

    rows = run_query(sql, tuple(params))

    resumen = None
    if rows:
        precios = [float(row[7]) for row in rows if row[7] is not None]
        if precios:
            resumen = {
                "minimo": min(precios),
                "maximo": max(precios),
                "promedio": sum(precios) / len(precios),
                "cantidad": len(precios),
            }

    return render_template(
        "index.html",
        rows=rows,
        producto=producto,
        barrio=barrio,
        resumen=resumen,
    )


if __name__ == "__main__":
    app.run(debug=True)
