# NEXO - Google Places Market Sweep

Herramienta CLI en Python para barrer supermercados y almacenes de Montevideo usando Google Places API (New) y persistir resultados en PostgreSQL + PostGIS.

## Requisitos
- Python 3.11+
- PostgreSQL 14+ con PostGIS
- Variables de entorno:
  - `GOOGLE_PLACES_API_KEY`
  - `DATABASE_URL` (por ejemplo `postgres://user:pass@localhost:5432/nexo`)

Instala dependencias:

```bash
pip install -r requirements.txt
```

## Esquema de base de datos
DDL en `migrations/001_create_tables.sql`:
- `stores`
- `store_external_ids`
- `store_snapshots_google`

Ejecuta la migración (requiere `psql`):
```bash
psql "$DATABASE_URL" -f migrations/001_create_tables.sql
```

## Uso
Barrido principal en grilla (Montevideo por defecto):
```bash
python -m src.places_sweep run --step-km 2.0 --radius-m 1500 --sleep 0.1
```

Refresco de snapshots expirados (TTL 30 días por defecto):
```bash
python -m src.places_sweep refresh --ttl-days 30
```

## Detalles de diseño
- Deduplicación por `place.id` vía `store_external_ids` (idempotente).
- Se almacena la geometría de Google solo en `store_snapshots_google.google_location` con `fetched_at` para TTL de 30 días.
- Manejo de rate limit/respuestas 5xx con backoff exponencial + jitter en el cliente de Google.
- El barrido cubre el bounding box de Montevideo (lat -34.95/-34.80, lon -56.30/-56.05) con grilla configurable (`step_km`).
