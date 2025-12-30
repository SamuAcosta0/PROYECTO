-- Schema for NEXO Google Places sweep
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS stores (
    store_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name TEXT NOT NULL,
    chain TEXT NULL,
    address TEXT NULL,
    barrio_id UUID NULL,
    geom geometry(Point, 4326) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS store_external_ids (
    store_id UUID REFERENCES stores(store_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    UNIQUE(source, external_id)
);

CREATE TABLE IF NOT EXISTS store_snapshots_google (
    external_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    formatted_address TEXT NOT NULL,
    primary_type TEXT NULL,
    types TEXT[] NULL,
    google_location geometry(Point, 4326) NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    raw_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_snapshots_google_fetched_at ON store_snapshots_google(fetched_at);
