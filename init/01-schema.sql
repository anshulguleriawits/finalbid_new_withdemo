-- Schema for the AuctionHistory dataset.
-- Runs automatically on first container start (mounted into
-- /docker-entrypoint-initdb.d). Fields mirror the auctionhistory.com JSON API.
--
-- Raw photos[] and insight{} are kept as jsonb so nothing is lost; the hot
-- columns are promoted to typed columns for filtering/indexing.
-- The human-readable "adapted" names live in column comments.

CREATE TABLE IF NOT EXISTS lots (
    id                 text PRIMARY KEY,              -- "iaai-46287854"
    source             text NOT NULL,                 -- copart | iaai
    lot_number         text,
    vin                text,
    year               int,
    make               text,                          -- raw, uppercase e.g. HONDA
    model              text,
    trim               text,
    title_status       text,                          -- clean | salvage | rebuilt | null
    primary_damage     text,
    secondary_damage   text,
    odometer           int,
    odometer_actual    boolean,
    price_usd          int,                           -- final sale price
    sale_date          date,
    location           text,
    flagged            boolean DEFAULT false,
    photos             jsonb DEFAULT '[]'::jsonb,     -- relative paths
    created_at         timestamptz,
    public_token       text,
    engine             text,
    fuel               text,
    sold_status        text,
    insight            jsonb,                         -- full market-insight block
    retail_usd         int,                           -- promoted from insight.retail_usd
    suspect_price      boolean DEFAULT false,

    -- Full source record exactly as returned by the auctionhistory.com API -
    -- nothing dropped (photos[], insight{}, trim, created_at, any future fields).
    raw                jsonb,

    -- Public lot page URL on auctionhistory.com (see lot_url() below)
    url                text,

    -- extras
    ingested_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT lots_source_chk CHECK (source IN ('copart', 'iaai'))
);

COMMENT ON TABLE  lots IS 'Sold vehicle auction lots aggregated from Copart & IAAI (via auctionhistory.com API).';
COMMENT ON COLUMN lots.odometer        IS 'Odometer miles.';
COMMENT ON COLUMN lots.odometer_actual IS 'True when the odometer reading is declared "actual".';
COMMENT ON COLUMN lots.price_usd       IS 'Final sale price in USD.';
COMMENT ON COLUMN lots.retail_usd      IS 'Estimated clean retail value (from insight).';
COMMENT ON COLUMN lots.url             IS 'Public lot page URL on auctionhistory.com (derived from make/model/vin).';

-- Slot slug helper: 'LEXUS' + 'TX 500H' -> 'lexus-tx-500h'
CREATE OR REPLACE FUNCTION lot_slug(p_make text, p_model text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT btrim(
           regexp_replace(
             lower(regexp_replace(trim(coalesce(p_make,'') || ' ' || coalesce(p_model,'')),
                                    '[^a-zA-Z0-9]+', '-', 'g')),
             '^-+', '', 'g'), '-')
$$;

-- Public lot page URL: https://auctionhistory.com/{slug}-vin-{vin}
CREATE OR REPLACE FUNCTION lot_url(p_make text, p_model text, p_vin text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(
           'https://auctionhistory.com/' || lot_slug(p_make, p_model)
                                    || '-vin-' || lower(p_vin),
           '-vin-')
$$;

COMMENT ON FUNCTION lot_url(text,text,text) IS
  'Public lot page URL on auctionhistory.com, e.g. lot_url(''LEXUS'',''TX 500H'',''5TDABAB64SS010648'')';

CREATE INDEX IF NOT EXISTS lots_url_idx ON lots (url);

-- Idempotent upsert target for the scraper.
CREATE UNIQUE INDEX IF NOT EXISTS lots_source_lotno_key ON lots (source, lot_number);

-- Common query paths
CREATE INDEX IF NOT EXISTS lots_vin_idx       ON lots (vin);
CREATE INDEX IF NOT EXISTS lots_sale_date_idx ON lots (sale_date DESC);
CREATE INDEX IF NOT EXISTS lots_make_idx      ON lots (make);
CREATE INDEX IF NOT EXISTS lots_model_idx     ON lots (model);
CREATE INDEX IF NOT EXISTS lots_price_idx     ON lots (price_usd);
CREATE INDEX IF NOT EXISTS lots_created_idx   ON lots (created_at DESC);

-- Case-insensitive text search helper (VIN / lot / make / model).
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS lots_make_trgm_idx  ON lots USING gin (make  gin_trgm_ops);
CREATE INDEX IF NOT EXISTS lots_model_trgm_idx ON lots USING gin (model gin_trgm_ops);

-- GIN index for querying the raw JSON document (jsonb_path_ops for @> / ? ops).
-- Disabled: nothing in the scraper/export queries `raw` with @> or ?, and it
-- costs 396 MB + a rewrite on every lots upsert. See init/05-bid-history-tuning.sql
-- to re-enable it if ad-hoc JSON search is ever needed.
-- CREATE INDEX IF NOT EXISTS lots_raw_gin_idx ON lots USING gin (raw jsonb_path_ops);

-- Optional: keep only the newest row per VIN handy for history lookups.
CREATE INDEX IF NOT EXISTS lots_vin_date_idx ON lots (vin, sale_date DESC);
