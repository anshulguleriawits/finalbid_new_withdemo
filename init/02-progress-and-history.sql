-- Additional tables: scrape progress tracking + per-vehicle bid history (derived from lots).
-- Idempotent — safe to run on an existing database.

-- ============================================================================
-- 1) scrape_pages — one row per (filter set, page number) that has been fetched
-- ============================================================================
-- Lets a crawl resume exactly where it stopped and lets several filter sets
-- (e.g. per-make shards) be tracked side by side. filter_key is a stable
-- signature of the query (e.g. md5 of the sorted filter params) so the same
-- logical crawl keeps the same key across runs.

CREATE TABLE IF NOT EXISTS scrape_pages (
    filter_key   text        NOT NULL,            -- stable hash/signature of the filter set
    filters      jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- the actual query params
    page         int         NOT NULL CHECK (page >= 1),
    per_page     int         NOT NULL DEFAULT 100,

    status       text        NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending','in_progress','done','empty','failed')),

    total        int,                             -- envelope total on first fetch
    lots_found   int         NOT NULL DEFAULT 0,  -- records in the response
    lots_written int         NOT NULL DEFAULT 0,  -- records persisted to lots
    attempts     int         NOT NULL DEFAULT 0,
    last_error   text,

    started_at   timestamptz,
    finished_at  timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (filter_key, page)
);

COMMENT ON TABLE  scrape_pages            IS 'Per-page crawl checkpoint: which pages of which filter set are fetched/persisted.';
COMMENT ON COLUMN scrape_pages.filter_key IS 'Stable signature of the filter set (e.g. md5 of sorted params); identifies a logical crawl.';
COMMENT ON COLUMN scrape_pages.status     IS 'pending | in_progress | done | empty (past end of results) | failed';
COMMENT ON COLUMN scrape_pages.total      IS 'Envelope "total" seen for this filter set (constant across pages).';

-- Fast "what is left to do / what failed" scans.
CREATE INDEX IF NOT EXISTS scrape_pages_todo_idx
    ON scrape_pages (filter_key, status)
    WHERE status IN ('pending','in_progress','failed');

-- Keep updated_at honest on any UPDATE.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS scrape_pages_touch ON scrape_pages;
CREATE TRIGGER scrape_pages_touch
    BEFORE UPDATE ON scrape_pages
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Handy progress view: how far each filter-set crawl has got.
CREATE OR REPLACE VIEW scrape_progress AS
SELECT
    filter_key,
    count(*)                                              AS pages_total,
    count(*) FILTER (WHERE status = 'done')               AS pages_done,
    count(*) FILTER (WHERE status = 'empty')              AS pages_empty,
    count(*) FILTER (WHERE status = 'failed')             AS pages_failed,
    count(*) FILTER (WHERE status IN ('pending','in_progress')) AS pages_remaining,
    max(total)                                            AS total_lots,
    sum(lots_written)                                     AS lots_written,
    min(started_at)                                       AS started_at,
    max(finished_at)                                      AS last_finished_at
FROM scrape_pages
GROUP BY filter_key;

COMMENT ON VIEW scrape_progress IS 'Aggregated crawl progress per filter_key.';


-- ============================================================================
-- 2) bid_history — ONE ROW PER VEHICLE (vin), derived from lots
-- ============================================================================
-- * Primary key is the VIN taken from lots.
-- * A vehicle appears once per sale in lots. sale_count > 1 => it has a real
--   bid/sale history.
-- * first_bid_* are populated ONLY when sale_count > 1 (a genuine earlier sale
--   exists). For a single-sale vehicle the first_bid_* columns stay NULL and
--   only the latest_* values are kept.
-- * bid_history is the full array of that vehicle's sales as JSON, oldest first,
--   each entry: lot_id, lot_number, platform (copart|iaai), sold_date, price,
--   title, damage, secondary_damage, location.

DROP VIEW IF EXISTS vin_price_history;
DROP TABLE IF EXISTS bid_history;

CREATE TABLE bid_history (
    vin              text PRIMARY KEY,

    sale_count       int  NOT NULL DEFAULT 1,
    has_bid_history  boolean GENERATED ALWAYS AS (sale_count > 1) STORED,

    -- latest (always present)
    latest_bid_price int,
    latest_bid_date  date,
    latest_lot_id    text REFERENCES lots(id) ON DELETE SET NULL,

    -- first (NULL unless there is a genuine earlier sale)
    first_bid_price  int,
    first_bid_date   date,
    first_lot_id     text REFERENCES lots(id) ON DELETE SET NULL,

    -- every sale for this VIN, oldest first, verbatim JSON
    bid_history      jsonb NOT NULL DEFAULT '[]'::jsonb,

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  bid_history           IS 'Per-vehicle sale/bid summary (one row per VIN), derived from lots.';
COMMENT ON COLUMN bid_history.vin      IS 'VIN from lots; PK.';
COMMENT ON COLUMN bid_history.sale_count     IS 'Number of recorded sales for this VIN (1 = no bid history).';
COMMENT ON COLUMN bid_history.has_bid_history IS 'True when the VIN sold more than once (sale_count > 1).';
COMMENT ON COLUMN bid_history.latest_bid_price IS 'Most recent sale price (USD).';
COMMENT ON COLUMN bid_history.latest_bid_date  IS 'Most recent sale date.';
COMMENT ON COLUMN bid_history.first_bid_price  IS 'Earliest sale price; NULL for single-sale vehicles.';
COMMENT ON COLUMN bid_history.first_bid_date   IS 'Earliest sale date; NULL for single-sale vehicles.';
COMMENT ON COLUMN bid_history.bid_history      IS 'JSON array of all this vehicle''s sales (oldest first): lot_id, lot_number, platform, sold_date, price, title, damage, secondary_damage, location.';

CREATE INDEX IF NOT EXISTS bid_history_latest_idx  ON bid_history (latest_bid_date DESC);
CREATE INDEX IF NOT EXISTS bid_history_first_idx   ON bid_history (first_bid_date DESC);
CREATE INDEX IF NOT EXISTS bid_history_multi_idx   ON bid_history (vin) WHERE sale_count > 1;
-- Disabled: nothing queries bid_history with @> / ?, and it costs 229 MB + a
-- rewrite on every bid_history update. See init/05-bid-history-tuning.sql.
-- CREATE INDEX IF NOT EXISTS bid_history_json_gin_idx ON bid_history USING gin (bid_history);

DROP TRIGGER IF EXISTS bid_history_touch ON bid_history;
CREATE TRIGGER bid_history_touch
    BEFORE UPDATE ON bid_history
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- Rebuild bid_history from lots. Idempotent: upserts every VIN present in lots
-- and removes VINs that no longer have any lots.
CREATE OR REPLACE FUNCTION refresh_bid_history() RETURNS bigint AS $$
DECLARE
    n bigint;
BEGIN
    INSERT INTO bid_history AS bh (
        vin, sale_count,
        latest_bid_price, latest_bid_date, latest_lot_id,
        first_bid_price,  first_bid_date,  first_lot_id,
        bid_history
    )
    SELECT
        l.vin,
        count(*)::int,
        -- latest (newest sale; tie-break on created_at then id for determinism)
        (array_agg(l.price_usd ORDER BY l.sale_date DESC NULLS LAST,
                                    l.created_at  DESC NULLS LAST, l.id DESC))[1],
        (array_agg(l.sale_date   ORDER BY l.sale_date DESC NULLS LAST,
                                    l.created_at  DESC NULLS LAST, l.id DESC))[1],
        (array_agg(l.id          ORDER BY l.sale_date DESC NULLS LAST,
                                    l.created_at  DESC NULLS LAST, l.id DESC))[1],
        -- first: only when the VIN genuinely sold more than once
        CASE WHEN count(*) > 1 THEN
             (array_agg(l.price_usd ORDER BY l.sale_date ASC NULLS LAST,
                                        l.created_at  ASC NULLS LAST, l.id ASC))[1] END,
        CASE WHEN count(*) > 1 THEN
             (array_agg(l.sale_date   ORDER BY l.sale_date ASC NULLS LAST,
                                        l.created_at  ASC NULLS LAST, l.id ASC))[1] END,
        CASE WHEN count(*) > 1 THEN
             (array_agg(l.id          ORDER BY l.sale_date ASC NULLS LAST,
                                        l.created_at  ASC NULLS LAST, l.id ASC))[1] END,
        -- every sale, oldest first
        jsonb_agg(
            jsonb_build_object(
                'lot_id',          l.id,
                'lot_number',      l.lot_number,
                'platform',        l.source,          -- copart | iaai
                'sold_date',       l.sale_date,
                'price',           l.price_usd,
                'title',           l.title_status,
                'damage',          l.primary_damage,
                'secondary_damage',l.secondary_damage,
                'location',        l.location
            )
            ORDER BY l.sale_date ASC NULLS LAST, l.created_at ASC NULLS LAST, l.id ASC
        )
    FROM lots l
    WHERE l.vin IS NOT NULL AND l.vin <> ''
    GROUP BY l.vin
    ON CONFLICT (vin) DO UPDATE SET
        sale_count       = EXCLUDED.sale_count,
        latest_bid_price = EXCLUDED.latest_bid_price,
        latest_bid_date  = EXCLUDED.latest_bid_date,
        latest_lot_id    = EXCLUDED.latest_lot_id,
        first_bid_price  = EXCLUDED.first_bid_price,
        first_bid_date   = EXCLUDED.first_bid_date,
        first_lot_id     = EXCLUDED.first_lot_id,
        bid_history      = EXCLUDED.bid_history,
        updated_at       = now();

    GET DIAGNOSTICS n = ROW_COUNT;

    -- drop VINs that no longer exist in lots
    DELETE FROM bid_history bh
    WHERE NOT EXISTS (SELECT 1 FROM lots l WHERE l.vin = bh.vin);

    RETURN n;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_bid_history() IS 'Rebuild bid_history from lots: one row per VIN with latest/first sale and the full JSON bid array.';


-- Convenience: aggregated sale-price view per VIN (over lots).
CREATE OR REPLACE VIEW vin_price_history AS
SELECT
    vin,
    count(*)                        AS sales_count,
    min(sale_date)                  AS first_sale,
    max(sale_date)                  AS last_sale,
    min(price_usd)                  AS min_price,
    max(price_usd)                  AS max_price,
    round(avg(price_usd))           AS avg_price,
    (array_agg(price_usd ORDER BY sale_date DESC))[1] AS latest_price
FROM lots
WHERE vin IS NOT NULL
GROUP BY vin;

COMMENT ON VIEW vin_price_history IS 'Aggregated sale-price history per VIN (from lots).';