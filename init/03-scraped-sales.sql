-- ============================================================================
-- 3) lots.scraped / lots.detailed flags + per-lot sale history (lot_sales)
-- ============================================================================
-- Idempotent — safe to run on an existing database (also on every `--init-db`).
--
--   scraped  -> the lot row has been persisted from a listing page.
--   detailed -> the lot's detail page was fetched (photos/engine captured).
--   history_captured -> the "Sale history of this VIN" table was parsed off the
--               detail page (so lot_sales is up to date for this lot).
--
-- The detail phase fetches a lot when `NOT (detailed AND history_captured)`.
-- That makes the sale-history backfill a one-time cost: existing rows already
-- have photos but no lot_sales yet, so they get re-detailed once, then skip.

ALTER TABLE lots ADD COLUMN IF NOT EXISTS scraped          boolean NOT NULL DEFAULT false;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS detailed         boolean NOT NULL DEFAULT false;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS history_captured boolean NOT NULL DEFAULT false;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS detail_attempts  integer NOT NULL DEFAULT 0;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS detail_error     text;

COMMENT ON COLUMN lots.scraped          IS 'True once the lot row has been persisted from a listing page.';
COMMENT ON COLUMN lots.detailed         IS 'True once the lot detail page has been fetched.';
COMMENT ON COLUMN lots.history_captured IS 'True once the per-lot sale history was parsed from the detail page.';
COMMENT ON COLUMN lots.detail_attempts  IS 'Consecutive failed detail fetches; reset to 0 on success.';
COMMENT ON COLUMN lots.detail_error     IS 'Last detail-fetch error; NULL after a successful fetch.';

-- Backfill the pre-flag state: everything already stored was scraped; rows with
-- photos had their detail page fetched. history_captured stays false so the next
-- run backfills lot_sales for them exactly once.
UPDATE lots SET scraped = true WHERE NOT scraped;
UPDATE lots
   SET detailed = true
 WHERE NOT detailed
   AND photos IS NOT NULL
   AND photos <> '[]'::jsonb
   AND photos <> '{}'::jsonb;

-- Cheap "what still needs a detail fetch" scan.
CREATE INDEX IF NOT EXISTS lots_detail_todo_idx
    ON lots (id)
    WHERE NOT (detailed AND history_captured);


-- ----------------------------------------------------------------------------
-- lot_sales — one row per recorded sale of a lot, parsed from the detail page's
-- "Sale history of this VIN" table (Date | Platform | Lot # | Final bid | Status).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lot_sales (
    source             text NOT NULL,             -- copart | iaai (lowercased platform)
    lot_number         text NOT NULL,
    sale_date          date NOT NULL,
    vin                text,
    final_bid          int,                        -- final bid for that sale (USD)
    status             text,                       -- Sold | On approval | ...
    auctions_on_record int,                        -- page summary for the VIN
    first_seen         int,                        -- page summary: first year seen
    raw                jsonb,                      -- verbatim parsed row
    ingested_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, lot_number, sale_date)
);

COMMENT ON TABLE  lot_sales                IS 'Per-lot sale history parsed from finalbid.vin detail pages (re-listings of the same lot).';
COMMENT ON COLUMN lot_sales.sale_date      IS 'Auction date of this particular sale.';
COMMENT ON COLUMN lot_sales.final_bid      IS 'Final bid for that sale (USD).';
COMMENT ON COLUMN lot_sales.status         IS 'Result of that sale: Sold | On approval | ...';

CREATE INDEX IF NOT EXISTS lot_sales_vin_idx  ON lot_sales (vin, sale_date DESC);
CREATE INDEX IF NOT EXISTS lot_sales_date_idx ON lot_sales (sale_date DESC);


-- ----------------------------------------------------------------------------
-- refresh_bid_history([vins]) — per-VIN bid_history from lots PLUS lot_sales.
-- ----------------------------------------------------------------------------
-- Each sale is a (vin, source, lot_number, sale_date) event. When a lot's detail
-- page was captured, lot_sales is authoritative for that lot's sales (the lots
-- row is just its latest listing, and its date can differ by a day from the
-- detail table). Only lots with NO captured history fall back to the lots row.
-- lot_sales rows are enriched with title/damage/location from the matching lots
-- row so the JSON keeps the listing detail.
--
--   refresh_bid_history()            -> rebuild every VIN (full sweep)
--   refresh_bid_history(ARRAY[...])  -> rebuild just those VINs (called right
--                                       after a page's lots are detailed)
DROP VIEW IF EXISTS vin_price_history;

-- VINs on finalbid.vin are not always 17 chars (shorter VINs exist), so the
-- original char(17) columns are widened to text. Done here (after the view that
-- depends on lots.vin is dropped, before it is recreated) so it is idempotent
-- and safe to run on an existing database.
ALTER TABLE lots      ALTER COLUMN vin TYPE text;
ALTER TABLE lot_sales ALTER COLUMN vin TYPE text;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'bid_history') THEN
        ALTER TABLE bid_history ALTER COLUMN vin TYPE text;
    END IF;
END $$;

-- 02 defines the zero-arg version; drop it so the defaulted signature below
-- isn't ambiguous when called with no arguments.
DROP FUNCTION IF EXISTS refresh_bid_history();

CREATE OR REPLACE FUNCTION refresh_bid_history(p_vins text[] DEFAULT NULL) RETURNS bigint AS $$
DECLARE
    n bigint;
BEGIN
    WITH src AS (
        -- captured sale history (authoritative for lots we detailed)
        SELECT COALESCE(s.vin, l.vin) AS vin,
               l.id              AS lot_id,
               s.lot_number,
               s.source,
               s.sale_date,
               s.final_bid       AS price,
               l.title_status,
               l.primary_damage,
               l.secondary_damage,
               l.location,
               s.status
          FROM lot_sales s
          LEFT JOIN lots l
                 ON l.source = s.source AND l.lot_number = s.lot_number
         WHERE p_vins IS NULL OR COALESCE(s.vin, l.vin) = ANY(p_vins)
        UNION ALL
        -- lots with no captured history fall back to the lot row itself
        SELECT l.vin,
               l.id,
               l.lot_number,
               l.source,
               l.sale_date,
               l.price_usd,
               l.title_status,
               l.primary_damage,
               l.secondary_damage,
               l.location,
               l.sold_status
          FROM lots l
         WHERE l.vin IS NOT NULL AND l.vin <> ''
           AND (p_vins IS NULL OR l.vin = ANY(p_vins))
           AND NOT EXISTS (
                 SELECT 1 FROM lot_sales s
                  WHERE s.source = l.source AND s.lot_number = l.lot_number)
    )
    , src2 AS (SELECT * FROM src WHERE vin IS NOT NULL AND vin <> '')
    INSERT INTO bid_history AS bh (
        vin, sale_count,
        latest_bid_price, latest_bid_date, latest_lot_id,
        first_bid_price,  first_bid_date,  first_lot_id,
        bid_history
    )
    SELECT
        vin,
        count(*)::int,
        (array_agg(price    ORDER BY sale_date DESC NULLS LAST, lot_id DESC NULLS LAST))[1],
        (array_agg(sale_date ORDER BY sale_date DESC NULLS LAST, lot_id DESC NULLS LAST))[1],
        (array_agg(lot_id   ORDER BY sale_date DESC NULLS LAST, lot_id DESC NULLS LAST))[1],
        CASE WHEN count(*) > 1 THEN
             (array_agg(price    ORDER BY sale_date ASC NULLS LAST, lot_id ASC NULLS LAST))[1] END,
        CASE WHEN count(*) > 1 THEN
             (array_agg(sale_date ORDER BY sale_date ASC NULLS LAST, lot_id ASC NULLS LAST))[1] END,
        CASE WHEN count(*) > 1 THEN
             (array_agg(lot_id   ORDER BY sale_date ASC NULLS LAST, lot_id ASC NULLS LAST))[1] END,
        jsonb_agg(
            jsonb_build_object(
                'lot_id',           lot_id,
                'lot_number',       lot_number,
                'platform',         source,
                'sold_date',        sale_date,
                'price',            price,
                'status',           status,
                'title',            title_status,
                'damage',           primary_damage,
                'secondary_damage', secondary_damage,
                'location',         location
            )
            ORDER BY sale_date ASC NULLS LAST, lot_id ASC NULLS LAST
        )
      FROM src2
     GROUP BY vin
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

    -- garbage-collect dropped VINs only on a full sweep
    IF p_vins IS NULL THEN
        DELETE FROM bid_history bh
         WHERE NOT EXISTS (SELECT 1 FROM lots l WHERE l.vin = bh.vin)
           AND NOT EXISTS (SELECT 1 FROM lot_sales s WHERE s.vin = bh.vin);
    END IF;

    RETURN n;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_bid_history(text[]) IS
  'Rebuild bid_history for the given VINs (or every VIN when called with no args).';

-- Aggregated sale-price view per VIN (now over lots + lot_sales).
CREATE OR REPLACE VIEW vin_price_history AS
SELECT
    vin,
    count(*)              AS sales_count,
    min(sale_date)        AS first_sale,
    max(sale_date)        AS last_sale,
    min(price)            AS min_price,
    max(price)            AS max_price,
    round(avg(price))     AS avg_price,
    (array_agg(price ORDER BY sale_date DESC NULLS LAST))[1] AS latest_price
FROM (
    -- same precedence as refresh_bid_history: captured history wins per lot
    SELECT COALESCE(s.vin, l.vin) AS vin, s.sale_date, s.final_bid AS price
      FROM lot_sales s
      LEFT JOIN lots l ON l.source = s.source AND l.lot_number = s.lot_number
    UNION ALL
    SELECT l.vin, l.sale_date, l.price_usd
      FROM lots l
     WHERE NOT EXISTS (SELECT 1 FROM lot_sales s
                        WHERE s.source = l.source AND s.lot_number = l.lot_number)
) s
WHERE vin IS NOT NULL AND vin <> ''
GROUP BY vin;

COMMENT ON VIEW vin_price_history IS 'Aggregated sale-price history per VIN (from lots + lot_sales).';
