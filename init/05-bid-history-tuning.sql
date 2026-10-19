-- ============================================================================
-- 5) bid_history / lots write-path tuning
-- ============================================================================
-- Idempotent — safe to run on an existing database (also on every `--init-db`).
--
-- Why: running several scrapers at once made every process stall for minutes.
-- Each one called `refresh_bid_history()` (no args) every BID_HISTORY_MINUTES,
-- i.e. a FULL rewrite of every VIN. That is one giant transaction that takes
-- row locks on all ~294k rows, so the sweeps queued behind each other and every
-- per-page refresh / load had to wait:
--
--   pid 69238  Lock/tuple  342s  <- waiting
--   pid 69318  Lock/tuple  277s  <- waiting
--   pid 69326  Lock/tuple  235s  <- waiting
--   pid 69251  IO           207s  <- actually working
--
-- 294,457 row updates per 30s on a 294,505-row table = one full sweep every
-- 30 seconds, continuously (n_tup_upd reached 82M, n_dead_tup 1.47M).
--
-- Three changes fix the contention:
--   1. only one full sweep may run at a time (advisory lock, others return 0);
--   2. unchanged rows are not rewritten (ON CONFLICT ... WHERE IS DISTINCT FROM),
--      so a sweep with nothing new touches almost nothing;
--   3. the targeted filter is index-friendly (`s.vin = ANY(...)` instead of
--      `COALESCE(s.vin, l.vin) = ANY(...)`, which forced a seq scan of
--      lot_sales on every call).
-- Plus autovacuum is made more aggressive on bid_history and two never-scanned
-- GIN indexes are dropped (625 MB of pure write amplification).

-- ----------------------------------------------------------------------------
-- refresh_bid_history([vins])
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_bid_history(p_vins text[] DEFAULT NULL) RETURNS bigint AS $$
DECLARE
    n bigint;
BEGIN
    -- Locking. Several scrapers refresh at once, and a full sweep rewrites every
    -- VIN in one transaction, so without this they queue behind each other and
    -- deadlock (observed: DeadlockDetected between a sweep and a per-page
    -- refresh).
    --
    -- Both kinds try the lock WITHOUT waiting:
    --   * a full sweep that cannot get it returns -1 - another process is
    --     already sweeping every VIN, so this one has nothing to add;
    --   * a targeted refresh that cannot get it returns -1 - a sweep is running
    --     and that sweep covers these VINs too, so the work is already being
    --     done. Waiting here is what made per-page refreshes hang for minutes.
    -- -1 (not 0) tells the caller "skipped", so it can retry later instead of
    -- assuming the VINs were refreshed.
    IF p_vins IS NULL THEN
        IF NOT pg_try_advisory_xact_lock(hashtext('refresh_bid_history')) THEN
            RETURN -1;
        END IF;
    ELSE
        IF NOT pg_try_advisory_xact_lock_shared(hashtext('refresh_bid_history')) THEN
            RETURN -1;
        END IF;
        -- Deterministic row-lock order, so two overlapping targeted refreshes
        -- cannot deadlock on each other either.
        p_vins := ARRAY(SELECT DISTINCT unnest(p_vins) ORDER BY 1);
    END IF;

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
         WHERE p_vins IS NULL
            OR s.vin = ANY(p_vins)
            OR (s.vin IS NULL AND l.vin = ANY(p_vins))
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
     ORDER BY vin
    ON CONFLICT (vin) DO UPDATE SET
        sale_count       = EXCLUDED.sale_count,
        latest_bid_price = EXCLUDED.latest_bid_price,
        latest_bid_date  = EXCLUDED.latest_bid_date,
        latest_lot_id    = EXCLUDED.latest_lot_id,
        first_bid_price  = EXCLUDED.first_bid_price,
        first_bid_date   = EXCLUDED.first_bid_date,
        first_lot_id     = EXCLUDED.first_lot_id,
        bid_history      = EXCLUDED.bid_history,
        updated_at       = now()
    -- Skip rows that did not change. Without this every sweep rewrote all
    -- ~294k rows (and all of their indexes) even when nothing new was scraped,
    -- which is what kept autovacuum permanently behind.
    WHERE (bh.sale_count, bh.latest_bid_price, bh.latest_bid_date, bh.latest_lot_id,
           bh.first_bid_price, bh.first_bid_date, bh.first_lot_id, bh.bid_history)
          IS DISTINCT FROM
          (EXCLUDED.sale_count, EXCLUDED.latest_bid_price, EXCLUDED.latest_bid_date,
           EXCLUDED.latest_lot_id, EXCLUDED.first_bid_price, EXCLUDED.first_bid_date,
           EXCLUDED.first_lot_id, EXCLUDED.bid_history);

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
  'Rebuild bid_history for the given VINs (or every VIN when called with no args). '
  'Full sweeps are serialised by an advisory lock and skip unchanged rows.';

-- ----------------------------------------------------------------------------
-- Autovacuum: bid_history is rewritten far more than any other table, so the
-- default scale_factor 0.2 lets ~59k dead tuples pile up before a pass even
-- starts. Reclaim much sooner and give the worker a bigger budget.
-- ----------------------------------------------------------------------------
ALTER TABLE bid_history SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit    = 2000,
    autovacuum_vacuum_cost_delay    = 1
);
ALTER TABLE lots SET (
    autovacuum_vacuum_scale_factor  = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

-- ----------------------------------------------------------------------------
-- Never-scanned GIN indexes (pg_stat_user_indexes.idx_scan = 0) that are
-- rebuilt on every upsert: 396 MB (lots.raw) + 229 MB (bid_history.bid_history).
-- Nothing in scrape_finalbid.py / export_csv.py / run_scraper.py queries these
-- with @> or ?, so they are pure write cost.
--
-- NOT dropped automatically: DROP INDEX takes ACCESS EXCLUSIVE on the table and
-- cannot use CONCURRENTLY inside this script's transaction, so on a live scrape
-- it would block writers. They have been dropped on the live DB already; on a
-- new database run these once by hand, outside a transaction:
--
--   DROP INDEX CONCURRENTLY IF EXISTS lots_raw_gin_idx;
--   DROP INDEX CONCURRENTLY IF EXISTS bid_history_json_gin_idx;
--   VACUUM (ANALYZE) bid_history;
--
-- init/01 and init/02 no longer create them, so `--init-db` will not bring them
-- back. Recreate them if ad-hoc JSON search is ever needed:
--   CREATE INDEX CONCURRENTLY lots_raw_gin_idx ON lots USING gin (raw jsonb_path_ops);
--   CREATE INDEX CONCURRENTLY bid_history_json_gin_idx ON bid_history USING gin (bid_history);
-- ----------------------------------------------------------------------------
