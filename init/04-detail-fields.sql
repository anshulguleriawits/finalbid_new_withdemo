-- ============================================================================
-- 4) Full detail-page fields on `lots`
-- ============================================================================
-- Idempotent — safe to run on an existing database (also on every `--init-db`).
--
-- The lot page (finalbid.vin/en/<make>/<model>/<year>/<lot>) renders a
-- <dt>/<dd> spec list. Earlier versions only promoted photos/engine/fuel to
-- columns and dropped the rest, so trim / secondary_damage / retail_usd and a
-- whole set of fields stayed empty on every row. These columns capture what the
-- page actually shows.
--
--   sale document      -> title_status        (already existed)
--   sale status        -> sold_status         (already existed)
--   actual cash value  -> retail_usd          (already existed, was never filled)
--   odometer           -> odometer / odometer_actual ("… · not actual")
--   vehicle type       -> vehicle_type
--   everything else    -> its own column below, plus the verbatim map in
--                         `detail_specs` so nothing the page shows is lost.

ALTER TABLE lots ADD COLUMN IF NOT EXISTS condition            text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS avg_final_bid        int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS est_repair           int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS body_color           text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS interior_color       text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS transmission         text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS drive                text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS vehicle_type         text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS loss_type            text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS keys_present         boolean;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS airbags              boolean;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS salvage_number       text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS seller               text;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS cylinders            int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS engine_displacement_l numeric(4,2);
ALTER TABLE lots ADD COLUMN IF NOT EXISTS engine_power_hp      int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS auctions_on_record   int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS first_seen           int;
ALTER TABLE lots ADD COLUMN IF NOT EXISTS detail_specs         jsonb;

COMMENT ON COLUMN lots.condition            IS 'Lot page "Condition" (e.g. Run & drive, Enhanced, Engine starts).';
COMMENT ON COLUMN lots.avg_final_bid        IS 'Lot page "Avg final bid" (USD, same make/model cohort).';
COMMENT ON COLUMN lots.est_repair           IS 'Lot page "Est. repair" (USD).';
COMMENT ON COLUMN lots.body_color           IS 'Lot page "Body color".';
COMMENT ON COLUMN lots.interior_color       IS 'Lot page "Interior color" (ld+json vehicleInteriorColor).';
COMMENT ON COLUMN lots.transmission         IS 'Lot page "Transmission".';
COMMENT ON COLUMN lots.drive                IS 'Lot page "Drive" (FWD/RWD/AWD).';
COMMENT ON COLUMN lots.vehicle_type         IS 'Lot page "Vehicle type" (ld+json bodyType).';
COMMENT ON COLUMN lots.loss_type            IS 'Lot page "Loss type" (IAAI lots).';
COMMENT ON COLUMN lots.keys_present         IS 'Lot page "Keys" (Yes/No).';
COMMENT ON COLUMN lots.airbags              IS 'Lot page "Airbags" (Yes/No, IAAI lots).';
COMMENT ON COLUMN lots.salvage_number       IS 'Lot page "Salvage #" (IAAI lots).';
COMMENT ON COLUMN lots.seller               IS 'Lot page "Seller".';
COMMENT ON COLUMN lots.cylinders            IS 'Cylinder count parsed from the lot page "Engine" string.';
COMMENT ON COLUMN lots.engine_displacement_l IS 'Engine displacement in litres (ld+json vehicleEngine.engineDisplacement).';
COMMENT ON COLUMN lots.engine_power_hp      IS 'Engine power in hp (ld+json vehicleEngine.enginePower).';
COMMENT ON COLUMN lots.auctions_on_record   IS 'Lot page "Auctions on record" count for this VIN.';
COMMENT ON COLUMN lots.first_seen           IS 'Lot page "First seen" year for this VIN.';
COMMENT ON COLUMN lots.detail_specs         IS 'Verbatim {label: value} map of every <dt>/<dd> row on the lot page.';
