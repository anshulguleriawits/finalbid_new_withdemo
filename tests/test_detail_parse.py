"""Tests for the FinalBid lot-page (detail) parsing.

Runs offline — the lot page is a small synthetic fixture that mirrors the real
markup (a <dt>/<dd> spec list plus an application/ld+json block).

    python -m pytest FinalBid/tests -q
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "scrape_finalbid.py"
_spec = importlib.util.spec_from_file_location("scrape_finalbid", SRC)
s = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(s)


SPECS = [
    ("Sale date", "Oct 13, 2026"),
    ("Location", "Wheeling, IL, USA"),
    ("Sale status", "Sold by Buy now"),
    ("Sale document", "Salvage title"),
    ("Condition", "Run &amp; drive"),
    ("Avg final bid", "$385"),
    ("Est. repair", "$0"),
    ("Actual cash value", "$1,500"),
    ("VIN", "1FADP3F28JL251670"),
    ("Lot #", "Copart 69291546"),
    ("Odometer", "92,047 mi"),
    ("Year", "2018"),
    ("Body color", "Gray"),
    ("Interior color", "Black"),
    ("Engine", "2L · 4 cyl · 160 hp"),
    ("Fuel", "Gas"),
    ("Transmission", "Automatic"),
    ("Drive", "FWD"),
    ("Vehicle type", "Automobile"),
    ("Primary damage", "Front end"),
    ("Secondary damage", "Mechanical"),
    ("Loss type", "Collision"),
    ("Keys", "Yes"),
    ("Airbags", "No"),
    ("Salvage #", "46530509"),
    ("Seller", "TitleMax"),
]

LDJSON = {
    "@context": "https://schema.org",
    "@type": "Vehicle",
    "name": "2018 Ford Focus SE",
    "vehicleIdentificationNumber": "1FADP3F28JL251670",
    "brand": {"@type": "Brand", "name": "Ford"},
    "model": "Focus",
    "modelDate": 2018,
    "image": ["https://finalbid.vin/_img/a.jpg", "https://finalbid.vin/_img/b.jpg"],
    "url": "https://finalbid.vin/en/ford/focus/2018/copart-69291546-1FADP3F28JL251670",
    "bodyType": "Automobile",
    "color": "Gray",
    "vehicleInteriorColor": "Black",
    "driveWheelConfiguration": "FWD",
    "mileageFromOdometer": {"@type": "QuantitativeValue", "value": 92047,
                            "unitCode": "SMI"},
    "fuelType": "Gas",
    "vehicleTransmission": "Automatic",
    "vehicleEngine": {
        "@type": "EngineSpecification",
        "engineDisplacement": {"@type": "QuantitativeValue", "value": 2, "unitCode": "LTR"},
        "enginePower": {"@type": "QuantitativeValue", "value": 160, "unitText": "hp"},
    },
}


def build_page(specs=SPECS, ldjson=LDJSON, odometer=None, sales=None):
    rows = "".join(
        f"<dt>{lab}</dt>\n<dd>{'%s' % (odometer if (lab == 'Odometer' and odometer) else val)}</dd>"
        for lab, val in specs
    )
    # the noisy "Auctions on record" blob that must be ignored as a spec row
    rows += ("<dt>Auctions on record 2026 First seen Date Platform Lot # Final bid "
             "Status Oct 12, 2026</dt><dd>" + "x" * 900 + "</dd>")
    sales = sales if sales is not None else """
        <h2>Sale history of this VIN</h2>
        <dd class="text-ink">5</dd><dt>Auctions on record</dt>
        <table><tr class="border-b border-line">
          <td>Oct 12, 2026</td><td>Copart</td><td>69291546</td>
          <td>$275</td><td>Sold</td></tr></table>
    """
    ld = "" if ldjson is None else (
        '<script type="application/ld+json">%s</script>' % json.dumps(ldjson))
    return (f"<html><head>"
            f'<meta name="description" content="Final bid and sold price: '
            f'Copart lot #69291546, Oct 13, 2026.">'
            f"</head><body><dl>{rows}</dl>{ld}{sales}</body></html>")


def make_lot(**over):
    lot = {
        "id": "copart-69291546", "source": "copart", "lot_number": "69291546",
        "vin": "1FADP3F28JL251670", "year": 2018, "make": "FORD",
        "model": "FOCUS SE", "trim": None, "title_status": None,
        "primary_damage": "", "secondary_damage": None, "odometer": None,
        "odometer_actual": None, "price_usd": None, "sale_date": None,
        "location": "", "photos": [], "url": LDJSON["url"],
    }
    lot.update(over)
    return lot


# ---------------------------------------------------------------------------
# small parsers
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("raw,expected", [
    ("$1,500", 1500), ("$0", 0), ("1,500", 1500), ("", None), (None, None),
])
def test_parse_money(raw, expected):
    assert s.parse_money(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("92,047 mi", (92047, True)),
    ("0 mi · not actual", (0, False)),
    ("0 mi \u00b7 not actual", (0, False)),
    ("122,399 mi", (146000 and 122399, True)),
    ("", (None, None)),
    ("not actual", (None, None)),
])
def test_parse_odometer(raw, expected):
    assert s.parse_odometer(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("Yes", True), ("No", False), ("YES", True), ("maybe", None), (None, None)])
def test_parse_bool_yn(raw, expected):
    assert s.parse_bool_yn(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("Salvage title", "salvage"), ("Clean title", "clean"),
    ("Rebuilt title", "rebuilt"), ("Salvage - Rebuilt", "salvage"),
    ("", None), (None, None)])
def test_norm_title_status(raw, expected):
    assert s.norm_title_status(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("2L · 4 cyl · 160 hp", 4), ("1.4L · 6 cyl · 147 hp", 6),
    ("No engine info", None), (None, None)])
def test_parse_cylinders(raw, expected):
    assert s.parse_cylinders(raw) == expected


# ---------------------------------------------------------------------------
# trim
# ---------------------------------------------------------------------------
def test_trim_from_ldjson():
    assert s.trim_from_ldjson(LDJSON) == "SE"
    assert s.trim_from_ldjson({**LDJSON, "name": "2018 Ford Focus"}) is None


@pytest.mark.parametrize("model,url,expected", [
    ("FOCUS SE", "https://finalbid.vin/en/ford/focus/2018/copart-1-X", "SE"),
    ("GRAND CHEROKEE LAREDO",
     "https://finalbid.vin/en/jeep/grand-cherokee/2011/copart-1-X", "LAREDO"),
    ("F-250 XL", "https://finalbid.vin/en/ford/f-250/2006/copart-1-X", "XL"),
    ("CRUZE 2LT", "https://finalbid.vin/en/chevrolet/cruze/2014/copart-1-X", "2LT"),
    ("RAV4", "https://finalbid.vin/en/toyota/rav4/2016/copart-1-X", None),
    ("FOCUS SE", None, None),
])
def test_extract_trim(model, url, expected):
    assert s.extract_trim(model, url) == expected


# ---------------------------------------------------------------------------
# spec list
# ---------------------------------------------------------------------------
def test_parse_specs_ignores_noise_rows():
    specs = s.parse_specs(build_page())
    assert specs["sale document"] == "Salvage title"
    assert specs["secondary damage"] == "Mechanical"
    assert specs["condition"] == "Run & drive"           # &amp; unescaped
    assert specs["salvage #"] == "46530509"
    assert len(specs) == len(SPECS)                      # blob row dropped


def test_apply_detail_parses_every_field():
    lot = s.apply_detail({"url": LDJSON["url"], "model": "FOCUS SE",
                          "vin": "1FADP3F28JL251670"}, build_page())
    assert lot["trim"] == "SE"
    assert lot["title_status"] == "salvage"
    assert lot["secondary_damage"] == "Mechanical"
    assert lot["primary_damage"] == "Front end"
    assert lot["retail_usd"] == 1500
    assert lot["avg_final_bid"] == 385
    assert lot["est_repair"] == 0
    assert lot["odometer"] == 92047 and lot["odometer_actual"] is True
    assert lot["body_color"] == "Gray"
    assert lot["interior_color"] == "Black"
    assert lot["transmission"] == "Automatic"
    assert lot["drive"] == "FWD"
    assert lot["vehicle_type"] == "Automobile"
    assert lot["condition"] == "Run & drive"
    assert lot["loss_type"] == "Collision"
    assert lot["keys_present"] is True
    assert lot["airbags"] is False
    assert lot["salvage_number"] == "46530509"
    assert lot["seller"] == "TitleMax"
    assert lot["cylinders"] == 4
    assert lot["engine_displacement_l"] == 2
    assert lot["engine_power_hp"] == 160
    assert lot["engine"] == "2L · 4 cyl · 160 hp"
    assert lot["sold_status"] == "Sold by Buy now"
    assert lot["location"] == "Wheeling, IL, USA"
    assert lot["photos"] == LDJSON["image"]
    assert lot["insight"]["retail_usd"] == 1500
    assert lot["detail_specs"]["engine"] == "2L · 4 cyl · 160 hp"
    assert lot["detailed"] is True and lot["history_captured"] is True
    assert lot["auctions_on_record"] == 5
    assert lot["sales"][0]["final_bid"] == 275


def test_apply_detail_not_actual_odometer():
    lot = s.apply_detail({"url": LDJSON["url"], "model": "FOCUS SE"},
                         build_page(odometer="0 mi · not actual"))
    assert lot["odometer"] == 0
    assert lot["odometer_actual"] is False


def test_apply_detail_without_ldjson_still_parses_specs():
    lot = s.apply_detail({"url": LDJSON["url"], "model": "FOCUS SE"},
                         build_page(ldjson=None))
    assert lot["trim"] == "SE"                 # from the URL slug
    assert lot["secondary_damage"] == "Mechanical"
    assert lot["retail_usd"] == 1500
    assert lot["vehicle_type"] == "Automobile"
    assert lot.get("photos") is None       # unset, so it can't clobber `[]`


def test_apply_detail_none_text_is_noop():
    lot = {"url": "u", "detailed": False}
    assert s.apply_detail(lot, None) == {"url": "u", "detailed": False}


def test_parse_archive_sets_trim_and_unknown_odometer_actual():
    card = ('<article><h3><a href="https://finalbid.vin/en/ford/focus/2018/'
            'copart-69291546-1FADP3F28JL251670">2018 FORD FOCUS SE</a></h3>'
            '<span>Front end · 92,047 mi · Wheeling, IL</span>'
            '<span>Clean title</span><span>Oct 13, 2026</span>'
            '<span>Sold</span></article>')
    lots = s.parse_archive(f"<div>{card}</div>")
    assert len(lots) == 1
    assert lots[0]["trim"] == "SE"
    assert lots[0]["odometer_actual"] is None
    assert lots[0]["secondary_damage"] is None


# ---------------------------------------------------------------------------
# sink plumbing
# ---------------------------------------------------------------------------
def test_to_row_matches_column_list():
    row = s.Db._to_row({"id": "x"})
    assert len(row) == len(s.Db.LOTS_COLS) + 1      # + url


def test_to_row_preserves_unknown_odometer_actual():
    row = s.Db._to_row({"id": "x", "odometer_actual": None})
    assert row[s.Db.LOTS_COLS.index("odometer_actual")] is None
    row = s.Db._to_row({"id": "x", "odometer_actual": False})
    assert row[s.Db.LOTS_COLS.index("odometer_actual")] is False


def test_assign_sql_never_blanks_a_column():
    assert "COALESCE" in s.Db._assign_sql("trim")
    assert "COALESCE" in s.Db._assign_sql("secondary_damage")
    assert "COALESCE" in s.Db._assign_sql("retail_usd")
    assert "COALESCE" in s.Db._assign_sql("price_usd")
    assert "COALESCE" in s.Db._assign_sql("location")
    assert "OR EXCLUDED.detailed" in s.Db._assign_sql("detailed")
    assert "OR EXCLUDED.history_captured" in s.Db._assign_sql("history_captured")
    assert "NULLIF(EXCLUDED.photos" in s.Db._assign_sql("photos")
