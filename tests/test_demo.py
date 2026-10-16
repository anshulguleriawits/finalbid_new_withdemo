"""Smoke tests for demo mode (offline: no network, no database).

    python -m pytest tests -q
"""
from __future__ import annotations

import importlib.util
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


demo = _load("demo")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
def test_archive_fixture_is_parsed_into_lots():
    rec = demo.make_lot_record(0, random.Random(1))
    html = demo.make_archive_page([rec], random.Random(1))
    lots = demo.sf.parse_archive(html)
    assert len(lots) == 1
    assert lots[0]["id"] == f"{rec['source']}-{rec['lot_number']}"
    assert lots[0]["vin"] == rec["vin"]
    assert lots[0]["year"] == rec["year"]


def test_unparseable_card_is_skipped_not_written():
    """i % 61 produces a href with no copart/iaai key — it must not yield a lot."""
    rec = demo.make_lot_record(60, random.Random(2))   # (i + 1) % 61 == 0
    html = demo.make_archive_page([rec], random.Random(2))
    assert demo.sf.parse_archive(html) == []


def test_short_vin_is_preserved():
    rec = demo.make_lot_record(36, random.Random(3))   # (i + 1) % 37 == 0
    assert len(rec["vin"]) == 14
    html = demo.make_archive_page([rec], random.Random(3))
    assert demo.sf.parse_archive(html)[0]["vin"] == rec["vin"]


def test_detail_fixture_fills_the_specs():
    rec = demo.make_lot_record(1, random.Random(4))
    page = demo.make_detail_page(rec, random.Random(4))
    lot = demo.sf.apply_detail({"url": rec["url"], "model": rec["model"],
                               "vin": rec["vin"]}, page)
    assert lot["detailed"] is True and lot["history_captured"] is True
    assert lot["engine"] and lot["cylinders"] == 4
    assert lot["body_color"] and lot["interior_color"]
    assert lot["title_status"] in ("clean", "salvage", "rebuilt")
    assert isinstance(lot["retail_usd"], int)


def test_detail_without_ldjson_still_parses():
    """i % 23 == 0 omits the ld+json block; specs + URL slug must still work."""
    rec = demo.make_lot_record(22, random.Random(5))   # (i + 1) % 23 == 0
    page = demo.make_detail_page(rec, random.Random(5))
    assert "ld+json" not in page
    lot = demo.sf.apply_detail({"url": rec["url"], "model": rec["model"]}, page)
    assert lot["detailed"] is True
    assert lot["trim"] == rec["trim_hint"]
    assert lot.get("photos") is None       # unset, so a re-walk cannot clobber []


def test_sale_history_rows_are_produced():
    rec = demo.make_lot_record(3, random.Random(6))   # (i + 1) % 4 == 0
    page = demo.make_detail_page(rec, random.Random(6))
    lot = demo.sf.apply_detail({"url": rec["url"], "model": rec["model"],
                               "vin": rec["vin"]}, page)
    assert len(lot["sales"]) >= 2
    assert lot["auctions_on_record"] >= len(lot["sales"])


# ---------------------------------------------------------------------------
# filled()
# ---------------------------------------------------------------------------
def test_filled_counts_zero_as_a_real_value():
    assert demo.filled(0) is True
    assert demo.filled(4250) is True
    assert demo.filled(False) is True       # keys_present = No is a real answer
    assert demo.filled(None) is False
    assert demo.filled("") is False
    assert demo.filled([]) is False          # no ld+json -> no photos
    assert demo.filled(["a.jpg"]) is True


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------
def test_run_demo_writes_jsonl(tmp_path, capsys):
    out = tmp_path / "demo.jsonl"
    rc = demo.run_offline(pages=1, per_page=6, detail=True, seed=3,
                          fail_every=0, show=0, out_path=str(out))
    assert rc == 0
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert rows
    # every row must have a usable id; the parser skips unparseable cards
    assert all(r["id"] and "-" in r["id"] for r in rows)
    assert any(r.get("engine") for r in rows)
    assert "nothing was written to a database" in capsys.readouterr().out


def test_run_demo_with_simulated_failures(capsys):
    rc = demo.run_offline(pages=1, per_page=24, detail=True, seed=3,
                          fail_every=5, show=0, out_path=None)
    assert rc == 0
    assert "simulated failures" in capsys.readouterr().out


def test_run_demo_listing_only(capsys):
    rc = demo.run_offline(pages=1, per_page=5, detail=False, seed=3,
                          fail_every=0, show=0, out_path=None)
    assert rc == 0
    assert "detail phase skipped" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# live mode: the scraper CLI must carry no database flags
# ---------------------------------------------------------------------------
def test_live_mode_disables_every_db_flag(monkeypatch, tmp_path):
    """Demo mode is "scrape for real, no DB" — assert that at the CLI level."""
    seen = {}
    out = tmp_path / "live.jsonl"
    row = json.dumps({"id": "copart-1", "source": "copart", "vin": "X",
                      "year": 2020, "make": "FORD", "model": "FOCUS",
                      "detailed": True})

    def fake_call(argv):
        # stand in for the scraper: append the rows it would have written
        seen["argv"] = argv
        out.write_text(row + "\n", encoding="utf-8")
        return 0

    monkeypatch.setattr(demo.subprocess, "call", fake_call)
    rc = demo.run_live(limit=5, detail=True, out_path=str(out),
                       concurrency=4, spacing=0.2, show=0, start_page=0)
    assert rc == 0
    argv = seen["argv"]
    assert "--load-db" not in argv
    assert "--track-pages" not in argv
    assert "--refresh-bid-history" not in argv
    assert "--dsn" not in argv
    assert argv[argv.index("--limit") + 1] == "5"
    assert argv[argv.index("--out") + 1] == str(out)
    assert seen["argv"][1].endswith("scrape_finalbid.py")


def test_live_mode_truncates_a_stale_out_file(monkeypatch, tmp_path):
    """The scraper appends to --out, so a leftover file must not leak in."""
    out = tmp_path / "live.jsonl"
    out.write_text('{"id": "stale"}\n', encoding="utf-8")

    def fake_call(argv):
        # the scraper appends, exactly as scrape_finalbid.py does
        with open(out, "a", encoding="utf-8") as fh:
            fh.write('{"id": "fresh-1", "detailed": true}\n')
        return 0

    monkeypatch.setattr(demo.subprocess, "call", fake_call)
    assert demo.run_live(limit=1, detail=False, out_path=str(out),
                         concurrency=1, spacing=0.1, show=0,
                         start_page=0) == 0
    rows = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
    assert [r["id"] for r in rows] == ["fresh-1"]