#!/usr/bin/env python3
"""
demo.py — scrape for real, without a database
============================================

Two modes:

    python demo.py                   # LIVE: hit finalbid.vin, write nothing to a DB
    python demo.py --offline         # OFFLINE: synthetic pages through the real parsers

Live mode is the real scraper (`scrape_finalbid.py`) with every database
feature switched off — no `--load-db`, no `--track-pages`, no
`--refresh-bid-history`, no DSN. Lots are mirrored to a JSONL file and the
rows are then summarised (sample table, per-field fill rate, price/odometer
stats) exactly as the offline mode does.

Because there is no `scrape_pages` checkpoint, a live demo run walks from
page 1 and stops at `--limit` lots (default 50). That is deliberate: it is a
bounded sample of the live archive, not a crawl.

    python demo.py --limit 200 --detail       # 200 lots with full detail pages
    python demo.py --limit 20 --no-detail     # listing rows only (1 req/lot)
    python demo.py --out data/demo_live.jsonl # keep the raw rows

Offline mode needs neither the network, Postgres nor httpx; it feeds
synthetic archive pages and lot pages through the same parsers.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load(name: str):
    """Load one of the project's scripts as a module (they are not a package)."""
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sf = _load("scrape_finalbid")          # the parsers (also used by the tests)
rs = _load("run_scraper")              # .env loading + scraper CLI assembly


# ===========================================================================
# Shared report
# ===========================================================================
REPORT_COLS = ["year", "make", "model", "title_status", "odometer",
               "engine", "price_usd", "photos"]

FILL_FIELDS = ["trim", "title_status", "primary_damage", "secondary_damage",
               "odometer", "odometer_actual", "engine", "cylinders",
               "engine_displacement_l", "engine_power_hp", "fuel",
               "transmission", "drive", "vehicle_type", "loss_type",
               "body_color", "interior_color", "condition", "keys_present",
               "airbags", "salvage_number", "seller", "retail_usd",
               "avg_final_bid", "est_repair", "auctions_on_record",
               "first_seen", "photos"]


def filled(v) -> bool:
    """Did the page actually give us this field?

    0 is a real parsed value (`$0` estimated repair, `0 mi` odometer) and
    `False` a real answer (`keys_present: No`), so only None / "" / an empty
    list count as unfilled. An empty photos list means the ld+json block was
    missing, not "zero photos".
    """
    if v is None or v == "":
        return False
    if isinstance(v, (list, dict)):
        return len(v) > 0
    return True


def _cell(v) -> str:
    if v is None or v == "":
        return "-"
    if isinstance(v, list):
        return str(len(v))
    if isinstance(v, bool):
        return "Y" if v else "N"
    return str(v)


def print_report(lots: list[dict], meta: dict) -> None:
    """Sample table + fill rate + summary for a list of parsed lots."""
    total = len(lots)
    show = min(meta.get("show", 8), total)

    print(f"\n\033[1m--- {show} of {total} lots ---\033[0m")
    print("  " + "  ".join(c[:22].ljust(22) for c in REPORT_COLS))
    for lot in lots[:show]:
        print("  " + "  ".join(
            _cell(lot.get(c))[:22].ljust(22 if c != "photos" else 4)
            for c in REPORT_COLS))

    if meta.get("detail"):
        print(f"\n\033[1m--- detail field fill rate ({total} lots) ---\033[0m")
        width = max(len(f) for f in FILL_FIELDS)
        for f in FILL_FIELDS:
            n = sum(1 for lot in lots if filled(lot.get(f)))
            bar = "#" * int(round(n / max(1, total) * 24))
            print(f"  {f.ljust(width)}  {n:>5}/{total}  {bar}")

    by_title: dict[str, int] = {}
    for lot in lots:
        by_title[str(lot.get("title_status"))] = \
            by_title.get(str(lot.get("title_status")), 0) + 1
    prices = [lot["price_usd"] for lot in lots if lot.get("price_usd")]
    odos = [lot["odometer"] for lot in lots
            if lot.get("odometer") and lot.get("odometer_actual")]
    not_actual = sum(1 for lot in lots if lot.get("odometer_actual") is False)
    sales = sum(len(lot.get("sales") or []) for lot in lots)
    multi = sum(1 for lot in lots if len(lot.get("sales") or []) > 1)
    photos = sum(len(lot.get("photos") or []) for lot in lots)

    print(f"\n\033[1m--- summary ---\033[0m")
    for k, v in meta.get("lines", []):
        print(f"  {k.ljust(23)} {v}")
    print(f"  {'detailed'.ljust(23)} "
          f"{sum(1 for l in lots if l.get('detailed'))}  "
          f"(failed {meta.get('detail_failed', 0)})")
    print(f"  {'sale-history rows'.ljust(23)} {sales}  "
          f"(from {multi} multi-auction VINs)")
    print(f"  {'photos'.ljust(23)} {photos}")
    if prices:
        print(f"  {'price'.ljust(23)} avg ${sum(prices) // len(prices):,}  "
              f"max ${max(prices):,}  (sold/known: {len(prices)})")
    if odos:
        print(f"  {'odometer (actual)'.ljust(23)} avg {sum(odos) // len(odos):,} mi")
    print(f"  {'odometer not actual'.ljust(23)} {not_actual} lots")
    print("  " + "title status".ljust(23)
          + ", ".join(f"{k}={v}" for k, v in sorted(by_title.items())))


# ===========================================================================
# LIVE mode — the real scraper, no database
# ===========================================================================
def run_live(limit: int, detail: bool, out_path: str | None,
             concurrency: int, spacing: float, show: int,
             start_page: int) -> int:
    env = rs.load_env()

    # A fresh file per run: the scraper appends to --out, so reusing a path
    # would mix this run's lots with the previous one's.
    if out_path is None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join("data", f"demo_live_{stamp}.jsonl")
    if os.path.exists(out_path):
        os.remove(out_path)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    overrides = {
        # every database feature off — this is the whole point of demo mode
        "LOAD_DB": "0",
        "TRACK_PAGES": "0",
        "REFRESH_BID_HISTORY": "0",
        "DSN": "",                 # keep it off the scraper CLI too
        "LIMIT": str(limit),
        "OUT": out_path,
        "DETAIL": "1" if detail else "0",
        "CONCURRENCY": str(concurrency),
        "SPACING": str(spacing),
    }
    if start_page:
        overrides["START_PAGE"] = str(start_page)

    argv = rs.build_args(env, overrides)
    print("\n\033[1m=== DEMO (live scrape, no database) ===\033[0m")
    print(f"  limit={limit} detail={detail} concurrency={concurrency} "
          f"spacing={spacing}s")
    print(f"  rows -> {out_path}\n")

    rc = subprocess.call(argv)
    if rc != 0:
        print(f"\n\033[31mscraper exited {rc}\033[0m", file=sys.stderr)
        return rc

    lots = []
    with open(out_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                lots.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    print_report(lots, {
        "detail": detail,
        "show": show,
        "detail_failed": 0,
        "lines": [
            ("rows scraped", len(lots)),
            ("distinct VINs",
             len({l.get("vin") for l in lots if l.get("vin")})),
            ("sources", ", ".join(sorted(
                {l.get("source") for l in lots if l.get("source")})) or "-"),
            ("distinct makes",
             len({l.get("make") for l in lots if l.get("make")})),
        ],
    })
    print(f"\n  raw rows: {out_path}")
    print("\n\033[1m[demo done — nothing was written to a database]\033[0m\n")
    return 0


# ===========================================================================
# OFFLINE mode — synthetic pages through the real parsers
# ===========================================================================
MAKES = [
    ("FORD", "focus", ["SE", "SEL", "ST"]),
    ("TOYOTA", "corolla", ["LE", "SE", "XLE"]),
    ("CHEVROLET", "cruze", ["LS", "2LT", "LT"]),
    ("HONDA", "civic", ["LX", "EX", "Sport"]),
    ("NISSAN", "altima", ["S", "SV", "SR"]),
]
COLORS = [("Gray", "Black"), ("White", "Beige"), ("Blue", "Gray"),
          ("Black", "Black"), ("Red", "Tan"), ("Silver", "Black")]
ENGINES = ["2L · 4 cyl · 160 hp", "1.5L · 4 cyl · 152 hp",
           "1.4L · 4 cyl · 138 hp", "2.5L · 4 cyl · 200 hp"]
DAMAGES = ["Front end", "Rear end", "All over", "Hail", "Water/Flood",
           "Mechanical", "Vandalism", "Side"]
TITLE_DOCS = ["Salvage title", "Clean title", "Rebuilt title"]
SOLD_STATES = ["Sold by Buy now", "Sold by Bid", "Sold", "Not Sold", "Unsold"]
LOCATIONS = ["Wheeling, IL", "Long Beach, CA", "Tampa, FL", "Houston, TX",
             "East Rutherford, NJ", "Phoenix, AZ"]
SELLERS = ["TitleMax", "Copart Direct", "IAAI", "Mannheim Auto"]
CONDITIONS = ["Run &amp; drive", "Not runnable", "Starts &amp; drives"]
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _date_str(d: datetime) -> str:
    return f"{MONTHS[d.month - 1]} {d.day}, {d.year}"


def _vin(rng: random.Random, short: bool = False) -> str:
    """17-char VIN, or a deliberately short one (real lots are sometimes 14)."""
    n = 14 if short else 17
    return "".join(rng.choice("0123456789ABCDEFGHJKLMNPRSTUVWXYZ")
                   for _ in range(n))


def make_lot_record(i: int, rng: random.Random) -> dict:
    """A synthetic lot, in the same shape `parse_archive` produces."""
    make, model_slug, trims = rng.choice(MAKES)
    trim = rng.choice(trims)
    year = rng.randint(2006, 2025)
    source = "copart" if i % 3 else "iaai"
    lot_number = str(60000000 + i * 137 + rng.randint(0, 99))
    # The `i + 1` offsets keep lot #0 an ordinary, clean lot.
    short_vin = (i + 1) % 37 == 0
    vin = _vin(rng, short=short_vin)
    title_doc = "Clean title" if (i + 1) % 53 == 0 else rng.choice(TITLE_DOCS)
    sold = rng.choice(SOLD_STATES)
    sale_date = datetime(2026, 1, 20) - timedelta(days=i % 300)
    url = (f"https://finalbid.vin/en/{make.lower()}/{model_slug}/{year}/"
           f"{source}-{lot_number}-{vin}")
    return {
        "i": i, "source": source, "lot_number": lot_number, "vin": vin,
        "year": year, "make": make, "model": f"{model_slug.upper()} {trim}",
        "trim_hint": trim, "url": url, "title_doc": title_doc, "sold": sold,
        "sale_date": sale_date, "damage": rng.choice(DAMAGES),
        "location": rng.choice(LOCATIONS),
        "odometer": rng.choice([rng.randint(1200, 240000), 0]),  # 0 = not actual
        "price": rng.choice([0, 25, 125, 385, 500, 1500, 4250, 9800]),
        "retail": rng.choice([1200, 3500, 9000, 14500, 26000]),
    }


def make_card(rec: dict, rng: random.Random) -> str:
    """One archive <article> card.

    Every 61st card gets a href with no parseable `copart-<n>[-<vin>]` key,
    the case `parse_archive` must skip with a warning rather than write out
    with a blank source.
    """
    key = (f"{rec['source']}-{rec['lot_number']}-{rec['vin']}"
           if (rec["i"] + 1) % 61 else "unknown-0000")
    href = (rec["url"] if (rec["i"] + 1) % 61 else
            "https://finalbid.vin/en/ford/focus/2018/" + key)
    odo = rec["odometer"]
    odo_txt = f"{odo:,} mi" + (" · not actual" if odo == 0 else "")
    price_txt = f"${rec['price']:,}" if rec["price"] else "Not sold"
    return f"""
<article>
  <h3><a href="{href}">{rec['year']} {rec['make']} {rec['model']}</a></h3>
  <span class="price">{price_txt}</span>
  <span class="meta">{rec['damage']} · {odo_txt} · {rec['location']}</span>
  <span class="title">{rec['title_doc']}</span>
  <span class="date">{_date_str(rec['sale_date'])}</span>
  <span class="status">{rec['sold']}</span>
  <span class="foot">{rec['title_doc']} · Keys · Run &amp; Drive</span>
</article>"""


def make_archive_page(records: list[dict], rng: random.Random) -> str:
    return ("<html><body><main>"
            + "".join(make_card(r, rng) for r in records)
            + "</main></body></html>")


def make_detail_page(rec: dict, rng: random.Random) -> str:
    """A lot page: <dt>/<dd> spec list + ld+json + the sale-history table."""
    body, interior = rng.choice(COLORS)
    engine = rng.choice(ENGINES)
    cyl = int(engine.split("cyl")[0].split("·")[-1])
    hp = int(engine.split("hp")[0].split("·")[-1])
    disp = float(engine.split("·")[0].strip().rstrip("L"))

    # every 23rd page has no ld+json block: the parsers must still work from
    # the <dt>/<dd> list and the URL slug alone.
    ld = ""
    if (rec["i"] + 1) % 23 != 0:
        ldjson = {
            "@context": "https://schema.org", "@type": "Vehicle",
            "name": f"{rec['year']} {rec['make'].title()} {rec['model'].title()}",
            "brand": {"@type": "Brand", "name": rec["make"].title()},
            "model": rec["model"].split()[0].title(),
            "modelDate": rec["year"],
            "image": [f"https://api.finalbid.com/photos/{rec['lot_number']}-{n}.jpg"
                      for n in range(1, rng.randint(2, 9))],
            "url": rec["url"], "bodyType": "Automobile",
            "vehicleInteriorColor": interior, "color": body,
            "mileageFromOdometer": {"@type": "QuantitativeValue",
                                    "value": rec["odometer"], "unitCode": "SMI"},
            "vehicleEngine": {
                "@type": "EngineSpecification",
                "engineDisplacement": {"@type": "QuantitativeValue",
                                       "value": disp, "unitCode": "LTR"},
                "enginePower": {"@type": "QuantitativeValue",
                                "value": hp, "unitText": "hp"},
            },
        }
        ld = '<script type="application/ld+json">%s</script>' % json.dumps(ldjson)

    # re-listing history: every 4th lot has been through 2-3 auctions
    sales_rows = ""
    if (rec["i"] + 1) % 4 == 0:
        n = rng.randint(2, 3)
        aor = n + rng.randint(0, 4)
        for k in range(n):
            d = rec["sale_date"] - timedelta(days=90 * (k + 1))
            sales_rows += (
                '<tr class="border-b border-line">'
                f"<td>{_date_str(d)}</td><td>{rec['source'].title()}</td>"
                f"<td>{rec['lot_number']}</td>"
                f"<td>${rng.randint(75, 2200):,}</td><td>Sold</td></tr>")
        sales_block = (
            '<h2>Sale history of this VIN</h2>'
            f'<dd class="text-ink">{aor}</dd><dt>Auctions on record</dt>'
            f'<dd class="text-ink">{rec["year"]}</dd><dt>First seen</dt>'
            f"<table>{sales_rows}</table>")
    else:
        sales_block = ""

    specs = [
        ("Sale date", _date_str(rec["sale_date"])),
        ("Location", rec["location"] + ", USA"),
        ("Sale status", rec["sold"]),
        ("Sale document", rec["title_doc"]),
        ("Condition", rng.choice(CONDITIONS)),
        ("Avg final bid",
         f"${rec['price']:,}" if rec["price"] else "Not available"),
        ("Est. repair", f"${rng.choice([0, 850, 2400, 6100]):,}"),
        ("Actual cash value", f"${rec['retail']:,}"),
        ("VIN", rec["vin"]),
        ("Lot #", f"{rec['source'].title()} {rec['lot_number']}"),
        ("Odometer", f"{rec['odometer']:,} mi"
                     + (" · not actual" if rec["odometer"] == 0 else "")),
        ("Year", str(rec["year"])),
        ("Body color", body),
        ("Interior color", interior),
        ("Engine", engine),
        ("Fuel", rng.choice(["Gas", "Gas", "Hybrid", "Diesel"])),
        ("Transmission", rng.choice(["Automatic", "Manual", "CVT"])),
        ("Drive", rng.choice(["FWD", "RWD", "AWD", "4WD"])),
        ("Vehicle type", rng.choice(["Automobile", "Sedan", "SUV", "Truck"])),
        ("Primary damage", rec["damage"]),
        ("Secondary damage", rng.choice(["Mechanical", "Rear end", "Not damaged"])),
        ("Loss type", rng.choice(["Collision", "Hail", "Water/Flood",
                                  "Theft", "Vandalism"])),
        ("Keys", rng.choice(["Yes", "No"])),
        ("Airbags", rng.choice(["Yes", "No"])),
        ("Salvage #", str(rng.randint(10000000, 99999999))),
        ("Seller", rng.choice(SELLERS)),
    ]
    rows = "".join(f"<dt>{lab}</dt><dd>{val}</dd>" for lab, val in specs)
    # the noisy header row that must NOT be read as a spec pair
    rows += ("<dt>Auctions on record Date Platform Lot # Final bid Status</dt>"
             + "<dd>" + "x" * 900 + "</dd>")

    return (f'<html><head><meta name="description" content="Final bid and sold '
            f'price: {rec["source"].title()} lot #{rec["lot_number"]}"></head>'
            f"<body><dl>{rows}</dl>{ld}{sales_block}</body></html>")


class DemoClient:
    """Serves the fixtures and fails a deterministic slice of requests.

    `get_text` / `fetch_many` have the same signatures as `AsyncFetcher`, so
    the demo exercises the same call shape the real crawl uses.
    """

    def __init__(self, records_by_url: dict, fail_every: int = 0):
        self.records_by_url = records_by_url
        self.fail_every = fail_every
        self.requests = 0
        self.failures = 0

    def _serve(self, url: str):
        self.requests += 1
        if self.fail_every and self.requests % self.fail_every == 0:
            self.failures += 1
            return None, f"simulated failure (every {self.fail_every}th request)"
        return self.records_by_url.get(url), None

    def get_text(self, url: str) -> str:
        text, err = self._serve(url)
        if text is None:
            raise sf.HttpError(503, err or f"no fixture for {url}")
        return text

    def fetch_many(self, urls):
        out = {}
        for u in urls:
            if not u:
                continue
            text, err = self._serve(u)
            out[u] = (text, 0.01, err)
        return out

    def close(self):
        pass


def run_offline(pages: int, per_page: int, detail: bool, seed: int,
                fail_every: int, show: int, out_path: str | None) -> int:
    import logging
    rng = random.Random(seed)
    sf.LOG = logging.getLogger("demo")
    logging.basicConfig(level=logging.INFO, format="    %(message)s",
                        datefmt="%H:%M:%S")

    total = pages * per_page
    records = [make_lot_record(i, rng) for i in range(total)]
    by_page: dict[int, list[dict]] = {}
    for n, rec in enumerate(records):
        by_page.setdefault(n // per_page + 1, []).append(rec)

    archive_by_url = {f"{sf.ARCHIVE_BASE}?page={p}": make_archive_page(recs, rng)
                      for p, recs in by_page.items()}
    detail_by_url = {rec["url"]: make_detail_page(rec, rng) for rec in records}
    client = DemoClient({**archive_by_url, **detail_by_url}, fail_every)

    print("\n\033[1m=== DEMO "
          "(offline: synthetic pages, no network, no database) ===\033[0m")
    print(f"  pages={pages} per_page={per_page} detail={detail} seed={seed}")
    if fail_every:
        print(f"  simulating 1 failure every {fail_every} requests")

    all_lots: list[dict] = []
    skipped = pages_ok = 0
    for page in sorted(by_page):
        try:
            html_text = client.get_text(f"{sf.ARCHIVE_BASE}?page={page}")
        except sf.HttpError:
            print(f"  page {page:>3}:   0 lots parsed (request failed)")
            continue
        lots = sf.parse_archive(html_text)
        pages_ok += 1
        skipped += len(by_page[page]) - len(lots)
        all_lots.extend(lots)
        print(f"  page {page:>3}: {len(lots):>3} lots parsed")

    sales: list[dict] = []
    detail_failed = 0
    if detail:
        fetched = client.fetch_many([lot["url"] for lot in all_lots])
        for lot in all_lots:
            text, _secs, _err = fetched.get(lot["url"], (None, 0.0, "no result"))
            if text is None:
                detail_failed += 1
                continue
            sf.apply_detail(lot, text)
            if not lot.get("detailed"):
                detail_failed += 1
                continue
            sales.extend(lot.get("sales") or [])
    else:
        print("  detail phase skipped (--no-detail)")

    print_report(all_lots, {
        "detail": detail,
        "show": show,
        "detail_failed": detail_failed,
        "lines": [
            ("pages walked", f"{pages_ok}/{pages}"),
            ("lots parsed", len(all_lots)),
            ("cards skipped (bad key)", skipped),
        ],
    })

    if out_path:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            for lot in all_lots:
                fh.write(json.dumps(lot, default=str) + "\n")
        print(f"\n  wrote {len(all_lots)} rows -> {out_path}")

    print(f"\n  requests served: {client.requests} "
          f"(simulated failures: {client.failures})")
    print("\n\033[1m[demo done — nothing was written to a database]\033[0m\n")
    return 0


# ===========================================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Scrape for real without a database "
                    "(--offline swaps in synthetic pages)")
    ap.add_argument("--offline", action="store_true",
                    help="use synthetic pages instead of the live site "
                         "(no network, no Postgres, no httpx)")

    live = ap.add_argument_group("live mode")
    live.add_argument("--limit", type=int, default=50,
                      help="stop after N lots (default 50)")
    live.add_argument("--no-detail", dest="detail", action="store_false",
                      help="listing rows only (1 request per lot)")
    live.add_argument("--concurrency", type=int, default=8,
                      help="parallel detail requests (default 8)")
    live.add_argument("--spacing", type=float, default=0.15,
                      help="minimum gap between request starts (default 0.15s)")
    live.add_argument("--start-page", type=int, default=0,
                      help="start the archive walk at this page")
    live.add_argument("--out", default="",
                      help="JSONL to write "
                           "(default data/demo_live_<timestamp>.jsonl)")

    off = ap.add_argument_group("offline mode")
    off.add_argument("--pages", type=int, default=3)
    off.add_argument("--per-page", type=int, default=24)
    off.add_argument("--seed", type=int, default=7)
    off.add_argument("--fail-every", type=int, default=0, metavar="N",
                     help="simulate a failed request every Nth one (0 = none)")

    ap.add_argument("--show", type=int, default=8,
                    help="how many lots to print in the sample table")
    args = ap.parse_args()
    ap.set_defaults(detail=True)

    if args.offline:
        return run_offline(args.pages, args.per_page, args.detail, args.seed,
                           args.fail_every, args.show, args.out or None)
    return run_live(args.limit, args.detail, args.out or None, args.concurrency,
                    args.spacing, args.show, args.start_page)


if __name__ == "__main__":
    sys.exit(main())