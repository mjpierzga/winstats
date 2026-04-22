#!/usr/bin/env python3
"""
Refresh PA wildlife rehab facilities from pawr.com.

Usage:
  python3 refresh_facilities.py
  python3 refresh_facilities.py --dry-run
  python3 refresh_facilities.py --diff
  python3 refresh_facilities.py --diff --dry-run

Behavior:
  - Scrapes all configured PA county pages from pawr.com.
  - Writes `pa_wildlife_rehab_facilities.csv` in this directory (unless --dry-run).
  - With `--diff`, compares new scrape to existing CSV and prints:
      * new facilities
      * removed facilities
      * changed fields for matched facilities
  - If a county page fails, logs error and continues.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import requests
from bs4 import BeautifulSoup


COUNTIES = [
    "Adams",
    "Allegheny",
    "Berks",
    "Bucks",
    "Butler",
    "Carbon",
    "Centre",
    "Chester",
    "Crawford",
    "Dauphin",
    "Lancaster",
    "Lehigh",
    "Luzerne",
    "Lycoming",
    "Monroe",
    "Montgomery",
    "Montour",
    "Philadelphia",
    "Schuylkill",
    "Snyder",
    "Westmoreland",
    "York",
]

CSV_COLUMNS = [
    "County",
    "Facility Name",
    "Contact Person",
    "Address",
    "City",
    "State",
    "Zip",
    "Phone",
    "Website",
    "Animals Accepted",
    "Status Notes",
]

COUNTY_URL = "https://www.pawr.com/{slug}-county"

ANIMAL_LINE_RE = re.compile(r"^(?=.*\bM\b|.*\bP\b|.*\bRVS\b|.*\bEND\b|.*\bRA\b)[A-Za-z,\s/&\-\u2013\.]+$")
CITY_STATE_ZIP_RE = re.compile(
    r"^\s*(?P<city>.+?),\s*P\.?A\.?\s*(?P<zip>\d{5}(?:-\d{4})?)\s*(?:\(.*\))?\s*$",
    re.IGNORECASE,
)
PHONE_RE = re.compile(r"(\+?1[\s\-\.])?(?:\(?\d{3}\)?[\s\-\.])\d{3}[\s\-\.]\d{4}(?:\s*(?:ext|x)\s*\d+)?", re.IGNORECASE)


@dataclass
class FacilityBlock:
    animals_raw: str
    lines: List[str]
    websites: List[str]


def county_slug(county: str) -> str:
    return county.lower().replace(" ", "-")


def normalize_space(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split())


def looks_like_animals_line(text: str) -> bool:
    t = normalize_space(text).strip()
    if not t:
        return False
    upper = t.upper()
    if "COUNTY" in upper or "WEBSITE" in upper:
        return False
    has_code = any(code in upper for code in ["M", "P", "R", "RVS", "END", "RA"])
    if not has_code:
        return False
    return bool(ANIMAL_LINE_RE.match(upper))


def normalize_animals(text: str) -> str:
    t = normalize_space(text).upper()
    t = t.replace(",", " ").replace("/", " ")
    t = t.replace("\u2013", " - ").replace("-", " ")
    codes = ["M", "P", "R", "RVS", "END", "RA"]
    found = []
    for c in codes:
        if re.search(rf"\b{re.escape(c)}\b", t):
            found.append(c)
    return " ".join(found)


def looks_like_status(text: str) -> bool:
    t = normalize_space(text).lower()
    markers = [
        "as of",
        "closed",
        "only",
        "rehabilitates",
        "not accepting",
        "temporar",
        "bats only",
        "available for educational programs",
        "facebook",
        "email:",
        "hours:",
        "intakes",
        "specializing in",
        "phone & fax",
    ]
    return any(m in t for m in markers)


def parse_entry_blocks(content: BeautifulSoup) -> List[FacilityBlock]:
    blocks: List[FacilityBlock] = []
    current: FacilityBlock | None = None

    website_queue: List[str] = []
    for a in content.find_all("a", href=True):
        href = normalize_space(a.get("href", ""))
        label = normalize_space(a.get_text(" ", strip=True)).lower()
        if href.startswith("http") and label == "website":
            website_queue.append(href)

    for token_raw in content.stripped_strings:
        token = normalize_space(token_raw)
        if not token:
            continue
        if token == "PA Wildlife Rehabilitators by County":
            break

        if looks_like_animals_line(token):
            if current and (current.animals_raw or current.lines or current.websites):
                blocks.append(current)
            current = FacilityBlock(animals_raw=token, lines=[], websites=[])
            continue

        if current is None:
            continue

        current.lines.append(token)
        if token.lower() == "website" and website_queue:
            current.websites.append(website_queue.pop(0))

    if current and (current.animals_raw or current.lines or current.websites):
        blocks.append(current)
    return blocks


def is_address_like(text: str) -> bool:
    t = normalize_space(text).lower()
    if re.search(r"\d", t):
        return True
    return any(k in t for k in ["po box", "p.o. box", "street", "st.", "road", "rd", "ave", "avenue", "drive", "dr."])


def looks_like_person_name(text: str) -> bool:
    t = normalize_space(text)
    if not t or re.search(r"\d", t):
        return False
    lowered = t.lower()
    org_markers = ["wildlife", "center", "centre", "rescue", "rehabilitation", "hospital", "care", "inc", "society", "friends"]
    if any(m in lowered for m in org_markers):
        return False
    words = t.replace(".", "").split()
    return 1 < len(words) <= 6


def looks_like_facility_name(text: str) -> bool:
    t = normalize_space(text).lower()
    markers = [
        "wildlife",
        "center",
        "centre",
        "rehab",
        "rehabilitation",
        "rescue",
        "care",
        "hospital",
        "works",
        "metro",
    ]
    return any(m in t for m in markers)


def parse_block_to_record(county: str, block: FacilityBlock) -> Dict[str, str]:
    lines = [normalize_space(x) for x in block.lines if normalize_space(x)]

    phone = ""
    city = ""
    state = "PA"
    zip_code = ""
    website = block.websites[0] if block.websites else ""
    status_notes: List[str] = []

    # Extract phone + city/state/zip while preserving non-matching lines.
    remainder: List[str] = []
    for line in lines:
        if not phone and (PHONE_RE.search(line) or re.search(r"\d{3}[-.\s]\d{3}[-.\s][A-Za-z0-9]{4,}", line)):
            phone = normalize_space(line.replace(" ext ", " ext "))
            continue
        m = CITY_STATE_ZIP_RE.match(line)
        if m and not city:
            city = normalize_space(m.group("city"))
            zip_code = normalize_space(m.group("zip"))
            continue
        if line.lower() == "website":
            continue
        remainder.append(line)

    # Remove obvious status lines early.
    working: List[str] = []
    for line in remainder:
        if looks_like_status(line):
            status_notes.append(line)
        else:
            working.append(line)

    facility_name = ""
    contact_lines: List[str] = []
    address_lines: List[str] = []

    # Identify facility by marker words first (most reliable on PAWR pages).
    facility_idx = next((i for i, line in enumerate(working) if looks_like_facility_name(line)), None)

    if facility_idx is not None:
        facility_name = working[facility_idx]
        contact_lines = [x for x in working[:facility_idx] if looks_like_person_name(x)]
        address_lines = working[facility_idx + 1 :]
    else:
        # Fallback to person -> facility -> address pattern.
        if working:
            if len(working) == 1:
                facility_name = working[0]
            elif looks_like_person_name(working[0]):
                # gather all leading person-name lines as contacts
                i = 0
                while i < len(working) and looks_like_person_name(working[i]):
                    contact_lines.append(working[i])
                    i += 1
                if i < len(working):
                    facility_name = working[i]
                    address_lines = working[i + 1 :]
                else:
                    facility_name = working[0]
                    contact_lines = []
            else:
                facility_name = working[0]
                address_lines = working[1:]

    # If no contact inferred yet, try collecting preceding name-like lines.
    if not contact_lines and facility_name:
        for line in working:
            if line == facility_name:
                break
            if looks_like_person_name(line):
                contact_lines.append(line)

    # If facility still looks like a person, swap with next candidate.
    if facility_name and looks_like_person_name(facility_name) and address_lines:
        facility_name, address_lines = address_lines[0], address_lines[1:]

    # Remove labels/notes that should not be in address.
    address_lines = [
        x
        for x in address_lines
        if normalize_space(x).lower() not in {"physical address"}
        and not looks_like_status(x)
    ]

    # Final cleanup for address line that also contains city/zip.
    cleaned_address: List[str] = []
    for line in address_lines:
        m = CITY_STATE_ZIP_RE.match(line)
        if m and not city:
            city = normalize_space(m.group("city"))
            zip_code = normalize_space(m.group("zip"))
            continue
        cleaned_address.append(line)
    address_lines = cleaned_address

    contact = "; ".join(dict.fromkeys([normalize_space(x) for x in contact_lines if normalize_space(x)]))
    address = " ".join([normalize_space(x).rstrip(",") for x in address_lines if normalize_space(x)]).strip()
    status = " ".join(dict.fromkeys([normalize_space(x) for x in status_notes if normalize_space(x)]))

    return {
        "County": county,
        "Facility Name": facility_name,
        "Contact Person": contact,
        "Address": address,
        "City": city,
        "State": state,
        "Zip": zip_code,
        "Phone": phone,
        "Website": website,
        "Animals Accepted": normalize_animals(block.animals_raw),
        "Status Notes": status,
    }


def scrape_county(county: str, session: requests.Session) -> List[Dict[str, str]]:
    url = COUNTY_URL.format(slug=county_slug(county))
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    content = soup.select_one("div.entry-content")
    if not content:
        raise ValueError("Missing div.entry-content on county page")

    blocks = parse_entry_blocks(content)
    rows = []
    for b in blocks:
        record = parse_block_to_record(county, b)
        if record["Facility Name"]:
            rows.append(record)
    return rows


def load_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [{k: normalize_space(v or "") for k, v in row.items()} for row in reader]


def write_csv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


def normalize_key(row: Dict[str, str]) -> Tuple[str, str]:
    return (normalize_space(row.get("County", "")).lower(), normalize_space(row.get("Facility Name", "")).lower())


def print_diff(old_rows: List[Dict[str, str]], new_rows: List[Dict[str, str]]) -> None:
    old_map = {normalize_key(r): r for r in old_rows if r.get("Facility Name")}
    new_map = {normalize_key(r): r for r in new_rows if r.get("Facility Name")}

    old_keys = set(old_map)
    new_keys = set(new_map)

    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    common = sorted(old_keys & new_keys)

    if not added and not removed:
        print("No added/removed facilities.")
    if added:
        print("New facilities:")
        for k in added:
            r = new_map[k]
            print(f"  + {r['County']}: {r['Facility Name']}")
    if removed:
        print("Removed facilities:")
        for k in removed:
            r = old_map[k]
            print(f"  - {r['County']}: {r['Facility Name']}")

    changed_count = 0
    for k in common:
        old = old_map[k]
        new = new_map[k]
        changed_fields = []
        for c in CSV_COLUMNS:
            if c in ("County", "Facility Name"):
                continue
            if normalize_space(old.get(c, "")) != normalize_space(new.get(c, "")):
                changed_fields.append(c)
        if changed_fields:
            changed_count += 1
            print(f"Changed: {new['County']} / {new['Facility Name']}")
            for c in changed_fields:
                print(f"  * {c}: '{old.get(c, '')}' -> '{new.get(c, '')}'")
    if changed_count == 0:
        print("No changed fields for existing facilities.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh PA wildlife rehab facilities CSV from pawr.com")
    parser.add_argument("--diff", action="store_true", help="Compare new scrape against existing CSV and print differences")
    parser.add_argument("--dry-run", action="store_true", help="Print CSV to stdout without writing file")
    args = parser.parse_args()

    out_path = Path(__file__).resolve().parent / "pa_wildlife_rehab_facilities.csv"
    old_rows = load_csv(out_path)

    session = requests.Session()
    session.headers.update({"User-Agent": "PA-Wildlife-Rehab-Refresh/1.0"})

    all_rows: List[Dict[str, str]] = []
    for county in COUNTIES:
        try:
            rows = scrape_county(county, session)
            all_rows.extend(rows)
            print(f"[ok] {county}: {len(rows)} facilities", file=sys.stderr)
        except Exception as exc:
            print(f"[error] {county}: {exc}", file=sys.stderr)
            continue

    # Keep stable ordering by county then facility.
    all_rows = sorted(
        all_rows,
        key=lambda r: (
            COUNTIES.index(r["County"]) if r["County"] in COUNTIES else 999,
            normalize_space(r["Facility Name"]).lower(),
        ),
    )

    if args.diff:
        print_diff(old_rows, all_rows)

    if args.dry_run:
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
        return 0

    write_csv(out_path, all_rows)
    print(f"Wrote {len(all_rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())