#!/usr/bin/env python3
"""
FEC Campaign Finance — Individual Contributions Pipeline

Reads bulk FEC individual contribution data (pipe-delimited) from ZIP,
joins to committee/candidate master files, and produces a static site
with letter-sharded search indices and MD5-keyed detail files.

Search dimensions:
1. Employer name — "Who at your company donated to whom?"
2. Zip code — "Who in your zip code are the top donors?"
3. Candidate/committee — "Who donated to this candidate?"

Data sources:
- indiv24.zip — Individual contributions (2023-2024 cycle)
- cm.txt — Committee master
- cn.txt — Candidate master
- ccl.txt — Candidate-committee linkage

All data from https://www.fec.gov/data/browse-data/?tab=bulk-data
"""

import hashlib
import json
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

PIPELINE_DIR = Path(__file__).parent
SITE_DIR = PIPELINE_DIR.parent / "site"

# Minimum total contribution to include an employer in the index
MIN_EMPLOYER_TOTAL = 1000  # $1,000
# Minimum individual total to include in zip code detail
MIN_INDIVIDUAL_TOTAL = 200  # FEC threshold
# Number of top recipients to show per employer
TOP_RECIPIENTS_PER_EMPLOYER = 20
# Number of top donors to show per zip code
TOP_DONORS_PER_ZIP = 50


def load_committees(path):
    """Load committee master file (pipe-delimited, no header)."""
    committees = {}
    with open(path, "r", encoding="latin-1") as f:
        for line in f:
            fields = line.strip().split("|")
            if len(fields) >= 2:
                cmte_id = fields[0]
                cmte_name = fields[1]
                # fields[12] = committee type (P=presidential, H=house, S=senate, etc.)
                cmte_type = fields[12] if len(fields) > 12 else ""
                # fields[10] = designation (A=authorized by candidate, etc.)
                designation = fields[10] if len(fields) > 10 else ""
                # fields[13] = connected organization
                connected_org = fields[13] if len(fields) > 13 else ""
                committees[cmte_id] = {
                    "name": cmte_name,
                    "type": cmte_type,
                    "designation": designation,
                    "connected_org": connected_org,
                }
    print(f"Loaded {len(committees):,} committees")
    return committees


def load_candidates(path):
    """Load candidate master file (pipe-delimited, no header)."""
    candidates = {}
    with open(path, "r", encoding="latin-1") as f:
        for line in f:
            fields = line.strip().split("|")
            if len(fields) >= 6:
                cand_id = fields[0]
                cand_name = fields[1]
                party = fields[2]
                election_year = fields[3]
                state = fields[4]
                office = fields[5]  # H=House, S=Senate, P=President
                district = fields[6] if len(fields) > 6 else ""
                candidates[cand_id] = {
                    "name": cand_name,
                    "party": party,
                    "year": election_year,
                    "state": state,
                    "office": office,
                    "district": district,
                }
    print(f"Loaded {len(candidates):,} candidates")
    return candidates


def load_linkage(path):
    """Load candidate-committee linkage (pipe-delimited, no header)."""
    # cmte_id -> cand_id mapping
    linkage = {}
    with open(path, "r", encoding="latin-1") as f:
        for line in f:
            fields = line.strip().split("|")
            if len(fields) >= 4:
                cand_id = fields[0]
                cmte_id = fields[3]
                linkage[cmte_id] = cand_id
    print(f"Loaded {len(linkage):,} committee-candidate linkages")
    return linkage


def resolve_recipient(cmte_id, committees, candidates, linkage):
    """Resolve a committee ID to a human-readable recipient name and party."""
    cmte = committees.get(cmte_id, {})
    cmte_name = cmte.get("name", cmte_id)

    # Try to link to a candidate
    cand_id = linkage.get(cmte_id)
    if cand_id and cand_id in candidates:
        cand = candidates[cand_id]
        return {
            "name": format_name(cand["name"]),
            "party": cand["party"],
            "office": cand["office"],
            "state": cand["state"],
            "district": cand.get("district", ""),
            "cmte_name": cmte_name,
            "cmte_id": cmte_id,
        }

    # No candidate link — return committee info
    return {
        "name": cmte_name,
        "party": "",
        "office": "",
        "state": "",
        "district": "",
        "cmte_name": cmte_name,
        "cmte_id": cmte_id,
    }


def format_name(name):
    """Convert 'LAST, FIRST MIDDLE' to 'First Last'."""
    if not name:
        return ""
    if "," in name:
        parts = name.split(",", 1)
        last = parts[0].strip().title()
        first = parts[1].strip().title() if len(parts) > 1 else ""
        return f"{first} {last}".strip()
    return name.strip().title()


def normalize_employer(emp):
    """Normalize employer name for grouping."""
    if not emp:
        return ""
    emp = emp.upper().strip()
    # Remove common suffixes
    emp = re.sub(r'\b(INC|LLC|LLP|CORP|CO|LTD|LP|PC|PA|PLLC|PLC)\.?\s*$', '', emp)
    emp = re.sub(r'[.,]+$', '', emp)
    emp = emp.strip()
    return emp


def md5_shard(key, n=256):
    """Return hex shard prefix for a key."""
    h = hashlib.md5(key.encode()).hexdigest()
    return h[:2]


def process_contributions(zip_path, committees, candidates, linkage):
    """Stream individual contributions from ZIP and aggregate."""
    print(f"Processing contributions from {zip_path}...")

    # Aggregation structures
    # employer_data[normalized_employer] = {
    #   "display_name": str,  # most common raw form
    #   "total": float,
    #   "count": int,
    #   "recipients": {cmte_id: {"total": float, "count": int}},
    #   "states": {state: count},
    # }
    employer_data = defaultdict(lambda: {
        "display_names": defaultdict(int),
        "total": 0.0,
        "count": 0,
        "recipients": defaultdict(lambda: {"total": 0.0, "count": 0}),
        "states": defaultdict(int),
    })

    # zip_data[zip5] = {
    #   "total": float,
    #   "count": int,
    #   "donors": {name: {"total": float, "count": int, "employer": str, "occupation": str}},
    #   "recipients": {cmte_id: {"total": float, "count": int}},
    # }
    zip_data = defaultdict(lambda: {
        "total": 0.0,
        "count": 0,
        "donors": defaultdict(lambda: {"total": 0.0, "count": 0, "employer": "", "occupation": ""}),
        "recipients": defaultdict(lambda: {"total": 0.0, "count": 0}),
        "state": "",
        "city": "",
    })

    # recipient_data[cmte_id] = {
    #   "total": float,
    #   "count": int,
    #   "top_employers": {employer: total},
    #   "top_states": {state: total},
    # }
    recipient_data = defaultdict(lambda: {
        "total": 0.0,
        "count": 0,
        "top_employers": defaultdict(float),
        "top_states": defaultdict(float),
    })

    # Global stats
    total_records = 0
    total_amount = 0.0
    skipped = 0

    # FEC individual contribution fields (pipe-delimited, no header):
    # 0: CMTE_ID, 1: AMNDT_IND, 2: RPT_TP, 3: TRANSACTION_PGI,
    # 4: IMAGE_NUM, 5: TRANSACTION_TP, 6: ENTITY_TP, 7: NAME,
    # 8: CITY, 9: STATE, 10: ZIP_CODE, 11: EMPLOYER,
    # 12: OCCUPATION, 13: TRANSACTION_DT, 14: TRANSACTION_AMT,
    # 15: OTHER_ID, 16: TRAN_ID, 17: FILE_NUM, 18: MEMO_CD,
    # 19: MEMO_TEXT, 20: SUB_ID

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the data file inside the ZIP
        names = zf.namelist()
        data_file = None
        for name in names:
            if name.endswith(".txt") or name.endswith(".csv"):
                data_file = name
                break
        if not data_file:
            # Try the first file
            data_file = names[0]
        print(f"Reading {data_file} from ZIP ({len(names)} files in archive)")

        with zf.open(data_file) as f:
            import io
            reader = io.TextIOWrapper(f, encoding="latin-1")
            for line_num, line in enumerate(reader, 1):
                if line_num % 1_000_000 == 0:
                    print(f"  Processed {line_num:,} lines... ({total_records:,} valid, ${total_amount:,.0f} total)")

                fields = line.strip().split("|")
                if len(fields) < 15:
                    skipped += 1
                    continue

                cmte_id = fields[0]
                name = fields[7]
                city = fields[8]
                state = fields[9]
                zip_code = fields[10][:5] if fields[10] else ""  # First 5 digits
                employer = fields[11]
                occupation = fields[12]
                try:
                    amount = float(fields[14])
                except (ValueError, IndexError):
                    skipped += 1
                    continue

                # Skip negative amounts (refunds) and zero
                if amount <= 0:
                    skipped += 1
                    continue

                total_records += 1
                total_amount += amount

                # Employer aggregation
                norm_emp = normalize_employer(employer)
                if norm_emp and len(norm_emp) >= 2:
                    emp = employer_data[norm_emp]
                    emp["display_names"][employer.strip()] += 1
                    emp["total"] += amount
                    emp["count"] += 1
                    emp["recipients"][cmte_id]["total"] += amount
                    emp["recipients"][cmte_id]["count"] += 1
                    if state:
                        emp["states"][state] += 1

                # Zip code aggregation
                if zip_code and len(zip_code) == 5 and zip_code.isdigit():
                    zd = zip_data[zip_code]
                    zd["total"] += amount
                    zd["count"] += 1
                    donor_key = name.strip().upper()
                    if donor_key:
                        d = zd["donors"][donor_key]
                        d["total"] += amount
                        d["count"] += 1
                        if employer:
                            d["employer"] = employer.strip()
                        if occupation:
                            d["occupation"] = occupation.strip()
                    zd["recipients"][cmte_id]["total"] += amount
                    zd["recipients"][cmte_id]["count"] += 1
                    if state and not zd["state"]:
                        zd["state"] = state
                    if city and not zd["city"]:
                        zd["city"] = city.strip().title()

                # Recipient aggregation
                rd = recipient_data[cmte_id]
                rd["total"] += amount
                rd["count"] += 1
                if norm_emp:
                    rd["top_employers"][norm_emp] += amount
                if state:
                    rd["top_states"][state] += amount

    print("\nProcessing complete:")
    print(f"  Total valid records: {total_records:,}")
    print(f"  Total amount: ${total_amount:,.0f}")
    print(f"  Skipped: {skipped:,}")
    print(f"  Unique employers: {len(employer_data):,}")
    print(f"  Unique zip codes: {len(zip_data):,}")
    print(f"  Unique recipients: {len(recipient_data):,}")

    return employer_data, zip_data, recipient_data, {
        "total_records": total_records,
        "total_amount": round(total_amount, 2),
        "skipped": skipped,
        "unique_employers": len(employer_data),
        "unique_zips": len(zip_data),
        "unique_recipients": len(recipient_data),
    }


def build_employer_index(employer_data, committees, candidates, linkage, output_dir):
    """Build letter-sharded employer search index and MD5-keyed detail files."""
    print("\nBuilding employer index...")
    index_dir = output_dir / "data" / "employers"
    detail_dir = output_dir / "data" / "e"
    index_dir.mkdir(parents=True, exist_ok=True)
    detail_dir.mkdir(parents=True, exist_ok=True)

    # Build index entries
    index_shards = defaultdict(list)
    detail_shards = defaultdict(dict)
    included = 0

    for norm_emp, data in employer_data.items():
        if data["total"] < MIN_EMPLOYER_TOTAL:
            continue

        # Pick most common display name
        display_name = max(data["display_names"].items(), key=lambda x: x[1])[0]
        display_name = display_name.strip().title()

        # First letter for index shard
        first_char = norm_emp[0].lower() if norm_emp and norm_emp[0].isalpha() else "_"

        index_entry = {
            "n": display_name,
            "t": round(data["total"], 2),
            "c": data["count"],
        }

        index_shards[first_char].append(index_entry)

        # Build detail
        emp_key = norm_emp.lower()
        shard = md5_shard(emp_key)

        # Resolve top recipients
        sorted_recipients = sorted(
            data["recipients"].items(),
            key=lambda x: x[1]["total"],
            reverse=True
        )[:TOP_RECIPIENTS_PER_EMPLOYER]

        recipients = []
        for cmte_id, rec_data in sorted_recipients:
            resolved = resolve_recipient(cmte_id, committees, candidates, linkage)
            recipients.append({
                "name": resolved["name"],
                "party": resolved["party"],
                "office": resolved["office"],
                "state": resolved["state"],
                "total": round(rec_data["total"], 2),
                "count": rec_data["count"],
            })

        # Top states
        sorted_states = sorted(data["states"].items(), key=lambda x: x[1], reverse=True)[:10]

        detail_shards.setdefault(shard, {})[emp_key] = {
            "name": display_name,
            "total": round(data["total"], 2),
            "count": data["count"],
            "recipients": recipients,
            "states": [{"state": s, "count": c} for s, c in sorted_states],
        }

        included += 1

    # Write index shards
    for letter, entries in index_shards.items():
        entries.sort(key=lambda x: x["t"], reverse=True)
        with open(index_dir / f"index_{letter}.json", "w") as f:
            json.dump(entries, f, separators=(",", ":"))

    # Write detail shards
    for shard, entries in detail_shards.items():
        with open(detail_dir / f"{shard}.json", "w") as f:
            json.dump(entries, f, separators=(",", ":"))

    print(f"  {included:,} employers included (>= ${MIN_EMPLOYER_TOTAL:,})")
    print(f"  {len(index_shards)} index shards, {len(detail_shards)} detail shards")
    return included


def build_zip_index(zip_data, committees, candidates, linkage, output_dir):
    """Build zip code search data — sharded by first 3 digits."""
    print("\nBuilding zip code index...")
    zip_dir = output_dir / "data" / "zips"
    zip_dir.mkdir(parents=True, exist_ok=True)

    # Group by first 3 digits of zip
    zip3_shards = defaultdict(dict)

    for zip5, data in zip_data.items():
        if data["total"] < 500:  # Skip very small zip codes
            continue

        prefix = zip5[:3]

        # Top donors
        sorted_donors = sorted(
            data["donors"].items(),
            key=lambda x: x[1]["total"],
            reverse=True
        )[:TOP_DONORS_PER_ZIP]

        donors = []
        for donor_name, d in sorted_donors:
            if d["total"] < MIN_INDIVIDUAL_TOTAL:
                continue
            donors.append({
                "n": format_name(donor_name),
                "t": round(d["total"], 2),
                "c": d["count"],
                "e": d["employer"][:50] if d["employer"] else "",
                "o": d["occupation"][:50] if d["occupation"] else "",
            })

        # Top recipients
        sorted_recipients = sorted(
            data["recipients"].items(),
            key=lambda x: x[1]["total"],
            reverse=True
        )[:10]

        recipients = []
        for cmte_id, rec_data in sorted_recipients:
            resolved = resolve_recipient(cmte_id, committees, candidates, linkage)
            recipients.append({
                "name": resolved["name"],
                "party": resolved["party"],
                "total": round(rec_data["total"], 2),
            })

        zip3_shards[prefix][zip5] = {
            "total": round(data["total"], 2),
            "count": data["count"],
            "city": data["city"],
            "state": data["state"],
            "donors": donors,
            "recipients": recipients,
        }

    # Write shards
    for prefix, entries in zip3_shards.items():
        with open(zip_dir / f"{prefix}.json", "w") as f:
            json.dump(entries, f, separators=(",", ":"))

    print(f"  {len(zip_data):,} zip codes total, {sum(len(v) for v in zip3_shards.values()):,} included")
    print(f"  {len(zip3_shards)} zip3 shards")
    return len(zip3_shards)


def build_recipient_index(recipient_data, committees, candidates, linkage, output_dir):
    """Build candidate/committee search index."""
    print("\nBuilding recipient index...")
    index_dir = output_dir / "data" / "recipients"
    detail_dir = output_dir / "data" / "r"
    index_dir.mkdir(parents=True, exist_ok=True)
    detail_dir.mkdir(parents=True, exist_ok=True)

    index_shards = defaultdict(list)
    detail_shards = defaultdict(dict)
    included = 0

    for cmte_id, data in recipient_data.items():
        if data["total"] < 5000:  # Skip very small recipients
            continue

        resolved = resolve_recipient(cmte_id, committees, candidates, linkage)
        display_name = resolved["name"]
        if not display_name or display_name == cmte_id:
            continue

        # First letter for index
        first_char = display_name[0].lower() if display_name and display_name[0].isalpha() else "_"

        index_entry = {
            "n": display_name,
            "p": resolved["party"],
            "o": resolved["office"],
            "s": resolved["state"],
            "t": round(data["total"], 2),
            "c": data["count"],
        }
        index_shards[first_char].append(index_entry)

        # Detail
        rec_key = display_name.lower()
        shard = md5_shard(rec_key)

        # Top employers
        sorted_employers = sorted(
            data["top_employers"].items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]

        # Top states
        sorted_states = sorted(
            data["top_states"].items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        detail_shards.setdefault(shard, {})[rec_key] = {
            "name": display_name,
            "party": resolved["party"],
            "office": resolved["office"],
            "state": resolved["state"],
            "cmte_name": resolved["cmte_name"],
            "total": round(data["total"], 2),
            "count": data["count"],
            "employers": [{"name": e.title(), "total": round(t, 2)} for e, t in sorted_employers],
            "states": [{"state": s, "total": round(t, 2)} for s, t in sorted_states],
        }
        included += 1

    # Write index shards
    for letter, entries in index_shards.items():
        entries.sort(key=lambda x: x["t"], reverse=True)
        with open(index_dir / f"index_{letter}.json", "w") as f:
            json.dump(entries, f, separators=(",", ":"))

    # Write detail shards
    for shard, entries in detail_shards.items():
        with open(detail_dir / f"{shard}.json", "w") as f:
            json.dump(entries, f, separators=(",", ":"))

    print(f"  {included:,} recipients included (>= $5,000)")
    print(f"  {len(index_shards)} index shards, {len(detail_shards)} detail shards")
    return included


def build_meta(stats, output_dir):
    """Write meta.json with dataset stats."""
    meta = {
        "name": "Campaign Finance Check",
        "description": "Search FEC individual contribution records by employer, zip code, or candidate.",
        "data_source": "Federal Election Commission (FEC) Bulk Data",
        "data_url": "https://www.fec.gov/data/browse-data/?tab=bulk-data",
        "cycle": "2023-2024",
        "total_contributions": stats["total_records"],
        "total_amount": stats["total_amount"],
        "unique_employers": stats["unique_employers"],
        "unique_zip_codes": stats["unique_zips"],
        "unique_recipients": stats["unique_recipients"],
        "data_freshness": "Updated nightly by FEC. Pipeline last run captures point-in-time snapshot.",
        "pipeline_version": "1.0.0",
    }
    with open(output_dir / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print("\nWrote meta.json")


def main():
    print("=" * 60)
    print("FEC Campaign Finance — Individual Contributions Pipeline")
    print("=" * 60)

    # Load supporting data
    committees = load_committees(PIPELINE_DIR / "cm.txt")
    candidates = load_candidates(PIPELINE_DIR / "cn.txt")
    linkage = load_linkage(PIPELINE_DIR / "ccl.txt")

    # Process contributions
    zip_path = PIPELINE_DIR / "indiv24.zip"
    if not zip_path.exists():
        print(f"ERROR: {zip_path} not found. Download it first:")
        print(f"  curl -L 'https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip' -o {zip_path}")
        sys.exit(1)

    employer_data, zip_data_agg, recipient_data, stats = process_contributions(
        zip_path, committees, candidates, linkage
    )

    # Build output
    output_dir = SITE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    n_emp = build_employer_index(employer_data, committees, candidates, linkage, output_dir)
    n_zip = build_zip_index(zip_data_agg, committees, candidates, linkage, output_dir)
    n_rec = build_recipient_index(recipient_data, committees, candidates, linkage, output_dir)

    stats["employers_indexed"] = n_emp
    stats["zip_shards"] = n_zip
    stats["recipients_indexed"] = n_rec
    build_meta(stats, output_dir)

    # Calculate total site size
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*.json"))
    print(f"\nTotal data size: {total_size / 1024 / 1024:.1f} MB")
    print("Done.")


if __name__ == "__main__":
    main()
