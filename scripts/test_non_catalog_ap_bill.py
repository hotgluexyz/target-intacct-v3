#!/usr/bin/env python3
"""Live check: can target-intacct-v3 create an AP bill line without ITEMID?

Uses PurchaseInvoices (APBILL) with an account-based line and no productName,
mirroring Precoro non-catalog items.

Usage:
  .venv/bin/python scripts/test_non_catalog_ap_bill.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from target_intacct_v3.client import IntacctSink
from target_intacct_v3.target import TargetIntacctV3

CONFIG_PATH = ROOT / ".secrets" / "target-config.json"

# Quiet the HTTP dump noise from the client logger
logging.getLogger("target-intacct-v3").setLevel(logging.WARNING)


def pick_first(mapping: dict | None, label: str) -> tuple[str, str]:
    if not mapping:
        raise SystemExit(f"No {label} found in this Intacct tenant.")
    name, value = next(iter(mapping.items()))
    return name, value


def pick_vendor(sink) -> tuple[str, str]:
    """Prefer known demo vendors that are approved for AP bills on this tenant."""
    preferred_ids = ["20003", "20008", "20010", "20014", "20023", "20007", "20005"]
    rows = sink.get_records("VENDOR", ["VENDORID", "NAME", "STATUS"])
    by_id = {
        r["VENDORID"]: r
        for r in rows
        if str(r.get("STATUS", "")).lower() == "active"
    }
    for vendor_id in preferred_ids:
        if vendor_id in by_id:
            row = by_id[vendor_id]
            return row["NAME"], row["VENDORID"]
    return pick_first(sink.get_vendors(), "vendors")


def run() -> None:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"Missing config at {CONFIG_PATH}")

    target = TargetIntacctV3(config=CONFIG_PATH, validate_config=False)
    sink = target.get_sink(
        "PurchaseInvoices",
        schema={"type": "object", "properties": {}},
    )

    print("Logging in and loading reference data...")
    sink.login()
    accounts = sink.get_accounts()
    locations = sink.get_locations()
    departments = sink.get_departments()
    items = sink.get_items()

    vendor_name, vendor_id = pick_vendor(sink)
    if "Office Supplies" not in accounts:
        raise SystemExit("Expected 'Office Supplies' GL account in this tenant.")
    account_name, account_no = "Office Supplies", accounts["Office Supplies"]

    location_name, location_id = (None, None)
    for preferred in ("Boston", "Dallas", "New York"):
        if preferred in (locations or {}):
            location_name, location_id = preferred, locations[preferred]
            break
    if not location_id and locations:
        location_name, location_id = pick_first(locations, "locations")

    dept_name, dept_id = (
        ("Admin", departments["Admin"])
        if departments and "Admin" in departments
        else pick_first(departments, "departments")
    )

    today = date.today().isoformat()
    invoice_number = f"HG-NONCAT-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    print("\nReference data selected:")
    print(f"  vendor:     {vendor_name!r} ({vendor_id})")
    print(f"  account:    {account_name!r} ({account_no})")
    print(f"  department: {dept_name!r} ({dept_id})")
    if location_id:
        print(f"  location:   {location_name!r} ({location_id})")
    print(f"  items in catalog: {len(items or {})}")

    # Case A: non-catalog / account-based line — no productName / ITEMID
    non_catalog_record = {
        "invoiceNumber": invoice_number,
        "number": invoice_number,
        "createdAt": f"{today}T00:00:00.000Z",
        "issueDate": today,
        "dueDate": today,
        "status": "AUTHORISED",
        "currency": "USD",
        "vendorNum": vendor_id,
        "description": "hotglue non-catalog AP bill smoke test",
        "lineItems": [
            {
                "description": "Non-catalog / blank-item style line",
                "totalPrice": 1.23,
                "accountName": account_name,
                "accountNumber": account_no,
                "departmentName": dept_name,
                "department": dept_name,
                # Spend Management on this tenant requires vendor on the line
                "supplierNumber": vendor_id,
                # intentionally no productName / productId
            }
        ],
    }
    if location_id:
        non_catalog_record["locationId"] = location_id
        non_catalog_record["lineItems"][0]["locationId"] = location_id

    print("\n=== Case A: AP bill WITHOUT productName (non-catalog) ===")
    prepared = sink.preprocess_record(non_catalog_record, {})
    if "error" in prepared:
        print("FAILED at preprocess:", prepared["error"])
        raise SystemExit(1)

    line = prepared["payload"]["APBILL"]["APBILLITEMS"]["APBILLITEM"][0]
    print("Prepared line:", {k: line[k] for k in sorted(line)})
    if "ITEMID" in line:
        print("UNEXPECTED: ITEMID present on non-catalog line:", line.get("ITEMID"))
        raise SystemExit(1)
    print("Confirmed: no ITEMID on prepared line (account-based).")

    bill_id, success, state = sink.upsert_record(prepared, {})
    print(f"SUCCESS: created/updated AP bill RECORDNO={bill_id} success={success}")
    if state:
        print(f"  state: {json.dumps(state)}")

    # Case B (optional): same shape but with a catalog productName
    if items:
        item_name, item_id = pick_first(items, "items")
        catalog_invoice = f"{invoice_number}-CAT"
        catalog_record = {
            **non_catalog_record,
            "invoiceNumber": catalog_invoice,
            "number": catalog_invoice,
            "description": "hotglue catalog AP bill smoke test",
            "lineItems": [
                {
                    **non_catalog_record["lineItems"][0],
                    "description": "Catalog item line",
                    "productName": item_name,
                    "totalPrice": 2.34,
                }
            ],
        }
        print("\n=== Case B: AP bill WITH productName (catalog) ===")
        print(f"  using item: {item_name!r} -> {item_id}")
        prepared_b = sink.preprocess_record(catalog_record, {})
        if "error" in prepared_b:
            print("FAILED at preprocess:", prepared_b["error"])
        else:
            line_b = prepared_b["payload"]["APBILL"]["APBILLITEMS"]["APBILLITEM"][0]
            print("Prepared line:", {k: line_b[k] for k in sorted(line_b)})
            print(f"ITEMID on line: {line_b.get('ITEMID')!r}")
            try:
                bill_id_b, success_b, _ = sink.upsert_record(prepared_b, {})
                print(
                    f"SUCCESS: created/updated catalog AP bill RECORDNO={bill_id_b} "
                    f"success={success_b}"
                )
            except Exception as exc:
                print(f"FAILED to create catalog AP bill: {exc}")

    print(
        "\nVerdict: Intacct accepted an AP bill line with ACCOUNTNO and no ITEMID. "
        "Non-catalog / account-based invoice lines are possible via PurchaseInvoices."
    )


if __name__ == "__main__":
    IntacctSink.vendors = None
    IntacctSink.accounts = None
    IntacctSink.locations = None
    IntacctSink.items = None
    IntacctSink.departments = None
    IntacctSink.departments_recordno = None
    IntacctSink.controlid_list = []
    run()
