import os
import time
import datetime as dt
from typing import Dict, List, Optional, Tuple, Iterable

import requests
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================

from pathlib import Path

PROJECT_DIR = Path(__file__).parent
ENV_PATH = PROJECT_DIR / ".env"      # lokálně může existovat, v GitHubu nevadí
OUTPUT_DIR = Path("outputs")         # GitHub-friendly

BASE_URL = "https://api.hubapi.com"

DEFAULT_PRODUCTS = ["Tapix", "EcoTrack", "ATM Nearby", "Labelling", "OpenData", "Subscription"]

# ✅ přidali jsme owner_* sloupce
SHEET_HEADERS = [
    "snapshot_week_start",   # pondělí daného týdne (idempotentní snapshot)
    "deal_id",
    "deal_name",
    "company_id",
    "company_name",
    "product_option",
    "product_raw",
    "pipeline_id",
    "pipeline_label",
    "dealstage_id",
    "dealstage_label",
    "amount",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
    "owner_id",
    "owner_name",
    "owner_email",
    "deal_url",
]

HEADER_FILL = PatternFill("solid", fgColor="D9E1F2")
HEADER_FONT = Font(bold=True)
TITLE_FONT = Font(bold=True, size=14)
WRAP = Alignment(vertical="center", wrap_text=True)

# =========================
# HELPERS
# =========================

def hubspot_request(token: str, method: str, path: str, *, params=None, json=None, retries: int = 5):
    url = f"{BASE_URL}{path}"
    headers = {"authorization": f"Bearer {token}", "Content-Type": "application/json"}

    last = None
    for attempt in range(retries):
        r = requests.request(method, url, headers=headers, params=params, json=json, timeout=60)
        last = r

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt, 20))
            continue

        r.raise_for_status()
        # některé endpointy vrací [] (list), jiné dict
        if r.text and r.text.strip():
            return r.json()
        return {}

    if last is not None:
        last.raise_for_status()
    raise RuntimeError("hubspot_request failed without response")


def week_start_iso(d: Optional[dt.date] = None) -> str:
    """Vrátí ISO datum pondělí aktuálního týdne (YYYY-MM-DD)."""
    if d is None:
        d = dt.date.today()
    monday = d - dt.timedelta(days=d.weekday())
    return monday.isoformat()


def excel_safe_sheet_name(name: str) -> str:
    return name[:31]


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def ensure_sheet_headers(ws):
    for c, h in enumerate(SHEET_HEADERS, start=1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = WRAP
        ws.column_dimensions[get_column_letter(c)].width = 18
    ws.freeze_panes = "A2"
    ws.sheet_view.showGridLines = False


def create_new_workbook(path: str, products: List[str]) -> Workbook:
    """Vytvoří nový workbook (uživatel smaže starý)."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws["A1"] = "Weekly HubSpot Deals by Product — Summary"
    ws["A1"].font = TITLE_FONT
    ws.sheet_view.showGridLines = False

    for p in products:
        sh = wb.create_sheet(title=excel_safe_sheet_name(p))
        sh.append(SHEET_HEADERS)
        ensure_sheet_headers(sh)

    wb.save(path)
    return wb


def find_product_property_name(token: str, label: str, explicit_name: Optional[str]) -> str:
    if explicit_name:
        return explicit_name

    props = hubspot_request(token, "GET", "/crm/v3/properties/deals")
    for p in props.get("results", []):
        if (p.get("label") or "").strip().lower() == label.strip().lower():
            return p["name"]

    raise RuntimeError(
        f"Nenašel jsem deal property s labelem '{label}'. "
        f"Dej do .env PRODUCT_PROPERTY_NAME=<internal_name>."
    )


def get_product_options_map(token: str, property_name: str) -> Dict[str, str]:
    prop = hubspot_request(token, "GET", f"/crm/v3/properties/deals/{property_name}")
    options = prop.get("options", []) or []
    return {o.get("value"): o.get("label", o.get("value")) for o in options if o.get("value")}


def get_pipelines_map(token: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    data = hubspot_request(token, "GET", "/crm/v3/pipelines/deals")
    pipeline_label = {}
    stage_label = {}

    for p in data.get("results", []):
        pid = p.get("id")
        pipeline_label[pid] = p.get("label", pid)
        for s in p.get("stages", []) or []:
            sid = s.get("id")
            stage_label[sid] = s.get("label", sid)

    return pipeline_label, stage_label


def list_all_deals(token: str, properties: List[str]) -> List[dict]:
    deals = []
    after = None

    while True:
        params = {
            "limit": 100,
            "properties": ",".join(properties),
            "archived": "false",
        }
        if after is not None:
            params["after"] = after

        page = hubspot_request(token, "GET", "/crm/v3/objects/deals", params=params)
        deals.extend(page.get("results", []))

        nxt = (page.get("paging") or {}).get("next") or {}
        after = nxt.get("after")
        if not after:
            break

    return deals


def split_multicheckbox(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(";") if p.strip()]


def get_owners_map(token: str) -> Dict[str, Dict[str, str]]:
    """
    owner_id -> {"name": "...", "email": "..."}
    Zkouší v3 endpoint; fallback legacy v2.
    """
    candidate_paths = ["/crm/v3/owners", "/owners/v2/owners"]
    data = None
    last_err = None
    for path in candidate_paths:
        try:
            data = hubspot_request(token, "GET", path)
            break
        except Exception as e:
            last_err = e
            data = None

    if data is None:
        raise RuntimeError(f"Nepodařilo se stáhnout owners. Poslední chyba: {last_err}")

    results = data.get("results", []) if isinstance(data, dict) else data

    owners: Dict[str, Dict[str, str]] = {}
    for o in results:
        oid = str(o.get("id") or "")
        if not oid:
            continue
        first = (o.get("firstName") or "").strip()
        last = (o.get("lastName") or "").strip()
        email = (o.get("email") or "").strip()
        name = (f"{first} {last}").strip() or email or oid
        owners[oid] = {"name": name, "email": email}

    return owners


def batch_read_deal_company_primary(token: str, deal_ids: List[str]) -> Dict[str, str]:
    """
    deal_id -> primary_company_id (nebo první associated)
    """
    if not deal_ids:
        return {}

    candidate_paths = [
        "/crm/v4/associations/deals/companies/batch/read",
        "/crm/v4/associations/deal/companies/batch/read",
        "/crm/v4/associations/deals/company/batch/read",
    ]

    result: Dict[str, str] = {}

    for batch in chunked(deal_ids, 1000):
        payload = {"inputs": [{"id": str(did)} for did in batch]}

        last_err = None
        data = None
        for path in candidate_paths:
            try:
                data = hubspot_request(token, "POST", path, json=payload)
                break
            except Exception as e:
                last_err = e
                data = None

        if data is None:
            raise RuntimeError(f"Nepodařilo se stáhnout associations deal->company. Poslední chyba: {last_err}")

        for rec in data.get("results", []):
            deal_id = str((rec.get("from") or {}).get("id"))
            tos = rec.get("to") or []

            primary_company_id = None
            fallback_first = None

            for t in tos:
                cid = str(t.get("toObjectId"))
                if not fallback_first:
                    fallback_first = cid

                assoc_types = t.get("associationTypes") or []
                if any((a.get("label") or "").lower() == "primary" for a in assoc_types):
                    primary_company_id = cid
                    break

            result[deal_id] = primary_company_id or fallback_first or ""

    return result


def batch_read_company_names(token: str, company_ids: List[str]) -> Dict[str, str]:
    """company_id -> company_name"""
    if not company_ids:
        return {}

    names: Dict[str, str] = {}
    for batch in chunked(company_ids, 100):
        payload = {
            "inputs": [{"id": str(cid)} for cid in batch],
            "properties": ["name"],
        }
        data = hubspot_request(
            token,
            "POST",
            "/crm/v3/objects/companies/batch/read",
            params={"archived": "false"},
            json=payload,
        )
        for r in data.get("results", []):
            cid = str(r.get("id"))
            props = r.get("properties") or {}
            names[cid] = props.get("name") or ""
    return names


def build_rows(
    deals: List[dict],
    snapshot_week_start: str,
    product_property_name: str,
    opt_map: Dict[str, str],
    pipeline_label: Dict[str, str],
    stage_label: Dict[str, str],
    products_interest: List[str],
    deal_to_company_id: Dict[str, str],
    company_id_to_name: Dict[str, str],
    owners_map: Dict[str, Dict[str, str]],
) -> Dict[str, List[List]]:
    want = {p.lower(): p for p in products_interest}
    rows_by_product: Dict[str, List[List]] = {p: [] for p in products_interest}

    # dedupe per product per snapshot: (product, deal_id)
    seen = set()

    for d in deals:
        deal_id = str(d.get("id"))
        props = d.get("properties") or {}

        dealname = props.get("dealname")
        amount = props.get("amount")
        closedate = props.get("closedate")
        createdate = props.get("createdate")
        lastmod = props.get("hs_lastmodifieddate")

        pipeline_id = props.get("pipeline")
        stage_id = props.get("dealstage")
        pipeline_lbl = pipeline_label.get(pipeline_id, pipeline_id)
        stage_lbl = stage_label.get(stage_id, stage_id)

        company_id = deal_to_company_id.get(deal_id, "") or ""
        company_name = company_id_to_name.get(company_id, "") if company_id else ""

        owner_id = str(props.get("hubspot_owner_id") or "")
        owner_name = owners_map.get(owner_id, {}).get("name", "") if owner_id else ""
        owner_email = owners_map.get(owner_id, {}).get("email", "") if owner_id else ""

        product_raw = props.get(product_property_name)
        product_values = split_multicheckbox(product_raw)
        product_labels = [opt_map.get(v, v) for v in product_values]

        for pl in product_labels:
            key = (pl or "").strip().lower()
            if key in want:
                sheet_product = want[key]
                if (sheet_product, deal_id) in seen:
                    continue
                seen.add((sheet_product, deal_id))

                deal_url = f"https://app.hubspot.com/contacts/deal/{deal_id}"

                rows_by_product[sheet_product].append([
                    snapshot_week_start,
                    deal_id,
                    dealname,
                    company_id,
                    company_name,
                    sheet_product,
                    product_raw,
                    pipeline_id,
                    pipeline_lbl,
                    stage_id,
                    stage_lbl,
                    amount,
                    closedate,
                    createdate,
                    lastmod,
                    owner_id,
                    owner_name,
                    owner_email,
                    deal_url
                ])

    return rows_by_product


def replace_rows_for_snapshot(ws, rows: List[List], snapshot_week_start: str):
    """
    Idempotentní snapshot:
    - smaže všechny řádky pro dané snapshot_week_start
    - vloží nové řádky
    Staré týdny se nikdy nepřepisují.
    """
    to_delete = []
    for i, r in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if not r or not r[0]:
            continue
        if str(r[0]) == snapshot_week_start:
            to_delete.append(i)

    for i in reversed(to_delete):
        ws.delete_rows(i, 1)

    for row in rows:
        ws.append(row)


def read_all_product_sheets(wb, products: List[str]) -> pd.DataFrame:
    frames = []
    for p in products:
        name = excel_safe_sheet_name(p)
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        data = []
        for r in ws.iter_rows(min_row=2, values_only=True):
            if not r or not r[0]:
                continue
            r = list(r)
            if len(r) < len(SHEET_HEADERS):
                r += [None] * (len(SHEET_HEADERS) - len(r))
            data.append(r[:len(SHEET_HEADERS)])
        if data:
            frames.append(pd.DataFrame(data, columns=SHEET_HEADERS))

    if not frames:
        return pd.DataFrame(columns=SHEET_HEADERS)
    return pd.concat(frames, ignore_index=True)


def rewrite_summary_sheet(wb, products: List[str]):
    df = read_all_product_sheets(wb, products)
    ws = wb["Summary"]
    ws.delete_rows(1, ws.max_row)

    ws["A1"] = "Weekly HubSpot Deals by Product — Summary"
    ws["A1"].font = TITLE_FONT
    ws.sheet_view.showGridLines = False

    if df.empty:
        ws["A3"] = "No data yet. Run the script once to populate product sheets."
        return

    # typing / cleanup
    df["snapshot_week_start"] = df["snapshot_week_start"].astype(str)
    df["product_option"] = df["product_option"].astype(str)
    df["pipeline_label"] = df["pipeline_label"].astype(str)
    df["dealstage_label"] = df["dealstage_label"].astype(str)
    df["owner_name"] = df["owner_name"].fillna("").astype(str)
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    weeks = sorted(df["snapshot_week_start"].unique().tolist())
    prod_order = products[:]

    # -------------------------
    # Table 1: counts by product x week
    # -------------------------
    ws["A3"] = "Table 1: Deal count by product (weekly snapshots)"
    ws["A3"].font = Font(bold=True)

    start_row = 5
    ws.cell(row=start_row, column=1, value="Product").font = HEADER_FONT
    ws.cell(row=start_row, column=1).fill = HEADER_FILL
    ws.cell(row=start_row, column=1).alignment = WRAP

    for i, w in enumerate(weeks, start=2):
        c = ws.cell(row=start_row, column=i, value=w)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = WRAP
        ws.column_dimensions[get_column_letter(i)].width = 14

    counts = (
        df.groupby(["product_option", "snapshot_week_start"])["deal_id"]
          .nunique()
          .reset_index(name="deal_count")
    )
    count_pivot = counts.pivot(index="product_option", columns="snapshot_week_start", values="deal_count").fillna(0).astype(int)

    for r_i, p in enumerate(prod_order, start=start_row + 1):
        ws.cell(row=r_i, column=1, value=p)
        for c_i, w in enumerate(weeks, start=2):
            val = int(count_pivot.loc[p, w]) if (p in count_pivot.index and w in count_pivot.columns) else 0
            ws.cell(row=r_i, column=c_i, value=val)

    # -------------------------
    # Table 2: sum amount by product x week
    # -------------------------
    ws["A20"] = "Table 2: Total amount by product (weekly snapshots)"
    ws["A20"].font = Font(bold=True)

    start_row2 = 22
    ws.cell(row=start_row2, column=1, value="Product").font = HEADER_FONT
    ws.cell(row=start_row2, column=1).fill = HEADER_FILL
    ws.cell(row=start_row2, column=1).alignment = WRAP

    for i, w in enumerate(weeks, start=2):
        c = ws.cell(row=start_row2, column=i, value=w)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = WRAP

    sums = (
        df.groupby(["product_option", "snapshot_week_start"])["amount_num"]
          .sum()
          .reset_index(name="amount_sum")
    )
    sum_pivot = sums.pivot(index="product_option", columns="snapshot_week_start", values="amount_sum").fillna(0.0)

    for r_i, p in enumerate(prod_order, start=start_row2 + 1):
        ws.cell(row=r_i, column=1, value=p)
        for c_i, w in enumerate(weeks, start=2):
            val = float(sum_pivot.loc[p, w]) if (p in sum_pivot.index and w in sum_pivot.columns) else 0.0
            cell = ws.cell(row=r_i, column=c_i, value=val)
            cell.number_format = "#,##0.00"

    # -------------------------
    # Table 3: tidy distribution incl. owner (Power BI friendly)
    # -------------------------
    ws["A37"] = "Table 3: Stage distribution incl. owner (tidy, good for Power BI)"
    ws["A37"].font = Font(bold=True)

    tidy_headers = [
        "snapshot_week_start",
        "product",
        "owner_name",
        "pipeline_label",
        "dealstage_label",
        "deal_count",
        "amount_sum",
    ]
    header_row = 39
    for c, h in enumerate(tidy_headers, start=1):
        cell = ws.cell(row=header_row, column=c, value=h)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = WRAP

    tidy = (
        df.groupby(["snapshot_week_start", "product_option", "owner_name", "pipeline_label", "dealstage_label"])
          .agg(deal_count=("deal_id", "nunique"), amount_sum=("amount_num", "sum"))
          .reset_index()
          .rename(columns={"product_option": "product"})
          .sort_values(["snapshot_week_start", "product", "owner_name", "pipeline_label", "dealstage_label"])
    )

    row = header_row + 1
    for _, rec in tidy.iterrows():
        ws.cell(row=row, column=1, value=rec["snapshot_week_start"])
        ws.cell(row=row, column=2, value=rec["product"])
        ws.cell(row=row, column=3, value=rec["owner_name"])
        ws.cell(row=row, column=4, value=rec["pipeline_label"])
        ws.cell(row=row, column=5, value=rec["dealstage_label"])
        ws.cell(row=row, column=6, value=int(rec["deal_count"]))
        c7 = ws.cell(row=row, column=7, value=float(rec["amount_sum"]))
        c7.number_format = "#,##0.00"
        row += 1

    # basic formatting
    ws.freeze_panes = "A5"
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 16
    ws.column_dimensions["C"].width = 22
    ws.column_dimensions["D"].width = 20
    ws.column_dimensions["E"].width = 24
    ws.column_dimensions["F"].width = 12
    ws.column_dimensions["G"].width = 14


def main():
    load_dotenv(ENV_PATH)

    token = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
    if not token:
        raise RuntimeError("Chybí HUBSPOT_PRIVATE_APP_TOKEN v .env")

    OUTPUT_DIR.mkdir(exist_ok=True)
    
    out_xlsx = str(OUTPUT_DIR / "hubspot_deals_by_product.xlsx")

    product_label = os.getenv("PRODUCT_PROPERTY_LABEL", "Product")
    explicit_prop = os.getenv("PRODUCT_PROPERTY_NAME")  # optional

    products_interest = DEFAULT_PRODUCTS[:]

    # ✅ snapshot pro týden = pondělí (idempotentní, bez duplicit)
    snapshot_week = week_start_iso()

    product_property_name = find_product_property_name(token, product_label, explicit_prop)
    opt_map = get_product_options_map(token, product_property_name)
    pipe_lbl, stage_lbl = get_pipelines_map(token)
    owners_map = get_owners_map(token)

    properties = [
        "dealname", "amount", "closedate", "createdate", "hs_lastmodifieddate",
        "pipeline", "dealstage",
        "hubspot_owner_id",
        product_property_name
    ]

    deals = list_all_deals(token, properties=properties)

    deal_ids = [str(d.get("id")) for d in deals if d.get("id")]
    deal_to_company = batch_read_deal_company_primary(token, deal_ids)

    company_ids = sorted({cid for cid in deal_to_company.values() if cid})
    company_names = batch_read_company_names(token, company_ids)

    rows_by_product = build_rows(
        deals=deals,
        snapshot_week_start=snapshot_week,
        product_property_name=product_property_name,
        opt_map=opt_map,
        pipeline_label=pipe_lbl,
        stage_label=stage_lbl,
        products_interest=products_interest,
        deal_to_company_id=deal_to_company,
        company_id_to_name=company_names,
        owners_map=owners_map,
    )

    # ✅ vytvoř nový soubor pokud neexistuje
    if not os.path.exists(out_xlsx):
        wb = create_new_workbook(out_xlsx, products_interest)
    else:
        wb = load_workbook(out_xlsx)
        if "Summary" not in wb.sheetnames:
            wb.create_sheet("Summary", 0)
        for p in products_interest:
            sh = excel_safe_sheet_name(p)
            if sh not in wb.sheetnames:
                wb.create_sheet(sh)
            ensure_sheet_headers(wb[sh])

    # ✅ zapiš snapshot pro tento týden (bez duplicit)
    for product, rows in rows_by_product.items():
        sh_name = excel_safe_sheet_name(product)
        ws = wb[sh_name]
        ensure_sheet_headers(ws)
        replace_rows_for_snapshot(ws, rows, snapshot_week)

    rewrite_summary_sheet(wb, products_interest)

    wb.save(out_xlsx)
    print(f"✅ Hotovo: {out_xlsx}")
    print(f"   snapshot_week_start={snapshot_week}, deals_fetched={len(deals)}, companies_found={len(company_ids)}")


if __name__ == "__main__":
    main()
