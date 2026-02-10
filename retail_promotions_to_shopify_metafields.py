import os
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple

import pyodbc
import requests

# Optional: load .env automatically if python-dotenv installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


"""
retail_promotions_to_shopify_metafields.py

Goal (per latest requirements):
- Banner must ALWAYS display REAL start/end dates from database
- But banner must APPEAR early:
    Sale: appear X days before real start
    Price Increase: appear Y days before real start
- And DISAPPEAR:
    Sale: disappear after real end
    Price Increase:
        if EndDate exists -> disappear after real end
        if EndDate missing -> disappear Z days after real start

Important constraints:
- Shopify stores only ONE set of dates: the REAL dates.
- We do NOT store "display window" dates in Shopify.
- Liquid controls display timing and formatting.
- Python controls data existence:
    - Write metafields when today is inside display window
    - Delete metafields when today is outside display window
"""


# =========================
# Config (ENV)
# =========================
class Config:
    # Shopify
    SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
    SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "").strip()
    SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01").strip()
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

    # DB
    DB_SERVER = os.getenv("DB_SERVER", r"sql01-union\sql2012").strip()
    DB_NAME = os.getenv("DB_NAME", "Ecomm_DB_PROD").strip()
    DB_USER = os.getenv("DB_USER", "ssis").strip()
    DB_PASSWORD = os.getenv("DB_PASSWORD", "ssis").strip()

    # X Y Z
    SALE_PRE_DAYS = int(os.getenv("SALE_PRE_DAYS", "0"))   # X
    PI_PRE_DAYS = int(os.getenv("PI_PRE_DAYS", "0"))       # Y
    PI_POST_DAYS = int(os.getenv("PI_POST_DAYS", "0"))     # Z (only used when PI has no end date)

    # Behavior
    DRY_RUN = os.getenv("DRY_RUN", "1").strip().lower() in ("1", "true", "yes")
    DB_ONLY = os.getenv("DB_ONLY", "0").strip().lower() in ("1", "true", "yes")
    SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.12"))

    # Metafields
    MF_NAMESPACE = "custom"
    MF_SALE_START = "promo_sale_start_date"
    MF_SALE_END = "promo_sale_end_date"
    MF_PI_START = "promo_pi_start_date"
    MF_PI_END = "promo_pi_end_date"


def require_env():
    if not Config.SHOPIFY_SHOP or not Config.SHOPIFY_TOKEN:
        raise ValueError("Missing SHOPIFY_SHOP or SHOPIFY_TOKEN. Put them in .env or environment variables.")


def normalize(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def to_date_only(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
        try:
            return datetime.fromisoformat(s.replace("Z", "")).date()
        except Exception:
            return None
    return None


# =========================
# Data Models
# =========================
@dataclass
class RetailPromoRow:
    id: int
    vendor: str
    entry_type: str
    start_date: date
    end_date: Optional[date]


@dataclass
class VendorPlan:
    vendor: str

    # Display windows (NOT written to Shopify)
    sale_display_start: Optional[date] = None
    sale_display_end: Optional[date] = None
    pi_display_start: Optional[date] = None
    pi_display_end: Optional[date] = None

    # Real dates (written to Shopify)
    sale_real_start: Optional[date] = None
    sale_real_end: Optional[date] = None
    pi_real_start: Optional[date] = None
    pi_real_end: Optional[date] = None   # Keep None if DB end is missing


# =========================
# DB Access
# =========================
class DatabaseConnection:
    def __init__(self):
        self.conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={Config.DB_SERVER};"
            f"DATABASE={Config.DB_NAME};"
            f"UID={Config.DB_USER};"
            f"PWD={Config.DB_PASSWORD}"
        )
        self.cursor = self.conn.cursor()

    def query(self, sql: str) -> List[Dict]:
        self.cursor.execute(sql)
        cols = [c[0] for c in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    def close(self):
        try:
            self.cursor.close()
        finally:
            self.conn.close()


class RetailPromotionsReader:
    """
    Reads promotions that should exist today based on display windows.

    Sale window:
      show_from = StartD - X
      show_to   = EndD

    Price Increase window:
      show_from = StartD - Y
      show_to   = EndD if exists else StartD + Z
    """
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def fetch_active_today(self, x: int, y: int, z: int) -> List[RetailPromoRow]:
        sql = f"""
        DECLARE @X INT = {x};
        DECLARE @Y INT = {y};
        DECLARE @Z INT = {z};

        WITH t AS (
            SELECT
                ID,
                Vendor,
                EntryType,
                TRY_CONVERT(date, Date_of_Start) AS StartD,
                TRY_CONVERT(date, Date_of_End)   AS EndD
            FROM Ecomm_DB_PROD.dbo.SM_Retail_Sales
        )
        SELECT
            ID,
            Vendor,
            EntryType,
            StartD AS Date_of_Start,
            EndD   AS Date_of_End
        FROM t
        WHERE
        (
            LTRIM(RTRIM(EntryType)) = 'Sale'
            AND StartD IS NOT NULL
            AND EndD IS NOT NULL
            AND DATEADD(day, -@X, StartD) <= CAST(GETDATE() AS date)
            AND EndD >= CAST(GETDATE() AS date)
        )
        OR
        (
            LTRIM(RTRIM(EntryType)) = 'Price Increase'
            AND StartD IS NOT NULL
            AND DATEADD(day, -@Y, StartD) <= CAST(GETDATE() AS date)
            AND COALESCE(EndD, DATEADD(day, @Z, StartD)) >= CAST(GETDATE() AS date)
        );
        """
        raw = self.db.query(sql)
        print("DEBUG fetch_active_today raw rows =", len(raw))

        rows: List[RetailPromoRow] = []
        for r in raw:
            vendor = (r.get("Vendor") or "").strip()
            entry_type = (r.get("EntryType") or "").strip()
            s = to_date_only(r.get("Date_of_Start"))
            e = to_date_only(r.get("Date_of_End"))

            if not vendor or not entry_type or not s:
                continue

            rows.append(RetailPromoRow(
                id=int(r["ID"]),
                vendor=vendor,
                entry_type=entry_type,
                start_date=s,
                end_date=e
            ))
        return rows


# =========================
# Aggregation
# =========================
def compute_display_window(row: RetailPromoRow, x: int, y: int, z: int) -> Tuple[date, date]:
    t = normalize(row.entry_type)

    if t == "sale":
        # appear X days early, disappear at real end
        return (row.start_date - timedelta(days=x)), (row.end_date or row.start_date)

    if t == "price increase":
        # appear Y days early, disappear at real end, else start + Z
        end_display = row.end_date if row.end_date else (row.start_date + timedelta(days=z))
        return (row.start_date - timedelta(days=y)), end_display

    return row.start_date, row.start_date


def aggregate_by_vendor(rows: List[RetailPromoRow], x: int, y: int, z: int) -> List[VendorPlan]:
    by_vendor: Dict[str, VendorPlan] = {}

    for r in rows:
        v = r.vendor
        w = by_vendor.get(v) or VendorPlan(vendor=v)
        t = normalize(r.entry_type)

        d_start, d_end = compute_display_window(r, x, y, z)

        if t == "sale":
            w.sale_display_start = d_start if w.sale_display_start is None else min(w.sale_display_start, d_start)
            w.sale_display_end = d_end if w.sale_display_end is None else max(w.sale_display_end, d_end)

            w.sale_real_start = r.start_date if w.sale_real_start is None else min(w.sale_real_start, r.start_date)
            real_end = r.end_date or r.start_date
            w.sale_real_end = real_end if w.sale_real_end is None else max(w.sale_real_end, real_end)

        elif t == "price increase":
            w.pi_display_start = d_start if w.pi_display_start is None else min(w.pi_display_start, d_start)
            w.pi_display_end = d_end if w.pi_display_end is None else max(w.pi_display_end, d_end)

            w.pi_real_start = r.start_date if w.pi_real_start is None else min(w.pi_real_start, r.start_date)

            # IMPORTANT: do NOT force an end date into Shopify if DB end is missing
            # Liquid can display "Starts on" when pi_end is missing.
            if r.end_date is not None:
                w.pi_real_end = r.end_date if w.pi_real_end is None else max(w.pi_real_end, r.end_date)

        by_vendor[v] = w

    return list(by_vendor.values())


# =========================
# Shopify GraphQL Client
# =========================
class ShopifyClient:
    def __init__(self):
        self.endpoint = f"https://{Config.SHOPIFY_SHOP}/admin/api/{Config.SHOPIFY_API_VERSION}/graphql.json"

    def graphql(self, query: str, variables: Optional[dict] = None, retries: int = 4) -> dict:
        headers = {
            "X-Shopify-Access-Token": Config.SHOPIFY_TOKEN,
            "Content-Type": "application/json",
        }
        payload = {"query": query, "variables": variables or {}}

        last_err = None
        for attempt in range(retries):
            try:
                resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=Config.REQUEST_TIMEOUT)

                if resp.status_code in (429, 500, 502, 503, 504):
                    last_err = RuntimeError(f"Temporary Shopify error {resp.status_code}: {resp.text}")
                    time.sleep(1.2 + attempt * 1.0)
                    continue

                resp.raise_for_status()
                data = resp.json()

                if data.get("errors"):
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")

                return data
            except Exception as e:
                last_err = e
                time.sleep(1.0 + attempt * 1.0)

        raise RuntimeError(f"Shopify GraphQL failed after retries: {last_err}")

    def find_collection_by_title_exact(self, title: str) -> Optional[Tuple[str, str]]:
        q = """
        query($q: String!) {
          collections(first: 20, query: $q) {
            nodes { id title }
          }
        }
        """
        target = normalize(title)

        data = self.graphql(q, {"q": f'title:"{title}"'})
        for n in data["data"]["collections"]["nodes"]:
            if normalize(n.get("title", "")) == target:
                return n["id"], n["title"]

        data2 = self.graphql(q, {"q": f"title:{title}"})
        for n in data2["data"]["collections"]["nodes"]:
            if normalize(n.get("title", "")) == target:
                return n["id"], n["title"]

        return None

    def list_product_ids_in_collection(self, collection_id: str) -> List[str]:
        ids: List[str] = []
        cursor = None
        has_next = True

        q = """
        query($id: ID!, $cursor: String) {
          collection(id: $id) {
            products(first: 250, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes { id }
            }
          }
        }
        """
        while has_next:
            data = self.graphql(q, {"id": collection_id, "cursor": cursor})
            conn = data["data"]["collection"]["products"]
            ids.extend([n["id"] for n in conn["nodes"]])
            has_next = conn["pageInfo"]["hasNextPage"]
            cursor = conn["pageInfo"]["endCursor"]

        return ids

    def list_product_ids_by_vendor(self, vendor: str) -> List[str]:
        ids: List[str] = []
        cursor = None
        has_next = True
        target = normalize(vendor)

        q = """
        query($q: String!, $cursor: String) {
          products(first: 250, after: $cursor, query: $q) {
            pageInfo { hasNextPage endCursor }
            nodes { id vendor }
          }
        }
        """
        qstr = f'vendor:"{vendor}"'

        while has_next:
            data = self.graphql(q, {"q": qstr, "cursor": cursor})
            conn = data["data"]["products"]

            for n in conn["nodes"]:
                if normalize(n.get("vendor", "")) == target:
                    ids.append(n["id"])

            has_next = conn["pageInfo"]["hasNextPage"]
            cursor = conn["pageInfo"]["endCursor"]

        return ids

    def metafields_set(self, metafields: List[dict]) -> None:
        m = """
        mutation($m: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $m) {
            metafields { id namespace key }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(m, {"m": metafields})
        errs = data["data"]["metafieldsSet"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldsSet userErrors: {errs}")

    def get_metafield_ids(self, product_id: str, namespace: str, keys: List[str]) -> Dict[str, Optional[str]]:
        q = """
        query($id: ID!, $idents: [HasMetafieldsIdentifier!]!) {
          product(id: $id) {
            metafields(identifiers: $idents) {
              id
              key
              namespace
            }
          }
        }
        """
        idents = [{"namespace": namespace, "key": k} for k in keys]
        data = self.graphql(q, {"id": product_id, "idents": idents})
        mfs = data.get("data", {}).get("product", {}).get("metafields", []) or []

        out = {k: None for k in keys}
        for mf in mfs:
            if mf and mf.get("key") in out:
                out[mf["key"]] = mf.get("id")
        return out

    def metafield_delete(self, metafield_id: str) -> None:
        m = """
        mutation($id: ID!) {
          metafieldDelete(input: {id: $id}) {
            deletedId
            userErrors { field message }
          }
        }
        """
        data = self.graphql(m, {"id": metafield_id})
        errs = data["data"]["metafieldDelete"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldDelete userErrors: {errs}")


def build_date_metafield(owner_id: str, namespace: str, key: str, d: date) -> dict:
    return {
        "ownerId": owner_id,
        "namespace": namespace,
        "key": key,
        "type": "date",
        "value": d.isoformat(),
    }


# =========================
# Main
# =========================
def main():
    print("DB_ONLY =", Config.DB_ONLY)

    if not Config.DB_ONLY:
        require_env()

    print("=== Retail Promotions -> Shopify Metafields (GraphQL) ===")
    today = datetime.now().date()
    print(f"Today: {today}")
    print(f"SALE_PRE_DAYS (X) = {Config.SALE_PRE_DAYS}")
    print(f"PI_PRE_DAYS   (Y) = {Config.PI_PRE_DAYS}")
    print(f"PI_POST_DAYS  (Z) = {Config.PI_POST_DAYS}")
    print(f"DRY_RUN = {Config.DRY_RUN}")
    print(f"DB_NAME = {Config.DB_NAME}")
    print("")

    db = DatabaseConnection()
    try:
        reader = RetailPromotionsReader(db)
        rows = reader.fetch_active_today(Config.SALE_PRE_DAYS, Config.PI_PRE_DAYS, Config.PI_POST_DAYS)
    finally:
        db.close()

    if not rows:
        print("No active retail promotions today. Nothing to write.")
        return

    vendor_plans = aggregate_by_vendor(rows, Config.SALE_PRE_DAYS, Config.PI_PRE_DAYS, Config.PI_POST_DAYS)
    print(f"Vendors to process: {len(vendor_plans)}")
    print("")

    if Config.DB_ONLY:
        print("DB_ONLY=1 so Shopify steps are skipped.")
        for w in vendor_plans:
            print(
                f"{w.vendor} | "
                f"Sale display: {w.sale_display_start}->{w.sale_display_end} real: {w.sale_real_start}->{w.sale_real_end} | "
                f"PI display: {w.pi_display_start}->{w.pi_display_end} real: {w.pi_real_start}->{w.pi_real_end}"
            )
        return

    shop = ShopifyClient()

    collection_cache: Dict[str, Optional[Tuple[str, str]]] = {}
    product_cache: Dict[str, List[str]] = {}

    updated_products = 0
    deleted_metafields = 0

    for w in vendor_plans:
        vendor = w.vendor
        print(f"[Vendor] {vendor}")

        print(f"  Sale display: {w.sale_display_start} -> {w.sale_display_end}")
        print(f"  Sale REAL:    {w.sale_real_start} -> {w.sale_real_end}")
        print(f"  PI display:   {w.pi_display_start} -> {w.pi_display_end}")
        print(f"  PI REAL:      {w.pi_real_start} -> {w.pi_real_end}")

        sale_should_exist = (
            w.sale_display_start is not None and w.sale_display_end is not None and
            w.sale_display_start <= today <= w.sale_display_end
        )
        pi_should_exist = (
            w.pi_display_start is not None and w.pi_display_end is not None and
            w.pi_display_start <= today <= w.pi_display_end
        )

        if vendor in collection_cache:
            col = collection_cache[vendor]
        else:
            col = shop.find_collection_by_title_exact(vendor)
            collection_cache[vendor] = col

        cache_key = f"{vendor}::{'collection' if col else 'vendor'}"

        if cache_key in product_cache:
            product_ids = product_cache[cache_key]
        else:
            if col:
                col_id, col_title = col
                print(f"  Collection matched: {col_title}")
                product_ids = shop.list_product_ids_in_collection(col_id)
            else:
                print("  Collection not found. Fallback: product.vendor")
                product_ids = shop.list_product_ids_by_vendor(vendor)

            product_cache[cache_key] = product_ids

        print(f"  Products found: {len(product_ids)}")

        for pid in product_ids:
            payload: List[dict] = []

            # WRITE: write REAL dates only
            if sale_should_exist and w.sale_real_start and w.sale_real_end:
                payload.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_SALE_START, w.sale_real_start))
                payload.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_SALE_END, w.sale_real_end))

            # PI: if only start exists, still write pi_start
            if pi_should_exist and w.pi_real_start:
                payload.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_PI_START, w.pi_real_start))
                # Only write PI_END if DB end exists
                if w.pi_real_end:
                    payload.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_PI_END, w.pi_real_end))

            keys_to_delete: List[str] = []
            if not sale_should_exist:
                keys_to_delete.extend([Config.MF_SALE_START, Config.MF_SALE_END])
            if not pi_should_exist:
                keys_to_delete.extend([Config.MF_PI_START, Config.MF_PI_END])

            if Config.DRY_RUN:
                if payload:
                    print(f"    DRY_RUN WRITE {pid}: {[(x['namespace'] + '.' + x['key'], x['value']) for x in payload]}")
                if keys_to_delete:
                    print(f"    DRY_RUN DELETE {pid}: {[(Config.MF_NAMESPACE + '.' + k) for k in keys_to_delete]}")
            else:
                if payload:
                    try:
                        shop.metafields_set(payload)
                        updated_products += 1
                    except Exception as e:
                        print(f"    WRITE ERROR {pid}: {e}")

                if keys_to_delete:
                    try:
                        id_map = shop.get_metafield_ids(pid, Config.MF_NAMESPACE, keys_to_delete)
                        for k in keys_to_delete:
                            mf_id = id_map.get(k)
                            if mf_id:
                                shop.metafield_delete(mf_id)
                                deleted_metafields += 1
                    except Exception as e:
                        print(f"    DELETE ERROR {pid}: {e}")

            time.sleep(Config.SLEEP_BETWEEN_CALLS)

        print("")

    print("=== Done ===")
    if Config.DRY_RUN:
        print("Dry run mode. No changes written.")
    else:
        print(f"Total products updated: {updated_products}")
        print(f"Total metafields deleted: {deleted_metafields}")


if __name__ == "__main__":
    main()
