import os
import sys
import time
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

import requests


# ==========
# Config
# ==========
SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "")         # example: "xxx.myshopify.com"
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")       # Admin API access token
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")
TIMEOUT = 30
SLEEP_SEC = 0.12

NAMESPACE = "custom"

# Metafield keys (Product level) — aligned to your store definitions
KEYS = {
    "sale_start": "custom_promo_start_date",
    "sale_end": "custom_promo_end_date",
    "pi_start": "custom_promo_pi_start_date",
    "pi_end": "custom_promo_pi_end_date",
}

# Your test inputs (no database)
PROMOS = [
    # Collection Title, Type, Start, End
    {"collection": "TESTXZ", "type": "price_increase", "start": "2.7", "end": "2.17"},
    {"collection": "Automated Collection", "type": "retail_sale", "start": "2.4", "end": "2.16"},
]


# ==========
# Helpers
# ==========
def require_env():
    if not SHOPIFY_SHOP or not SHOPIFY_TOKEN:
        raise ValueError("Missing SHOPIFY_SHOP or SHOPIFY_TOKEN. Set them as environment variables first.")

def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def parse_mm_dd(s: str, default_year: Optional[int] = None) -> str:
    """
    Accepts:
      "2.5"  -> Feb 5
      "2/5"  -> Feb 5
      "02-05" -> Feb 5
      "2026-02-05" -> kept as is
    Returns ISO date string: YYYY-MM-DD
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty date")

    # already ISO
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s

    year = default_year or datetime.now().year
    for sep in [".", "/", "-"]:
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 2:
                m = int(parts[0])
                d = int(parts[1])
                return date(year, m, d).isoformat()

    raise ValueError(f"Unrecognized date format: {s}")

def graphql(query: str, variables: Optional[dict] = None) -> dict:
    # Allow SHOPIFY_SHOP to be either 'example.myshopify.com' or
    # a full URL like 'https://example.myshopify.com'. Normalize it.
    base = SHOPIFY_SHOP or ""
    if base.startswith("http://") or base.startswith("https://"):
        base = base.rstrip("/")
    else:
        base = f"https://{base}"
    url = f"{base}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": query, "variables": variables or {}}, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError(data["errors"])
    return data

def find_collection_by_title_exact(title: str) -> Optional[Tuple[str, str]]:
    q = """
    query($q: String!) {
      collections(first: 20, query: $q) {
        nodes { id title }
      }
    }
    """
    target = norm(title)

    # try exact search
    data = graphql(q, {"q": f'title:"{title}"'})
    for n in data["data"]["collections"]["nodes"]:
        if norm(n["title"]) == target:
            return n["id"], n["title"]

    # fallback
    data2 = graphql(q, {"q": f"title:{title}"})
    for n in data2["data"]["collections"]["nodes"]:
        if norm(n["title"]) == target:
            return n["id"], n["title"]

    return None

def list_product_ids_in_collection(collection_id: str) -> List[str]:
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
        data = graphql(q, {"id": collection_id, "cursor": cursor})
        conn = data["data"]["collection"]["products"]
        ids.extend([n["id"] for n in conn["nodes"]])
        has_next = conn["pageInfo"]["hasNextPage"]
        cursor = conn["pageInfo"]["endCursor"]

    return ids

def metafields_set(metafields: List[dict]) -> None:
    m = """
    mutation($m: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $m) {
        metafields { id namespace key }
        userErrors { field message }
      }
    }
    """
    # Debug: print payload summary
    try:
        print(f"    Writing metafields: {[(mf['ownerId'], mf['namespace'], mf['key']) for mf in metafields]}")
    except Exception:
        pass

    data = graphql(m, {"m": metafields})
    # Debug: show full response for investigation
    print(f"    GraphQL response: {data}")

    errs = data.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        print(f"    Metafield userErrors: {errs}")
        raise RuntimeError(errs)

def build_metafields(product_id: str, promo_type: str, start_iso: str, end_iso: str) -> List[dict]:
    if promo_type == "retail_sale":
        k_start, k_end = KEYS["sale_start"], KEYS["sale_end"]
    elif promo_type == "price_increase":
        k_start, k_end = KEYS["pi_start"], KEYS["pi_end"]
    else:
        raise ValueError(f"Unknown promo_type: {promo_type}")

    return [
        {
            "ownerId": product_id,
            "namespace": NAMESPACE,
            "key": k_start,
            "type": "date",
            "value": start_iso,
        },
        {
            "ownerId": product_id,
            "namespace": NAMESPACE,
            "key": k_end,
            "type": "date",
            "value": end_iso,
        },
    ]


# ==========
# Main
# ==========
def main():
    require_env()
    dry_run = "--dry-run" in sys.argv

    year = datetime.now().year  # assume current year for "2.5" style
    print(f"Shop: {SHOPIFY_SHOP}")
    print(f"API: {API_VERSION}")
    print(f"Dry run: {dry_run}")
    print("")

    for p in PROMOS:
        title = p["collection"]
        promo_type = p["type"]
        start_iso = parse_mm_dd(p["start"], default_year=year)
        end_iso = parse_mm_dd(p["end"], default_year=year)

        print(f"[Promo] {title} | {promo_type} | {start_iso} to {end_iso}")

        col = find_collection_by_title_exact(title)
        if not col:
            print("  Collection not found. Skip.")
            continue

        col_id, col_title = col
        print(f"  Matched collection: {col_title}")

        product_ids = list_product_ids_in_collection(col_id)
        print(f"  Products: {len(product_ids)}")

        for pid in product_ids:
            payload = build_metafields(pid, promo_type, start_iso, end_iso)

            if dry_run:
                print(f"    DRY_RUN {pid}: {[(x['key'], x['value']) for x in payload]}")
            else:
                metafields_set(payload)

            time.sleep(SLEEP_SEC)

        print("")

    print("Done.")

if __name__ == "__main__":
    main()
