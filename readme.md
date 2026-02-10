# SSMS -> Shopify Metafields (Banner Dates)

Purpose: write REAL promo dates from SSMS into Shopify product metafields. Banner timing is controlled by X/Y/Z.

X/Y/Z:
X = SALE_PRE_DAYS. Sale banner appears X days before REAL sale start. Disappears after REAL sale end.
Y = PI_PRE_DAYS. Price Increase banner appears Y days before REAL PI start.
Z = PI_POST_DAYS. If PI has no end date, banner disappears Z days after REAL PI start.

Shopify metafields to create (Product, namespace `custom`, type `date`):
custom.promo_sale_start_date
custom.promo_sale_end_date
custom.promo_pi_start_date
custom.promo_pi_end_date

Install (Windows):
python -m pip install requests pyodbc python-dotenv

Run:
python retail_promotions_to_shopify_metafields.py

.env (same folder as the script):
SHOPIFY_SHOP=xxx.myshopify.com
SHOPIFY_TOKEN=shpat_xxx
SHOPIFY_API_VERSION=2025-01
DB_SERVER=sql01-union\sql2012
DB_NAME=Ecomm_DB_PROD
DB_USER=ssis
DB_PASSWORD=ssis
SALE_PRE_DAYS=0
PI_PRE_DAYS=0
PI_POST_DAYS=0
DRY_RUN=1
DB_ONLY=0

Modes:
DB_ONLY=1 means read SSMS only, no Shopify calls.
DRY_RUN=1 means print actions only. DRY_RUN=0 means write and delete.

Keep Liquid consistent:
SALE_LEAD_DAYS = SALE_PRE_DAYS
PI_LEAD_DAYS = PI_PRE_DAYS
PI_FALLBACK_DAYS = PI_POST_DAYS
