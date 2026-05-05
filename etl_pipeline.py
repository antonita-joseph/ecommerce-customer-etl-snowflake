"""
E-Commerce Customer ETL Pipeline
=================================
Dataset : Brazilian E-Commerce Public Dataset by Olist
         (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
Target  : Snowflake — schema ECOMMERCE, table DIM_CUSTOMERS
"""

import os
import logging
import hashlib
from datetime import datetime

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SNOWFLAKE CONFIG  (use env-vars in production)
# ─────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "user":       os.getenv("SNOWFLAKE_USER",     "your_user"),
    "password":   os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
    "account":    os.getenv("SNOWFLAKE_ACCOUNT",  "your_account"),   # e.g. abc123.us-east-1
    "warehouse":  os.getenv("SNOWFLAKE_WH",       "COMPUTE_WH"),
    "database":   os.getenv("SNOWFLAKE_DB",       "ANALYTICS"),
    "schema":     os.getenv("SNOWFLAKE_SCHEMA",   "ECOMMERCE"),
}

RAW_CUSTOMERS_CSV  = "data/olist_customers_dataset.csv"
RAW_ORDERS_CSV     = "data/olist_orders_dataset.csv"
RAW_PAYMENTS_CSV   = "data/olist_order_payments_dataset.csv"


# ══════════════════════════════════════════════
#  PHASE 1 — EXTRACT
# ══════════════════════════════════════════════

def extract() -> dict[str, pd.DataFrame]:
    """
    Read raw CSV files from disk (or swap in a DB/API call).
    Returns a dict of DataFrames keyed by logical name.
    """
    log.info("── EXTRACT ──────────────────────────────────")

    customers = pd.read_csv(RAW_CUSTOMERS_CSV)
    log.info(f"  customers  : {len(customers):>7,} rows  {customers.shape[1]} cols")

    orders = pd.read_csv(RAW_ORDERS_CSV, parse_dates=[
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
    ])
    log.info(f"  orders     : {len(orders):>7,} rows  {orders.shape[1]} cols")

    payments = pd.read_csv(RAW_PAYMENTS_CSV)
    log.info(f"  payments   : {len(payments):>7,} rows  {payments.shape[1]} cols")

    return {"customers": customers, "orders": orders, "payments": payments}


# ══════════════════════════════════════════════
#  PHASE 2 — TRANSFORM
# ══════════════════════════════════════════════

def _mask_id(raw_id: str) -> str:
    """SHA-256 the customer ID — keeps referential integrity, protects PII."""
    return hashlib.sha256(raw_id.encode()).hexdigest()[:16]


def transform(raw: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Business rules applied in the staging area before hitting Snowflake.

    Steps
    -----
    1. Standardise column names & types
    2. Deduplicate customers
    3. Derive order metrics per customer
    4. Classify customers into value segments
    5. Anonymise the customer key
    6. Add pipeline audit columns
    """
    log.info("── TRANSFORM ────────────────────────────────")

    customers = raw["customers"].copy()
    orders    = raw["orders"].copy()
    payments  = raw["payments"].copy()

    # ── 1. Standardise ───────────────────────────────────────────────────────
    customers.columns = customers.columns.str.lower().str.replace(" ", "_")
    orders.columns    = orders.columns.str.lower().str.replace(" ", "_")
    payments.columns  = payments.columns.str.lower().str.replace(" ", "_")

    customers["customer_city"]  = customers["customer_city"].str.title().str.strip()
    customers["customer_state"] = customers["customer_state"].str.upper().str.strip()
    log.info("  ✓ column names standardised")

    # ── 2. Deduplicate ───────────────────────────────────────────────────────
    before = len(customers)
    customers = customers.drop_duplicates(subset="customer_unique_id")
    log.info(f"  ✓ deduplication : {before - len(customers)} duplicates removed")

    # ── 3. Order metrics per customer ────────────────────────────────────────
    # Total revenue per order
    order_value = (
        payments
        .groupby("order_id")["payment_value"]
        .sum()
        .reset_index()
        .rename(columns={"payment_value": "order_value"})
    )

    orders_enriched = orders.merge(order_value, on="order_id", how="left")

    # Delivered orders only  (exclude cancelled / unavailable)
    delivered = orders_enriched[orders_enriched["order_status"] == "delivered"].copy()

    # Days to deliver
    delivered["days_to_deliver"] = (
        (delivered["order_delivered_customer_date"] -
         delivered["order_purchase_timestamp"])
        .dt.days
    )

    # Aggregate to customer level
    customer_stats = (
        delivered
        .groupby("customer_id")
        .agg(
            total_orders      = ("order_id",         "count"),
            total_revenue     = ("order_value",       "sum"),
            avg_order_value   = ("order_value",       "mean"),
            avg_days_deliver  = ("days_to_deliver",   "mean"),
            first_order_date  = ("order_purchase_timestamp", "min"),
            last_order_date   = ("order_purchase_timestamp", "max"),
        )
        .reset_index()
    )
    log.info("  ✓ order metrics aggregated")

    # ── 4. Customer value segmentation ───────────────────────────────────────
    def segment(row) -> str:
        if row["total_orders"] >= 3 and row["total_revenue"] >= 500:
            return "VIP"
        elif row["total_orders"] >= 2 or row["total_revenue"] >= 200:
            return "LOYAL"
        else:
            return "NEW"

    customer_stats["customer_segment"] = customer_stats.apply(segment, axis=1)
    log.info("  ✓ customer segments assigned")

    # ── 5. Join & anonymise ──────────────────────────────────────────────────
    dim = customers.merge(customer_stats, on="customer_id", how="left")

    # Fill customers who never placed a delivered order
    dim["total_orders"]     = dim["total_orders"].fillna(0).astype(int)
    dim["total_revenue"]    = dim["total_revenue"].fillna(0.0).round(2)
    dim["avg_order_value"]  = dim["avg_order_value"].fillna(0.0).round(2)
    dim["avg_days_deliver"] = dim["avg_days_deliver"].fillna(0.0).round(1)
    dim["customer_segment"] = dim["customer_segment"].fillna("NO_ORDER")

    dim["customer_key"] = dim["customer_unique_id"].apply(_mask_id)
    log.info("  ✓ customer IDs anonymised (SHA-256 truncated)")

    # ── 6. Audit columns ─────────────────────────────────────────────────────
    now = datetime.utcnow()
    dim["etl_loaded_at"]    = now
    dim["etl_pipeline_ver"] = "1.0.0"

    # ── Final column selection & rename ──────────────────────────────────────
    dim = dim[[
        "customer_key",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "avg_days_deliver",
        "first_order_date",
        "last_order_date",
        "customer_segment",
        "etl_loaded_at",
        "etl_pipeline_ver",
    ]].rename(columns={
        "customer_zip_code_prefix": "zip_code",
    })

    log.info(f"  ✓ final shape : {dim.shape[0]:,} rows × {dim.shape[1]} cols")
    log.info(f"  ✓ segments    : {dim['customer_segment'].value_counts().to_dict()}")
    return dim


# ══════════════════════════════════════════════
#  PHASE 3 — LOAD
# ══════════════════════════════════════════════

DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    customer_key        VARCHAR(16)      NOT NULL PRIMARY KEY,
    customer_city       VARCHAR(100),
    customer_state      CHAR(2),
    zip_code            VARCHAR(10),
    total_orders        INTEGER          DEFAULT 0,
    total_revenue       FLOAT            DEFAULT 0,
    avg_order_value     FLOAT            DEFAULT 0,
    avg_days_deliver    FLOAT,
    first_order_date    TIMESTAMP_NTZ,
    last_order_date     TIMESTAMP_NTZ,
    customer_segment    VARCHAR(20),
    etl_loaded_at       TIMESTAMP_NTZ,
    etl_pipeline_ver    VARCHAR(20)
);
"""

DDL_TRUNCATE = "TRUNCATE TABLE IF EXISTS DIM_CUSTOMERS;"


def load(df: pd.DataFrame) -> None:
    """
    Full-refresh load into Snowflake DIM_CUSTOMERS.
    Swap TRUNCATE+INSERT for a MERGE statement if you need incremental loads.
    """
    log.info("── LOAD ─────────────────────────────────────")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    try:
        # Ensure target table exists
        cur.execute(DDL_CREATE_TABLE)
        log.info("  ✓ DDL executed (CREATE TABLE IF NOT EXISTS)")

        # Full refresh
        cur.execute(DDL_TRUNCATE)
        log.info("  ✓ table truncated (full-refresh mode)")

        # Bulk load via Snowflake's optimised write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name="DIM_CUSTOMERS",
            quote_identifiers=False,
        )

        if success:
            log.info(f"  ✓ loaded {nrows:,} rows in {nchunks} chunk(s)")
        else:
            raise RuntimeError("write_pandas reported failure")

    finally:
        cur.close()
        conn.close()
        log.info("  ✓ Snowflake connection closed")


# ══════════════════════════════════════════════
#  ORCHESTRATOR
# ══════════════════════════════════════════════

def run_pipeline() -> None:
    log.info("═" * 50)
    log.info("  E-COMMERCE CUSTOMER ETL  —  START")
    log.info("═" * 50)
    start = datetime.utcnow()

    raw        = extract()
    clean      = transform(raw)
    load(clean)

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("═" * 50)
    log.info(f"  PIPELINE COMPLETE  ({elapsed:.1f}s)")
    log.info("═" * 50)


if __name__ == "__main__":
    run_pipeline()
