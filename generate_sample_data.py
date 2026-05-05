"""
generate_sample_data.py
-----------------------
Creates realistic sample CSVs that mirror the Olist schema so the
ETL pipeline can be run without downloading the full Kaggle dataset.
"""

import os
import random
import hashlib
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

os.makedirs("data", exist_ok=True)

STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]
CITIES = {
    "SP": ["Sao Paulo", "Campinas", "Guarulhos", "Santo Andre"],
    "RJ": ["Rio De Janeiro", "Niteroi", "Duque De Caxias"],
    "MG": ["Belo Horizonte", "Uberlandia", "Contagem"],
    "RS": ["Porto Alegre", "Caxias Do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringa"],
    "SC": ["Florianopolis", "Joinville", "Blumenau"],
    "BA": ["Salvador", "Feira De Santana"],
    "GO": ["Goiania", "Aparecida De Goiania"],
    "PE": ["Recife", "Olinda", "Caruaru"],
    "CE": ["Fortaleza", "Caucaia"],
}
STATUSES = ["delivered", "delivered", "delivered", "shipped", "cancelled", "unavailable"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]

N_CUSTOMERS = 1_000
N_ORDERS    = 1_400      # ~1.4 orders per customer on average


def fake_id(prefix: str, i: int) -> str:
    return hashlib.md5(f"{prefix}{i}".encode()).hexdigest()


# ── customers ────────────────────────────────────────────────────────────────
customer_rows = []
for i in range(N_CUSTOMERS):
    state = random.choice(STATES)
    customer_rows.append({
        "customer_id":           fake_id("cust", i),
        "customer_unique_id":    fake_id("uniq", i),
        "customer_zip_code_prefix": f"{random.randint(10000, 99999)}",
        "customer_city":         random.choice(CITIES[state]),
        "customer_state":        state,
    })

customers_df = pd.DataFrame(customer_rows)
# Inject ~2 % duplicates on customer_unique_id
dups = customers_df.sample(frac=0.02).copy()
customers_df = pd.concat([customers_df, dups], ignore_index=True)
customers_df.to_csv("data/olist_customers_dataset.csv", index=False)
print(f"customers  : {len(customers_df):,} rows (includes {len(dups)} dupes)")


# ── orders ───────────────────────────────────────────────────────────────────
base_date = datetime(2017, 1, 1)
order_rows = []
for i in range(N_ORDERS):
    cust        = random.choice(customer_rows)
    purchase_dt = base_date + timedelta(days=random.randint(0, 700),
                                        hours=random.randint(0, 23))
    approved_dt = purchase_dt + timedelta(hours=random.randint(1, 48))
    deliver_dt  = approved_dt + timedelta(days=random.randint(3, 30))
    status      = random.choice(STATUSES)

    order_rows.append({
        "order_id":                          fake_id("ord", i),
        "customer_id":                       cust["customer_id"],
        "order_status":                      status,
        "order_purchase_timestamp":          purchase_dt.isoformat(),
        "order_approved_at":                 approved_dt.isoformat(),
        "order_delivered_carrier_date":      (approved_dt + timedelta(days=2)).isoformat(),
        "order_delivered_customer_date":     deliver_dt.isoformat() if status == "delivered" else "",
        "order_estimated_delivery_date":     (approved_dt + timedelta(days=15)).isoformat(),
    })

orders_df = pd.DataFrame(order_rows)
orders_df.to_csv("data/olist_orders_dataset.csv", index=False)
print(f"orders     : {len(orders_df):,} rows")


# ── payments ─────────────────────────────────────────────────────────────────
payment_rows = []
for row in order_rows:
    n_installments = random.choice([1, 1, 1, 2, 3, 6, 12])
    payment_rows.append({
        "order_id":            row["order_id"],
        "payment_sequential":  1,
        "payment_type":        random.choice(PAYMENT_TYPES),
        "payment_installments": n_installments,
        "payment_value":       round(random.uniform(20, 800), 2),
    })

payments_df = pd.DataFrame(payment_rows)
payments_df.to_csv("data/olist_order_payments_dataset.csv", index=False)
print(f"payments   : {len(payments_df):,} rows")

print("\nSample data written to ./data/")
