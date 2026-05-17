import os
from datetime import datetime, timezone

import pandas as pd
import requests


ORDERS_API_URL = os.getenv("ORDERS_API_URL", "http://96.9.212.102:8000/orders")
OUTPUT_DIR = os.getenv("ORDERS_DATA_LAKE_DIR", "/opt/airflow/data_lake/orders")


ORDER_COLUMNS = [
    "order_id",
    "user_id",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "eval_set",
]

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "add_to_cart_order",
    "reordered",
]


def _extract_orders(payload):
    if isinstance(payload, dict):
        orders = payload.get("orders", [])
    elif isinstance(payload, list):
        orders = payload
    else:
        raise ValueError("Format response API tidak dikenali. Harus berupa object atau list.")

    if not isinstance(orders, list):
        raise ValueError("Field 'orders' harus berupa array.")

    return orders


def _flatten_orders(orders):
    rows = []
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for order in orders:
        if not isinstance(order, dict):
            continue

        order_data = {column: order.get(column) for column in ORDER_COLUMNS}
        products = order.get("products") or []

        if not products:
            empty_product = {column: None for column in PRODUCT_COLUMNS}
            rows.append({**order_data, **empty_product, "ingested_at": ingested_at})
            continue

        for product in products:
            if not isinstance(product, dict):
                continue

            product_data = {column: product.get(column) for column in PRODUCT_COLUMNS}
            rows.append({**order_data, **product_data, "ingested_at": ingested_at})

    return rows


def fetch_orders():
    print(f"Mengambil data orders dari API: {ORDERS_API_URL}")

    try:
        response = requests.get(ORDERS_API_URL, timeout=30)
        response.raise_for_status()

        payload = response.json()
        orders = _extract_orders(payload)
        rows = _flatten_orders(orders)

        if not rows:
            raise ValueError("API berhasil diakses, tetapi tidak ada data order yang bisa diproses.")

        df = pd.DataFrame(rows)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"]).dt.floor("ms")

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(OUTPUT_DIR, f"order_items_{current_time}.parquet")
        df.to_parquet(
            output_path,
            index=False,
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )

        total_orders = payload.get("total_orders", len(orders)) if isinstance(payload, dict) else len(orders)
        print(
            f"Sukses menyimpan {len(df)} baris item dari {total_orders} orders "
            f"ke {output_path}"
        )
    except Exception as exc:
        print(f"Gagal menarik data orders: {exc}")
        raise


if __name__ == "__main__":
    fetch_orders()
