#!/usr/bin/env python3
"""
Ingestion script for fashion_store_sales (Minio -> Postgres), filtered by sale_date.

Usage:
  python main.py 20250616

Expected environment variables:
  # Minio
  MINIO_ENDPOINT=localhost:9000
  MINIO_ACCESS_KEY=minioadmin
  MINIO_SECRET_KEY=minioadmin
  MINIO_SECURE=false
  MINIO_BUCKET=folder_source
  MINIO_OBJECT_KEY=fashion_store_sales.csv   # the object key in the bucket

  # Postgres
  PG_HOST=localhost
  PG_PORT=5432
  PG_DB=postgres
  PG_USER=postgres
  PG_PASSWORD=postgres
  PG_SCHEMA=public   # optional; default public

Notes:
- Idempotent via UPSERT (ON CONFLICT) on primary / unique keys.
- Assumes you created tables:
  dim_channel(channel_id PK, channel_name UNIQUE)
  dim_campaign(campaign_id PK, channel_id FK, campaign_name, UNIQUE(channel_id,campaign_name))
  dim_category(category_id PK, category_name UNIQUE)
  dim_brand(brand_id PK, brand_name UNIQUE)
  dim_color(color_id PK, color_name UNIQUE)
  dim_size(size_id PK, size_label UNIQUE)
  dim_country(country_id PK, country_name UNIQUE)
  dim_customer(customer_id PK, ... , email UNIQUE)
  dim_product(product_id PK, ...)

  fact_sale(sale_id PK, sale_date, customer_id FK, campaign_id FK)
  fact_sale_item(item_id PK, sale_id FK, product_id FK, quantity,
                discount_percent)
"""

import argparse
import io
import logging
import os
from dotenv import load_dotenv
import sys
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from minio import Minio

load_dotenv()
# -----------------------------
# Logging
# -----------------------------
def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


# -----------------------------
# Helpers: parsing & cleaning
# -----------------------------
def parse_yyyymmdd(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date '{s}'. Expected YYYYMMDD.") from e


def to_int(x):
    if pd.isna(x):
        return None
    return int(x)


def to_decimal(x) -> Decimal:
    if pd.isna(x):
        return Decimal("0")
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid decimal value: {x}") from e


def percent_str_to_decimal(x) -> Decimal:
    """
    CSV discount_percent is like '12.50%'. Convert to 0.1250
    """
    if pd.isna(x):
        return Decimal("0")
    s = str(x).strip()
    if s.endswith("%"):
        s = s[:-1]
    if s == "":
        return Decimal("0")
    # handle comma decimals just in case
    s = s.replace(",", ".")
    d = Decimal(s) / Decimal("100")
    # clamp to [0,1] defensively
    if d < 0:
        d = Decimal("0")
    if d > 1:
        d = Decimal("1")
    return d


# -----------------------------
# Minio: read CSV bytes
# -----------------------------
def get_minio_client() -> Minio:
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    secure = os.getenv("MINIO_SECURE", "false").lower() in {"1", "true", "t", "yes", "y"}
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def read_csv_from_minio() -> pd.DataFrame:
    bucket = os.getenv("MINIO_BUCKET", "folder-source")
    object_key = os.getenv("MINIO_OBJECT_KEY", "fashion_store_sales.csv")

    client = get_minio_client()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"Minio bucket '{bucket}' does not exist.")

    logging.info("Downloading from Minio: bucket=%s key=%s", bucket, object_key)
    resp = client.get_object(bucket, object_key)
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()

    return pd.read_csv(io.BytesIO(data))


# -----------------------------
# Postgres: connect & upserts
# -----------------------------
def pg_connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def qualify(schema: str, table: str) -> str:
    schema = schema or "public"
    return f"{schema}.{table}"


def upsert_simple_dim(
    cur,
    schema: str,
    table: str,
    id_col: str,
    natural_col: str,
    values: Sequence[str],
) -> Dict[str, int]:
    """
    Insert distinct values into a simple dimension table with a single natural key column.
    Returns mapping natural_value -> id.
    """
    values = sorted({v for v in values if v is not None and str(v).strip() != ""})
    if not values:
        return {}

    qtable = qualify(schema, table)

    insert_sql = f"""
        INSERT INTO {qtable} ({natural_col})
        VALUES %s
        ON CONFLICT ({natural_col}) DO NOTHING
    """
    execute_values(cur, insert_sql, [(v,) for v in values], page_size=1000)

    select_sql = f"""
        SELECT {id_col}, {natural_col}
        FROM {qtable}
        WHERE {natural_col} = ANY(%s)
    """
    cur.execute(select_sql, (values,))
    rows = cur.fetchall()
    return {r[1]: r[0] for r in rows}


def upsert_dim_campaign(
    cur,
    schema: str,
    channel_map: Dict[str, int],
    channel_campaign_pairs: Sequence[Tuple[str, str]],
) -> Dict[Tuple[str, str], int]:
    """
    Upsert campaigns with unique(channel_id,campaign_name).
    Returns mapping (channel_name, campaign_name) -> campaign_id
    """
    cleaned = []
    for channel_name, campaign_name in channel_campaign_pairs:
        if not channel_name or not campaign_name:
            continue
        channel_id = channel_map.get(channel_name)
        if channel_id is None:
            continue
        cleaned.append((channel_id, campaign_name))

    cleaned = sorted(set(cleaned))
    if not cleaned:
        return {}

    qtable = qualify(schema, "dim_campaign")

    insert_sql = f"""
        INSERT INTO {qtable} (channel_id, campaign_name)
        VALUES %s
        ON CONFLICT (channel_id, campaign_name) DO NOTHING
    """
    execute_values(cur, insert_sql, cleaned, page_size=1000)

    # fetch ids
    cur.execute(
        f"""
        SELECT campaign_id, channel_id, campaign_name
        FROM {qtable}
        WHERE (channel_id, campaign_name) IN %s
        """,
        (tuple(cleaned),),
    )
    rows = cur.fetchall()

    # Build reverse channel_id -> channel_name for mapping key
    channel_id_to_name = {v: k for k, v in channel_map.items()}
    out = {}
    for campaign_id, channel_id, campaign_name in rows:
        channel_name = channel_id_to_name.get(channel_id)
        if channel_name:
            out[(channel_name, campaign_name)] = campaign_id
    return out


def upsert_dim_customer(cur, schema: str, rows: Sequence[Tuple]):
    """
    dim_customer (customer_id is PK). SCD1 update.
    Columns expected:
      customer_id, first_name, last_name, email, gender, age_range, signup_date, country_id
    """
    if not rows:
        return
    qtable = qualify(schema, "dim_customer")
    sql = f"""
        INSERT INTO {qtable} (
          customer_id, first_name, last_name, email, gender, age_range, signup_date, country_id
        )
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
          first_name = EXCLUDED.first_name,
          last_name  = EXCLUDED.last_name,
          email      = EXCLUDED.email,
          gender     = EXCLUDED.gender,
          age_range  = EXCLUDED.age_range,
          signup_date= EXCLUDED.signup_date,
          country_id = EXCLUDED.country_id
    """
    execute_values(cur, sql, rows, page_size=1000)


def upsert_dim_product(cur, schema: str, rows: Sequence[Tuple]):
    """
    dim_product (product_id is PK). SCD1 update.
    Columns expected:
      product_id, product_name, category_id, brand_id, color_id, size_id,
      cost_price, original_price
    """
    if not rows:
        return
    qtable = qualify(schema, "dim_product")
    sql = f"""
        INSERT INTO {qtable} (
          product_id, product_name, category_id, brand_id, color_id, size_id,
          cost_price, original_price
        )
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE SET
          product_name   = EXCLUDED.product_name,
          category_id    = EXCLUDED.category_id,
          brand_id       = EXCLUDED.brand_id,
          color_id       = EXCLUDED.color_id,
          size_id        = EXCLUDED.size_id,
          cost_price     = EXCLUDED.cost_price,
          original_price = EXCLUDED.original_price
    """
    execute_values(cur, sql, rows, page_size=1000)


def upsert_fact_sale(cur, schema: str, rows: Sequence[Tuple]):
    """
    fact_sale: (sale_id PK)
    Columns: sale_id, sale_date, total_amount, customer_id, campaign_id
    """
    if not rows:
        return
    qtable = qualify(schema, "fact_sale")
    sql = f"""
        INSERT INTO {qtable} (sale_id, sale_date,total_amount, customer_id, campaign_id)
        VALUES %s
        ON CONFLICT (sale_id) DO UPDATE SET
          sale_date   = EXCLUDED.sale_date,
          total_amount = EXCLUDED.total_amount,
          customer_id = EXCLUDED.customer_id,
          campaign_id = EXCLUDED.campaign_id
    """
    execute_values(cur, sql, rows, page_size=1000)


def upsert_fact_sale_item(cur, schema: str, rows: Sequence[Tuple]):
    """
    fact_sale_item: (item_id PK)
    Columns: item_id, sale_id, product_id, quantity, discount_percent
    """
    if not rows:
        return
    qtable = qualify(schema, "fact_sale_item")
    sql = f"""
        INSERT INTO {qtable} (
          item_id, sale_id, product_id, quantity,
          discount_percent
        )
        VALUES %s
        ON CONFLICT (item_id) DO UPDATE SET
          sale_id          = EXCLUDED.sale_id,
          product_id       = EXCLUDED.product_id,
          quantity         = EXCLUDED.quantity,
          discount_percent = EXCLUDED.discount_percent
    """
    execute_values(cur, sql, rows, page_size=1000)


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("run_date", help="Date in YYYYMMDD format, e.g. 20250616")
    args = parser.parse_args()

    run_dt = parse_yyyymmdd(args.run_date)
    schema = os.getenv("PG_SCHEMA", "public")

    # 1) Read + filter
    df = read_csv_from_minio()
    if "sale_date" not in df.columns:
        raise RuntimeError("CSV must contain column 'sale_date'.")

    # normalize sale_date to date
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    df = df[df["sale_date"] == run_dt].copy()

    if df.empty:
        logging.info("No rows found for sale_date=%s. Nothing to ingest.", run_dt.isoformat())
        return

    logging.info("Rows to ingest for %s: %d", run_dt.isoformat(), len(df))

    # 2) Clean columns needed
    # discount_percent like "12.50%"
    df["discount_percent_dec"] = df["discount_percent"].apply(percent_str_to_decimal)

    # 3) Connect to Postgres + transaction
    conn = pg_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # 3.1 Upsert simple dimensions
            channel_map = upsert_simple_dim(cur, schema, "dim_channel", "channel_id", "channel_name", df["channel"].tolist())

            category_map = upsert_simple_dim(cur, schema, "dim_category", "category_id", "category_name", df["category"].tolist())
            brand_map = upsert_simple_dim(cur, schema, "dim_brand", "brand_id", "brand_name", df["brand"].tolist())
            color_map = upsert_simple_dim(cur, schema, "dim_color", "color_id", "color_name", df["color"].tolist())
            size_map = upsert_simple_dim(cur, schema, "dim_size", "size_id", "size_label", df["size"].tolist())
            country_map = upsert_simple_dim(cur, schema, "dim_country", "country_id", "country_name", df["country"].tolist())

            # 3.2 Campaigns (composite unique)
            pairs = list(zip(df["channel"].astype(str), df["channel_campaigns"].astype(str)))
            campaign_map = upsert_dim_campaign(cur, schema, channel_map, pairs)

            # 3.3 Customers (PK = customer_id)
            cust_rows = []
            for r in df[["customer_id", "first_name", "last_name", "email", "gender", "age_range", "signup_date", "country"]].drop_duplicates().itertuples(index=False):
                customer_id, first_name, last_name, email, gender, age_range, signup_date, country = r
                if pd.isna(customer_id):
                    continue
                country_id = country_map.get(str(country)) if not pd.isna(country) else None
                # dates
                signup_date_dt = pd.to_datetime(signup_date, errors="coerce").date() if not pd.isna(signup_date) else None
                cust_rows.append((
                    int(customer_id),
                    str(first_name) if not pd.isna(first_name) else None,
                    str(last_name) if not pd.isna(last_name) else None,
                    str(email) if not pd.isna(email) else None,
                    str(gender) if not pd.isna(gender) else None,
                    str(age_range) if not pd.isna(age_range) else None,
                    signup_date_dt,
                    country_id
                ))
            upsert_dim_customer(cur, schema, cust_rows)

            # 3.4 Products (PK = product_id)
            prod_rows = []
            for r in df[[
                "product_id", "product_name", "category", "brand", "color", "size",
                "cost_price", "original_price"
            ]].drop_duplicates().itertuples(index=False):
                (
                    product_id, product_name, category, brand, color, size
                    , cost_price, original_price
                ) = r
                if pd.isna(product_id):
                    continue
                prod_rows.append((
                    int(product_id),
                    str(product_name) if not pd.isna(product_name) else None,
                    category_map.get(str(category)) if not pd.isna(category) else None,
                    brand_map.get(str(brand)) if not pd.isna(brand) else None,
                    color_map.get(str(color)) if not pd.isna(color) else None,
                    size_map.get(str(size)) if not pd.isna(size) else None,
                    float(to_decimal(cost_price)),
                    float(to_decimal(original_price)),
                ))
            upsert_dim_product(cur, schema, prod_rows)

            # 3.5 fact_sale (PK sale_id)
            sale_rows = []
            for r in df[["sale_id", "sale_date","total_amount", "customer_id", "channel", "channel_campaigns"]].drop_duplicates(subset=["sale_id"]).itertuples(index=False):
                sale_id, sale_date_dt, total_amount, customer_id, channel, campaign = r
                if pd.isna(sale_id) or pd.isna(customer_id):
                    continue
                camp_id = campaign_map.get((str(channel), str(campaign)))
                if camp_id is None:
                    raise RuntimeError(f"Missing campaign_id mapping for channel={channel} campaign={campaign}")
                sale_rows.append((
                    int(sale_id),
                    sale_date_dt,
                    float(total_amount),
                    int(customer_id),
                    int(camp_id)
                ))
            upsert_fact_sale(cur, schema, sale_rows)

            # 3.6 fact_sale_item (PK item_id)
            item_rows = []
            for r in df[[
                "item_id", "sale_id", "product_id", "quantity",
                "discount_percent_dec"
            ]].drop_duplicates(subset=["item_id"]).itertuples(index=False):
                item_id, sale_id, product_id, quantity, discount_percent_dec = r
                if pd.isna(item_id) or pd.isna(sale_id) or pd.isna(product_id):
                    continue
                item_rows.append((
                    int(item_id),
                    int(sale_id),
                    int(product_id),
                    int(quantity),
                    float(discount_percent_dec),
                ))
            upsert_fact_sale_item(cur, schema, item_rows)

        conn.commit()
        logging.info("Ingestion committed successfully for %s.", run_dt.isoformat())

    except Exception:
        conn.rollback()
        logging.exception("Ingestion failed. Rolled back transaction.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
