"""
Airflow DAG: Minio (S3 API) -> Postgres ingestion, filtered by sale_date

✅ Requirements covered:
- Airflow Connections for Postgres + Minio
- Same logic as the Python ingestion script (filter by YYYYMMDD date)
- Idempotent via UPSERT (ON CONFLICT)
- Minimal error handling + logging

--- Airflow Connections to create ---

1) Minio connection (S3 API)
Conn Id:   minio
Conn Type: Amazon Web Services  (or "aws")
Login:     <MINIO_ACCESS_KEY>
Password:  <MINIO_SECRET_KEY>
Extra (JSON example):
{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1",
  "verify": false
}

2) Postgres connection
Conn Id:   pg_sales
Conn Type: Postgres
Host:      postgres
Schema:    postgres
Login:     postgres
Password:  postgres
Port:      5432

--- Airflow Variables (recommended) ---
MINIO_BUCKET         = folder_source
MINIO_OBJECT_KEY     = fashion_store_sales.csv
PG_SCHEMA            = public

Run:
- scheduled daily or manual
- run date defaults to ds_nodash (YYYYMMDD)
- you can override at trigger time: {"run_date":"20250616"}
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Tuple

import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from psycopg2.extras import execute_values


# -----------------------------
# Helpers
# -----------------------------
def parse_yyyymmdd(s: str) -> datetime.date:
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError as e:
        raise AirflowFailException(f"Invalid run_date '{s}'. Expected YYYYMMDD.") from e


def percent_str_to_decimal(x) -> Decimal:
    """
    CSV discount_percent might be like '12.50%'. Convert to 0.1250
    """
    if pd.isna(x):
        return Decimal("0")
    s = str(x).strip()
    if s.endswith("%"):
        s = s[:-1]
    if not s:
        return Decimal("0")
    s = s.replace(",", ".")
    d = Decimal(s) / Decimal("100")
    if d < 0:
        return Decimal("0")
    if d > 1:
        return Decimal("1")
    return d


def q(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def upsert_simple_dim(cur, schema: str, table: str, id_col: str, natural_col: str, values) -> Dict[str, int]:
    values = sorted({str(v).strip() for v in values if v is not None and str(v).strip() != ""})
    if not values:
        return {}

    insert_sql = f"""
        INSERT INTO {q(schema, table)} ({natural_col})
        VALUES %s
        ON CONFLICT ({natural_col}) DO NOTHING
    """
    execute_values(cur, insert_sql, [(v,) for v in values], page_size=1000)

    cur.execute(
        f"""
        SELECT {id_col}, {natural_col}
        FROM {q(schema, table)}
        WHERE {natural_col} = ANY(%s)
        """,
        (values,),
    )
    rows = cur.fetchall()
    return {r[1]: r[0] for r in rows}


def upsert_dim_campaign(cur, schema: str, channel_map: Dict[str, int], pairs: List[Tuple[str, str]]):
    cleaned = []
    for channel_name, campaign_name in pairs:
        if not channel_name or not campaign_name:
            continue
        cid = channel_map.get(channel_name)
        if cid is None:
            continue
        cleaned.append((cid, campaign_name))

    cleaned = sorted(set(cleaned))
    if not cleaned:
        return {}

    execute_values(
        cur,
        f"""
        INSERT INTO {q(schema, "dim_campaign")} (channel_id, campaign_name)
        VALUES %s
        ON CONFLICT (channel_id, campaign_name) DO NOTHING
        """,
        cleaned,
        page_size=1000,
    )

    # fetch back
    cur.execute(
        f"""
        SELECT campaign_id, channel_id, campaign_name
        FROM {q(schema, "dim_campaign")}
        WHERE (channel_id, campaign_name) IN %s
        """,
        (tuple(cleaned),),
    )
    rows = cur.fetchall()

    channel_id_to_name = {v: k for k, v in channel_map.items()}
    out = {}
    for campaign_id, channel_id, campaign_name in rows:
        cname = channel_id_to_name.get(channel_id)
        if cname:
            out[(cname, campaign_name)] = campaign_id
    return out


def upsert_dim_customer(cur, schema: str, rows: List[Tuple]):
    if not rows:
        return
    execute_values(
        cur,
        f"""
        INSERT INTO {q(schema, "dim_customer")} (
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
        """,
        rows,
        page_size=1000,
    )


def upsert_dim_product(cur, schema: str, rows: List[Tuple]):
    if not rows:
        return
    execute_values(
        cur,
        f"""
        INSERT INTO {q(schema, "dim_product")} (
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
        """,
        rows,
        page_size=1000,
    )


def upsert_fact_sale(cur, schema: str, rows: List[Tuple]):
    if not rows:
        return
    execute_values(
        cur,
        f"""
        INSERT INTO {q(schema, "fact_sale")} (sale_id, sale_date,total_amount, customer_id, campaign_id)
        VALUES %s
        ON CONFLICT (sale_id) DO UPDATE SET
          sale_date   = EXCLUDED.sale_date,
          total_amount = EXCLUDED.total_amount,
          customer_id = EXCLUDED.customer_id,
          campaign_id = EXCLUDED.campaign_id
        """,
        rows,
        page_size=1000,
    )


def upsert_fact_sale_item(cur, schema: str, rows: List[Tuple]):
    if not rows:
        return
    execute_values(
        cur,
        f"""
        INSERT INTO {q(schema, "fact_sale_item")} (
          item_id, sale_id, product_id, quantity, discount_percent
        )
        VALUES %s
        ON CONFLICT (item_id) DO UPDATE SET
          sale_id          = EXCLUDED.sale_id,
          product_id       = EXCLUDED.product_id,
          quantity         = EXCLUDED.quantity,
          discount_percent = EXCLUDED.discount_percent
        """,
        rows,
        page_size=1000,
    )


# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="fashion_sales_minio_to_postgres",
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    tags=["data-eng", "minio", "postgres", "ingestion"],
    is_paused_upon_creation=False,
    default_args={"retries": 1},
)
def build_dag():
    @task
    def ingest(**context):
        # Run date: priority 1) dagrun.conf 2) ds_nodash
        dagrun = context.get("dag_run")
        conf = (dagrun.conf or {}) if dagrun else {}
        run_date_str = conf.get("run_date") or context["ds_nodash"]
        run_dt = parse_yyyymmdd(run_date_str)

        schema = Variable.get("PG_SCHEMA", default_var="public")
        bucket = Variable.get("MINIO_BUCKET", default_var="folder-source")
        object_key = Variable.get("MINIO_OBJECT_KEY", default_var="fashion_store_sales _ Data Eng.csv")

        logging.info("Starting ingestion for sale_date=%s", run_dt.isoformat())
        logging.info("Minio source: bucket=%s, key=%s", bucket, object_key)
        logging.info("Target schema: %s", schema)

        # --- Read from Minio via S3Hook ---
        s3 = S3Hook(aws_conn_id="minio")
        try:
            obj = s3.get_key(key=object_key, bucket_name=bucket)
            if obj is None:
                raise AirflowFailException(f"Object not found in Minio: s3://{bucket}/{object_key}")
            data = obj.get()["Body"].read()
        except Exception as e:
            raise AirflowFailException(f"Failed to read from Minio (s3://{bucket}/{object_key}).") from e

        # --- Load CSV ---
        df = pd.read_csv(io.BytesIO(data))
        if "sale_date" not in df.columns:
            raise AirflowFailException("CSV must contain column 'sale_date'.")

        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
        df = df[df["sale_date"] == run_dt].copy()

        if df.empty:
            logging.info("No rows for %s. Nothing to ingest.", run_dt.isoformat())
            return "NO_DATA"

        # Normalize discount fields
        df["discount_percent_dec"] = df["discount_percent"].apply(percent_str_to_decimal)

        # --- Write to Postgres in a single transaction ---
        pg = PostgresHook(postgres_conn_id="pg_sales")
        conn = pg.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # dims
                channel_map = upsert_simple_dim(cur, schema, "dim_channel", "channel_id", "channel_name", df["channel"].tolist())
                category_map = upsert_simple_dim(cur, schema, "dim_category", "category_id", "category_name", df["category"].tolist())
                brand_map = upsert_simple_dim(cur, schema, "dim_brand", "brand_id", "brand_name", df["brand"].tolist())
                color_map = upsert_simple_dim(cur, schema, "dim_color", "color_id", "color_name", df["color"].tolist())
                size_map = upsert_simple_dim(cur, schema, "dim_size", "size_id", "size_label", df["size"].tolist())
                country_map = upsert_simple_dim(cur, schema, "dim_country", "country_id", "country_name", df["country"].tolist())

                # campaign
                pairs = list(zip(df["channel"].astype(str), df["channel_campaigns"].astype(str)))
                campaign_map = upsert_dim_campaign(cur, schema, channel_map, pairs)

                # customers
                cust_rows: List[Tuple] = []
                for r in df[
                    ["customer_id", "first_name", "last_name", "email", "gender", "age_range", "signup_date", "country"]
                ].drop_duplicates().itertuples(index=False):
                    customer_id, first_name, last_name, email, gender, age_range, signup_date, country = r
                    if pd.isna(customer_id):
                        continue
                    country_id = country_map.get(str(country)) if not pd.isna(country) else None
                    signup_date_dt = pd.to_datetime(signup_date, errors="coerce").date() if not pd.isna(signup_date) else None
                    cust_rows.append(
                        (
                            int(customer_id),
                            None if pd.isna(first_name) else str(first_name),
                            None if pd.isna(last_name) else str(last_name),
                            None if pd.isna(email) else str(email),
                            None if pd.isna(gender) else str(gender),
                            None if pd.isna(age_range) else str(age_range),
                            signup_date_dt,
                            country_id,
                        )
                    )
                upsert_dim_customer(cur, schema, cust_rows)

                # products
                prod_rows: List[Tuple] = []
                for r in df[
                    ["product_id", "product_name", "category", "brand", "color", "size", "cost_price", "original_price"]
                ].drop_duplicates().itertuples(index=False):
                    product_id, product_name, category, brand, color, size, cost_price, original_price = r
                    if pd.isna(product_id):
                        continue
                    prod_rows.append(
                        (
                            int(product_id),
                            None if pd.isna(product_name) else str(product_name),
                            category_map.get(str(category)) if not pd.isna(category) else None,
                            brand_map.get(str(brand)) if not pd.isna(brand) else None,
                            color_map.get(str(color)) if not pd.isna(color) else None,
                            size_map.get(str(size)) if not pd.isna(size) else None,
                            float(Decimal(str(cost_price)) if not pd.isna(cost_price) else Decimal("0")),
                            float(Decimal(str(original_price)) if not pd.isna(original_price) else Decimal("0")),
                        )
                    )
                upsert_dim_product(cur, schema, prod_rows)

                # fact_sale
                sale_rows: List[Tuple] = []
                for r in df[["sale_id", "sale_date","total_amount","customer_id", "channel", "channel_campaigns"]].drop_duplicates(subset=["sale_id"]).itertuples(index=False):
                    sale_id, sale_date_dt, total_amount, customer_id, channel, campaign = r
                    if pd.isna(sale_id) or pd.isna(customer_id):
                        continue
                    camp_id = campaign_map.get((str(channel), str(campaign)))
                    if camp_id is None:
                        raise AirflowFailException(f"Missing campaign_id mapping for channel={channel} campaign={campaign}")
                    sale_rows.append((int(sale_id), sale_date_dt, float(total_amount), int(customer_id), int(camp_id)))
                upsert_fact_sale(cur, schema, sale_rows)

                # fact_sale_item
                item_rows: List[Tuple] = []
                for r in df[
                    ["item_id", "sale_id", "product_id", "quantity", "discount_percent_dec"]
                ].drop_duplicates(subset=["item_id"]).itertuples(index=False):
                    item_id, sale_id, product_id, quantity, discount_percent_dec = r
                    if pd.isna(item_id) or pd.isna(sale_id) or pd.isna(product_id):
                        continue
                    item_rows.append(
                        (
                            int(item_id),
                            int(sale_id),
                            int(product_id),
                            int(quantity),
                            float(discount_percent_dec),
                        )
                    )
                upsert_fact_sale_item(cur, schema, item_rows)

            conn.commit()
            logging.info("Ingestion committed successfully for %s (%d rows).", run_dt.isoformat(), len(df))
            return f"OK:{len(df)}"

        except Exception as e:
            conn.rollback()
            logging.exception("Ingestion failed. Rolled back.")
            raise AirflowFailException("Ingestion failed (transaction rolled back).") from e
        finally:
            conn.close()

    ingest()


dag = build_dag()
