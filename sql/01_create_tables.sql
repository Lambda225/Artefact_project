CREATE DATABASE artefect_db;
\c artefect_db

-- =========================================================
-- DIMENSIONS DE DOMAINE
-- =========================================================

CREATE TABLE dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_campaign (
    campaign_id SERIAL PRIMARY KEY,
    channel_id INT NOT NULL REFERENCES dim_channel(channel_id),
    campaign_name TEXT NOT NULL,
    UNIQUE (channel_id, campaign_name)
);

CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_color (
    color_id SERIAL PRIMARY KEY,
    color_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_size (
    size_id SERIAL PRIMARY KEY,
    size_label TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- =========================================================
-- DIMENSIONS METIER
-- =========================================================

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT ,
    email TEXT UNIQUE,
    gender TEXT,
    age_range TEXT,
    signup_date DATE,
    country_id INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INT NOT NULL REFERENCES dim_category(category_id),
    brand_id INT NOT NULL REFERENCES dim_brand(brand_id),
    color_id INT NOT NULL REFERENCES dim_color(color_id),
    size_id INT NOT NULL REFERENCES dim_size(size_id),
    cost_price NUMERIC(10,2) CHECK (cost_price >= 0),
    original_price NUMERIC(10,2) CHECK (original_price >= 0)
);

-- =========================================================
-- FAITS TRANSACTIONNELS
-- =========================================================

CREATE TABLE fact_sale (
    sale_id INT PRIMARY KEY,
    sale_date DATE NOT NULL,
    customer_id INT NOT NULL REFERENCES dim_customer(customer_id),
    campaign_id INT NOT NULL REFERENCES dim_campaign(campaign_id)
);

CREATE TABLE fact_sale_item (
    item_id INT PRIMARY KEY,
    sale_id INT NOT NULL REFERENCES fact_sale(sale_id),
    product_id INT NOT NULL REFERENCES dim_product(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    discount_percent NUMERIC(5,4) NOT NULL CHECK (discount_percent BETWEEN 0 AND 1)
);
