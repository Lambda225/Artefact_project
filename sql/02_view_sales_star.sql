-- =========================================================
-- VUE EN ETOILE : SALES_STAR_VIEW
-- =========================================================
\c artefect_db

CREATE OR REPLACE VIEW vw_sales_star AS
SELECT
    -- =======================
    -- FACTS
    -- =======================
    fs.sale_id,
    fsi.item_id,
    fs.sale_date,
    fsi.quantity,
    fsi.discount_percent,

    -- Valeurs calculées (PAS stockées en DKNF)
    CASE WHEN fsi.discount_percent > 0 THEN 1 ELSE 0 END AS discounted,
    (fsi.discount_percent * dp.original_price) AS discount_applied,
    dp.original_price - (fsi.discount_percent * dp.original_price) AS unit_price,
    (fsi.quantity * dp.original_price) AS gross_amount,
    (dp.original_price - (fsi.discount_percent * dp.original_price)) * fsi.quantity  AS item_total,

    -- =======================
    -- DIMENSIONS CLIENT
    -- =======================
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.gender,
    dc.age_range,
    dc.signup_date,
    dco.country_name,

    -- =======================
    -- DIMENSIONS PRODUIT
    -- =======================
    dp.product_id,
    dp.product_name,
    dcat.category_name,
    db.brand_name,
    dcol.color_name,
    ds.size_label,
    dp.cost_price,
    dp.original_price,

    -- =======================
    -- DIMENSIONS MARKETING
    -- =======================
    dch.channel_name,
    dca.campaign_name

FROM fact_sale fs
JOIN fact_sale_item fsi
    ON fs.sale_id = fsi.sale_id

JOIN dim_customer dc
    ON fs.customer_id = dc.customer_id
JOIN dim_country dco
    ON dc.country_id = dco.country_id

JOIN dim_product dp
    ON fsi.product_id = dp.product_id
JOIN dim_category dcat
    ON dp.category_id = dcat.category_id
JOIN dim_brand db
    ON dp.brand_id = db.brand_id
JOIN dim_color dcol
    ON dp.color_id = dcol.color_id
JOIN dim_size ds
    ON dp.size_id = ds.size_id

JOIN dim_campaign dca
    ON fs.campaign_id = dca.campaign_id
JOIN dim_channel dch
    ON dca.channel_id = dch.channel_id;
