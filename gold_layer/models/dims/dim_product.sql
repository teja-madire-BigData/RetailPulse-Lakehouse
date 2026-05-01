SELECT
    product_id AS product_sk,
    product_id,
    sku,
    name AS product_name,
    category,
    subcategory,
    brand,
    base_price,
    current_price,
    final_price,
    price_vs_base,
    CASE
        WHEN base_price < 20    THEN 'budget'
        WHEN base_price < 100   THEN 'mid-range'
        WHEN base_price < 500   THEN 'premium'
        ELSE 'luxury'
    END AS price_band,
    is_active,
    discount_active,
    discount_pct,
    is_low_stock,
    rating,
    review_count,
    silver_processed_date

FROM {{ source('ecom_silver', 'silver_products') }}