WITH orders AS (
    SELECT * FROM {{ source('ecom_silver', 'silver_orders') }}
),

dim_product     AS (SELECT * FROM {{ ref('dim_product') }}),
dim_user        AS (SELECT * FROM {{ ref('dim_user') }}),
dim_date        AS (SELECT * FROM {{ ref('dim_date') }}),
dim_currency    AS (SELECT * FROM {{ ref('dim_currency') }}),
dim_seller      AS (SELECT * FROM {{ ref('dim_seller') }})

SELECT
    o.order_id,
    COALESCE(dp.product_sk,  -1)        AS product_sk,
    COALESCE(du.user_sk,     -1)        AS user_sk,
    COALESCE(dd.date_sk,     -1)        AS date_sk,
    COALESCE(dc.currency_sk, 'USD')     AS currency_sk,
    COALESCE(ds.seller_sk,   -1)        AS top_seller_sk,
    o.order_status,
    o.channel,
    o.payment_method,
    o.currency,
    CAST(o.ordered_at AS TIMESTAMP(3))              AS ordered_at,
    CAST(o.estimated_delivery_at AS TIMESTAMP(3))   AS estimated_delivery_at,
    o.gross_total,
    o.discount_total,
    o.net_total,
    o.shipping_cost,
    o.tax_amount,
    o.order_total,
    ROUND(o.order_total / COALESCE(dc.usd_exchange_rate, 1.0), 2)   AS order_total_usd,
    ROUND(o.net_total   / COALESCE(dc.usd_exchange_rate, 1.0), 2)   AS net_total_usd,
    o.item_count,
    o.total_quantity,
    o.top_category,
    o.top_seller_id,
    o.products_json,
    o.is_gift,
    o.promo_code_used,
    o.has_discount,
    o.order_value_band,
    o.days_to_delivery,
    dp.category                         AS product_category,
    dp.price_band                       AS product_price_band,
    du.customer_tier,
    du.country                          AS user_country,
    du.spend_segment,
    ds.seller_name                      AS top_seller_name,
    ds.category_specialisation          AS seller_category,
    ds.seller_tier,
    ds.fulfillment_band,
    ds.commission_rate,
    dd.year                             AS order_year,
    dd.month                            AS order_month,
    dd.quarter                          AS order_quarter,
    dd.is_weekend,
    o.silver_processed_date

FROM orders o

LEFT JOIN dim_user du
    ON o.user_id = du.user_id

LEFT JOIN dim_date dd
    ON CAST(CAST(o.ordered_at AS TIMESTAMP(3)) AS DATE) = dd.date

LEFT JOIN dim_currency dc
    ON o.currency = dc.code

LEFT JOIN dim_seller ds
    ON o.top_seller_id = ds.seller_id

LEFT JOIN (
    SELECT
        category,
        product_sk,
        price_band
    FROM (
        SELECT
            category,
            product_sk,
            price_band,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY product_sk) AS rn
        FROM {{ ref('dim_product') }}
    )
    WHERE rn = 1
) dp
    ON o.top_category = dp.category