SELECT
    date_sk,
    CAST(CAST(ordered_at AS TIMESTAMP(3)) AS DATE) AS order_date,
    order_year,
    order_month,
    order_quarter,
    product_category AS category,
    channel,
    customer_tier,
    user_country,
    seller_tier,
    seller_category,
    COUNT(order_id) AS total_orders,
    COUNT(DISTINCT user_sk)  AS unique_customers,
    COUNT(DISTINCT top_seller_sk) AS unique_sellers,
    SUM(item_count) AS total_items,
    SUM(total_quantity) AS total_units,
    ROUND(SUM(order_total_usd), 2) AS revenue_usd,
    ROUND(SUM(net_total_usd), 2) AS net_revenue_usd,
    ROUND(AVG(order_total_usd), 2) AS avg_order_value_usd,
    ROUND(MAX(order_total_usd), 2) AS max_order_value_usd,
    ROUND(SUM(order_total_usd * commission_rate), 2) AS total_commission_usd,
    SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) AS discounted_orders,
    ROUND(
        SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(order_id), 0), 1
    ) AS discount_rate_pct,
    SUM(CASE WHEN order_value_band = 'low'     THEN 1 ELSE 0 END) AS low_value_orders,
    SUM(CASE WHEN order_value_band = 'medium'  THEN 1 ELSE 0 END) AS medium_value_orders,
    SUM(CASE WHEN order_value_band = 'high'    THEN 1 ELSE 0 END) AS high_value_orders,
    SUM(CASE WHEN order_value_band = 'premium' THEN 1 ELSE 0 END) AS premium_value_orders,
    SUM(CASE WHEN is_gift         THEN 1 ELSE 0 END) AS gift_orders,
    SUM(CASE WHEN promo_code_used THEN 1 ELSE 0 END) AS promo_orders,
    SUM(CASE WHEN order_status = 'delivered'   THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN order_status = 'cancelled'   THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN order_status = 'returned'    THEN 1 ELSE 0 END) AS returned_orders,
    SUM(CASE WHEN order_status = 'refunded'    THEN 1 ELSE 0 END) AS refunded_orders,
    is_weekend

FROM {{ ref('fact_orders') }}
WHERE order_status NOT IN ('cancelled', 'refunded')

GROUP BY
    date_sk,
    CAST(CAST(ordered_at AS TIMESTAMP(3)) AS DATE),
    order_year,
    order_month,
    order_quarter,
    product_category,
    channel,
    customer_tier,
    user_country,
    seller_tier,
    seller_category,
    is_weekend

ORDER BY order_date DESC