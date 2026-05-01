SELECT
    seller_id AS seller_sk,
    seller_id,
    seller_name,
    email,
    country,
    city,
    category_specialisation,
    commission_rate,
    joined_at,
    seller_rating,
    total_reviews,
    is_verified,
    account_status,
    total_products_listed,
    fulfillment_score,
    seller_tier,
    account_age_days,
    CASE
        WHEN fulfillment_score >= 95 THEN 'excellent'
        WHEN fulfillment_score >= 85 THEN 'good'
        WHEN fulfillment_score >= 70 THEN 'average'
        ELSE 'poor'
    END AS fulfillment_band,

    silver_processed_date

FROM {{ source('ecom_silver', 'silver_sellers') }}
WHERE account_status != 'suspended'