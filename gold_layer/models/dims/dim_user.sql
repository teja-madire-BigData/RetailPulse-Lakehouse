SELECT
    user_id                                         AS user_sk,
    user_id,
    first_name,
    last_name,
    email_domain,
    is_valid_email,
    gender,
    country,
    city,
    preferred_category,
    marketing_opt_in,
    account_status,
    customer_tier,
    lifetime_spend_usd,
    is_verified,
    session_count,
    account_age_days,
    CASE
        WHEN account_age_days < 90   THEN 'new'
        WHEN account_age_days < 365  THEN 'growing'
        WHEN account_age_days < 730  THEN 'established'
        ELSE 'loyal'
    END                                             AS account_age_segment,

    CASE
        WHEN lifetime_spend_usd < 100   THEN 'low'
        WHEN lifetime_spend_usd < 500   THEN 'medium'
        WHEN lifetime_spend_usd < 2000  THEN 'high'
        ELSE 'vip'
    END                                             AS spend_segment,
    silver_processed_date

FROM {{ source('ecom_silver', 'silver_users') }}
WHERE account_status != 'suspended' 