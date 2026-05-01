CREATE TABLE ecom_silver.silver_users (
    user_id                 INT,
    first_name              STRING,
    last_name               STRING,
    is_valid_email          BOOLEAN,
    email_domain            STRING,
    phone_country_code      STRING,     
    phone_number_clean      STRING,     
    has_country_code        BOOLEAN,
    gender                  STRING,
    date_of_birth           STRING,
    country                 STRING,
    city                    STRING,
    address_line            STRING,
    postal_code             STRING,
    created_at              TIMESTAMP,
    preferred_category      STRING,
    marketing_opt_in        BOOLEAN,
    account_status          STRING,     
    customer_tier           STRING,     
    last_login_at           TIMESTAMP,
    session_count           INT,
    lifetime_spend_usd      DOUBLE,
    cart_item_count         INT,
    wishlist_item_count     INT,
    is_verified             BOOLEAN,
    account_age_days        INT,
    last_updated_at         TIMESTAMP,
    last_run_id             STRING,
    run_id                  STRING,
    run_timestamp           TIMESTAMP,
    silver_processed_date   DATE
)
LOCATION 's3://<bucket_name>/ecom_silver/silver_users/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format'     = 'parquet'
);

CREATE TABLE ecom_silver.silver_products (
    product_id              INT,
    seller_id               INT,
    sku                     STRING,
    name                    STRING,
    category                STRING,
    subcategory             STRING,
    brand                   STRING,
    base_price              DOUBLE,
    weight_kg               DOUBLE,
    is_active               BOOLEAN,
    current_price           DOUBLE,
    discount_active         BOOLEAN,
    discount_pct            DOUBLE,
    final_price             DOUBLE,
    stock_quantity          INT,
    is_low_stock            BOOLEAN,
    rating                  DOUBLE,
    review_count            INT,
    price_vs_base           DOUBLE,  
    last_updated_at         TIMESTAMP,  
    last_run_id             STRING,     
    run_id                  STRING,
    run_timestamp           TIMESTAMP,
    silver_processed_date   DATE
)
LOCATION 's3://<bucket_name>/ecom_silver/silver_products/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format'     = 'parquet'
);

CREATE TABLE ecom_silver.silver_orders (
    order_id                STRING,
    user_id                 INT,
    order_status            STRING,   
    channel                 STRING,
    payment_method          STRING,
    currency                STRING,
    ordered_at              TIMESTAMP,
    estimated_delivery_at   TIMESTAMP,
    gross_total             DOUBLE,
    discount_total          DOUBLE,
    net_total               DOUBLE,
    shipping_cost           DOUBLE,
    tax_rate                DOUBLE,
    tax_amount              DOUBLE,
    order_total             DOUBLE,
    item_count              INT,
    total_quantity          INT,
    top_category            STRING,
    top_seller_id           INT,
    products_json           STRING,
    is_gift                 BOOLEAN,
    promo_code_used         BOOLEAN,
    has_discount            BOOLEAN,   
    order_value_band        STRING,     
    days_to_delivery        INT,        
    last_run_id             STRING,     
    run_id                  STRING,    
    run_timestamp           TIMESTAMP, 
    silver_processed_date   DATE      
)
PARTITIONED BY (silver_processed_date)
LOCATION 's3://<bucket_name>/ecom_silver/silver_orders/'
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format'                        = 'parquet'
);

CREATE TABLE ecom_silver.silver_fx_rates (
    base_currency           STRING,
    target_currency         STRING,
    exchange_rate           DOUBLE,
    rate_date_utc           STRING,
    rate_unix               BIGINT,
    next_update_utc         STRING,
    pair                    STRING,     
    last_run_id             STRING,
    run_id                  STRING,
    run_timestamp           TIMESTAMP,
    silver_processed_date   DATE
)
PARTITIONED BY (silver_processed_date)
LOCATION 's3://<bucket_name>/ecom_silver/silver_fx_rates/'
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format'                        = 'parquet'
);
 
CREATE TABLE ecom_silver.silver_weather (
    city                    STRING,
    country                 STRING,
    lat                     DOUBLE,
    lon                     DOUBLE,
    observation_time        STRING,
    temperature_c           DOUBLE,
    feels_like_c            DOUBLE,
    humidity_pct            DOUBLE,
    precipitation_mm        DOUBLE,
    rain_mm                 DOUBLE,
    weather_code            INT,
    weather_description     STRING,
    wind_speed_kmh          DOUBLE,
    wind_direction_deg      DOUBLE,
    surface_pressure_hpa    DOUBLE,
    cloud_cover_pct         DOUBLE,
    visibility_m            DOUBLE,
    is_day                  BOOLEAN,
    timezone                STRING,
    last_run_id             STRING,
    run_id                  STRING,
    run_timestamp           TIMESTAMP,
    silver_processed_date   DATE
)
PARTITIONED BY (silver_processed_date)
LOCATION 's3://<bucket_name>/ecom_silver/silver_weather/'
TBLPROPERTIES (
    'table_type'                    = 'ICEBERG',
    'format'                        = 'parquet'
);

CREATE TABLE ecom_silver.silver_sellers (
    seller_id                   INT,
    seller_name                 STRING,
    email                       STRING,
    country                     STRING,
    city                        STRING,
    category_specialisation     STRING,
    commission_rate             DOUBLE,
    joined_at                   STRING,
    seller_rating               DOUBLE,
    total_reviews               INT,
    is_verified                 BOOLEAN,
    account_status              STRING,
    total_products_listed       INT,
    fulfillment_score           DOUBLE,
    seller_tier                 STRING,
    account_age_days            INT,
    last_updated_at             TIMESTAMP,
    last_run_id                 STRING,
    run_id                      STRING,
    run_timestamp               TIMESTAMP,
    silver_processed_date       DATE
)
LOCATION 's3://<bucket_name>/ecom_silver/silver_sellers/'
TBLPROPERTIES (
    'table_type'                   = 'ICEBERG',
    'format'                       = 'parquet'
);