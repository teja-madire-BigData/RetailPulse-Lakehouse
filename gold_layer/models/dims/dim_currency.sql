WITH latest_rates AS (
    SELECT
        target_currency,
        exchange_rate,
        silver_processed_date,
        ROW_NUMBER() OVER (
            PARTITION BY target_currency
            ORDER BY silver_processed_date DESC
        ) AS rn
    FROM {{ source('ecom_silver', 'silver_fx_rates') }}
),

currency_meta AS (
    SELECT *
    FROM (VALUES
        ('USD', 'US Dollar',          'United States', '$'),
        ('EUR', 'Euro',               'European Union', '€'),
        ('GBP', 'British Pound',      'United Kingdom', '£'),
        ('INR', 'Indian Rupee',       'India',          '₹'),
        ('AED', 'UAE Dirham',         'UAE',            'AED'),
        ('SGD', 'Singapore Dollar',   'Singapore',      'SGD'),
        ('JPY', 'Japanese Yen',       'Japan',          '¥'),
        ('AUD', 'Australian Dollar',  'Australia',      'AUD'),
        ('CAD', 'Canadian Dollar',    'Canada',         'CAD'),
        ('CHF', 'Swiss Franc',        'Switzerland',    'CHF'),
        ('CNY', 'Chinese Yuan',       'China',          'CNY'),
        ('HKD', 'Hong Kong Dollar',   'Hong Kong',      'HKD'),
        ('KRW', 'South Korean Won',   'South Korea',    'KRW'),
        ('MYR', 'Malaysian Ringgit',  'Malaysia',       'MYR'),
        ('THB', 'Thai Baht',          'Thailand',       'THB'),
        ('SAR', 'Saudi Riyal',        'Saudi Arabia',   'SAR'),
        ('QAR', 'Qatari Riyal',       'Qatar',          'QAR'),
        ('BRL', 'Brazilian Real',     'Brazil',         'BRL'),
        ('MXN', 'Mexican Peso',       'Mexico',         'MXN'),
        ('ZAR', 'South African Rand', 'South Africa',   'ZAR'),
        ('NZD', 'New Zealand Dollar', 'New Zealand',    'NZD')
    ) AS t(code, name, region, symbol)
)

SELECT
    cm.code                                         AS currency_sk,
    cm.code,
    cm.name                                         AS currency_name,
    cm.region,
    cm.symbol,
    COALESCE(lr.exchange_rate, 1.0)                 AS usd_exchange_rate,
    lr.silver_processed_date                        AS rate_date
FROM currency_meta cm
LEFT JOIN latest_rates lr
    ON cm.code = lr.target_currency
    AND lr.rn = 1