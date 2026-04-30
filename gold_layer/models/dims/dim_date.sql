WITH date_spine AS (
    SELECT
        date_add(
            'day',
            sequence.n,
            (SELECT MIN(CAST(ordered_at AS DATE))
             FROM {{ source('ecom_silver', 'silver_orders') }})
        ) AS date
    FROM (
        SELECT row_number() OVER () - 1 AS n
        FROM {{ source('ecom_silver', 'silver_orders') }}
        LIMIT 1095
    ) sequence
    WHERE date_add(
        'day',
        sequence.n,
        (SELECT MIN(CAST(ordered_at AS DATE))
         FROM {{ source('ecom_silver', 'silver_orders') }})
    ) <= current_date
)

SELECT
    CAST(date_format(date, '%Y%m%d') AS INT)        AS date_sk,
    date,
    date_format(date, '%Y-%m-%d')                   AS date_str,
    year(date)                                      AS year,
    quarter(date)                                   AS quarter,
    month(date)                                     AS month,
    date_format(date, '%b')                         AS month_name,
    week(date)                                      AS week_of_year,
    day(date)                                       AS day_of_month,
    day_of_week(date)                               AS day_of_week,
    date_format(date, '%W')                         AS day_name,
    CASE WHEN day_of_week(date) IN (1, 7)
         THEN TRUE ELSE FALSE END                   AS is_weekend,
    CASE WHEN month(date) IN (11, 12)
         THEN TRUE ELSE FALSE END                   AS is_holiday_season,
    CONCAT('Q', CAST(quarter(date) AS VARCHAR), ' ',
           CAST(year(date) AS VARCHAR))             AS quarter_label,
    CONCAT(date_format(date, '%b'), ' ',
           CAST(year(date) AS VARCHAR))             AS month_label
FROM date_spine
ORDER BY date