WITH spine AS (
    SELECT
        explode(
            SEQUENCE(
                DATE '2024-01-01',
                DATE '2026-12-31',
                INTERVAL 1 DAY
            )
        ) AS date_day
)
SELECT
    date_day,
    YEAR(date_day) AS YEAR,
    MONTH(date_day) AS MONTH,
    date_format(
        date_day,
        'MMMM'
    ) AS month_name,
    date_format(
        date_day,
        'EEEE'
    ) AS day_name,
    (dayofweek(date_day) IN (1, 7)) AS is_weekend
FROM
    spine
