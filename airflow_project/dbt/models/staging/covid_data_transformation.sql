WITH daily_data AS (
    SELECT 
        country_region,
        date,
        SUM(confirmed) as total_confirmed
    FROM covid_raw_data
    GROUP BY country_region, date
)

SELECT 
    country_region,
    date,
    total_confirmed,
    LAG(total_confirmed, 1) OVER (PARTITION BY country_region ORDER BY date) as prev_day_confirmed,
    (total_confirmed - LAG(total_confirmed, 1) OVER (PARTITION BY country_region ORDER BY date)) as new_confirmed
FROM daily_data
ORDER BY country_region, date;
