WITH value_frequencies AS (
    -- Replace `column_name` and `your_table` with your actual column and table names
    SELECT
        country_region,                    -- The column you want to analyze
        COUNT(*) AS frequency           -- Count how often each value appears
    FROM {{ ref('covid_raw_data') }}        -- Referencing your source table
    WHERE country_region IS NOT NULL       -- Optionally exclude NULL values
    GROUP BY country_region                -- Group by the column to count frequencies
)

SELECT
    country_region,
    frequency
FROM value_frequencies
ORDER BY frequency DESC                 -- Order by frequency, descending
LIMIT 5                                 -- Limit to top 5 most frequent values
