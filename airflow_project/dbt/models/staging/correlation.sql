WITH stats AS (
    SELECT
        AVG(confirmed) AS avg_confirmed,
        AVG(deaths) AS avg_deaths,
        SUM((confirmed - AVG(confirmed)) * (deaths - AVG(deaths))) OVER () AS covariance,
        SQRT(SUM((confirmed - AVG(confirmed)) ^ 2) OVER ()) AS std_confirmed,
        SQRT(SUM((deaths - AVG(deaths)) ^ 2) OVER ()) AS std_deaths
    FROM {{ ref('covid_raw_data') }}
    WHERE confirmed IS NOT NULL AND deaths IS NOT NULL
)

SELECT
    covariance / (std_confirmed * std_deaths) AS correlation
FROM stats
