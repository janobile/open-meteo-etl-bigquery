SELECT
    city_name,
    DATE(measure_datetime) AS date,
    AVG(temperature_celsius) AS mean_temperature_celsius,
    MIN(temperature_celsius) AS min_temperature_celsius,
    MAX(temperature_celsius) AS max_temperature_celsius
FROM
    `effortless-lock-396523.open_meteo.weather_data`
WHERE
    DATE(measure_datetime) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL 10 DAY)
GROUP BY
    city_name, date
ORDER BY
    date, city_name