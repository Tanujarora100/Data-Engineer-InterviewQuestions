WITH CTE AS(
    SELECT
        C.COUNTRY_NAME,
        AVG(W.WEATHER_STATE) AS AVERAGE_WEATHER
    FROM
        COUNTRIES AS C
        JOIN WEATHER AS W ON W.COUNTRY_ID = C.COUNTRY_ID
    WHERE
        YEAR(DAY) = 2019
        AND MONTH(DAY) = 11
    GROUP BY
        C.COUNTRY_NAME
)
SELECT
    COUNTRY_NAME,
    CASE WHEN AVERAGE_WEATHER <= 15 THEN 'Cold' WHEN AVERAGE_WEATHER >= 25 THEN 'Hot' ELSE 'Warm' END AS WEATHER_TYPE
FROM
    CTE;