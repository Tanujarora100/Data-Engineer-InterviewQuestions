WITH CTE AS(
    SELECT
        CITY,
        AVG(PRICE) AVG_PRICE_PER_CITY
    FROM
        LISTINGS
    GROUP BY
        CITY
    ORDER BY
        CITY
)
SELECT
    CITY
FROM
    CTE
WHERE
    AVG_PRICE_PER_CITY >(
        SELECT
            AVG(PRICE) AS NATIONAL_AVERAGE
        FROM
            LISTINGS
    )