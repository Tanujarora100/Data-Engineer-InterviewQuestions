WITH CTE AS(
    SELECT
        CITY,
        STATE,
        AVG(MKT_PRICE) AS AVG_PRICE
    FROM
        zillow_transactions
    GROUP BY
        1,
        2
)
SELECT
    CITY
FROM
    CTE
WHERE
    AVG_PRICE > (
        SELECT
            AVG(MKT_PRICE)
        FROM
            zillow_transactions
    )
ORDER BY
    CITY