WITH CTE AS(
    SELECT
        S.SELLER_NAME AS SELLER_NAME,
        O.SELLER_ID AS SELLER_ID,
        YEAR(O.SALE_DATE) AS SALE_YEAR,
        COUNT(1) AS TOTAL_ORDERS
    FROM
        ORDERS AS O
        JOIN SELLER AS S ON O.SELLER_ID = S.SELLER_ID
    GROUP BY
        1,
        2,
        3
    HAVING
        SALE_YEAR = 2020
)
SELECT
    seller_name
FROM
    SELLER
WHERE
    seller_name NOT IN (
        SELECT
            SELLER_NAME
        FROM
            CTE
    )
ORDER BY
    SELLER_NAME ASC