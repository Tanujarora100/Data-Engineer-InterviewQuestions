WITH CTE AS(
    SELECT
        C.ID,
        C.FIRST_NAME,
        C.CITY,
        COUNT(*) AS TOTAL_ORDERS,
        SUM(O.TOTAL_ORDER_COST) AS TOTAL_ORDER_COST
    FROM
        CUSTOMERS AS C
        INNER JOIN ORDERS AS O ON C.ID = O.CUST_ID
    GROUP BY
        1,
        2,
        3
    HAVING
        COUNT(*) > 3
        AND SUM(TOTAL_ORDER_COST) > 100
)
SELECT
    FIRST_NAME,
    CITY,
    TOTAL_ORDERS AS ORDER_COUNT,
    TOTAL_ORDER_COST AS TOTAL_COST
FROM
    CTE