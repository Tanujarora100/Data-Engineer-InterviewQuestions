WITH CTE AS (
    SELECT
        U.CITY,
        T.STATUS
    FROM
        TRADES AS T
        JOIN USERS AS U ON U.USER_ID = T.USER_ID
    where
        t.status = 'Completed'
)
SELECT
    CITY,
    COUNT(*)
FROM
    CTE AS TOTAL_ORDERS
GROUP BY
    1
ORDER BY
    2 DESC
limit
    3;