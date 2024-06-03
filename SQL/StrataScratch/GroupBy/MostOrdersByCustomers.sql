WITH CTE AS(
    SELECT
        C.id,
        C.FIRST_NAME,
        COUNT(o.id) AS ORDERCOUNT
    FROM
        customers AS C
        JOIN orders AS o ON o.cust_id = C.id
    GROUP BY
        1,
        2
    ORDER BY
        COUNT(o.id) DESC
    LIMIT
        1
)
SELECT
    first_name,
    ORDERCOUNT
FROM
    CTE;