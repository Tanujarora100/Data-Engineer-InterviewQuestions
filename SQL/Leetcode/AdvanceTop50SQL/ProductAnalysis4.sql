SELECT
    USER_ID,
    PRODUCT_ID
FROM
    (
        SELECT
            S.USER_ID,
            S.PRODUCT_ID,
            SUM(S.QUANTITY * P.PRICE) AS TOTAL_SPEND,
            DENSE_RANK() OVER(
                PARTITION BY S.USER_ID
                ORDER BY
                    SUM(S.QUANTITY * P.PRICE) DESC
            ) AS RNK
        FROM
            SALES AS S
            JOIN PRODUCT AS P ON S.PRODUCT_ID = P.PRODUCT_ID
        GROUP BY
            S.USER_ID,
            S.PRODUCT_ID
        ORDER BY
            TOTAL_SPEND DESC
    ) AS SUB_QUERY
WHERE
    RNK = 1