WITH CTE AS (
    SELECT
        I.INVOICE_ID,
        P.PRODUCT_ID,
        SUM(I.QUANTITY) AS QUANTITY,
        SUM(P.PRICE * I.QUANTITY) AS PRICE
    FROM
        Purchases AS I
        JOIN PRODUCTS AS P ON I.PRODUCT_ID = P.PRODUCT_ID
    GROUP BY
        I.INVOICE_ID,
        P.PRODUCT_ID
),
TEMP_CTE AS (
    SELECT
        *,
        RANK() OVER(
            PARTITION BY PRODUCT_ID
            ORDER BY
                PRICE DESC,
                INVOICE_ID ASC
        ) AS RNK
    FROM
        CTE
)
SELECT
    PRODUCT_ID,
    QUANTITY,
    PRICE
FROM
    TEMP_CTE
WHERE
    RNK = 1