WITH RANKED_CTE AS (
    SELECT
        CATEGORY,
        PRODUCT,
        SUM(SPEND) AS TOTAL_SPEND,
        ROW_NUMBER() OVER(
            PARTITION BY CATEGORY
            ORDER BY
                SUM(SPEND) DESC
        ) AS RNK
    FROM
        PRODUCT_SPEND
    WHERE
        EXTRACT(
            YEAR
            FROM
                TRANSACTION_DATE
        ) = 2022
    GROUP BY
        CATEGORY,
        PRODUCT
)
SELECT
    CATEGORY,
    PRODUCT,
    TOTAL_SPEND
FROM
    RANKED_CTE
WHERE
    RNK <= 2