WITH CTE AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY USER_ID
            ORDER BY
                TRANSACTION_DATE ASC
        ) AS RNK
    FROM
        TRANSACTIONS
)
SELECT
    USER_ID,
    SPEND,
    TRANSACTION_DATE
FROM
    CTE
WHERE
    RNK = 3;