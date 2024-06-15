SELECT
    COUNT(DISTINCT CUSTOMER_ID) AS RICH_COUNT
FROM
    (
        SELECT
            CUSTOMER_ID
        FROM
            STORE
        WHERE
            AMOUNT > 500
    ) AS SUB_QUERY