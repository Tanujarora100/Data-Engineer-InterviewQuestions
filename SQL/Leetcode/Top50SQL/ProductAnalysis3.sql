SELECT
    PRODUCT_ID,
    YEAR AS FIRST_YEAR,
    QUANTITY,
    PRICE
FROM
    (
        SELECT
            *,
            DENSE_RANK() OVER(
                PARTITION BY PRODUCT_ID
                ORDER BY
                    YEAR ASC
            ) AS SALE_YEAR
        FROM
            SALES
    ) AS SUB_QUERY
WHERE
    SALE_YEAR = 1