SELECT
    PRODUCT_CLASS
FROM
    (
        SELECT
            P.PRODUCT_CLASS,
            RANK() OVER(
                ORDER BY
                    SUM(O.UNITS_SOLD * O.COST_IN_DOLLARS) DESC
            ) AS RNK
        FROM
            online_products AS P
            JOIN online_orders AS O ON P.PRODUCT_ID = O.PRODUCT_ID
        GROUP BY
            P.PRODUCT_CLASS
    ) AS SUB_QUERY
WHERE
    RNK <= 3
ORDER BY
    PRODUCT_CLASS DESC