SELECT
    EXTRACT(
        MONTH
        FROM
            SUBMIT_DATE
    ) AS MTH,
    PRODUCT_ID AS PRODUCT,
    ROUND(AVG(STARS), 2) AS AVG_STARS
FROM
    reviews
GROUP BY
    1,
    2
ORDER BY
    MTH,
    PRODUCT