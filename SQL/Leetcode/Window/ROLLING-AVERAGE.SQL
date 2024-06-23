WITH MonthlySales AS(
    SELECT
        MONTH(B.ORDER_DATE) AS MONTH,
        SUM(A.UNIT_PRICE * B.QUANTITY) AS SALES
    FROM
        amazon_books AS A
        JOIN book_orders AS B ON A.BOOK_ID = B.BOOK_ID
    WHERE
        YEAR(B.ORDER_DATE) = 2022
    GROUP BY
        MONTH
    ORDER BY
        MONTH
)
SELECT
    MONTH,
    SALES,
    ROUND(
        AVG(SALES) OVER (
            ORDER BY
                MONTH ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        )
    ) AS ROLLING_AVERAGE
FROM
    MonthlySales;