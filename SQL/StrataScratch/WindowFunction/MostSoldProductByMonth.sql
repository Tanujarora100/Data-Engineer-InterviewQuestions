WITH cte AS (
    SELECT
        EXTRACT(
            MONTH
            FROM
                invoicedate
        ) AS month,
        description,
        SUM(quantity * unitprice) AS total_paid,
        DENSE_RANK() OVER (
            PARTITION BY EXTRACT(
                MONTH
                FROM
                    invoicedate
            )
            ORDER BY
                SUM(quantity * unitprice) DESC
        ) AS rnk
    FROM
        online_retail
    GROUP BY
        month,
        description
)
SELECT
    month,
    description,
    total_paid
FROM
    cte
where
    rnk = 1