WITH cte AS (
    SELECT
        user_id,
        purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY
                purchase_date
        ) AS rnk,
        LAG(purchase_date, 1) OVER (
            PARTITION BY user_id
            ORDER BY
                purchase_date
        ) AS last_purchase_days
    FROM
        purchases
)
SELECT
    distinct user_id
from
    cte
where
    datediff(purchase_date, last_purchase_days) <= 7;