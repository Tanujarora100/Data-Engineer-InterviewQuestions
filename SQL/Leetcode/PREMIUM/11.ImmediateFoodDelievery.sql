WITH CTE AS (
    SELECT
        *,
        CASE WHEN customer_pref_delivery_date = order_date THEN 'Y' ELSE NULL END AS FLAG
    FROM
        Delivery
)
SELECT
    ROUND(
        (CAST(COUNT(FLAG) AS FLOAT)) / COUNT(delivery_id) * 100,
        2
    ) AS IMMEDIATE_PERCENTAGE
FROM
    CTE;