WITH lowest_cost AS (
    SELECT
        cust_id,
        MIN(total_order_cost) AS lowest_cost
    FROM
        orders
    GROUP BY
        1
)
SELECT
    c.id,
    c.first_name,
    lc.lowest_cost
FROM
    lowest_cost lc
    JOIN customers c ON lc.cust_id = c.id;