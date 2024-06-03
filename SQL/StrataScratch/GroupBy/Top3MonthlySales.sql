WITH base_cte AS (
    SELECT SUM(order_value) AS total_orders, 
           DATE_FORMAT(order_date, '%Y-%m') AS combo_date
    FROM fct_customer_sales
    GROUP BY combo_date
    ORDER BY total_orders DESC
)
SELECT combo_date,total_orders
FROM base_cte
LIMIT 3;
