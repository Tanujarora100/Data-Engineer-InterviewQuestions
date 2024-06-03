WITH january_sales AS (
    SELECT *
    FROM sales_data
    WHERE month = '2024-01'
),
ranked_sales AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_category ORDER BY total_sales DESC) AS sales_rank
    FROM january_sales
)

select seller_id, total_sales, product_category, market_place, month
from ranked_sales
where sales_rank<=3;