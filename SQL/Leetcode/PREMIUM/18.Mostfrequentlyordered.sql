with CTE as (
    select
        customer_id,
        product_id,
        rank() over(
            partition by customer_id
            order by
                count(product_id) desc
        ) as rnk
    from
        orders
    group by
        customer_id,
        product_id
)
SELECT
    c.customer_id,
    c.product_id,
    p.product_name
from
    cte as c
    join products as p on c.product_id = p.product_id
where
    c.rnk = 1;