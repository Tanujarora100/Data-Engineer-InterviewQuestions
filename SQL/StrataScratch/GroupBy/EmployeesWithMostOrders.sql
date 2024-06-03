with cte as(
    select
        e.id,
        e.first_name,
        e.last_name,
        rank() over(
            order by
                count(distinct order_id) desc
        ) as total_orders
    from
        shopify_employees as e
        join shopify_orders as o on o.resp_employee_id = e.id
    group by
        1,
        2,
        3
)
select
    first_name,
    last_name
from
    cte
where
    total_orders = 1;