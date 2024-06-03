with cte as (
    select
        c.id,
        c.first_name,
        c.last_name,
        sum(o.total_order_cost) as total_order_cost
    from
        customers as c
        inner join card_orders as o on o.cust_id = c.id
    group by
        1,
        2,
        3
),
ranked_cte as (
    select
        id,
        first_name,
        last_name,
        total_order_cost,
        dense_rank() over (
            order by
                total_order_cost desc
        ) as rnk
    from
        cte
)
select
    id,
    first_name,
    last_name
from
    ranked_cte
where
    rnk = 3;