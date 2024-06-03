with cte as(
    select
        *
    from
        customers as c
        inner join orders as o on o.cust_id = c.id
    where
        o.order_date between '2019-02-01'
        and '2019-05-01'
)
select
    first_name,
    sum(total_order_cost) as total_order_cost,
    order_date
from
    cte
group by
    1,
    3
order by
    total_order_cost desc
limit
    1;