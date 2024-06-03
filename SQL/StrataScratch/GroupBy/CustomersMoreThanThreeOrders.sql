with cte as(
    select
        cust_id,
        count(cust_id) as customer_count
    from
        orders
    group by
        cust_id
    order by
        customer_count desc
)
select
    cust_id as customer_id
from
    cte
where
    customer_count > 3
order by
    customer_id desc;