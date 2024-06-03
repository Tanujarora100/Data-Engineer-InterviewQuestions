select
    cust_id,
    sum(total_order_cost) as total_revenue
from
    orders
where
    EXTRACT(
        MONTH
        from
            order_date
    ) = 3
    and EXTRACT(
        YEAR
        from
            order_date
    ) = 2019
group by
    cust_id
order by
    total_revenue desc;