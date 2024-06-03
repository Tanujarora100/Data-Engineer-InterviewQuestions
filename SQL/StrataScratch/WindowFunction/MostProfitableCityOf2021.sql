with cte as (
    select
        *
    from
        lyft_orders as l
        inner join lyft_payment_details as p on l.order_id = p.order_id
    where
        extract(
            year
            from
                order_date
        ) = 2021
),
ranked_cte as(
    select
        city,
        extract(
            month
            from
                order_date
        ) as p_month,
        sum(order_fare) as profit,
        rank() over(
            order by
                sum(order_fare) desc
        ) as rnk
    from
        cte
    group by
        1,
        2
    order by
        profit desc
)
select
    city,
    p_month,
    profit
from
    ranked_cte
where
    rnk = 1;