with cte as (
    select
        l.country,
        l.city,
        p.promo_code,
        p.order_fare,
        p.order_date
    from
        lyft_orders as l
        join lyft_payments as p on l.order_id = p.order_id
),
filtered_cte as (
    select
        city,
        count(promo_code) as promo_code_count
    from
        cte
    where
        promo_code = 'False'
        and EXTRACT(
            MONTH
            FROM
                ORDER_DATE
        ) = 8
        AND EXTRACT(
            YEAR
            FROM
                ORDER_DATE
        ) = 2021
    group by
        city
)
select
    city
from
    filtered_cte
order by
    promo_code_count desc
limit
    2;