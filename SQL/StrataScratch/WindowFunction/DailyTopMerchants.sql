WITH cte AS (
    SELECT m.id,m.name, o.total_amount_earned, DATE(o.order_timestamp) AS order_date
    FROM merchant_details AS m
    JOIN order_details AS o
    ON m.id = o.merchant_id
),
ranked_cte as(
select order_date as order_day , name, sum(total_amount_earned) as total_amount_earned,
dense_rank() over(partition by order_date 
    order by sum(total_amount_earned) desc)
    as ranks
    from cte
    group by id
)
select order_day,name,ranks as ranking 
    from ranked_cte 
    where ranks<=3;