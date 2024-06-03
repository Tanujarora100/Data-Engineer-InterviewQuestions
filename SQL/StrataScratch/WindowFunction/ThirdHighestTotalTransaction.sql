with base_cte as(
select 
    c.id, c.first_name, c.last_name, sum(ct.total_order_cost) as total_sum
from 
    customers as c
join
    card_orders as ct
on 
    c.id=ct.cust_id
group by 
    c.id,c.first_name,c.last_name
order by 
    total_sum desc
),
ranked_cte as(
select 
    id, first_name,last_name, total_sum
, dense_rank() 
    over(order by total_sum desc) as final_rank
from 
    base_cte
)
select 
    id,first_name,last_name
from 
    ranked_cte
where final_rank=3;