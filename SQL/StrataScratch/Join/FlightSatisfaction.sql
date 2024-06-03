WITH cte as(
select s.cust_id,s.class, s.satisfaction,l.age
from survey_results as s
join 
    loyalty_customers as l
on
    l.cust_id=s.cust_id

)

select class as CLASS, round(avg(satisfaction)) as pc_score
from cte
where age 
    between 30 and 40
group by 
    class
order by 
    pc_score desc;