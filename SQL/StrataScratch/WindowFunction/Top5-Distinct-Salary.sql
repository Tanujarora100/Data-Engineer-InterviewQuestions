with cte as(
    select
        salary,
        dense_rank() over(
            order by
                salary desc
        ) as rnk
    from
        worker
)
select
    distinct salary
from
    cte
where
    rnk <= 5
order by
    salary desc;