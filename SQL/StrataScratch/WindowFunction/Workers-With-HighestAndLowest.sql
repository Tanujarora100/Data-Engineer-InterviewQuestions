with ranked_cte as(
    select
        worker_id,
        salary,
        department,
        rank() over (
            order by
                salary desc
        ) as highest_salary,
        rank() over(
            order by
                salary
        ) as lowest_salary
    from
        worker
)
select
    worker_id,
    salary,
    department,
    -- Add a new column in the result
    case when lowest_salary = 1 then 'Lowest Salary' else 'Highest Salary' END as salary_type
from
    ranked_cte
where
    lowest_salary = 1
    or highest_salary = 1;