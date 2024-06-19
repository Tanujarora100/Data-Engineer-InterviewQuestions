with cte as(
    select
        dep_id,
        count(emp_id) as total_employees,
        dense_rank() over(
            order by
                count(emp_id) desc
        ) as rnk
    from
        employees
    group by
        dep_id
)
select
    emp_name as manager_name,
    dep_id
from
    employees
where
    dep_id in (
        select
            dep_id
        from
            cte
        where
            rnk = 1
    )
    and position = 'Manager'
order by
    dep_id