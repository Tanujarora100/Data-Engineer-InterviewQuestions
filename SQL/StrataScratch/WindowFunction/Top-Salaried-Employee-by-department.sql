with cte as(
select concat(first_name,' ',last_name) as 
    employee_name,
    department,salary,
    rank() 
    over(partition by department order by salary desc) as ranks
    from worker
)
select department, employee_name, salary 
    from cte
    where ranks=1