with cte as(
    select
        worker_ref_id,
        sum(bonus) as total_bonus
    from
        sf_bonus
    group by
        1
),
-- Find out the total bonus for each worker_id
grouped_cte as (
    select
        s.employee_title,
        s.sex,
        avg(cte.total_bonus + s.salary) as avg_compensation
    from
        sf_employee as s
        inner join cte -- Inner join as question states disregard those which do not have a bonus.
        on s.id = cte.worker_ref_id
    group by
        1,
        2
)
select
    *
from
    grouped_cte;
    /* Find the average total compensation based on employee titles and gender. Total compensation is calculated by adding both the salary and bonus of each employee. However, not every employee receives a bonus so disregard employees without bonuses in your calculation. Employee can receive more than one bonus.
    Output the employee title, gender (i.e., sex), along with the average total compensation.
    
    Tables: sf_employee, sf_bonus */