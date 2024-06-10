with female as(
    select
        department,
        count(distinct id) as total_females,
        sum(salary) as total_salary
    from
        employee
    where
        sex = 'F'
    group by
        1
),
male as(
    select
        department,
        count(distinct id) as total_males,
        sum(salary) as total_salary
    from
        employee
    where
        sex = 'M'
    group by
        1
)
select
    f.department,
    f.total_females,
    f.total_salary,
    m.total_males,
    m.total_salary
from
    female as f
    inner join male as m on f.department = m.department


-- department | sex | #of emps | total salary

select 
department,
COUNT(CASE WHEN sex = 'F' THEN id END) AS female_counts,
SUM(CASE WHEN sex = 'F' THEN salary END) AS fem_sal,
COUNT(CASE WHEN sex = 'M' THEN id END) AS male_counts,
SUM(CASE WHEN sex = 'M' THEN salary END) AS mal_sal

from employee

GROUP BY 1